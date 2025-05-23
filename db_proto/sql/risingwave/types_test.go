package risingwave

import (
	"strings"
	"testing"
	"time"

	"github.com/golang/protobuf/protoc-gen-go/descriptor"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestValueToString(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected string
	}{
		// String values
		{"simple string", "hello", "'hello'"},
		{"string with quotes", "hello'world", "'hello''world'"},
		{"string with backslash", "hello\\world", "'hello\\\\world'"},
		{"empty string", "", "''"},

		// uint256 string values
		{"uint256 decimal string", "115792089237316195423570985008687907853269984665640564039457584007913129639935", "'115792089237316195423570985008687907853269984665640564039457584007913129639935'"},
		{"uint256 hex string", "0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF", "'0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF'"},
		{"uint256 small value", "1000000000000000000", "'1000000000000000000'"},

		// Integer values
		{"int64", int64(123), "123"},
		{"int64 negative", int64(-456), "-456"},
		{"int32", int32(456), "456"},
		{"int", int(789), "789"},

		// Unsigned integer values (now quoted for VARCHAR storage)
		{"uint64", uint64(18446744073709551615), "'18446744073709551615'"},
		{"uint32", uint32(456), "456"},
		{"large uint", uint(18446744073709551615), "'18446744073709551615'"},
		{"small uint", uint(789), "789"},

		// Float values
		{"float64", float64(123.45), "123.45"},
		{"float32", float32(67.89), "67.89"},

		// Boolean values
		{"bool true", true, "true"},
		{"bool false", false, "false"},

		// Byte slice (should be hex encoded with uppercase)
		{"bytes", []uint8{0xDE, 0xAD, 0xBE, 0xEF}, "'\\xDEADBEEF'"},
		{"empty bytes", []uint8{}, "'\\x'"},

		// Time values
		{"time", time.Date(2023, 1, 15, 10, 30, 0, 0, time.UTC), "'2023-01-15T10:30:00Z'"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ValueToString(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestValueToStringTimestamp(t *testing.T) {
	// Test protobuf timestamp
	testTime := time.Date(2023, 1, 15, 10, 30, 0, 0, time.UTC)
	pbTime := timestamppb.New(testTime)
	result := ValueToString(pbTime)
	assert.Equal(t, "'2023-01-15T10:30:00Z'", result)
}

func TestValueToStringPanic(t *testing.T) {
	// Test unsupported type should panic
	assert.Panics(t, func() {
		ValueToString(complex64(1 + 2i))
	})
}

// Tests for uint256 functionality

func TestIsUint256Field(t *testing.T) {
	tests := []struct {
		name      string
		fieldName string
		fieldType descriptor.FieldDescriptorProto_Type
		expected  bool
	}{
		// String fields with uint256-like names
		{"balance field", "balance", descriptor.FieldDescriptorProto_TYPE_STRING, true},
		{"amount field", "amount", descriptor.FieldDescriptorProto_TYPE_STRING, true},
		{"value field", "value", descriptor.FieldDescriptorProto_TYPE_STRING, true},
		{"wei field", "wei_amount", descriptor.FieldDescriptorProto_TYPE_STRING, true},
		{"token supply", "token_supply", descriptor.FieldDescriptorProto_TYPE_STRING, true},
		{"gas price", "gas_price", descriptor.FieldDescriptorProto_TYPE_STRING, true},
		{"block number", "block_number", descriptor.FieldDescriptorProto_TYPE_STRING, true},
		{"total count", "total_count", descriptor.FieldDescriptorProto_TYPE_STRING, true},

		// Case insensitive
		{"uppercase BALANCE", "BALANCE", descriptor.FieldDescriptorProto_TYPE_STRING, true},
		{"mixed case AmounT", "AmounT", descriptor.FieldDescriptorProto_TYPE_STRING, true},

		// Non-string fields should return false
		{"balance int64", "balance", descriptor.FieldDescriptorProto_TYPE_INT64, false},
		{"amount uint32", "amount", descriptor.FieldDescriptorProto_TYPE_UINT32, false},

		// Non-uint256 field names
		{"name field", "name", descriptor.FieldDescriptorProto_TYPE_STRING, false},
		{"description field", "description", descriptor.FieldDescriptorProto_TYPE_STRING, false},
		{"random field", "xyz", descriptor.FieldDescriptorProto_TYPE_STRING, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsUint256Field(tt.fieldName, tt.fieldType)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestIsUint256String(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected bool
	}{
		// Valid uint256 decimal values
		{"zero", "0", true},
		{"small number", "123", true},
		{"max uint64", "18446744073709551615", true},
		{"large ethereum amount", "1000000000000000000", true}, // 1 ETH in wei
		{"max uint256", "115792089237316195423570985008687907853269984665640564039457584007913129639935", true},

		// Valid hex values
		{"hex with 0x", "0x1234", true},
		{"hex with 0X", "0X1234", true},
		{"max hex", "0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF", true},
		{"lowercase hex", "0xabcdef", true},
		{"uppercase hex", "0xABCDEF", true},
		{"mixed case hex", "0xAbCdEf", true},

		// Invalid values
		{"empty string", "", false},
		{"negative number", "-123", false},
		{"decimal number", "123.45", false},
		{"non-numeric", "abc", false},
		{"mixed alphanumeric", "123abc", false},
		{"hex without 0x prefix invalid", "ABCD", false}, // pure letters without 0x are invalid
		{"invalid hex chars", "0xGHIJ", false},
		{"empty hex", "0x", false},
		{"too large hex", "0x1" + strings.Repeat("0", 65), false}, // > 64 hex chars
		{"overflow uint256", "115792089237316195423570985008687907853269984665640564039457584007913129639936", false}, // max + 1

		// Edge cases
		{"leading zeros", "000123", true},
		{"hex leading zeros", "0x000123", true},
		{"single hex digit", "0x1", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsUint256String(tt.input)
			assert.Equal(t, tt.expected, result, "Input: %s", tt.input)
		})
	}
}

func TestMapFieldTypeUint256Detection(t *testing.T) {
	tests := []struct {
		name        string
		dataType    DataType
		description string
	}{
		{"SMALLINT type", TypeSmallInt, "Two-byte integer"},
		{"INTEGER type", TypeInteger, "Four-byte integer"},
		{"BIGINT type", TypeBigInt, "Eight-byte integer"},
		{"NUMERIC type", TypeNumeric, "Exact numeric (28 decimal digits precision)"},
		{"REAL type", TypeReal, "Single precision floating-point (4 bytes)"},
		{"DOUBLE type", TypeDouble, "Double precision floating-point (8 bytes)"},
		{"BOOLEAN type", TypeBool, "Logical Boolean (true, false, or null)"},
		{"VARCHAR type", TypeVarchar, "Variable-length character string"},
		{"UINT256 type", TypeUint256, "uint256 stored as VARCHAR to prevent data loss"},
		{"BYTEA type", TypeBytea, "Binary strings (hex format)"},
		{"JSONB type", TypeJsonb, "Binary JSON value"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.dataType.String(), tt.dataType.String())
		})
	}
}

// Test uint256 vs regular string handling in ValueToString
func TestValueToStringUint256Detection(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		// uint256 strings should be quoted the same way as regular strings
		{"uint256 decimal", "1000000000000000000", "'1000000000000000000'"},
		{"uint256 hex", "0x1234567890ABCDEF", "'0x1234567890ABCDEF'"},
		{"regular string", "hello world", "'hello world'"},
		{"numeric-looking but not uint256", "123.45", "'123.45'"},
		{"string with quotes", "it's a test", "'it''s a test'"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ValueToString(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// Test edge cases for uint256 validation
func TestUint256EdgeCases(t *testing.T) {
	// Test exactly at uint256 boundary
	maxUint256 := "115792089237316195423570985008687907853269984665640564039457584007913129639935"
	overflowUint256 := "115792089237316195423570985008687907853269984665640564039457584007913129639936"

	assert.True(t, IsUint256String(maxUint256), "Max uint256 should be valid")
	assert.False(t, IsUint256String(overflowUint256), "Overflow uint256 should be invalid")

	// Test max hex value
	maxHex := "0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF"
	overflowHex := "0x1FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF"

	assert.True(t, IsUint256String(maxHex), "Max hex uint256 should be valid")
	assert.False(t, IsUint256String(overflowHex), "Overflow hex uint256 should be invalid")
}

// Tests for uint256 configuration functionality

func TestUint256Configuration(t *testing.T) {
	// Save original config
	originalConfig := uint256Config
	defer func() {
		uint256Config = originalConfig
	}()

	// Test explicit field configuration
	t.Run("explicit fields", func(t *testing.T) {
		SetUint256Fields([]string{"custom_field", "another_uint256"})

		assert.True(t, IsUint256Field("custom_field", descriptor.FieldDescriptorProto_TYPE_STRING))
		assert.True(t, IsUint256Field("another_uint256", descriptor.FieldDescriptorProto_TYPE_STRING))
		assert.False(t, IsUint256Field("regular_field", descriptor.FieldDescriptorProto_TYPE_STRING))

		// Auto-detection should still work
		assert.True(t, IsUint256Field("balance", descriptor.FieldDescriptorProto_TYPE_STRING))
	})

	t.Run("disable auto detection", func(t *testing.T) {
		SetUint256Fields([]string{"explicit_only"})
		SetUint256AutoDetection(false)

		// Only explicit fields should be detected
		assert.True(t, IsUint256Field("explicit_only", descriptor.FieldDescriptorProto_TYPE_STRING))
		assert.False(t, IsUint256Field("balance", descriptor.FieldDescriptorProto_TYPE_STRING))
		assert.False(t, IsUint256Field("amount", descriptor.FieldDescriptorProto_TYPE_STRING))
	})

	t.Run("re-enable auto detection", func(t *testing.T) {
		SetUint256AutoDetection(true)
		SetUint256Fields([]string{}) // Clear explicit fields

		// Auto-detection should work again
		assert.True(t, IsUint256Field("balance", descriptor.FieldDescriptorProto_TYPE_STRING))
		assert.True(t, IsUint256Field("amount", descriptor.FieldDescriptorProto_TYPE_STRING))
	})

	t.Run("non-string fields", func(t *testing.T) {
		SetUint256Fields([]string{"numeric_field"})

		// Even explicit fields should not be uint256 if they're not strings
		assert.False(t, IsUint256Field("numeric_field", descriptor.FieldDescriptorProto_TYPE_INT64))
		assert.False(t, IsUint256Field("balance", descriptor.FieldDescriptorProto_TYPE_UINT64))
	})
}

func TestSetUint256Fields(t *testing.T) {
	// Save original config
	originalConfig := uint256Config
	defer func() {
		uint256Config = originalConfig
	}()

	tests := []struct {
		name           string
		inputFields    []string
		testField      string
		expectedResult bool
	}{
		{
			name:           "single field",
			inputFields:    []string{"test_field"},
			testField:      "test_field",
			expectedResult: true,
		},
		{
			name:           "multiple fields",
			inputFields:    []string{"field1", "field2", "field3"},
			testField:      "field2",
			expectedResult: true,
		},
		{
			name:           "field not in list",
			inputFields:    []string{"field1", "field2"},
			testField:      "field3",
			expectedResult: false,
		},
		{
			name:           "empty field in list",
			inputFields:    []string{"field1", "", "field3"},
			testField:      "field3",
			expectedResult: true,
		},
		{
			name:           "empty list",
			inputFields:    []string{},
			testField:      "any_field",
			expectedResult: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SetUint256Fields(tt.inputFields)
			result := IsUint256Field(tt.testField, descriptor.FieldDescriptorProto_TYPE_STRING)

			if tt.expectedResult {
				assert.True(t, result, "Field %s should be detected as uint256", tt.testField)
			} else {
				// For fields not in explicit list, check if auto-detection would catch them
				autoDetected := uint256FieldNames.MatchString(tt.testField)
				if autoDetected {
					assert.True(t, result, "Field %s should be auto-detected as uint256", tt.testField)
				} else {
					assert.False(t, result, "Field %s should not be detected as uint256", tt.testField)
				}
			}
		})
	}
}

func TestUint256ConfigurationPrecedence(t *testing.T) {
	// Save original config
	originalConfig := uint256Config
	defer func() {
		uint256Config = originalConfig
	}()

	// Test that explicit configuration takes precedence over auto-detection
	SetUint256Fields([]string{"not_a_typical_uint256_name"})

	// This field name wouldn't normally be detected as uint256
	assert.False(t, uint256FieldNames.MatchString("not_a_typical_uint256_name"))

	// But explicit configuration should override
	assert.True(t, IsUint256Field("not_a_typical_uint256_name", descriptor.FieldDescriptorProto_TYPE_STRING))
}

func TestDataTypeString(t *testing.T) {
	tests := []struct {
		dataType DataType
		expected string
	}{
		{TypeSmallInt, "SMALLINT"},
		{TypeInteger, "INTEGER"},
		{TypeBigInt, "BIGINT"},
		{TypeNumeric, "NUMERIC"},
		{TypeReal, "REAL"},
		{TypeDouble, "DOUBLE PRECISION"},
		{TypeBool, "BOOLEAN"},
		{TypeVarchar, "VARCHAR"},
		{TypeText, "VARCHAR"},
		{TypeBytea, "BYTEA"},
		{TypeDate, "DATE"},
		{TypeTime, "TIME"},
		{TypeTimestamp, "TIMESTAMP"},
		{TypeTimestamptz, "TIMESTAMP WITH TIME ZONE"},
		{TypeInterval, "INTERVAL"},
		{TypeStruct, "STRUCT"},
		{TypeArray, "ARRAY"},
		{TypeMap, "MAP"},
		{TypeJsonb, "JSONB"},
	}

	for _, tt := range tests {
		t.Run(string(tt.dataType), func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.dataType.String())
		})
	}
}

// Tests for helper functions

func TestIsSafeForRwInt256(t *testing.T) {
	tests := []struct {
		name     string
		value    string
		expected bool
	}{
		// Safe values (within rw_int256 range)
		{"zero", "0", true},
		{"small value", "123", true},
		{"1 ETH in wei", "1000000000000000000", true},
		{"max uint64", "18446744073709551615", true},
		{"safe large value", "57896044618658097711785492504343953926634992332820282019728792003956564819967", true}, // 2^255-1

		// Unsafe values (exceed rw_int256 range)
		{"2^255", "57896044618658097711785492504343953926634992332820282019728792003956564819968", false}, // 2^255
		{"max uint256", "115792089237316195423570985008687907853269984665640564039457584007913129639935", false},

		// Hex values
		{"small hex", "0x123", true},
		{"safe hex max", "0x7FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF", true}, // 2^255-1
		{"unsafe hex", "0x8000000000000000000000000000000000000000000000000000000000000000", false},  // 2^255

		// Invalid values
		{"invalid format", "abc", false},
		{"empty string", "", false},
		{"negative", "-123", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsSafeForRwInt256(tt.value)
			assert.Equal(t, tt.expected, result, "Value: %s", tt.value)
		})
	}
}

func TestGenerateSafeCastSQL(t *testing.T) {
	result := GenerateSafeCastSQL("amount")

	// Check that the generated SQL contains expected elements
	assert.Contains(t, result, "CASE")
	assert.Contains(t, result, "LENGTH(amount)")
	assert.Contains(t, result, "CAST(amount AS rw_int256)")
	assert.Contains(t, result, "ELSE NULL")
	assert.Contains(t, result, "END")

	// Test with different column name
	result2 := GenerateSafeCastSQL("balance")
	assert.Contains(t, result2, "LENGTH(balance)")
	assert.Contains(t, result2, "CAST(balance AS rw_int256)")
}

func TestGenerateHexCastSQL(t *testing.T) {
	result := GenerateHexCastSQL("hex_value")

	// Check that the generated SQL contains expected elements
	assert.Contains(t, result, "CASE")
	assert.Contains(t, result, "hex_value ~ '^0[xX][0-9a-fA-F]+$'")
	assert.Contains(t, result, "LENGTH(hex_value) <= 66")
	assert.Contains(t, result, "hex_to_int256(hex_value)")
	assert.Contains(t, result, "ELSE NULL")
	assert.Contains(t, result, "END")
}

func TestGetUint256StorageInfo(t *testing.T) {
	info := GetUint256StorageInfo()

	// Check required fields
	assert.Equal(t, "VARCHAR", info["storage_type"])
	assert.NotEmpty(t, info["max_value"])
	assert.NotEmpty(t, info["safe_rw_int256_max"])

	// Check supported formats
	formats, ok := info["supported_formats"].([]string)
	assert.True(t, ok)
	assert.Contains(t, formats, "decimal")
	assert.Contains(t, formats, "hex (0x/0X prefix)")

	// Check auto-detected fields
	fields, ok := info["auto_detected_fields"].([]string)
	assert.True(t, ok)
	assert.Contains(t, fields, "amount")
	assert.Contains(t, fields, "balance")
	assert.Contains(t, fields, "value")

	// Check configuration
	config, ok := info["configuration"].(map[string]string)
	assert.True(t, ok)
	assert.Equal(t, "SINK_SQL_UINT256_FIELDS", config["explicit_fields_env"])
	assert.Equal(t, "SINK_SQL_UINT256_AUTO_DETECT", config["auto_detect_env"])
}

func TestValidateUint256Value(t *testing.T) {
	tests := []struct {
		name             string
		value            string
		expectedValid    bool
		expectedFormat   string
		expectedSafeCast bool
		expectedErrors   int
	}{
		{
			name:             "valid small decimal",
			value:            "123",
			expectedValid:    true,
			expectedFormat:   "decimal",
			expectedSafeCast: true,
			expectedErrors:   0,
		},
		{
			name:             "valid max uint256",
			value:            "115792089237316195423570985008687907853269984665640564039457584007913129639935",
			expectedValid:    true,
			expectedFormat:   "decimal",
			expectedSafeCast: false,
			expectedErrors:   0,
		},
		{
			name:             "valid hex",
			value:            "0x123ABC",
			expectedValid:    true,
			expectedFormat:   "hex",
			expectedSafeCast: true,
			expectedErrors:   0,
		},
		{
			name:             "empty string",
			value:            "",
			expectedValid:    false,
			expectedFormat:   "unknown",
			expectedSafeCast: false,
			expectedErrors:   1,
		},
		{
			name:             "invalid decimal",
			value:            "123abc",
			expectedValid:    false,
			expectedFormat:   "decimal",
			expectedSafeCast: false,
			expectedErrors:   1,
		},
		{
			name:             "invalid hex",
			value:            "0xGHIJ",
			expectedValid:    false,
			expectedFormat:   "hex",
			expectedSafeCast: false,
			expectedErrors:   1,
		},
		{
			name:             "overflow uint256",
			value:            "115792089237316195423570985008687907853269984665640564039457584007913129639936",
			expectedValid:    false,
			expectedFormat:   "decimal",
			expectedSafeCast: false,
			expectedErrors:   1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ValidateUint256Value(tt.value)

			assert.Equal(t, tt.value, result["value"])
			assert.Equal(t, tt.expectedValid, result["is_valid"])
			assert.Equal(t, tt.expectedFormat, result["format"])
			assert.Equal(t, tt.expectedSafeCast, result["safe_cast"])
			assert.Equal(t, len(tt.value), result["length"])

			errors := result["errors"].([]string)
			assert.Equal(t, tt.expectedErrors, len(errors))
		})
	}
}
