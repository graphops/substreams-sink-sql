package clickhouse

import (
	"testing"
	"time"

	sql2 "github.com/streamingfast/substreams-sink-sql/db_proto/sql"
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

		// Integer values
		{"int64", int64(123), "123"},
		{"int64 negative", int64(-456), "-456"},
		{"int32", int32(456), "456"},
		{"int", int(789), "789"},

		// Unsigned integer values
		{"uint64", uint64(123), "123"},
		{"uint32", uint32(456), "456"},
		{"uint", uint(789), "789"},

		// Float values
		{"float64", float64(123.45), "123.45"},
		{"float32", float32(67.89), "67.89"},

		// Boolean values
		{"bool true", true, "true"},
		{"bool false", false, "false"},

		// Byte slice (should be base64 encoded)
		{"bytes", []uint8{0xDE, 0xAD, 0xBE, 0xEF}, "'3q2+7w=='"},
		{"empty bytes", []uint8{}, "''"},

		// Time values
		{"time", time.Date(2023, 1, 15, 10, 30, 0, 0, time.UTC), "'2023-01-15 10:30:00'"},
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
	assert.Equal(t, "'2023-01-15 10:30:00'", result)
}

func TestValueToStringPanic(t *testing.T) {
	// Test unsupported type should panic
	assert.Panics(t, func() {
		ValueToString(complex64(1 + 2i))
	})
}

func TestDataTypeString(t *testing.T) {
	tests := []struct {
		dataType DataType
		expected string
	}{
		{TypeInteger8, "Int8"},
		{TypeInteger16, "Int16"},
		{TypeInteger32, "Int32"},
		{TypeInteger64, "Int64"},
		{TypeInteger128, "Int128"},
		{TypeInteger256, "Int256"},
		{TypeUInt8, "UInt8"},
		{TypeUInt16, "UInt16"},
		{TypeUInt32, "UInt32"},
		{TypeUInt64, "UInt64"},
		{TypeUInt128, "UInt128"},
		{TypeUInt256, "UInt256"},
		{TypeFloat32, "Float32"},
		{TypeFloat64, "Float64"},
		{TypeBool, "Bool"},
		{TypeString, "String"},
		{TypeVarchar, "VARCHAR"},
		{TypeDateTime, "DateTime"},
		{TypeDateTime64, "DateTime64"},
		{TypeFixedString, "FixedString"},
		{TypeDecimal32, "Decimal32"},
		{TypeDecimal64, "Decimal64"},
		{TypeDecimal128, "Decimal128"},
		{TypeDecimal256, "Decimal256"},
	}

	for _, tt := range tests {
		t.Run(string(tt.dataType), func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.dataType.String())
		})
	}
}

func TestMapSemanticType(t *testing.T) {
	tests := []struct {
		name          string
		semanticType  sql2.SemanticType
		expectedSQL   string
		shouldSupport bool
	}{
		{
			name:          "uint256 maps to String",
			semanticType:  sql2.SemanticUint256,
			expectedSQL:   "String",
			shouldSupport: true,
		},
		{
			name:          "int256 maps to String",
			semanticType:  sql2.SemanticInt256,
			expectedSQL:   "String",
			shouldSupport: true,
		},
		{
			name:          "address maps to FixedString(42)",
			semanticType:  sql2.SemanticAddress,
			expectedSQL:   "FixedString(42)",
			shouldSupport: true,
		},
		{
			name:          "hash maps to FixedString(66)",
			semanticType:  sql2.SemanticHash,
			expectedSQL:   "FixedString(66)",
			shouldSupport: true,
		},
		{
			name:          "json maps to String",
			semanticType:  sql2.SemanticJSON,
			expectedSQL:   "String",
			shouldSupport: true,
		},
		{
			name:          "uuid maps to String",
			semanticType:  sql2.SemanticUUID,
			expectedSQL:   "String",
			shouldSupport: true,
		},
		{
			name:          "unix_timestamp maps to DateTime",
			semanticType:  sql2.SemanticUnixTimestamp,
			expectedSQL:   "DateTime",
			shouldSupport: true,
		},
		{
			name:          "unix_timestamp_ms maps to DateTime64(3)",
			semanticType:  sql2.SemanticUnixTimestampMS,
			expectedSQL:   "DateTime64(3)",
			shouldSupport: true,
		},
		{
			name:          "unsupported type",
			semanticType:  sql2.SemanticType("unsupported"),
			expectedSQL:   "",
			shouldSupport: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sqlType, supported := MapSemanticType(tt.semanticType)

			assert.Equal(t, tt.shouldSupport, supported, "MapSemanticType() supported")
			assert.Equal(t, tt.expectedSQL, sqlType, "MapSemanticType() sqlType")

			// Test SupportsSemanticType consistency
			assert.Equal(t, tt.shouldSupport, SupportsSemanticType(tt.semanticType), "SupportsSemanticType() consistency")
		})
	}
}

func TestConvertToUInt256(t *testing.T) {
	tests := []struct {
		name        string
		value       interface{}
		formatHint  string
		expected    string
		shouldError bool
	}{
		{
			name:        "hex string with 0x prefix",
			value:       "0x1234567890abcdef",
			formatHint:  "hex",
			expected:    "0x1234567890abcdef",
			shouldError: false,
		},
		{
			name:        "hex string without 0x prefix with hex hint",
			value:       "1234567890abcdef",
			formatHint:  "hex",
			expected:    "0x1234567890abcdef",
			shouldError: false,
		},
		{
			name:        "decimal string",
			value:       "123456789012345678901234567890",
			formatHint:  "decimal",
			expected:    "123456789012345678901234567890",
			shouldError: false,
		},
		{
			name:        "byte array",
			value:       []byte{0x12, 0x34, 0x56, 0x78},
			formatHint:  "",
			expected:    "0x12345678",
			shouldError: false,
		},
		{
			name:        "int64 value",
			value:       int64(12345),
			formatHint:  "",
			expected:    "12345",
			shouldError: false,
		},
		{
			name:        "uint64 value",
			value:       uint64(12345),
			formatHint:  "",
			expected:    "12345",
			shouldError: false,
		},
		{
			name:        "unsupported type",
			value:       float64(123.45),
			formatHint:  "",
			expected:    "",
			shouldError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := convertToUInt256(tt.value, tt.formatHint)

			if tt.shouldError {
				assert.Error(t, err, "convertToUInt256() should error")
				return
			}

			assert.NoError(t, err, "convertToUInt256() should not error")
			assert.Equal(t, tt.expected, result, "convertToUInt256() result")
		})
	}
}

func TestConvertToInt256(t *testing.T) {
	tests := []struct {
		name        string
		value       interface{}
		formatHint  string
		expected    string
		shouldError bool
	}{
		{
			name:        "hex string with 0x prefix",
			value:       "0x1234567890abcdef",
			formatHint:  "hex",
			expected:    "0x1234567890abcdef",
			shouldError: false,
		},
		{
			name:        "decimal string",
			value:       "123456789012345678901234567890",
			formatHint:  "decimal",
			expected:    "123456789012345678901234567890",
			shouldError: false,
		},
		{
			name:        "int64 value",
			value:       int64(-12345),
			formatHint:  "",
			expected:    "-12345",
			shouldError: false,
		},
		{
			name:        "unsupported type",
			value:       float64(123.45),
			formatHint:  "",
			expected:    "",
			shouldError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := convertToInt256(tt.value, tt.formatHint)

			if tt.shouldError {
				assert.Error(t, err, "convertToInt256() should error")
				return
			}

			assert.NoError(t, err, "convertToInt256() should not error")
			assert.Equal(t, tt.expected, result, "convertToInt256() result")
		})
	}
}

func TestConvertToFixedStringAddress(t *testing.T) {
	tests := []struct {
		name        string
		value       interface{}
		expected    string
		shouldError bool
	}{
		{
			name:        "valid address with 0x prefix",
			value:       "0x742d35cc6636C0532925a3b8D0A3e5A5F2d5De8e",
			expected:    "'0x742d35cc6636C0532925a3b8D0A3e5A5F2d5De8e'",
			shouldError: false,
		},
		{
			name:        "valid address without 0x prefix",
			value:       "742d35cc6636C0532925a3b8D0A3e5A5F2d5De8e",
			expected:    "'0x742d35cc6636C0532925a3b8D0A3e5A5F2d5De8e'",
			shouldError: false,
		},
		{
			name:        "20-byte array",
			value:       []byte{0x74, 0x2d, 0x35, 0xcc, 0x66, 0x36, 0xc0, 0x53, 0x29, 0x25, 0xa3, 0xb8, 0xd0, 0xa3, 0xe5, 0xa5, 0xf2, 0xd5, 0xde, 0x8e},
			expected:    "'0x742d35cc6636c0532925a3b8d0a3e5a5f2d5de8e'",
			shouldError: false,
		},
		{
			name:        "invalid address length",
			value:       "0x742d35cc6636C0532925a3b8D0A3e5A5F2d5De",
			expected:    "",
			shouldError: true,
		},
		{
			name:        "unsupported type",
			value:       123,
			expected:    "",
			shouldError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := convertToFixedStringAddress(tt.value)

			if tt.shouldError {
				assert.Error(t, err, "convertToFixedStringAddress() should error")
				return
			}

			assert.NoError(t, err, "convertToFixedStringAddress() should not error")
			assert.Equal(t, tt.expected, result, "convertToFixedStringAddress() result")
		})
	}
}

func TestConvertSemanticValue(t *testing.T) {
	tests := []struct {
		name         string
		semanticType sql2.SemanticType
		value        interface{}
		formatHint   string
		shouldError  bool
	}{
		{
			name:         "uint256 conversion",
			semanticType: sql2.SemanticUint256,
			value:        "0x123456789",
			formatHint:   "hex",
			shouldError:  false,
		},
		{
			name:         "int256 conversion",
			semanticType: sql2.SemanticInt256,
			value:        "123456789",
			formatHint:   "decimal",
			shouldError:  false,
		},
		{
			name:         "address conversion",
			semanticType: sql2.SemanticAddress,
			value:        "0x742d35cc6636C0532925a3b8D0A3e5A5F2d5De8e",
			formatHint:   "",
			shouldError:  false,
		},
		{
			name:         "string conversion",
			semanticType: sql2.SemanticJSON,
			value:        `{"key": "value"}`,
			formatHint:   "",
			shouldError:  false,
		},
		{
			name:         "unix timestamp conversion",
			semanticType: sql2.SemanticUnixTimestamp,
			value:        int64(1640995200),
			formatHint:   "",
			shouldError:  false,
		},
		{
			name:         "unix timestamp ms conversion",
			semanticType: sql2.SemanticUnixTimestampMS,
			value:        int64(1640995200000),
			formatHint:   "",
			shouldError:  false,
		},
		{
			name:         "fallback to default conversion",
			semanticType: sql2.SemanticType("unknown"),
			value:        "test",
			formatHint:   "",
			shouldError:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ConvertSemanticValue(tt.semanticType, tt.value, tt.formatHint)

			if tt.shouldError {
				assert.Error(t, err, "ConvertSemanticValue() should error")
				return
			}

			assert.NoError(t, err, "ConvertSemanticValue() should not error")
			assert.NotEmpty(t, result, "ConvertSemanticValue() should return non-empty result")
		})
	}
}
