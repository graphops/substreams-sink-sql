package postgres

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	sql2 "github.com/streamingfast/substreams-sink-sql/db_proto/sql"
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
		{"uint64", uint64(123), "'123'"},
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

func TestDataTypeString(t *testing.T) {
	tests := []struct {
		dataType DataType
		expected string
	}{
		{TypeNumeric, "NUMERIC"},
		{TypeInteger, "INTEGER"},
		{TypeBool, "BOOLEAN"},
		{TypeBigInt, "BIGINT"},
		{TypeDecimal, "DECIMAL"},
		{TypeDouble, "DOUBLE PRECISION"},
		{TypeText, "TEXT"},
		{TypeBlob, "BLOB"},
		{TypeVarchar, "VARCHAR(255)"},
		{TypeTimestamp, "TIMESTAMP"},
		{TypeTimestamptz, "TIMESTAMP WITH TIME ZONE"},
		{TypeJsonb, "JSONB"},
		{TypeUUID, "UUID"},
		{TypeChar, "CHAR"},
	}

	for _, tt := range tests {
		t.Run(string(tt.dataType), func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.dataType.String())
		})
	}
}

func TestMapSemanticType(t *testing.T) {
	tests := []struct {
		name           string
		semanticType   sql2.SemanticType
		expectedSQL    string
		shouldSupport  bool
	}{
		{
			name:          "uint256 maps to NUMERIC(78,0)",
			semanticType:  sql2.SemanticUint256,
			expectedSQL:   "NUMERIC(78,0)",
			shouldSupport: true,
		},
		{
			name:          "int256 maps to NUMERIC(78,0)",
			semanticType:  sql2.SemanticInt256,
			expectedSQL:   "NUMERIC(78,0)",
			shouldSupport: true,
		},
		{
			name:          "address maps to CHAR(42)",
			semanticType:  sql2.SemanticAddress,
			expectedSQL:   "CHAR(42)",
			shouldSupport: true,
		},
		{
			name:          "hash maps to CHAR(66)",
			semanticType:  sql2.SemanticHash,
			expectedSQL:   "CHAR(66)",
			shouldSupport: true,
		},
		{
			name:          "json maps to JSONB",
			semanticType:  sql2.SemanticJSON,
			expectedSQL:   "JSONB",
			shouldSupport: true,
		},
		{
			name:          "uuid maps to UUID",
			semanticType:  sql2.SemanticUUID,
			expectedSQL:   "UUID",
			shouldSupport: true,
		},
		{
			name:          "unix_timestamp maps to TIMESTAMP WITH TIME ZONE",
			semanticType:  sql2.SemanticUnixTimestamp,
			expectedSQL:   "TIMESTAMP WITH TIME ZONE",
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

func TestConvertToNumeric(t *testing.T) {
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
			expected:    "1311768467294899695", // converted to decimal
			shouldError: false,
		},
		{
			name:        "hex string without 0x prefix with hex hint",
			value:       "1234567890abcdef",
			formatHint:  "hex",
			expected:    "1311768467294899695", // converted to decimal
			shouldError: false,
		},
		{
			name:        "decimal string",
			value:       "123456789012345678901234567890",
			formatHint:  "decimal",
			expected:    "'123456789012345678901234567890'",
			shouldError: false,
		},
		{
			name:        "byte array",
			value:       []byte{0x12, 0x34, 0x56, 0x78},
			formatHint:  "",
			expected:    "305419896", // converted to decimal
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
			result, err := convertToNumeric(tt.value, tt.formatHint)
			
			if tt.shouldError {
				assert.Error(t, err, "convertToNumeric() should error")
				return
			}
			
			assert.NoError(t, err, "convertToNumeric() should not error")
			assert.Equal(t, tt.expected, result, "convertToNumeric() result")
		})
	}
}

func TestConvertToAddress(t *testing.T) {
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
			name:        "invalid byte array length",
			value:       []byte{0x74, 0x2d, 0x35},
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
			result, err := convertToAddress(tt.value)
			
			if tt.shouldError {
				assert.Error(t, err, "convertToAddress() should error")
				return
			}
			
			assert.NoError(t, err, "convertToAddress() should not error")
			assert.Equal(t, tt.expected, result, "convertToAddress() result")
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
			name:         "address conversion",
			semanticType: sql2.SemanticAddress,
			value:        "0x742d35cc6636C0532925a3b8D0A3e5A5F2d5De8e",
			formatHint:   "",
			shouldError:  false,
		},
		{
			name:         "jsonb conversion",
			semanticType: sql2.SemanticJSON,
			value:        `{"key": "value"}`,
			formatHint:   "",
			shouldError:  false,
		},
		{
			name:         "uuid conversion",
			semanticType: sql2.SemanticUUID,
			value:        "550e8400-e29b-41d4-a716-446655440000",
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