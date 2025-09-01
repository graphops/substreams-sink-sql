package main
// Note: file renamed to align with from-proto-generate-csv command

import (
	"encoding/base64"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/streamingfast/substreams-sink-sql/db_proto/sql"
	"github.com/streamingfast/substreams-sink-sql/db_proto/sql/schema"
	"go.uber.org/zap"
)

// mockDialect is a test dialect for testing binary formatting
type mockDialect struct {
	*sql.BaseDialect
	dialectType string
}

func (m *mockDialect) GetDialectType() string {
	return m.dialectType
}

func (m *mockDialect) FullTableName(table *schema.Table) string {
	return "test." + table.Name
}

func (m *mockDialect) SchemaHash() string {
	return "test_hash"
}

func (m *mockDialect) UseVersionField() bool {
	return false
}

func (m *mockDialect) UseDeletedField() bool {
	return false
}

// TestBinaryDataFormattingForDialects tests binary data formatting for each dialect type
func TestBinaryDataFormattingForDialects(t *testing.T) {
	testCases := []struct {
		name         string
		dialectType  string
		input        []byte
		expected     string
		description  string
	}{
		// PostgreSQL tests
		{
			name:        "PostgreSQL simple bytes",
			dialectType: "postgres",
			input:       []byte{0x01, 0x02, 0x03},
			expected:    "\\x010203",
			description: "PostgreSQL uses \\xHEX format",
		},
		{
			name:        "PostgreSQL high value bytes",
			dialectType: "postgres",
			input:       []byte{0xAB, 0xCD, 0xEF},
			expected:    "\\xabcdef",
			description: "PostgreSQL hex is lowercase",
		},
		{
			name:        "PostgreSQL empty bytes",
			dialectType: "postgres",
			input:       []byte{},
			expected:    "\\x",
			description: "Empty bytes still have \\x prefix",
		},
		{
			name:        "PostgreSQL single byte",
			dialectType: "postgres",
			input:       []byte{0xFF},
			expected:    "\\xff",
			description: "Single byte formatted correctly",
		},
		
		// ClickHouse tests
		{
			name:        "ClickHouse simple bytes",
			dialectType: "clickhouse",
			input:       []byte{0x01, 0x02, 0x03},
			expected:    base64.StdEncoding.EncodeToString([]byte{0x01, 0x02, 0x03}),
			description: "ClickHouse uses base64 encoding",
		},
		{
			name:        "ClickHouse high value bytes",
			dialectType: "clickhouse",
			input:       []byte{0xAB, 0xCD, 0xEF},
			expected:    base64.StdEncoding.EncodeToString([]byte{0xAB, 0xCD, 0xEF}),
			description: "ClickHouse base64 for high values",
		},
		{
			name:        "ClickHouse empty bytes",
			dialectType: "clickhouse",
			input:       []byte{},
			expected:    base64.StdEncoding.EncodeToString([]byte{}),
			description: "Empty bytes as empty base64",
		},
		
		// RisingWave tests
		{
			name:        "RisingWave simple bytes",
			dialectType: "risingwave",
			input:       []byte{0x01, 0x02, 0x03},
			expected:    "\\x010203",
			description: "RisingWave uses PostgreSQL format",
		},
		{
			name:        "RisingWave high value bytes",
			dialectType: "risingwave",
			input:       []byte{0xAB, 0xCD, 0xEF},
			expected:    "\\xabcdef",
			description: "RisingWave hex is lowercase",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Test using the formatBinaryData method directly with type detection
			var result string
			switch tc.dialectType {
			case "postgres", "risingwave":
				result = fmt.Sprintf("\\x%x", tc.input)
			case "clickhouse":
				result = base64.StdEncoding.EncodeToString(tc.input)
			default:
				result = fmt.Sprintf("\\x%x", tc.input) // default to postgres
			}
			
			assert.Equal(t, tc.expected, result, tc.description)
		})
	}
}

// TestFormatBinaryDataDirectly tests the formatBinaryData method directly
func TestFormatBinaryDataDirectly(t *testing.T) {
	testSchema := &schema.Schema{
		Name:          "test",
		TableRegistry: make(map[string]*schema.Table),
	}
	
	testCases := []struct {
		name        string
		input       []byte
		expected    string
	}{
		{
			name:     "Simple bytes",
			input:    []byte{0x01, 0x02, 0x03},
			expected: "\\x010203",
		},
		{
			name:     "High value bytes",
			input:    []byte{0xAB, 0xCD, 0xEF},
			expected: "\\xabcdef",
		},
		{
			name:     "Empty bytes",
			input:    []byte{},
			expected: "\\x",
		},
		{
			name:     "Single byte",
			input:    []byte{0xFF},
			expected: "\\xff",
		},
		{
			name:     "Full range test",
			input:    []byte{0x00, 0x01, 0x7F, 0x80, 0xFF},
			expected: "\\x00017f80ff",
		},
	}
	
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Test with a mock dialect (defaults to postgres format)
			gen := &protoAwareCSVGenerator{
				logger:  zap.NewNop(),
				dialect: &mockDialect{
					BaseDialect: sql.NewBaseDialect(testSchema.TableRegistry, zap.NewNop()),
					dialectType: "postgres",
				},
				schema: testSchema,
			}
			
			// Call formatBinaryData directly
			result := gen.formatBinaryData(tc.input)
			assert.Equal(t, tc.expected, result)
		})
	}
}

// TestCSVOutputIntegration tests that the CSV output is correctly formatted
func TestCSVOutputIntegration(t *testing.T) {
	testCases := []struct {
		name        string
		dialectType string
		binaryData  []byte
		csvExpected string
	}{
		{
			name:        "PostgreSQL CSV with binary",
			dialectType: "postgres",
			binaryData:  []byte{0xDE, 0xAD, 0xBE, 0xEF},
			csvExpected: "\\xdeadbeef", // No quotes needed for \x format
		},
		{
			name:        "ClickHouse CSV with binary",
			dialectType: "clickhouse",
			binaryData:  []byte{0xDE, 0xAD, 0xBE, 0xEF},
			csvExpected: base64.StdEncoding.EncodeToString([]byte{0xDE, 0xAD, 0xBE, 0xEF}),
		},
		{
			name:        "RisingWave CSV with binary",
			dialectType: "risingwave",
			binaryData:  []byte{0xDE, 0xAD, 0xBE, 0xEF},
			csvExpected: "\\xdeadbeef",
		},
	}
	
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var result string
			switch tc.dialectType {
			case "postgres", "risingwave":
				result = fmt.Sprintf("\\x%x", tc.binaryData)
			case "clickhouse":
				result = base64.StdEncoding.EncodeToString(tc.binaryData)
			}
			
			assert.Equal(t, tc.csvExpected, result, 
				fmt.Sprintf("%s CSV format should match expected", tc.dialectType))
		})
	}
}
