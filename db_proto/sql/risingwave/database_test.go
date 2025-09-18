package risingwave

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// Test the tableName helper function that combines schema and table names
func TestTableName(t *testing.T) {
	tests := []struct {
		schema   string
		table    string
		expected string
	}{
		{"public", "users", "public.users"},
		{"test_schema", "test_table", "test_schema.test_table"},
		{"my_schema", "_blocks_", "my_schema._blocks_"},
		{"", "table", ".table"}, // Edge case: empty schema
	}

	for _, test := range tests {
		t.Run(test.expected, func(t *testing.T) {
			result := tableName(test.schema, test.table)
			assert.Equal(t, test.expected, result)
		})
	}
}

// Test value conversion for RisingWave-specific handling
func TestValueConversion(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected string
	}{
		{"string", "hello", "'hello'"},
		{"string with quotes", "it's", "'it''s'"},
		{"int64", int64(123), "123"},
		{"uint64", uint64(456), "456"},
		{"bool true", true, "true"},
		{"bool false", false, "false"},
		{"time", time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC), "'2023-01-01 00:00:00.000000+00:00'"},
		{"bytes", []byte{0xDE, 0xAD}, "'\\xDEAD'"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := ValueToString(test.input)
			assert.Equal(t, test.expected, result)
		})
	}
}
