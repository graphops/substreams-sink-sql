package risingwave

import (
	"strings"
	"testing"

	"github.com/streamingfast/substreams-sink-sql/db_proto/sql/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestDialectRisingwave_UseVersionField(t *testing.T) {
	d := &DialectRisingwave{}
	assert.False(t, d.UseVersionField())
}

func TestDialectRisingwave_UseDeletedField(t *testing.T) {
	d := &DialectRisingwave{}
	assert.False(t, d.UseDeletedField())
}

func TestDialectRisingwave_FullTableName(t *testing.T) {
	d := &DialectRisingwave{schemaName: "public"}
	table := &schema.Table{Name: "users"}
	
	expected := "public.users"
	actual := d.FullTableName(table)
	
	assert.Equal(t, expected, actual)
}

func TestDialectRisingwave_SchemaHash(t *testing.T) {
	logger := zap.NewNop()
	
	// Create two identical dialects
	d1, err := NewDialectRisingwave("test_schema", map[string]*schema.Table{}, logger)
	require.NoError(t, err)
	
	d2, err := NewDialectRisingwave("test_schema", map[string]*schema.Table{}, logger)
	require.NoError(t, err)
	
	// Their schema hashes should be identical
	assert.Equal(t, d1.SchemaHash(), d2.SchemaHash())
	
	// Create a dialect with different schema
	d3, err := NewDialectRisingwave("different_schema", map[string]*schema.Table{}, logger)
	require.NoError(t, err)
	
	// Schema hash should be different
	assert.NotEqual(t, d1.SchemaHash(), d3.SchemaHash())
}

func TestDialectRisingwave_Init(t *testing.T) {
	logger := zap.NewNop()
	d, err := NewDialectRisingwave("test_schema", map[string]*schema.Table{}, logger)
	require.NoError(t, err)
	
	// Should have at least the _blocks_ primary key
	assert.GreaterOrEqual(t, len(d.PrimaryKeySql), 1)
	
	// Find the _blocks_ primary key
	found := false
	for _, pk := range d.PrimaryKeySql {
		if pk.Table == "_blocks_" {
			found = true
			assert.Contains(t, pk.Sql, "alter table test_schema._blocks_ add constraint block_pk primary key (number)")
		}
	}
	assert.True(t, found, "Primary key constraint not found for _blocks_ table")
}

func TestDialectRisingwave_CreateTableStaticSql(t *testing.T) {
	// Test that the static SQL contains expected RisingWave-specific elements
	sql := strings.ToLower(risingwaveStaticSql)
	
	// Check schema creation
	assert.Contains(t, sql, "create schema if not exists")
	
	// Check _sink_info_ table
	assert.Contains(t, sql, "_sink_info_")
	assert.Contains(t, sql, "schema_hash varchar primary key")
	
	// Check _cursor_ table
	assert.Contains(t, sql, "_cursor_")
	assert.Contains(t, sql, "name varchar primary key")
	assert.Contains(t, sql, "cursor varchar not null")
	
	// Check _blocks_ table
	assert.Contains(t, sql, "_blocks_")
	assert.Contains(t, sql, "number integer")
	assert.Contains(t, sql, "hash varchar not null")
	assert.Contains(t, sql, "timestamp timestamp with time zone not null")
}

