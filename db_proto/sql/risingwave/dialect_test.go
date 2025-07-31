package risingwave

import (
	"strings"
	"testing"

	"github.com/golang/protobuf/protoc-gen-go/descriptor"
	"github.com/jhump/protoreflect/desc"
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

	// With empty table registry, hash should be consistent
	// Note: Schema name doesn't affect hash, only table structures do
	assert.NotEmpty(t, d1.SchemaHash(), "Schema hash should not be empty")
}

func TestDialectRisingwave_Init(t *testing.T) {
	logger := zap.NewNop()
	d, err := NewDialectRisingwave("test_schema", map[string]*schema.Table{}, logger)
	require.NoError(t, err)

	// RisingWave dialect uses inline constraints, so PrimaryKeySql should be empty
	// Primary keys are defined directly in CREATE TABLE statements
	assert.Equal(t, 0, len(d.PrimaryKeySql), "RisingWave should not use ALTER TABLE for primary keys")

	// With empty table registry, no user-defined CREATE TABLE statements should be generated
	// System tables (_blocks_, _cursor_, etc.) are handled via static SQL in CreateDatabase method
	assert.Equal(t, 0, len(d.CreateTableSql), "No CREATE TABLE statements should be generated with empty table registry")

	// Verify that static SQL is properly formatted and contains system tables
	assert.NotEmpty(t, risingwaveStaticSql, "Static SQL should not be empty")
	assert.Contains(t, risingwaveStaticSql, "_blocks_", "Static SQL should contain _blocks_ table")
	assert.Contains(t, risingwaveStaticSql, "_cursor_", "Static SQL should contain _cursor_ table")
	assert.Contains(t, risingwaveStaticSql, "_sink_info_", "Static SQL should contain _sink_info_ table")
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
	assert.Contains(t, sql, "on conflict overwrite", "RisingWave should use ON CONFLICT OVERWRITE")

	// Check _blocks_ table
	assert.Contains(t, sql, "_blocks_")
	assert.Contains(t, sql, "number integer")
	assert.Contains(t, sql, "hash varchar not null")
	assert.Contains(t, sql, "timestamp timestamp with time zone not null")
}

func TestDialectRisingwave_CreateTable_SimpleTable(t *testing.T) {
	logger := zap.NewNop()

	// Create mock field descriptors
	stringField := createMockFieldDescriptor("name", descriptor.FieldDescriptorProto_TYPE_STRING)
	intField := createMockFieldDescriptor("age", descriptor.FieldDescriptorProto_TYPE_INT32)

	table := &schema.Table{
		Name: "users",
		Columns: []*schema.Column{
			{Name: "name", FieldDescriptor: stringField},
			{Name: "age", FieldDescriptor: intField},
		},
	}

	tableRegistry := map[string]*schema.Table{"users": table}
	d, err := NewDialectRisingwave("public", tableRegistry, logger)
	require.NoError(t, err)

	// Should have one CREATE TABLE statement
	assert.Equal(t, 1, len(d.CreateTableSql))

	sql := d.CreateTableSql["users"]
	assert.Contains(t, sql, "CREATE TABLE  IF NOT EXISTS public.users")
	assert.Contains(t, sql, "block_number INTEGER NOT NULL")
	assert.Contains(t, sql, "block_timestamp TIMESTAMP WITH TIME ZONE NOT NULL")
	assert.Contains(t, sql, `"name" CHARACTER VARYING`)
	assert.Contains(t, sql, `"age" INTEGER`)

	// Should not contain any foreign key constraints
	assert.NotContains(t, sql, "FOREIGN KEY")
	assert.NotContains(t, sql, "REFERENCES")
}

func TestDialectRisingwave_CreateTable_WithPrimaryKey(t *testing.T) {
	logger := zap.NewNop()

	idField := createMockFieldDescriptor("id", descriptor.FieldDescriptorProto_TYPE_STRING)
	nameField := createMockFieldDescriptor("name", descriptor.FieldDescriptorProto_TYPE_STRING)

	table := &schema.Table{
		Name: "users",
		PrimaryKey: &schema.PrimaryKey{
			Name:            "id",
			FieldDescriptor: idField,
		},
		Columns: []*schema.Column{
			{Name: "id", FieldDescriptor: idField, IsPrimaryKey: true},
			{Name: "name", FieldDescriptor: nameField},
		},
	}

	tableRegistry := map[string]*schema.Table{"users": table}
	d, err := NewDialectRisingwave("public", tableRegistry, logger)
	require.NoError(t, err)

	sql := d.CreateTableSql["users"]
	assert.Contains(t, sql, "id CHARACTER VARYING PRIMARY KEY")
	assert.Contains(t, sql, `"name" CHARACTER VARYING`)

	// Primary key should not be duplicated
	assert.Equal(t, 1, strings.Count(sql, "id CHARACTER VARYING"))
}

func TestDialectRisingwave_CreateTable_ChildTable(t *testing.T) {
	logger := zap.NewNop()

	// Parent table
	parentIdField := createMockFieldDescriptor("instruction_id", descriptor.FieldDescriptorProto_TYPE_STRING)
	parentTable := &schema.Table{
		Name: "instructions",
		PrimaryKey: &schema.PrimaryKey{
			Name:            "instruction_id",
			FieldDescriptor: parentIdField,
		},
		Columns: []*schema.Column{
			{Name: "instruction_id", FieldDescriptor: parentIdField, IsPrimaryKey: true},
		},
	}

	// Child table
	amountField := createMockFieldDescriptor("amount", descriptor.FieldDescriptorProto_TYPE_UINT64)
	childTable := &schema.Table{
		Name: "mints",
		ChildOf: &schema.ChildOf{
			ParentTable:      "instructions",
			ParentTableField: "instruction_id",
		},
		Columns: []*schema.Column{
			{Name: "amount", FieldDescriptor: amountField},
		},
	}

	tableRegistry := map[string]*schema.Table{
		"instructions": parentTable,
		"mints":        childTable,
	}

	d, err := NewDialectRisingwave("public", tableRegistry, logger)
	require.NoError(t, err)

	sql := d.CreateTableSql["mints"]
	assert.Contains(t, sql, "CREATE TABLE  IF NOT EXISTS public.mints")
	assert.Contains(t, sql, "block_number INTEGER NOT NULL")
	assert.Contains(t, sql, "block_timestamp TIMESTAMP WITH TIME ZONE NOT NULL")
	assert.Contains(t, sql, "instruction_id CHARACTER VARYING NOT NULL")
	assert.Contains(t, sql, `"amount" NUMERIC`)

	// Should not contain foreign key constraints
	assert.NotContains(t, sql, "FOREIGN KEY")
}

func TestDialectRisingwave_CreateTable_WithUniqueConstraint(t *testing.T) {
	logger := zap.NewNop()

	emailField := createMockFieldDescriptor("email", descriptor.FieldDescriptorProto_TYPE_STRING)
	nameField := createMockFieldDescriptor("name", descriptor.FieldDescriptorProto_TYPE_STRING)

	table := &schema.Table{
		Name: "users",
		Columns: []*schema.Column{
			{Name: "email", FieldDescriptor: emailField, IsUnique: true},
			{Name: "name", FieldDescriptor: nameField},
		},
	}

	tableRegistry := map[string]*schema.Table{"users": table}
	d, err := NewDialectRisingwave("public", tableRegistry, logger)
	require.NoError(t, err)

	sql := d.CreateTableSql["users"]
	assert.Contains(t, sql, `"email" CHARACTER VARYING UNIQUE`)
	assert.Contains(t, sql, `"name" CHARACTER VARYING`)
}

func TestDialectRisingwave_CreateTable_SkipsRepeatedFields(t *testing.T) {
	logger := zap.NewNop()

	tagsField := createMockFieldDescriptor("tags", descriptor.FieldDescriptorProto_TYPE_STRING)
	nameField := createMockFieldDescriptor("name", descriptor.FieldDescriptorProto_TYPE_STRING)

	table := &schema.Table{
		Name: "users",
		Columns: []*schema.Column{
			{Name: "tags", FieldDescriptor: tagsField, IsRepeated: true},
			{Name: "name", FieldDescriptor: nameField},
		},
	}

	tableRegistry := map[string]*schema.Table{"users": table}
	d, err := NewDialectRisingwave("public", tableRegistry, logger)
	require.NoError(t, err)

	sql := d.CreateTableSql["users"]
	assert.NotContains(t, sql, "tags")
	assert.Contains(t, sql, `"name" CHARACTER VARYING`)
}

func TestDialectRisingwave_CreateTable_PreventsDuplicateColumns(t *testing.T) {
	logger := zap.NewNop()

	// Create a scenario where a column might be added twice
	idField := createMockFieldDescriptor("block_number", descriptor.FieldDescriptorProto_TYPE_INT32)
	nameField := createMockFieldDescriptor("name", descriptor.FieldDescriptorProto_TYPE_STRING)

	table := &schema.Table{
		Name: "test_table",
		Columns: []*schema.Column{
			{Name: "block_number", FieldDescriptor: idField}, // This should be skipped since block_number is added automatically
			{Name: "name", FieldDescriptor: nameField},
		},
	}

	tableRegistry := map[string]*schema.Table{"test_table": table}
	d, err := NewDialectRisingwave("public", tableRegistry, logger)
	require.NoError(t, err)

	sql := d.CreateTableSql["test_table"]

	// block_number should appear only once
	assert.Equal(t, 1, strings.Count(sql, "block_number"))
	assert.Contains(t, sql, `"name" CHARACTER VARYING`)
}

func TestDialectRisingwave_CreateInsertFromDescriptor_SimpleTable(t *testing.T) {
	nameField := createMockFieldDescriptor("name", descriptor.FieldDescriptorProto_TYPE_STRING)
	ageField := createMockFieldDescriptor("age", descriptor.FieldDescriptorProto_TYPE_INT32)

	table := &schema.Table{
		Name: "users",
		Columns: []*schema.Column{
			{Name: "name", FieldDescriptor: nameField},
			{Name: "age", FieldDescriptor: ageField},
		},
	}

	d := &DialectRisingwave{schemaName: "public"}

	sql, err := createInsertFromDescriptor(table, d)
	require.NoError(t, err)

	expected := `INSERT INTO public.users (block_number, block_timestamp, "name", "age") VALUES ($1, $2, $3, $4)`
	assert.Equal(t, expected, sql)
}

func TestDialectRisingwave_CreateInsertFromDescriptor_WithPrimaryKey(t *testing.T) {
	idField := createMockFieldDescriptor("id", descriptor.FieldDescriptorProto_TYPE_STRING)
	nameField := createMockFieldDescriptor("name", descriptor.FieldDescriptorProto_TYPE_STRING)

	table := &schema.Table{
		Name: "users",
		PrimaryKey: &schema.PrimaryKey{
			Name:            "id",
			FieldDescriptor: idField,
		},
		Columns: []*schema.Column{
			{Name: "id", FieldDescriptor: idField, IsPrimaryKey: true},
			{Name: "name", FieldDescriptor: nameField},
		},
	}

	d := &DialectRisingwave{schemaName: "public"}

	sql, err := createInsertFromDescriptor(table, d)
	require.NoError(t, err)

	expected := `INSERT INTO public.users (block_number, block_timestamp, id, "name") VALUES ($1, $2, $3, $4)`
	assert.Equal(t, expected, sql)
}

func TestDialectRisingwave_CreateInsertFromDescriptor_ChildTable(t *testing.T) {
	amountField := createMockFieldDescriptor("amount", descriptor.FieldDescriptorProto_TYPE_UINT64)

	table := &schema.Table{
		Name: "mints",
		ChildOf: &schema.ChildOf{
			ParentTable:      "instructions",
			ParentTableField: "instruction_id",
		},
		Columns: []*schema.Column{
			{Name: "amount", FieldDescriptor: amountField},
		},
	}

	d := &DialectRisingwave{schemaName: "public"}

	sql, err := createInsertFromDescriptor(table, d)
	require.NoError(t, err)

	expected := `INSERT INTO public.mints (block_number, block_timestamp, instruction_id, "amount") VALUES ($1, $2, $3, $4)`
	assert.Equal(t, expected, sql)
}

func TestDialectRisingwave_CreateInsertFromDescriptorAcc_SimpleTable(t *testing.T) {
	nameField := createMockFieldDescriptor("name", descriptor.FieldDescriptorProto_TYPE_STRING)
	ageField := createMockFieldDescriptor("age", descriptor.FieldDescriptorProto_TYPE_INT32)

	table := &schema.Table{
		Name: "users",
		Columns: []*schema.Column{
			{Name: "name", FieldDescriptor: nameField},
			{Name: "age", FieldDescriptor: ageField},
		},
	}

	d := &DialectRisingwave{schemaName: "public"}

	sql, err := createInsertFromDescriptorAcc(table, d)
	require.NoError(t, err)

	expected := `INSERT INTO public.users (block_number, block_timestamp, "name", "age") VALUES `
	assert.Equal(t, expected, sql)
}

func TestDialectRisingwave_CreateInsertFromDescriptorAcc_WithPrimaryKey(t *testing.T) {
	idField := createMockFieldDescriptor("id", descriptor.FieldDescriptorProto_TYPE_STRING)
	nameField := createMockFieldDescriptor("name", descriptor.FieldDescriptorProto_TYPE_STRING)

	table := &schema.Table{
		Name: "users",
		PrimaryKey: &schema.PrimaryKey{
			Name:            "id",
			FieldDescriptor: idField,
		},
		Columns: []*schema.Column{
			{Name: "id", FieldDescriptor: idField, IsPrimaryKey: true},
			{Name: "name", FieldDescriptor: nameField},
		},
	}

	d := &DialectRisingwave{schemaName: "public"}

	sql, err := createInsertFromDescriptorAcc(table, d)
	require.NoError(t, err)

	expected := `INSERT INTO public.users (block_number, block_timestamp, id, "name") VALUES `
	assert.Equal(t, expected, sql)
}

func TestDialectRisingwave_TableName(t *testing.T) {
	tests := []struct {
		schema   string
		table    string
		expected string
	}{
		{"public", "users", "public.users"},
		{"test_schema", "orders", "test_schema.orders"},
		{"", "table", ".table"},
	}

	for _, tt := range tests {
		t.Run(tt.schema+"_"+tt.table, func(t *testing.T) {
			result := tableName(tt.schema, tt.table)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestDialectRisingwave_NoForeignKeyConstraints(t *testing.T) {
	logger := zap.NewNop()

	// Create tables with foreign key relationships
	userIdField := createMockFieldDescriptor("user_id", descriptor.FieldDescriptorProto_TYPE_STRING)
	orderIdField := createMockFieldDescriptor("order_id", descriptor.FieldDescriptorProto_TYPE_STRING)

	userTable := &schema.Table{
		Name: "users",
		PrimaryKey: &schema.PrimaryKey{
			Name:            "user_id",
			FieldDescriptor: userIdField,
		},
		Columns: []*schema.Column{
			{Name: "user_id", FieldDescriptor: userIdField, IsPrimaryKey: true},
		},
	}

	orderTable := &schema.Table{
		Name: "orders",
		PrimaryKey: &schema.PrimaryKey{
			Name:            "order_id",
			FieldDescriptor: orderIdField,
		},
		Columns: []*schema.Column{
			{Name: "order_id", FieldDescriptor: orderIdField, IsPrimaryKey: true},
			{
				Name:            "user_id",
				FieldDescriptor: userIdField,
				ForeignKey: &schema.ForeignKey{
					Table:      "users",
					TableField: "user_id",
				},
			},
		},
	}

	tableRegistry := map[string]*schema.Table{
		"users":  userTable,
		"orders": orderTable,
	}

	d, err := NewDialectRisingwave("public", tableRegistry, logger)
	require.NoError(t, err)

	// Should have no foreign key constraints
	assert.Equal(t, 0, len(d.ForeignKeySql))

	// But should still create the tables with the foreign key columns
	orderSQL := d.CreateTableSql["orders"]
	assert.Contains(t, orderSQL, `"user_id" CHARACTER VARYING`)
	assert.NotContains(t, orderSQL, "FOREIGN KEY")
	assert.NotContains(t, orderSQL, "REFERENCES")
}

func TestDialectRisingwave_SchemaHashConsistency(t *testing.T) {
	logger := zap.NewNop()

	nameField := createMockFieldDescriptor("name", descriptor.FieldDescriptorProto_TYPE_STRING)
	table := &schema.Table{
		Name: "users",
		Columns: []*schema.Column{
			{Name: "name", FieldDescriptor: nameField},
		},
	}

	tableRegistry := map[string]*schema.Table{"users": table}

	// Create multiple dialects with same configuration
	d1, err := NewDialectRisingwave("public", tableRegistry, logger)
	require.NoError(t, err)

	d2, err := NewDialectRisingwave("public", tableRegistry, logger)
	require.NoError(t, err)

	// Hashes should be identical
	assert.Equal(t, d1.SchemaHash(), d2.SchemaHash())

	// Create dialect with different table
	ageField := createMockFieldDescriptor("age", descriptor.FieldDescriptorProto_TYPE_INT32)
	differentTable := &schema.Table{
		Name: "users",
		Columns: []*schema.Column{
			{Name: "name", FieldDescriptor: nameField},
			{Name: "age", FieldDescriptor: ageField},
		},
	}

	differentRegistry := map[string]*schema.Table{"users": differentTable}
	d3, err := NewDialectRisingwave("public", differentRegistry, logger)
	require.NoError(t, err)

	// Hash should be different
	assert.NotEqual(t, d1.SchemaHash(), d3.SchemaHash())
}

func TestDialectRisingwave_TypeMapping(t *testing.T) {
	tests := []struct {
		name        string
		protoType   descriptor.FieldDescriptorProto_Type
		expectedSQL string
	}{
		{"string", descriptor.FieldDescriptorProto_TYPE_STRING, "CHARACTER VARYING"},
		{"int32", descriptor.FieldDescriptorProto_TYPE_INT32, "INTEGER"},
		{"int64", descriptor.FieldDescriptorProto_TYPE_INT64, "BIGINT"},
		{"uint32", descriptor.FieldDescriptorProto_TYPE_UINT32, "BIGINT"},
		{"uint64", descriptor.FieldDescriptorProto_TYPE_UINT64, "NUMERIC"},
		{"float", descriptor.FieldDescriptorProto_TYPE_FLOAT, "REAL"},
		{"double", descriptor.FieldDescriptorProto_TYPE_DOUBLE, "DOUBLE PRECISION"},
		{"bool", descriptor.FieldDescriptorProto_TYPE_BOOL, "BOOLEAN"},
		{"bytes", descriptor.FieldDescriptorProto_TYPE_BYTES, "BYTEA"},
	}

	logger := zap.NewNop()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			field := createMockFieldDescriptor("test_field", tt.protoType)

			table := &schema.Table{
				Name: "test_table",
				Columns: []*schema.Column{
					{Name: "test_field", FieldDescriptor: field},
				},
			}

			tableRegistry := map[string]*schema.Table{"test_table": table}
			d, err := NewDialectRisingwave("public", tableRegistry, logger)
			require.NoError(t, err)

			sql := d.CreateTableSql["test_table"]
			assert.Contains(t, sql, tt.expectedSQL, "Type mapping for %s should produce %s", tt.name, tt.expectedSQL)
		})
	}
}

func TestDialectRisingwave_ComplexTableStructure(t *testing.T) {
	logger := zap.NewNop()

	// Create a complex table with multiple column types and constraints
	idField := createMockFieldDescriptor("id", descriptor.FieldDescriptorProto_TYPE_STRING)
	nameField := createMockFieldDescriptor("name", descriptor.FieldDescriptorProto_TYPE_STRING)
	ageField := createMockFieldDescriptor("age", descriptor.FieldDescriptorProto_TYPE_INT32)
	emailField := createMockFieldDescriptor("email", descriptor.FieldDescriptorProto_TYPE_STRING)
	balanceField := createMockFieldDescriptor("balance", descriptor.FieldDescriptorProto_TYPE_UINT64)
	activeField := createMockFieldDescriptor("active", descriptor.FieldDescriptorProto_TYPE_BOOL)

	table := &schema.Table{
		Name: "complex_users",
		PrimaryKey: &schema.PrimaryKey{
			Name:            "id",
			FieldDescriptor: idField,
		},
		Columns: []*schema.Column{
			{Name: "id", FieldDescriptor: idField, IsPrimaryKey: true},
			{Name: "name", FieldDescriptor: nameField},
			{Name: "age", FieldDescriptor: ageField},
			{Name: "email", FieldDescriptor: emailField, IsUnique: true},
			{Name: "balance", FieldDescriptor: balanceField},
			{Name: "active", FieldDescriptor: activeField},
		},
	}

	tableRegistry := map[string]*schema.Table{"complex_users": table}
	d, err := NewDialectRisingwave("public", tableRegistry, logger)
	require.NoError(t, err)

	sql := d.CreateTableSql["complex_users"]

	// Check all expected elements are present
	assert.Contains(t, sql, "CREATE TABLE  IF NOT EXISTS public.complex_users")
	assert.Contains(t, sql, "id CHARACTER VARYING PRIMARY KEY")
	assert.Contains(t, sql, `"name" CHARACTER VARYING`)
	assert.Contains(t, sql, `"age" INTEGER`)
	assert.Contains(t, sql, `"email" CHARACTER VARYING UNIQUE`)
	assert.Contains(t, sql, `"balance" NUMERIC`)
	assert.Contains(t, sql, `"active" BOOLEAN`)
	assert.Contains(t, sql, "block_number INTEGER NOT NULL")
	assert.Contains(t, sql, "block_timestamp TIMESTAMP WITH TIME ZONE NOT NULL")

	// Ensure no foreign keys
	assert.NotContains(t, sql, "FOREIGN KEY")
	assert.NotContains(t, sql, "REFERENCES")
}

func TestDialectRisingwave_MultipleChildTables(t *testing.T) {
	logger := zap.NewNop()

	// Parent table
	parentIdField := createMockFieldDescriptor("transaction_id", descriptor.FieldDescriptorProto_TYPE_STRING)
	parentTable := &schema.Table{
		Name: "transactions",
		PrimaryKey: &schema.PrimaryKey{
			Name:            "transaction_id",
			FieldDescriptor: parentIdField,
		},
		Columns: []*schema.Column{
			{Name: "transaction_id", FieldDescriptor: parentIdField, IsPrimaryKey: true},
		},
	}

	// First child table
	transferAmountField := createMockFieldDescriptor("amount", descriptor.FieldDescriptorProto_TYPE_UINT64)
	transferTable := &schema.Table{
		Name: "transfers",
		ChildOf: &schema.ChildOf{
			ParentTable:      "transactions",
			ParentTableField: "transaction_id",
		},
		Columns: []*schema.Column{
			{Name: "amount", FieldDescriptor: transferAmountField},
		},
	}

	// Second child table
	logMessageField := createMockFieldDescriptor("message", descriptor.FieldDescriptorProto_TYPE_STRING)
	logTable := &schema.Table{
		Name: "logs",
		ChildOf: &schema.ChildOf{
			ParentTable:      "transactions",
			ParentTableField: "transaction_id",
		},
		Columns: []*schema.Column{
			{Name: "message", FieldDescriptor: logMessageField},
		},
	}

	tableRegistry := map[string]*schema.Table{
		"transactions": parentTable,
		"transfers":    transferTable,
		"logs":         logTable,
	}

	d, err := NewDialectRisingwave("public", tableRegistry, logger)
	require.NoError(t, err)

	// Check parent table
	parentSQL := d.CreateTableSql["transactions"]
	assert.Contains(t, parentSQL, "transaction_id CHARACTER VARYING PRIMARY KEY")

	// Check first child table
	transferSQL := d.CreateTableSql["transfers"]
	assert.Contains(t, transferSQL, "transaction_id CHARACTER VARYING NOT NULL")
	assert.Contains(t, transferSQL, `"amount" NUMERIC`)

	// Check second child table
	logSQL := d.CreateTableSql["logs"]
	assert.Contains(t, logSQL, "transaction_id CHARACTER VARYING NOT NULL")
	assert.Contains(t, logSQL, `"message" CHARACTER VARYING`)

	// All should have block metadata
	for _, sql := range []string{parentSQL, transferSQL, logSQL} {
		assert.Contains(t, sql, "block_number INTEGER NOT NULL")
		assert.Contains(t, sql, "block_timestamp TIMESTAMP WITH TIME ZONE NOT NULL")
	}
}

func TestDialectRisingwave_EmptyTableRegistry(t *testing.T) {
	logger := zap.NewNop()

	d, err := NewDialectRisingwave("test_schema", map[string]*schema.Table{}, logger)
	require.NoError(t, err)

	// Should have no CREATE TABLE statements
	assert.Equal(t, 0, len(d.CreateTableSql))

	// Should have no constraints
	assert.Equal(t, 0, len(d.PrimaryKeySql))
	assert.Equal(t, 0, len(d.ForeignKeySql))
	assert.Equal(t, 0, len(d.UniqueConstraintSql))

	// But should still have a valid schema hash
	assert.NotEmpty(t, d.SchemaHash())
}

func TestDialectRisingwave_GetTable(t *testing.T) {
	logger := zap.NewNop()

	nameField := createMockFieldDescriptor("name", descriptor.FieldDescriptorProto_TYPE_STRING)
	table := &schema.Table{
		Name: "users",
		Columns: []*schema.Column{
			{Name: "name", FieldDescriptor: nameField},
		},
	}

	tableRegistry := map[string]*schema.Table{"users": table}
	d, err := NewDialectRisingwave("public", tableRegistry, logger)
	require.NoError(t, err)

	// Should be able to retrieve the table
	retrievedTable := d.GetTable("users")
	assert.NotNil(t, retrievedTable)
	assert.Equal(t, "users", retrievedTable.Name)

	// Should return nil for non-existent table
	nonExistentTable := d.GetTable("non_existent")
	assert.Nil(t, nonExistentTable)
}

func TestDialectRisingwave_GetTables(t *testing.T) {
	logger := zap.NewNop()

	nameField := createMockFieldDescriptor("name", descriptor.FieldDescriptorProto_TYPE_STRING)
	ageField := createMockFieldDescriptor("age", descriptor.FieldDescriptorProto_TYPE_INT32)

	usersTable := &schema.Table{
		Name: "users",
		Columns: []*schema.Column{
			{Name: "name", FieldDescriptor: nameField},
		},
	}

	ordersTable := &schema.Table{
		Name: "orders",
		Columns: []*schema.Column{
			{Name: "age", FieldDescriptor: ageField},
		},
	}

	tableRegistry := map[string]*schema.Table{
		"users":  usersTable,
		"orders": ordersTable,
	}

	d, err := NewDialectRisingwave("public", tableRegistry, logger)
	require.NoError(t, err)

	tables := d.GetTables()
	assert.Equal(t, 2, len(tables))

	// Check that both tables are present (order doesn't matter)
	tableNames := make([]string, len(tables))
	for i, table := range tables {
		tableNames[i] = table.Name
	}
	assert.Contains(t, tableNames, "users")
	assert.Contains(t, tableNames, "orders")
}

// Helper function to create mock field descriptors
func createMockFieldDescriptor(name string, fieldType descriptor.FieldDescriptorProto_Type) *desc.FieldDescriptor {
	// Create a minimal field descriptor for testing
	// In real usage, these would come from protobuf reflection
	fieldNumber := int32(1) // Valid field number (must be > 0)
	proto := &descriptor.FieldDescriptorProto{
		Name:   &name,
		Type:   &fieldType,
		Number: &fieldNumber,
	}

	// Create a mock message descriptor
	msgProto := &descriptor.DescriptorProto{
		Name:  stringPtr("TestMessage"),
		Field: []*descriptor.FieldDescriptorProto{proto},
	}

	// Create file descriptor
	fileProto := &descriptor.FileDescriptorProto{
		Name:        stringPtr("test.proto"),
		MessageType: []*descriptor.DescriptorProto{msgProto},
	}

	// Build descriptors
	fileDesc, err := desc.CreateFileDescriptor(fileProto)
	if err != nil {
		panic(err)
	}

	msgDesc := fileDesc.GetMessageTypes()[0]
	fieldDesc := msgDesc.GetFields()[0]

	return fieldDesc
}

func stringPtr(s string) *string {
	return &s
}
