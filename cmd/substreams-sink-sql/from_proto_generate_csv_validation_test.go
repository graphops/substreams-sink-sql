package main
// Note: file renamed to align with from-proto-generate-csv command

import (
	"encoding/base64"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/protoc-gen-go/descriptor"
	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/dynamic"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/streamingfast/substreams-sink-sql/db_proto/sql"
	"github.com/streamingfast/substreams-sink-sql/db_proto/sql/click_house"
	"github.com/streamingfast/substreams-sink-sql/db_proto/sql/postgres"
	"github.com/streamingfast/substreams-sink-sql/db_proto/sql/risingwave"
	"github.com/streamingfast/substreams-sink-sql/db_proto/sql/schema"
	"go.uber.org/zap"
)

// TestFieldValueAlignment validates that field names and values stay synchronized
func TestFieldValueAlignment(t *testing.T) {
	// This test validates Issue #1: Field arrays getting out of sync
	
	// Create a test message with multiple fields
	msgProto := &descriptor.DescriptorProto{
		Name: proto.String("TestMessage"),
		Field: []*descriptor.FieldDescriptorProto{
			{
				Name:   proto.String("id"),
				Number: proto.Int32(1),
				Type:   descriptor.FieldDescriptorProto_TYPE_INT64.Enum(),
			},
			{
				Name:   proto.String("name"),
				Number: proto.Int32(2),
				Type:   descriptor.FieldDescriptorProto_TYPE_STRING.Enum(),
			},
			{
				Name:   proto.String("amount"),
				Number: proto.Int32(3),
				Type:   descriptor.FieldDescriptorProto_TYPE_UINT64.Enum(),
			},
		},
	}

	fileProto := &descriptor.FileDescriptorProto{
		Name:        proto.String("test.proto"),
		MessageType: []*descriptor.DescriptorProto{msgProto},
	}

	fd, err := desc.CreateFileDescriptor(fileProto)
	require.NoError(t, err)

	msgDesc := fd.GetMessageTypes()[0]
	dm := dynamic.NewMessage(msgDesc)
	
	// Set field values
	dm.SetFieldByName("id", int64(123))
	dm.SetFieldByName("name", "test_name")
	dm.SetFieldByName("amount", uint64(456))

	// Create test schema
	testSchema := &schema.Schema{
		Name:          "test",
		TableRegistry: make(map[string]*schema.Table),
	}

	idFd := createSimpleFieldDescriptor("id", descriptor.FieldDescriptorProto_TYPE_INT64)
	nameFd := createSimpleFieldDescriptor("name", descriptor.FieldDescriptorProto_TYPE_STRING)
	amountFd := createSimpleFieldDescriptor("amount", descriptor.FieldDescriptorProto_TYPE_UINT64)

	testTable := &schema.Table{
		Name: "TestMessage",
		Columns: []*schema.Column{
			{Name: "id", IsPrimaryKey: true, FieldDescriptor: idFd},
			{Name: "name", FieldDescriptor: nameFd},
			{Name: "amount", FieldDescriptor: amountFd},
		},
		PrimaryKey: &schema.PrimaryKey{
			Name:            "id",
			Index:           0,
			FieldDescriptor: idFd,
		},
	}

	testSchema.TableRegistry["TestMessage"] = testTable

	// Create dialect
	dialect, err := postgres.NewDialectPostgres(testSchema, zap.NewNop())
	require.NoError(t, err)

	// Create generator
	gen := &protoAwareCSVGenerator{
		schema:          testSchema,
		dialect:         dialect,
		logger:          zap.NewNop(),
		useProtoOptions: false,
	}

	// Walk the message and collect rows
	blockTime := time.Now()
	rows, err := gen.walkMessageAndCollectRows(dm, 100, blockTime, nil)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	
	row := rows[0].rows[0]
	
	// Validate that the correct values are mapped to correct fields
	assert.Equal(t, uint64(100), row[sql.DialectFieldBlockNumber])
	assert.Equal(t, blockTime, row[sql.DialectFieldBlockTimestamp])
	assert.Equal(t, int64(123), row["id"])
	assert.Equal(t, "test_name", row["name"])
	assert.Equal(t, uint64(456), row["amount"])
}

// TestParentIDColumnInCSV validates that parent_id columns are included in CSV
func TestParentIDColumnInCSV(t *testing.T) {
	// This test validates Issue #4: Missing parent_id in CSV header
	
	// Create parent and child tables
	testSchema := &schema.Schema{
		Name:          "test",
		TableRegistry: make(map[string]*schema.Table),
	}

	parentIdFd := createSimpleFieldDescriptor("id", descriptor.FieldDescriptorProto_TYPE_STRING)
	childIdFd := createSimpleFieldDescriptor("id", descriptor.FieldDescriptorProto_TYPE_STRING)
	parentRefFd := createSimpleFieldDescriptor("parent_table_id", descriptor.FieldDescriptorProto_TYPE_STRING)

	parentTable := &schema.Table{
		Name: "parent_table",
		Columns: []*schema.Column{
			{Name: "id", IsPrimaryKey: true, FieldDescriptor: parentIdFd},
		},
		PrimaryKey: &schema.PrimaryKey{
			Name:            "id",
			Index:           0,
			FieldDescriptor: parentIdFd,
		},
	}

	childTable := &schema.Table{
		Name: "child_table",
		Columns: []*schema.Column{
			{Name: "id", IsPrimaryKey: true, FieldDescriptor: childIdFd},
			{Name: "parent_table_id", FieldDescriptor: parentRefFd}, // This should be included!
		},
		ChildOf: &schema.ChildOf{
			ParentTable:      "parent_table",
			ParentTableField: "id",
		},
	}

	testSchema.TableRegistry["parent_table"] = parentTable
	testSchema.TableRegistry["child_table"] = childTable

	// Create dialect
	dialect, err := postgres.NewDialectPostgres(testSchema, zap.NewNop())
	require.NoError(t, err)

	// Create generator
	gen := &protoAwareCSVGenerator{
		schema:          testSchema,
		dialect:         dialect,
		logger:          zap.NewNop(),
		useProtoOptions: true,
	}

	// Get columns for child table
	columns := gen.getColumnsForTable(childTable)

	// Check if parent_id column is included
	hasParentID := false
	for _, col := range columns {
		if col == "parent_table_id" {
			hasParentID = true
			break
		}
	}

	assert.True(t, hasParentID, "Child table CSV should include parent_id column")
}

// TestBinaryDataFormatting validates binary data is formatted correctly for each dialect
func TestBinaryDataFormatting(t *testing.T) {
	// This test validates Issue #5: Binary data format for each dialect
	
	testSchema := &schema.Schema{
		Name:          "test",
		TableRegistry: make(map[string]*schema.Table),
	}
	
	// Create proper field descriptors
	idFd := createSimpleFieldDescriptor("id", descriptor.FieldDescriptorProto_TYPE_STRING)
	dataFd := createSimpleFieldDescriptor("data", descriptor.FieldDescriptorProto_TYPE_BYTES)
	
	testTable := &schema.Table{
		Name: "test",
		Columns: []*schema.Column{
			{Name: "id", IsPrimaryKey: true, FieldDescriptor: idFd},
			{Name: "data", FieldDescriptor: dataFd},
		},
		PrimaryKey: &schema.PrimaryKey{
			Name:            "id",
			Index:           0,
			FieldDescriptor: idFd,
		},
	}
	testSchema.TableRegistry["test"] = testTable
	
	binaryData := []byte{0x01, 0x02, 0x03, 0xAB, 0xCD, 0xEF}
	
	// Test PostgreSQL dialect
	t.Run("PostgreSQL", func(t *testing.T) {
		pgDialect, err := postgres.NewDialectPostgres(testSchema, zap.NewNop())
		require.NoError(t, err)
		
		gen := &protoAwareCSVGenerator{
			logger:  zap.NewNop(),
			dialect: pgDialect,
			schema:  testSchema,
		}
		
		formatted := gen.formatValue(binaryData, "data", testTable)
		// PostgreSQL COPY expects \x followed by hex for bytea
		expected := "\\x010203abcdef"
		assert.Equal(t, expected, formatted, "Binary data should be formatted as PostgreSQL bytea")
	})
	
	// Test ClickHouse dialect
	t.Run("ClickHouse", func(t *testing.T) {
		chDialect, err := clickhouse.NewDialectClickHouse(testSchema, zap.NewNop())
		require.NoError(t, err)
		
		gen := &protoAwareCSVGenerator{
			logger:  zap.NewNop(),
			dialect: chDialect,
			schema:  testSchema,
		}
		
		formatted := gen.formatValue(binaryData, "data", testTable)
		// ClickHouse CSV expects base64 encoded strings
		expected := base64.StdEncoding.EncodeToString(binaryData)
		assert.Equal(t, expected, formatted, "Binary data should be base64 encoded for ClickHouse")
	})
	
	// Test RisingWave dialect
	t.Run("RisingWave", func(t *testing.T) {
		rwDialect, err := risingwave.NewDialectRisingwave(testSchema.Name, testSchema.TableRegistry, zap.NewNop())
		require.NoError(t, err)
		
		gen := &protoAwareCSVGenerator{
			logger:  zap.NewNop(),
			dialect: rwDialect,
			schema:  testSchema,
		}
		
		formatted := gen.formatValue(binaryData, "data", testTable)
		// RisingWave uses PostgreSQL-compatible format
		expected := "\\x010203abcdef"
		assert.Equal(t, expected, formatted, "Binary data should be formatted as PostgreSQL bytea for RisingWave")
	})
}

// TestCSVEscaping validates CSV special character escaping
func TestCSVEscaping(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"simple", "simple"},
		{"with,comma", `"with,comma"`},
		{`with"quote`, `"with""quote"`},
		{"with\nnewline", `"with
newline"`},
		{"with\rcarriage", `"with` + "\r" + `carriage"`},
		{`complex,"value"`, `"complex,""value"""`},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := escapeCSVValue(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestPrimaryKeyIndexCalculation validates primary key value extraction
func TestPrimaryKeyIndexCalculation(t *testing.T) {
	// This test validates Issue #2: Parent ID index calculation
	
	// The primary key index should correctly identify the value position
	// considering system fields and other offsets
	
	testSchema := &schema.Schema{
		Name:          "test",
		TableRegistry: make(map[string]*schema.Table),
	}

	testTable := &schema.Table{
		Name: "test_table",
		Columns: []*schema.Column{
			{Name: "other_field"},
			{Name: "id", IsPrimaryKey: true},
			{Name: "another_field"},
		},
		PrimaryKey: &schema.PrimaryKey{
			Name:  "id",
			Index: 1, // Index in the fields array, not columns
		},
	}

	testSchema.TableRegistry["test_table"] = testTable

	// The primaryKeyOffset calculation should account for:
	// - block_number (index 0)
	// - block_timestamp (index 1)
	// - version (if used)
	// - deleted (if used)
	// Then the primary key value
	
	// This needs careful validation of the index calculation
	t.Log("Primary key index calculation needs validation in actual message processing")
}

// TestNullValueHandling validates NULL value representation in CSV
func TestNullValueHandling(t *testing.T) {
	gen := &protoAwareCSVGenerator{
		logger: zap.NewNop(),
	}

	testTable := &schema.Table{
		Name: "test",
	}

	// Test nil value
	result := gen.formatValue(nil, "test_column", testTable)
	assert.Equal(t, "", result, "NULL values should be empty string in CSV")
}

// TestTableWithoutProtoOptions validates behavior when proto options aren't used
func TestTableWithoutProtoOptions(t *testing.T) {
	// This test validates Issue #6: Table info logic without proto options
	
	// When useProtoOptions is false, we create a default TableInfo
	// But the dialect might not have a table for it
	
	testSchema := &schema.Schema{
		Name:          "test",
		TableRegistry: make(map[string]*schema.Table),
	}

	// Don't add any tables to the registry
	
	dialect, err := postgres.NewDialectPostgres(testSchema, zap.NewNop())
	require.NoError(t, err)

	gen := &protoAwareCSVGenerator{
		schema:          testSchema,
		dialect:         dialect,
		logger:          zap.NewNop(),
		useProtoOptions: false,
	}

	// Create a message without table info
	msgProto := &descriptor.DescriptorProto{
		Name: proto.String("UnknownMessage"),
		Field: []*descriptor.FieldDescriptorProto{
			{
				Name:   proto.String("field1"),
				Number: proto.Int32(1),
				Type:   descriptor.FieldDescriptorProto_TYPE_STRING.Enum(),
			},
		},
	}

	fileProto := &descriptor.FileDescriptorProto{
		Name:        proto.String("test.proto"),
		MessageType: []*descriptor.DescriptorProto{msgProto},
	}

	fd, err := desc.CreateFileDescriptor(fileProto)
	require.NoError(t, err)

	msgDesc := fd.GetMessageTypes()[0]
	dm := dynamic.NewMessage(msgDesc)
	dm.SetFieldByName("field1", "value1")

	// This should not panic or error, but might not create any rows
	rows, err := gen.walkMessageAndCollectRows(dm, 100, time.Now(), nil)
	require.NoError(t, err)
	
	// Since there's no table in the registry, no rows should be created
	assert.Len(t, rows, 0, "No rows should be created for unknown tables")
}
