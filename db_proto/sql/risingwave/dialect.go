package risingwave

import (
	"database/sql"
	"encoding/hex"
	"fmt"
	"hash/fnv"
	"sort"
	"strings"

	sql2 "github.com/streamingfast/substreams-sink-sql/db_proto/sql"
	"github.com/streamingfast/substreams-sink-sql/db_proto/sql/schema"
	"go.uber.org/zap"
)

const risingwaveStaticSql = `
	CREATE SCHEMA IF NOT EXISTS "%s";

	CREATE TABLE IF NOT EXISTS "%s"._sink_info_ (
		schema_hash VARCHAR PRIMARY KEY
	);

	CREATE TABLE IF NOT EXISTS "%s"._cursor_ (
		name VARCHAR PRIMARY KEY,
		cursor VARCHAR NOT NULL
	) ON CONFLICT OVERWRITE;

	CREATE TABLE IF NOT EXISTS "%s"._blocks_ (
		number INTEGER PRIMARY KEY,
		hash VARCHAR NOT NULL,
		timestamp TIMESTAMP WITH TIME ZONE NOT NULL
	);
`

type DialectRisingwave struct {
	*sql2.BaseDialect
	schemaName string
}

func NewDialectRisingwave(schemaName string, tableRegistry map[string]*schema.Table, logger *zap.Logger) (*DialectRisingwave, error) {
	d := &DialectRisingwave{
		BaseDialect: sql2.NewBaseDialect(tableRegistry, logger),
		schemaName:  schemaName,
	}

	err := d.init()
	if err != nil {
		return nil, fmt.Errorf("initializing dialect: %w", err)
	}

	for _, table := range tableRegistry {
		err := d.createTable(table)
		if err != nil {
			return nil, fmt.Errorf("handling table %q: %w", table.Name, err)
		}
	}

	return d, nil
}

func (d *DialectRisingwave) UseVersionField() bool {
	return false
}

func (d *DialectRisingwave) UseDeletedField() bool {
	return false
}

func (d *DialectRisingwave) init() error {
	return nil
}

func (d *DialectRisingwave) createTable(table *schema.Table) error {
	var sb strings.Builder
	addedColumns := make(map[string]struct{})

	tableName := d.FullTableName(table)

	sb.WriteString(fmt.Sprintf("CREATE TABLE  IF NOT EXISTS %s (", tableName))

	// Add primary key if it exists
	var primaryKeyFieldName string
	if table.PrimaryKey != nil {
		pk := table.PrimaryKey
		primaryKeyFieldName = pk.Name
		sb.WriteString(fmt.Sprintf("%s %s PRIMARY KEY,", pk.Name, MapFieldType(pk.FieldDescriptor)))
		addedColumns[pk.Name] = struct{}{}
	}

	// Always add block metadata columns
	sb.WriteString(" block_number INTEGER NOT NULL,")
	sb.WriteString(" block_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,")
	addedColumns["block_number"] = struct{}{}
	addedColumns["block_timestamp"] = struct{}{}

	// Add parent key for child tables
	if table.ChildOf != nil {
		parentTable, parentFound := d.TableRegistry[table.ChildOf.ParentTable]
		if !parentFound {
			return fmt.Errorf("parent table %q not found", table.ChildOf.ParentTable)
		}
		fieldFound := false
		for _, parentField := range parentTable.Columns {
			if parentField.Name == table.ChildOf.ParentTableField {
				if _, exists := addedColumns[parentField.Name]; !exists {
					sb.WriteString(fmt.Sprintf("%s %s NOT NULL,", parentField.Name, MapFieldType(parentField.FieldDescriptor)))
					addedColumns[parentField.Name] = struct{}{}
				}
				fieldFound = true
				break
			}
		}
		if !fieldFound {
			return fmt.Errorf("field %q not found in table %q", table.ChildOf.ParentTableField, table.ChildOf.ParentTable)
		}
	}

	// Add all regular columns from the protobuf message
	for _, f := range table.Columns {
		// Skip if already added
		if _, exists := addedColumns[f.Name]; exists {
			continue
		}

		// Skip primary key (already handled above)
		if f.Name == primaryKeyFieldName {
			continue
		}

		fieldQuotedName := f.QuotedName()

		// Skip repeated fields (not supported in SQL)
		if f.IsRepeated {
			continue
		}

		// Skip message fields that don't map to simple columns
		if f.IsMessage && !IsWellKnownType(f.FieldDescriptor) {
			continue
		}

		// Handle foreign key fields (but don't add constraints since RisingWave doesn't support them)
		if f.ForeignKey != nil {
			foreignTable, found := d.TableRegistry[f.ForeignKey.Table]
			if !found {
				return fmt.Errorf("foreign table %q not found", f.ForeignKey.Table)
			}

			var foreignField *schema.Column
			for _, field := range foreignTable.Columns {
				if field.Name == f.ForeignKey.TableField {
					foreignField = field
					break
				}
			}
			if foreignField == nil {
				return fmt.Errorf("foreign field %q not found in table %q", f.ForeignKey.TableField, f.ForeignKey.Table)
			}
		}

		// Determine field type
		fieldType := MapFieldType(f.FieldDescriptor)
		if f.IsUnique {
			fieldType = fieldType + " UNIQUE"
		}

		// Add the column
		sb.WriteString(fmt.Sprintf("%s %s", fieldQuotedName, fieldType))
		sb.WriteString(",")
		addedColumns[f.Name] = struct{}{}
	}

	// Remove the last comma
	temp := sb.String()
	temp = temp[:len(temp)-1]
	sb = strings.Builder{}
	sb.WriteString(temp)

	sb.WriteString("\n);\n")

	d.AddCreateTableSql(table.Name, sb.String())

	return nil
}

func (d *DialectRisingwave) CreateDatabase(tx *sql.Tx) error {
	staticSql := fmt.Sprintf(risingwaveStaticSql, d.schemaName, d.schemaName, d.schemaName, d.schemaName)
	_, err := tx.Exec(staticSql)
	if err != nil {
		return fmt.Errorf("executing static staticSql: %w\n%s", err, staticSql)
	}

	for _, statement := range d.CreateTableSql {
		d.Logger.Info("executing create statement", zap.String("sql", statement))
		_, err := tx.Exec(statement)
		if err != nil {
			return fmt.Errorf("executing create statement: %w %s", err, statement)
		}
	}
	return nil
}

func (d *DialectRisingwave) FullTableName(table *schema.Table) string {
	return tableName(d.schemaName, table.Name)
}

// todo: move to postgress database
func (d *DialectRisingwave) SchemaHash() string {
	h := fnv.New64a()

	var buf []byte

	// SchemaHash tableCreateStatements
	var sqls []string
	for _, sql := range d.CreateTableSql {
		sqls = append(sqls, sql)
		//buf = append(buf, []byte(sql)...)
	}

	sort.Strings(sqls)
	for _, sql := range sqls {
		buf = append(buf, []byte(sql)...)
	}

	var pk []string
	for _, constraint := range d.PrimaryKeySql {
		pk = append(pk, constraint.Sql)
	}
	sort.Strings(pk)
	for _, constraint := range pk {
		buf = append(buf, []byte(constraint)...)
	}

	var fk []string
	for _, constraint := range d.ForeignKeySql {
		fk = append(fk, constraint.Sql)
	}
	sort.Strings(fk)
	for _, constraint := range fk {
		buf = append(buf, []byte(constraint)...)
	}

	var uniques []string
	for _, constraint := range d.UniqueConstraintSql {
		uniques = append(uniques, constraint.Sql)
	}
	sort.Strings(uniques)
	for _, constraint := range uniques {
		buf = append(buf, []byte(constraint)...)
	}

	//todo: hum... is this useful?
	//var accumulators []string
	//for _, sql := range d.InsertSql {
	//	accumulators = append(accumulators, sql)
	//}
	//sort.Strings(accumulators)
	//for _, sql := range accumulators {
	//	buf = append(buf, []byte(sql)...)
	//}

	_, err := h.Write(buf)
	if err != nil {
		panic("unable to write to hash")
	}

	data := h.Sum(nil)
	return hex.EncodeToString(data)
}

func tableName(schemaName string, tableName string) string {
	return fmt.Sprintf("%s.%s", schemaName, tableName)
}
