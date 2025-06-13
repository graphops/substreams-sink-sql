package risingwave

import (
	"database/sql"
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
	"time"

	sql2 "github.com/streamingfast/substreams-sink-sql/db_proto/sql"
	"github.com/streamingfast/substreams-sink-sql/db_proto/sql/schema"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type RowInserter struct {
	insertQueries    map[string]string
	insertStatements map[string]*sql.Stmt
	logger           *zap.Logger
}

func NewRowInserter(logger *zap.Logger) (*RowInserter, error) {
	logger = logger.Named("risingwave inserter")

	return &RowInserter{
		logger: logger,
	}, nil
}

func (i *RowInserter) init(database *Database) error {
	tables := database.dialect.GetTables()
	insertStatements := map[string]*sql.Stmt{}
	insertQueries := map[string]string{}

	for _, table := range tables {
		query, err := createInsertFromDescriptor(table, database.dialect)
		if err != nil {
			return fmt.Errorf("creating insert from descriptor for table %q: %w", table.Name, err)
		}
		insertQueries[table.Name] = query

		stmt, err := database.db.Prepare(query)
		if err != nil {
			return fmt.Errorf("preparing statement %q: %w", query, err)
		}
		insertStatements[table.Name] = stmt
	}

	// RisingWave supports RETURNING clause as of release #7094
	insertQueries["_blocks_"] = fmt.Sprintf("INSERT INTO %s (number, hash, timestamp) VALUES ($1, $2, $3) RETURNING number", tableName(database.schema.Name, "_blocks_"))
	bs, err := database.db.Prepare(insertQueries["_blocks_"])
	if err != nil {
		return fmt.Errorf("preparing statement %q: %w", insertQueries["_blocks_"], err)
	}
	insertStatements["_blocks_"] = bs

	// RisingWave doesn't support PostgreSQL's ON CONFLICT syntax in INSERT statements
	// The _cursor_ table is created with ON CONFLICT OVERWRITE to handle this automatically
	insertQueries["_cursor_"] = fmt.Sprintf("INSERT INTO %s (name, cursor) VALUES ($1, $2)", tableName(database.schema.Name, "_cursor_"))
	cs, err := database.db.Prepare(insertQueries["_cursor_"])
	if err != nil {
		return fmt.Errorf("preparing statement %q: %w", insertQueries["_cursor_"], err)
	}
	insertStatements["_cursor_"] = cs

	i.insertStatements = insertStatements
	i.insertQueries = insertQueries

	return nil
}

func createInsertFromDescriptor(table *schema.Table, dialect sql2.Dialect) (string, error) {
	tableName := dialect.FullTableName(table)
	fields := table.Columns

	var fieldNames []string
	var placeholders []string

	fieldCount := 0
	returningField := ""
	if table.PrimaryKey != nil {
		returningField = table.PrimaryKey.Name
	}

	// Add block_number
	fieldCount++
	fieldNames = append(fieldNames, "block_number")
	placeholders = append(placeholders, fmt.Sprintf("$%d", fieldCount))
	
	// Add block_timestamp
	fieldCount++
	fieldNames = append(fieldNames, "block_timestamp")
	placeholders = append(placeholders, fmt.Sprintf("$%d", fieldCount))

	if pk := table.PrimaryKey; pk != nil {
		fieldCount++
		returningField = pk.Name
		fieldNames = append(fieldNames, pk.Name)
		placeholders = append(placeholders, fmt.Sprintf("$%d", fieldCount))
	}

	if table.ChildOf != nil {
		fieldCount++
		fieldNames = append(fieldNames, table.ChildOf.ParentTableField)
		placeholders = append(placeholders, fmt.Sprintf("$%d", fieldCount))
	}

	for _, field := range fields {
		if field.Name == returningField {
			continue
		}
		if field.IsRepeated || field.IsExtension {
			continue
		}
		fieldCount++
		fieldNames = append(fieldNames, field.QuotedName())
		placeholders = append(placeholders, fmt.Sprintf("$%d", fieldCount))
	}

	return fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		tableName,
		strings.Join(fieldNames, ", "),
		strings.Join(placeholders, ", "),
	), nil
}

func (i *RowInserter) insert(table string, values []any, database *Database) error {
	i.logger.Debug("inserting row", zap.String("table", table), zap.Any("values", values))
	stmt := i.insertStatements[table]
	stmt = database.wrapInsertStatement(stmt)

	for i, value := range values {
		switch v := value.(type) {
		case uint64:
			values[i] = strconv.FormatUint(v, 10)
		case []uint8:
			values[i] = base64.StdEncoding.EncodeToString(v)
		case *timestamppb.Timestamp:
			values[i] = "'" + v.AsTime().Format(time.RFC3339) + "'"
		}
	}

	_, err := stmt.Exec(values...)
	if err != nil {
		insert := i.insertQueries[table]
		return fmt.Errorf("risingwave row inserter: querying insert %q: %w", insert, err)
	}

	return nil
}

func (i *RowInserter) flush(database *Database) error {
	return nil
}


