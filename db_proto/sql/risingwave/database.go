package risingwave

import (
	"context"
	pqsql "database/sql"
	"fmt"
	"hash/fnv"
	"time"

	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/dynamic"
	sink "github.com/streamingfast/substreams-sink"
	"github.com/streamingfast/substreams-sink-sql/db_changes/db"
	"github.com/streamingfast/substreams-sink-sql/db_proto/sql"
	"github.com/streamingfast/substreams-sink-sql/db_proto/sql/schema"
	"go.uber.org/zap"
)

// Database represents a RisingWave database connection.
// Important: RisingWave does not support read-write transactions and operates in autocommit mode.
// This means:
// - All data modifications are immediately committed
// - No rollback capability for individual operations
// - ACID transaction semantics are not available
// - Suitable for streaming/append-only workloads
type Database struct {
	*sql.BaseDatabase
	db             *pqsql.DB
	tx             *pqsql.Tx // Always nil for RisingWave due to autocommit mode
	schema         *schema.Schema
	logger         *zap.Logger
	dialect        *DialectRisingwave
	inserter       pgInserter
	flusher        pgFlusher
	useConstraints bool
}

func NewDatabase(schema *schema.Schema, dsn *db.DSN, moduleOutputType string, rootMessageDescriptor *desc.MessageDescriptor, useProtoOptions bool, useConstraints bool, logger *zap.Logger) (*Database, error) {
	logger = logger.Named("risingwave")

	connectionString := dsn.ConnString()
	logger.Info("connecting to db", zap.String("dsn", connectionString))
	logger.Info("RisingWave operates in autocommit mode - no transaction semantics available")
	sqlDB, err := pqsql.Open(dsn.SqlDriver(), connectionString)
	if err != nil {
		return nil, fmt.Errorf("open db connection: %w", err)
	}

	if reachable, err := isDatabaseReachable(sqlDB); !reachable {
		return nil, fmt.Errorf("database not reachable: %w", err)
	}

	dialect, err := NewDialectRisingwave(schema.Name, schema.TableRegistry, logger)
	if err != nil {
		return nil, fmt.Errorf("creating risingwave dialect: %w", err)
	}

	baseDB, err := sql.NewBaseDatabase(moduleOutputType, rootMessageDescriptor, useProtoOptions, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create base database: %w", err)
	}
	database := &Database{
		db:             sqlDB,
		schema:         schema,
		useConstraints: useConstraints,
		BaseDatabase:   baseDB,
		dialect:        dialect,
		logger:         logger,
	}

	return database, nil
}

func (d *Database) Open() error {
	if d.useConstraints {
		inserter, err := NewRowInserter(d.logger)
		if err != nil {
			return fmt.Errorf("creating row inserter: %w", err)
		}
		if err := inserter.init(d); err != nil {
			return fmt.Errorf("initializing row inserter: %w", err)
		}
		d.inserter = inserter
		d.flusher = inserter
	} else {
		inserter, err := NewAccumulatorInserter(d.logger)
		if err != nil {
			return fmt.Errorf("creating accumulator inserter: %w", err)
		}
		if err := inserter.init(d); err != nil {
			return fmt.Errorf("initializing accumulator inserter: %w", err)
		}
		d.inserter = inserter
		d.flusher = inserter
	}
	return nil
}

func (d *Database) GetDialect() sql.Dialect {
	return d.dialect
}

func (d *Database) CreateDatabase(useConstraints bool) error {
	err := d.createDatabase()
	if err != nil {
		return fmt.Errorf("creating database: %w", err)
	}

	if useConstraints {
		err = d.applyConstraints()
		if err != nil {
			return fmt.Errorf("applying constraints: %w", err)
		}
	}

	return nil
}

func (d *Database) createDatabase() error {
	staticSql := fmt.Sprintf(risingwaveStaticSql, d.schema.Name, d.schema.Name, d.schema.Name, d.schema.Name)
	_, err := d.execSql(staticSql)
	if err != nil {
		return fmt.Errorf("executing static staticSql: %w\n%s", err, staticSql)
	}

	for _, statement := range d.dialect.CreateTableSql {
		d.logger.Info("executing create statement", zap.String("sql", statement))
		_, err := d.execSql(statement)
		if err != nil {
			return fmt.Errorf("executing create statement: %w %s", err, statement)
		}
	}
	return nil
}

func (d *Database) applyConstraints() error {
	startAt := time.Now()
	for _, constraint := range d.dialect.PrimaryKeySql {
		d.logger.Info("executing pk statement", zap.String("sql", constraint.Sql))
		_, err := d.execSql(constraint.Sql)
		if err != nil {
			return fmt.Errorf("executing pk statement: %w %s", err, constraint.Sql)
		}
	}
	for _, constraint := range d.dialect.UniqueConstraintSql {
		d.logger.Info("executing unique statement", zap.String("sql", constraint.Sql))
		_, err := d.execSql(constraint.Sql)
		if err != nil {
			return fmt.Errorf("executing unique statement: %w %s", err, constraint.Sql)
		}
	}
	for _, constraint := range d.dialect.ForeignKeySql {
		d.logger.Info("executing fk constraint statement", zap.String("sql", constraint.Sql))
		_, err := d.execSql(constraint.Sql)
		if err != nil {
			return fmt.Errorf("executing fk constraint statement: %w %s", err, constraint.Sql)
		}
	}
	d.logger.Info("applying constraints", zap.Duration("duration", time.Since(startAt)))
	return nil
}

func (d *Database) BeginTransaction() (err error) {
	// RisingWave does not support read-write transactions. According to RisingWave docs:
	// "The BEGIN command starts the read-write transaction mode, which is not supported yet in RisingWave.
	// For compatibility reasons, this command will still succeed but no transaction is actually started."
	// 
	// Since no actual transaction is started, we operate in autocommit mode and set tx to nil
	// to ensure all subsequent operations use the database connection directly.
	d.logger.Debug("RisingWave: skipping transaction begin, using autocommit mode")
	d.tx = nil
	return nil
}

func (d *Database) CommitTransaction() (err error) {
	// RisingWave operates in autocommit mode since read-write transactions are not supported.
	// All changes are automatically committed when executed.
	d.logger.Debug("RisingWave: commit is no-op in autocommit mode")
	
	// Defensive check: if somehow a transaction was started (shouldn't happen), commit it
	if d.tx != nil {
		d.logger.Warn("RisingWave: unexpected transaction found during commit, attempting to commit")
		err = d.tx.Commit()
		if err != nil {
			return fmt.Errorf("committing unexpected transaction: %w", err)
		}
		d.tx = nil
	}
	return nil
}

func (d *Database) RollbackTransaction() {
	// RisingWave operates in autocommit mode and does not support traditional rollback.
	// In streaming databases, data modifications are typically append-only.
	// ROLLBACK documentation was not found for RisingWave, suggesting it may not be supported.
	d.logger.Debug("RisingWave: rollback is no-op in autocommit mode")
	
	// Defensive check: if somehow a transaction was started (shouldn't happen), attempt rollback
	if d.tx != nil {
		d.logger.Warn("RisingWave: unexpected transaction found during rollback, attempting to rollback")
		err := d.tx.Rollback()
		if err != nil {
			// Log error but don't panic since RisingWave may not support rollback
			d.logger.Error("RisingWave: rollback failed on unexpected transaction", zap.Error(err))
		}
		d.tx = nil
	}
}

func (d *Database) wrapInsertStatement(stmt *pqsql.Stmt) *pqsql.Stmt {
	// RisingWave operates in autocommit mode, always use the original statement
	// since d.tx should always be nil for RisingWave
	if d.tx != nil {
		// This should not happen for RisingWave, but handle defensively
		d.logger.Warn("RisingWave: unexpected transaction found when wrapping statement")
		stmt = d.tx.Stmt(stmt)
	}
	return stmt
}

// execSql executes SQL using the database connection in autocommit mode
// RisingWave operates in autocommit mode, so we always use the direct database connection
func (d *Database) execSql(query string, args ...any) (pqsql.Result, error) {
	if d.tx != nil {
		// This should not happen for RisingWave since we never create transactions
		d.logger.Warn("RisingWave: unexpected transaction found during SQL execution, using transaction")
		return d.tx.Exec(query, args...)
	}
	return d.db.Exec(query, args...)
}

func (d *Database) Insert(table string, values []any) error {
	return d.inserter.insert(table, values, d)
}

func (d *Database) WalkMessageDescriptorAndInsert(dm *dynamic.Message, blockNum uint64, blockTimestamp time.Time, parent *sql.Parent) (time.Duration, error) {
	return d.BaseDatabase.WalkMessageDescriptorAndInsertWithDialect(dm, blockNum, blockTimestamp, parent, d.dialect, d)
}

func (d *Database) InsertBlock(blockNum uint64, hash string, timestamp time.Time) error {
	d.logger.Debug("inserting _blocks_", zap.Uint64("block_num", blockNum), zap.String("block_hash", hash))
	err := d.inserter.insert("_blocks_", []any{blockNum, hash, timestamp}, d)
	if err != nil {
		return fmt.Errorf("inserting block %d: %w", blockNum, err)
	}

	return nil
}

func (d *Database) Flush() (time.Duration, error) {
	startFlush := time.Now()
	err := d.flusher.flush(d)
	if err != nil {
		return 0, fmt.Errorf("flushing: %w", err)
	}
	return time.Since(startFlush), nil
}

func (d *Database) FetchSinkInfo(schemaName string) (*sql.SinkInfo, error) {
	query := fmt.Sprintf("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = '%s' AND table_name = '_sink_info_')", schemaName)

	var exist bool
	err := d.db.QueryRow(query).Scan(&exist)
	if err != nil {
		return nil, fmt.Errorf("checking if sync_info table exists: %w", err)
	}
	if !exist {
		return nil, nil
	}

	out := &sql.SinkInfo{}

	err = d.db.QueryRow(fmt.Sprintf("SELECT schema_hash FROM %s._sink_info_", d.schema.Name)).Scan(&out.SchemaHash)
	if err != nil {
		return nil, fmt.Errorf("fetching sync info: %w", err)
	}
	return out, nil

}

func (d *Database) StoreSinkInfo(schemaName string, schemaHash string) error {
	_, err := d.execSql(fmt.Sprintf("INSERT INTO %s._sink_info_ (schema_hash) VALUES ($1)", schemaName), schemaHash)
	if err != nil {
		return fmt.Errorf("storing schema hash: %w", err)
	}
	return nil
}

func (d *Database) UpdateSinkInfoHash(schemaName string, newHash string) error {
	_, err := d.execSql(fmt.Sprintf("UPDATE %s._sink_info_ SET schema_hash = $1", schemaName), newHash)
	if err != nil {
		return fmt.Errorf("updating schema hash: %w", err)
	}
	return nil
}

func (d *Database) FetchCursor() (*sink.Cursor, error) {
	query := fmt.Sprintf("SELECT cursor FROM %s WHERE name = $1", tableName(d.schema.Name, "_cursor_"))

	rows, err := d.db.Query(query, "cursor")
	if err != nil {
		return nil, fmt.Errorf("selecting cursor: %w", err)
	}
	defer rows.Close()

	if rows.Next() {
		var cursor string
		err = rows.Scan(&cursor)

		return sink.NewCursor(cursor)
	}
	return nil, nil
}

func (d *Database) StoreCursor(cursor *sink.Cursor) error {
	err := d.inserter.insert("_cursor_", []any{"cursor", cursor.String()}, d)
	if err != nil {
		return fmt.Errorf("inserting cursor: %w", err)
	}

	return err
}

func (d *Database) HandleBlocksUndo(lastValidBlockNum uint64) (err error) {
	// RisingWave operates in autocommit mode - execute operations directly without transactions
	d.logger.Info("undoing blocks", zap.Uint64("last_valid_block_num", lastValidBlockNum))
	
	query := fmt.Sprintf(`DELETE FROM %s._blocks_ WHERE "number" > $1`, d.schema.Name)
	result, err := d.execSql(query, lastValidBlockNum)
	if err != nil {
		return fmt.Errorf("deleting block from %d: %w", lastValidBlockNum, err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("fetching rows affected: %w", err)
	}
	d.logger.Info("undo completed", zap.Int64("row_affected", rowsAffected))

	return nil
}

func (d *Database) Clone() sql.Database {
	base := d.BaseClone()
	d.BaseDatabase = base
	return d
}

func (d *Database) DatabaseHash(schemaName string) (uint64, error) {
	query := `
SELECT
    c.table_name,
    c.column_name,
    c.is_nullable,
    c.data_type,
    c.character_maximum_length,
    c.numeric_precision,
    c.numeric_precision_radix,
    c.numeric_scale,
    c.datetime_precision,
    c.interval_precision,
    c.is_generated,
    c.is_updatable,
    tc.constraint_name,
    tc.table_name,
    tc.constraint_type,
    kcu.column_name,
    kcu.table_name,
    kcu.column_name,
    ccu.constraint_name,
    ccu.table_name,
    ccu.column_name
FROM
    information_schema.columns c
        LEFT JOIN
    information_schema.constraint_column_usage ccu
    ON c.table_name = ccu.table_name
        AND c.column_name = ccu.column_name
        AND c.table_schema = ccu.table_schema
        LEFT JOIN
    information_schema.key_column_usage kcu
    ON ccu.constraint_name = kcu.constraint_name
        AND c.table_schema = kcu.table_schema
        LEFT JOIN
    information_schema.table_constraints tc
    ON kcu.constraint_name = tc.constraint_name
        AND kcu.table_schema = tc.table_schema
WHERE
    c.table_schema = '%s'
ORDER BY
    c.table_name,
    c.column_name,
    tc.table_name,
    tc.constraint_name,
    kcu.table_name,
    kcu.column_name,
    kcu.constraint_name;
`

	query = fmt.Sprintf(query, schemaName)

	rows, err := d.db.Query(query)
	if err != nil {
		return 0, fmt.Errorf("executing query to compute schema hash: %w", err)
	}
	defer rows.Close()

	h := fnv.New64a()
	columns, err := rows.Columns()
	if err != nil {
		return 0, fmt.Errorf("fetching columns for hashing: %w", err)
	}

	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	for rows.Next() {
		err = rows.Scan(valuePtrs...)
		if err != nil {
			return 0, fmt.Errorf("scanning row for hashing: %w", err)
		}

		for _, val := range values {
			var str string
			if val != nil {
				str = fmt.Sprintf("%v", val)
			}
			_, err = h.Write([]byte(str))
			if err != nil {
				return 0, fmt.Errorf("hashing value %q: %w", str, err)
			}
		}
	}

	if err = rows.Err(); err != nil {
		return 0, fmt.Errorf("iterating rows: %w", err)
	}

	return h.Sum64(), nil
}

func isDatabaseReachable(db *pqsql.DB) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	err := db.PingContext(ctx)
	if err != nil {
		return false, err
	}
	return true, nil
}