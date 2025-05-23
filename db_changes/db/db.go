package db

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"

	"github.com/jimsmart/schema"
	"github.com/streamingfast/logging"
	orderedmap "github.com/wk8/go-ordered-map/v2"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Make the typing a bit easier
type OrderedMap[K comparable, V any] struct {
	*orderedmap.OrderedMap[K, V]
}

func NewOrderedMap[K comparable, V any]() *OrderedMap[K, V] {
	return &OrderedMap[K, V]{OrderedMap: orderedmap.New[K, V]()}
}

type SystemTableError struct {
	error
}

type Loader struct {
	*sql.DB

	entries      *OrderedMap[string, *OrderedMap[string, *Operation]]
	entriesCount uint64
	tables       map[string]*TableInfo
	cursorTable  *TableInfo

	handleReorgs            bool
	batchBlockFlushInterval int
	batchRowFlushInterval   int
	liveBlockFlushInterval  int
	moduleMismatchMode      OnModuleHashMismatch

	dialect Dialect

	logger *zap.Logger
	tracer logging.Tracer

	testTx *TestTx // used for testing: if non-nil, 'loader.BeginTx()' will return this object instead of a real *sql.Tx
	dsn    *DSN
}

func NewLoader(
	dsn *DSN,
	cursorTableName string, historyTableName string, clickhouseCluster string,
	batchBlockFlushInterval int,
	batchRowFlushInterval int,
	liveBlockFlushInterval int,
	OnModuleHashMismatch string,
	handleReorgs *bool,
	logger *zap.Logger,
	tracer logging.Tracer,
) (*Loader, error) {

	sqlDB, err := sql.Open(dsn.SqlDriver(), dsn.ConnString())
	if err != nil {
		return nil, fmt.Errorf("open db connection: %w", err)
	}

	// RisingWave-specific connection optimizations to minimize transaction status bugs
	logger.Info("RisingWave DEBUG [DB LOADER] - Checking driver for optimizations",
		zap.String("dsn_driver", dsn.Driver()),
		zap.String("sql_driver", dsn.SqlDriver()))

	if dsn.Driver() == "risingwave" {
		logger.Info("RisingWave DEBUG [DB LOADER] - Applying RisingWave connection optimizations")
		// Use minimal connection pooling to reduce transaction state issues
		sqlDB.SetMaxOpenConns(2)    // Limit concurrent connections
		sqlDB.SetMaxIdleConns(1)    // Keep minimal idle connections
		sqlDB.SetConnMaxLifetime(0) // No connection lifetime limit
		sqlDB.SetConnMaxIdleTime(0) // No idle timeout
		logger.Info("RisingWave DEBUG [DB LOADER] - RisingWave connection optimizations applied")
	} else {
		logger.Info("RisingWave DEBUG [DB LOADER] - Not applying RisingWave optimizations",
			zap.String("driver", dsn.Driver()))
	}

	dialect, err := newDialect(dsn, sqlDB.Driver(), dsn.Schema(), cursorTableName, historyTableName, clickhouseCluster)
	if err != nil {
		return nil, fmt.Errorf("get dialect: %w", err)
	}

	moduleMismatchMode, err := ParseOnModuleHashMismatch(OnModuleHashMismatch)
	if err != nil {
		return nil, fmt.Errorf("parse on module hash mismatch: %w", err)
	}

	l := &Loader{
		DB:                      sqlDB,
		dsn:                     dsn,
		entries:                 NewOrderedMap[string, *OrderedMap[string, *Operation]](),
		tables:                  map[string]*TableInfo{},
		batchBlockFlushInterval: batchBlockFlushInterval,
		batchRowFlushInterval:   batchRowFlushInterval,
		liveBlockFlushInterval:  liveBlockFlushInterval,
		moduleMismatchMode:      moduleMismatchMode,
		dialect:                 dialect,
		logger:                  logger,
		tracer:                  tracer,
	}

	if handleReorgs == nil {
		// automatic detection
		l.handleReorgs = !l.dialect.OnlyInserts()
	} else {
		l.handleReorgs = *handleReorgs
	}

	if l.handleReorgs && l.dialect.OnlyInserts() {
		return nil, fmt.Errorf("driver %s does not support reorg handling. You must use set a non-zero undo-buffer-size", sqlDB.Driver())
	}

	logger.Info("created new DB loader",
		zap.Int("batch_block_flush_interval", batchBlockFlushInterval),
		zap.Int("batch_row_flush_interval", batchRowFlushInterval),
		zap.Int("live_block_flush_interval", liveBlockFlushInterval),
		zap.Stringer("on_module_hash_mismatch", moduleMismatchMode),
		zap.Bool("handle_reorgs", l.handleReorgs),
		zap.String("dialect", fmt.Sprintf("%T", l.dialect)),
	)

	return l, nil
}

func newDialect(dsn *DSN, driver driver.Driver, schemaName string, cursorTableName string, historyTableName string, clickHouseClusterName string) (Dialect, error) {
	// Use DSN driver name first, fallback to SQL driver type for unknown DSN drivers
	dsnDriver := dsn.Driver()
	driverType := fmt.Sprintf("%T", driver)

	switch dsnDriver {
	case "postgres":
		return NewPostgresDialect(schemaName, cursorTableName, historyTableName), nil
	case "risingwave":
		return NewRisingwaveDialect(schemaName, cursorTableName, historyTableName), nil
	case "clickhouse":
		return NewClickhouseDialect(schemaName, cursorTableName, clickHouseClusterName), nil
	default:
		// Fallback to driver type for unknown DSN drivers
		switch driverType {
		case "*pq.Driver":
			return NewPostgresDialect(schemaName, cursorTableName, historyTableName), nil
		case "*clickhouse.stdDriver":
			return NewClickhouseDialect(schemaName, cursorTableName, clickHouseClusterName), nil
		default:
			return nil, fmt.Errorf("unsupported driver: %s (dsn: %s)", driverType, dsnDriver)
		}
	}
}

type Tx interface {
	Rollback() error
	Commit() error
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

func (l *Loader) Begin() (Tx, error) {
	return l.BeginTx(context.Background(), nil)
}

func (l *Loader) BeginTx(ctx context.Context, opts *sql.TxOptions) (Tx, error) {
	if l.testTx != nil {
		return l.testTx, nil
	}

	// Add debugging for RisingWave connection issues
	l.logger.Info("RisingWave DEBUG [DB LOADER] - About to call DB.BeginTx()")

	// RisingWave-specific workaround: Use autocommit mode instead of transactions
	if l.dsn.Driver() == "risingwave" {
		l.logger.Info("RisingWave DEBUG [DB LOADER] - Using RisingWave autocommit workaround (no transactions)")
		// Return a fake transaction that just wraps the connection
		return &RisingWaveAutocommitTx{
			conn:   l.DB,
			logger: l.logger,
		}, nil
	}

	// Check connection state before beginning transaction
	if pingErr := l.DB.Ping(); pingErr != nil {
		l.logger.Error("RisingWave DEBUG [DB LOADER] - Connection ping failed before BeginTx", zap.Error(pingErr))
	} else {
		l.logger.Info("RisingWave DEBUG [DB LOADER] - Connection ping successful before BeginTx")
	}

	// Check connection stats
	stats := l.DB.Stats()
	l.logger.Info("RisingWave DEBUG [DB LOADER] - Connection pool stats",
		zap.Int("open_connections", stats.OpenConnections),
		zap.Int("in_use", stats.InUse),
		zap.Int("idle", stats.Idle),
		zap.Int64("wait_count", stats.WaitCount),
		zap.Duration("wait_duration", stats.WaitDuration),
		zap.Int64("max_idle_closed", stats.MaxIdleClosed),
		zap.Int64("max_idle_time_closed", stats.MaxIdleTimeClosed),
		zap.Int64("max_lifetime_closed", stats.MaxLifetimeClosed))

	// Add a small delay to see if timing is the issue
	l.logger.Info("RisingWave DEBUG [DB LOADER] - About to call BeginTx after ping")

	tx, err := l.DB.BeginTx(ctx, opts)
	if err != nil {
		l.logger.Error("RisingWave DEBUG [DB LOADER] - DB.BeginTx() failed", zap.Error(err))

		// Try ping again after failure to see connection state
		if pingErr := l.DB.Ping(); pingErr != nil {
			l.logger.Error("RisingWave DEBUG [DB LOADER] - Connection ping failed after BeginTx failure", zap.Error(pingErr))
		} else {
			l.logger.Info("RisingWave DEBUG [DB LOADER] - Connection ping still successful after BeginTx failure")
		}

		return nil, err
	}
	l.logger.Info("RisingWave DEBUG [DB LOADER] - DB.BeginTx() success")

	return tx, nil
}

// RisingWaveAutocommitTx is a fake transaction that uses autocommit mode for RisingWave
type RisingWaveAutocommitTx struct {
	conn   *sql.DB
	logger *zap.Logger
}

func (tx *RisingWaveAutocommitTx) Rollback() error {
	tx.logger.Info("RisingWave DEBUG [AUTOCOMMIT TX] - Rollback called (no-op in autocommit mode)")
	return nil
}

func (tx *RisingWaveAutocommitTx) Commit() error {
	tx.logger.Info("RisingWave DEBUG [AUTOCOMMIT TX] - Commit called (no-op in autocommit mode)")
	return nil
}

func (tx *RisingWaveAutocommitTx) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	tx.logger.Info("RisingWave DEBUG [AUTOCOMMIT TX] - Executing query", zap.String("query", query))
	return tx.conn.ExecContext(ctx, query, args...)
}

func (tx *RisingWaveAutocommitTx) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	tx.logger.Info("RisingWave DEBUG [AUTOCOMMIT TX] - Querying", zap.String("query", query))
	return tx.conn.QueryContext(ctx, query, args...)
}

func (l *Loader) BatchBlockFlushInterval() int {
	return l.batchBlockFlushInterval
}

func (l *Loader) LiveBlockFlushInterval() int {
	return l.liveBlockFlushInterval
}

func (l *Loader) FlushNeeded() bool {
	totalRows := 0
	// todo keep a running count when inserting/deleting rows directly
	for pair := l.entries.Oldest(); pair != nil; pair = pair.Next() {
		totalRows += pair.Value.Len()
	}
	return totalRows > l.batchRowFlushInterval
}

func (l *Loader) LoadTables(schemaName string, cursorTableName string, historyTableName string) error {
	schemaTables, err := schema.Tables(l.DB)
	if err != nil {
		return fmt.Errorf("retrieving table and schemaName: %w", err)
	}

	seenCursorTable := false
	seenHistoryTable := false
	for schemaTableName, columns := range schemaTables {
		tableName := schemaTableName[1]
		l.logger.Debug("processing schemaName's table",
			zap.String("schema_name", schemaName),
			zap.String("table_name", tableName),
		)

		if schemaTableName[0] != schemaName {
			continue
		}

		if tableName == cursorTableName {
			if err := l.validateCursorTables(columns, schemaName, cursorTableName); err != nil {
				return fmt.Errorf("invalid cursors table: %w", err)
			}

			seenCursorTable = true
		}
		if tableName == historyTableName {
			seenHistoryTable = true
		}

		columnByName := make(map[string]*ColumnInfo, len(columns))
		for _, f := range columns {
			columnByName[f.Name()] = &ColumnInfo{
				name:             f.Name(),
				escapedName:      EscapeIdentifier(f.Name()),
				databaseTypeName: f.DatabaseTypeName(),
				scanType:         f.ScanType(),
			}
		}

		key, err := schema.PrimaryKey(l.DB, schemaName, tableName)
		if err != nil {
			return fmt.Errorf("get primary key: %w", err)
		}

		l.tables[tableName], err = NewTableInfo(schemaName, tableName, key, columnByName)
		if err != nil {
			return fmt.Errorf("invalid table: %w", err)
		}
	}

	if !seenCursorTable {
		return &SystemTableError{fmt.Errorf(`%s.%s table is not found`, EscapeIdentifier(schemaName), cursorTableName)}
	}
	if l.handleReorgs && !seenHistoryTable {
		return &SystemTableError{fmt.Errorf("%s.%s table is not found and reorgs handling is enabled", EscapeIdentifier(schemaName), historyTableName)}
	}

	l.cursorTable = l.tables[cursorTableName]

	return nil
}

func (l *Loader) validateCursorTables(columns []*sql.ColumnType, schemaName string, cursorTableName string) (err error) {
	if len(columns) != 4 {
		return &SystemTableError{fmt.Errorf("table requires 4 columns ('id', 'cursor', 'block_num', 'block_id')")}
	}
	columnsCheck := map[string]string{
		"block_num": "int64",
		"block_id":  "string",
		"cursor":    "string",
		"id":        "string",
	}
	for _, f := range columns {
		columnName := f.Name()
		if _, found := columnsCheck[columnName]; !found {
			return &SystemTableError{fmt.Errorf("unexpected column %q in cursors table", columnName)}
		}
		expectedType := columnsCheck[columnName]
		actualType := f.ScanType().Kind().String()
		if expectedType != actualType {
			return &SystemTableError{fmt.Errorf("column %q has invalid type, expected %q has %q", columnName, expectedType, actualType)}
		}
		delete(columnsCheck, columnName)
	}
	if len(columnsCheck) != 0 {
		for k := range columnsCheck {
			return &SystemTableError{fmt.Errorf("missing column %q from cursors", k)}
		}
	}
	key, err := schema.PrimaryKey(l.DB, schemaName, cursorTableName)
	if err != nil {
		return &SystemTableError{fmt.Errorf("failed getting primary key: %w", err)}
	}
	if len(key) == 0 {
		return &SystemTableError{fmt.Errorf("primary key not found: %w", err)}
	}
	if key[0] != "id" {
		return &SystemTableError{fmt.Errorf("column 'id' should be primary key not %q", key[0])}
	}
	return nil
}

func (l *Loader) GetColumnsForTable(name string) []string {
	columns := make([]string, 0, len(l.tables[name].columnsByName))
	for column := range l.tables[name].columnsByName {
		// check if column is empty
		if len(column) > 0 {
			columns = append(columns, column)
		}
	}
	return columns
}

func (l *Loader) GetAvailableTablesInSchema() []string {
	tables := make([]string, len(l.tables))
	i := 0
	for table := range l.tables {
		tables[i] = table
		i++
	}
	return tables
}

func (l *Loader) HasTable(tableName string) bool {
	if _, found := l.tables[tableName]; found {
		return true
	}
	return false
}

func (l *Loader) MarshalLogObject(encoder zapcore.ObjectEncoder) error {
	encoder.AddUint64("entries_count", l.entriesCount)
	return nil
}

// Setup creates the schemaName, cursors and history table where the <schemaBytes> is a byte array
// taken from somewhere.
func (l *Loader) Setup(ctx context.Context, schemaName string, userSql string, withPostgraphile bool) error {
	if userSql != "" {
		if err := l.dialect.ExecuteSetupScript(ctx, l, userSql); err != nil {
			return fmt.Errorf("exec userSql: %w", err)
		}
	}

	if err := l.setupCursorTable(ctx, schemaName, withPostgraphile); err != nil {
		return fmt.Errorf("setup cursor table: %w", err)
	}

	if err := l.setupHistoryTable(ctx, schemaName, withPostgraphile); err != nil {
		return fmt.Errorf("setup history table: %w", err)
	}

	return nil
}

func (l *Loader) setupCursorTable(ctx context.Context, schemaName string, withPostgraphile bool) error {
	query := l.dialect.GetCreateCursorQuery(schemaName, withPostgraphile)
	_, err := l.ExecContext(ctx, query)
	return err
}

func (l *Loader) setupHistoryTable(ctx context.Context, schemaName string, withPostgraphile bool) error {
	if l.dialect.OnlyInserts() {
		return nil
	}
	query := l.dialect.GetCreateHistoryQuery(schemaName, withPostgraphile)
	_, err := l.ExecContext(ctx, query)
	return err
}

// GetIdentifier returns <database>/<schema> suitable for user presentation
func (l *Loader) GetIdentifier() string {
	return fmt.Sprintf("%s/%s", l.dsn.schema, l.dsn.schema)
}

type obfuscatedString string

func (s obfuscatedString) String() string {
	if len(s) == 0 {
		return "<unset>"
	}

	return "********"
}
