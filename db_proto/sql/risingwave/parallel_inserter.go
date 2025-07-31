package risingwave

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
	sql2 "github.com/streamingfast/substreams-sink-sql/db_proto/sql"
	"github.com/streamingfast/substreams-sink-sql/db_proto/sql/schema"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ParallelInserter implements high-performance parallel insertion for RisingWave
// It uses batched INSERT statements for optimal bulk loading performance (RisingWave compatible)
type ParallelInserter struct {
	logger         *zap.Logger
	csvBuffers     map[string]*CSVBuffer // Actually tab-delimited buffers for RisingWave
	bufferMutex    sync.RWMutex
	flushThreshold int // Number of rows to accumulate before flushing
	maxBufferSize  int // Maximum memory per buffer (bytes)
	workerIndex    int // Index of this worker for pool selection
}

// CSVBuffer manages data accumulation for a single table (tab-delimited format for RisingWave)
type CSVBuffer struct {
	buffer     *bytes.Buffer
	rowCount   int
	fieldNames []string
	lastFlush  time.Time
	mutex      sync.Mutex
}

// NewParallelInserter creates a new parallel inserter optimized for RisingWave bulk loading
func NewParallelInserter(logger *zap.Logger) (*ParallelInserter, error) {
	logger = logger.Named("risingwave_parallel_inserter")

	return &ParallelInserter{
		logger:         logger,
		csvBuffers:     make(map[string]*CSVBuffer),
		flushThreshold: 1000,            // Default flush threshold, will be configured during init
		maxBufferSize:  4 * 1024 * 1024, // 4MB max buffer size
		workerIndex:    -1,              // Will be set during init
	}, nil
}

// NewCSVBuffer creates a new buffer for a table (tab-delimited format for RisingWave batched INSERT)
func NewCSVBuffer(fieldNames []string) *CSVBuffer {
	buffer := &bytes.Buffer{}

	return &CSVBuffer{
		buffer:     buffer,
		rowCount:   0,
		fieldNames: fieldNames,
		lastFlush:  time.Now(),
	}
}

// init initializes the parallel inserter for a specific database
func (i *ParallelInserter) init(database *Database) error {
	// Configure flush threshold from database settings
	if database.csvFlushThreshold > 0 {
		i.flushThreshold = database.csvFlushThreshold
	}

	tables := database.dialect.GetTables()

	i.bufferMutex.Lock()
	defer i.bufferMutex.Unlock()

	// Initialize CSV buffers for each table
	for _, table := range tables {
		fieldNames, err := i.getFieldNamesForTable(table, database.dialect)
		if err != nil {
			return fmt.Errorf("getting field names for table %q: %w", table.Name, err)
		}

		i.csvBuffers[table.Name] = NewCSVBuffer(fieldNames)
		i.logger.Debug("initialized CSV buffer",
			zap.String("table", table.Name),
			zap.Strings("fields", fieldNames),
		)
	}

	// Initialize special tables
	i.csvBuffers["_blocks_"] = NewCSVBuffer([]string{"number", "hash", "timestamp"})
	i.csvBuffers["_cursor_"] = NewCSVBuffer([]string{"name", "cursor"})

	i.logger.Info("parallel inserter initialized",
		zap.Int("table_count", len(i.csvBuffers)),
		zap.Int("flush_threshold", i.flushThreshold),
		zap.Int("max_buffer_size", i.maxBufferSize),
	)

	return nil
}

// getFieldNamesForTable extracts field names for a table in correct insertion order
func (i *ParallelInserter) getFieldNamesForTable(table *schema.Table, dialect sql2.Dialect) ([]string, error) {
	var fieldNames []string

	// Add standard block metadata columns first
	fieldNames = append(fieldNames, "block_number")
	fieldNames = append(fieldNames, "block_timestamp")

	// Add primary key if exists
	if pk := table.PrimaryKey; pk != nil {
		fieldNames = append(fieldNames, pk.Name)
	}

	// Add parent table field for child tables
	if table.ChildOf != nil {
		fieldNames = append(fieldNames, table.ChildOf.ParentTableField)
	}

	// Add all other fields (excluding primary key since it's already added)
	for _, field := range table.Columns {
		if table.PrimaryKey != nil && field.Name == table.PrimaryKey.Name {
			continue // Skip primary key, already added
		}

		if field.IsRepeated || field.IsExtension {
			continue // Skip unsupported field types
		}

		fieldNames = append(fieldNames, field.Name)
	}

	return fieldNames, nil
}

// insert adds a row to the appropriate CSV buffer
func (i *ParallelInserter) insert(table string, values []any, database *Database) error {
	i.bufferMutex.RLock()
	csvBuffer, exists := i.csvBuffers[table]
	i.bufferMutex.RUnlock()

	if !exists {
		return fmt.Errorf("CSV buffer not found for table %q", table)
	}

	csvBuffer.mutex.Lock()
	defer csvBuffer.mutex.Unlock()

	// Ensure values match expected field count
	expectedFieldCount := len(csvBuffer.fieldNames)
	if len(values) != expectedFieldCount {
		i.logger.Debug("value count mismatch",
			zap.String("table", table),
			zap.Int("got_values", len(values)),
			zap.Int("expected_fields", expectedFieldCount),
			zap.Strings("field_names", csvBuffer.fieldNames),
		)

		// Adjust values to match expected field count
		adjustedValues := make([]any, expectedFieldCount)
		for i := 0; i < expectedFieldCount; i++ {
			if i < len(values) {
				adjustedValues[i] = values[i]
			} else {
				adjustedValues[i] = "" // Pad with empty strings
			}
		}
		values = adjustedValues
	}

	// Convert values to tab-delimited format
	tabRow := make([]string, len(values))
	for idx, value := range values {
		tabRow[idx] = i.convertValueToString(value)
	}

	// Write tab-delimited row to buffer
	line := strings.Join(tabRow, "\t") + "\n"
	_, err := csvBuffer.buffer.WriteString(line)
	if err != nil {
		return fmt.Errorf("writing tab-delimited row to buffer: %w", err)
	}

	csvBuffer.rowCount++

	// Check if we should flush this buffer
	shouldFlush := csvBuffer.rowCount >= i.flushThreshold ||
		csvBuffer.buffer.Len() >= i.maxBufferSize ||
		time.Since(csvBuffer.lastFlush) > 30*time.Second // Time-based flush

	if shouldFlush {
		return i.flushTableBuffer(table, csvBuffer, database)
	}

	return nil
}

// convertValueToString converts various value types to tab-delimited compatible strings
func (i *ParallelInserter) convertValueToString(value any) string {
	switch v := value.(type) {
	case string:
		// Escape special characters for tab-delimited format
		v = strings.ReplaceAll(v, "\\", "\\\\") // Escape backslashes first
		v = strings.ReplaceAll(v, "\t", "\\t")  // Escape tabs
		v = strings.ReplaceAll(v, "\n", "\\n")  // Escape newlines
		v = strings.ReplaceAll(v, "\r", "\\r")  // Escape carriage returns
		return v
	case int64:
		return strconv.FormatInt(v, 10)
	case int32:
		return strconv.FormatInt(int64(v), 10)
	case int:
		return strconv.FormatInt(int64(v), 10)
	case uint64:
		return strconv.FormatUint(v, 10)
	case uint32:
		return strconv.FormatUint(uint64(v), 10)
	case uint:
		return strconv.FormatUint(uint64(v), 10)
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	case float32:
		return strconv.FormatFloat(float64(v), 'f', -1, 32)
	case []uint8:
		// For RisingWave, encode bytes as hex without 0x prefix for CSV
		return fmt.Sprintf("\\x%x", v)
	case bool:
		return strconv.FormatBool(v)
	case time.Time:
		return v.Format(time.RFC3339)
	case *timestamppb.Timestamp:
		return v.AsTime().Format(time.RFC3339)
	case nil:
		return ""
	default:
		return fmt.Sprintf("%v", v)
	}
}

// flushTableBuffer flushes a single table's buffer using batched INSERT (RisingWave compatible)
func (i *ParallelInserter) flushTableBuffer(tableName string, csvBuffer *CSVBuffer, database *Database) error {
	if csvBuffer.rowCount == 0 {
		return nil // Nothing to flush
	}

	pool := database.GetConnectionPool()
	if pool == nil {
		return fmt.Errorf("no connection pool available for parallel insertion")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquiring connection from pool: %w", err)
	}
	defer conn.Release()

	// Parse buffer data back into rows for batched INSERT
	rows := i.parseBufferToRows(csvBuffer)
	if len(rows) == 0 {
		return nil
	}

	// Use batched INSERT statements since RisingWave doesn't support COPY FROM STDIN
	fullTableName := fmt.Sprintf("%s.%s", database.schema.Name, tableName)

	startTime := time.Now()
	rowsInserted, err := i.executeBatchedInsert(ctx, conn, fullTableName, csvBuffer.fieldNames, rows)
	if err != nil {
		return fmt.Errorf("batched INSERT failed for table %q: %w", tableName, err)
	}

	duration := time.Since(startTime)

	i.logger.Debug("buffer flushed with batched INSERT",
		zap.String("table", tableName),
		zap.Int("rows_inserted", rowsInserted),
		zap.Int("expected_rows", csvBuffer.rowCount),
		zap.Duration("duration", duration),
		zap.Float64("rows_per_second", float64(rowsInserted)/duration.Seconds()),
	)

	// Reset the buffer
	csvBuffer.buffer.Reset()
	csvBuffer.rowCount = 0
	csvBuffer.lastFlush = time.Now()

	return nil
}

// parseBufferToRows converts tab-delimited buffer content back to rows for INSERT
func (i *ParallelInserter) parseBufferToRows(csvBuffer *CSVBuffer) [][]string {
	bufferContent := csvBuffer.buffer.String()
	if bufferContent == "" {
		return nil
	}

	lines := strings.Split(strings.TrimSpace(bufferContent), "\n")
	rows := make([][]string, 0, len(lines))

	expectedFieldCount := len(csvBuffer.fieldNames)

	for _, line := range lines {
		if line == "" {
			continue
		}
		// Split by tabs and unescape special characters
		fields := strings.Split(line, "\t")

		// Ensure all rows have the same number of fields
		if len(fields) != expectedFieldCount {
			i.logger.Debug("adjusting row field count",
				zap.Int("got_fields", len(fields)),
				zap.Int("expected_fields", expectedFieldCount),
				zap.String("line", line),
			)
			// Pad with empty strings if too few fields
			for len(fields) < expectedFieldCount {
				fields = append(fields, "")
			}
			// Truncate if too many fields
			if len(fields) > expectedFieldCount {
				fields = fields[:expectedFieldCount]
			}
		}

		for j, field := range fields {
			// Unescape special characters
			field = strings.ReplaceAll(field, "\\r", "\r")
			field = strings.ReplaceAll(field, "\\n", "\n")
			field = strings.ReplaceAll(field, "\\t", "\t")
			field = strings.ReplaceAll(field, "\\\\", "\\") // Unescape backslashes last
			fields[j] = field
		}
		rows = append(rows, fields)
	}

	return rows
}

// executeBatchedInsert executes batched INSERT statements for better RisingWave compatibility
func (i *ParallelInserter) executeBatchedInsert(ctx context.Context, conn *pgxpool.Conn, tableName string, fieldNames []string, rows [][]string) (int, error) {
	if len(rows) == 0 {
		return 0, nil
	}

	// Build INSERT statement with multiple VALUES
	fieldNamesList := `("` + strings.Join(fieldNames, `", "`) + `")`

	// Split into batches of up to 1000 rows for optimal performance
	batchSize := 1000
	totalInserted := 0

	for i := 0; i < len(rows); i += batchSize {
		end := i + batchSize
		if end > len(rows) {
			end = len(rows)
		}
		batch := rows[i:end]

		// Build VALUES clause
		valueStrings := make([]string, len(batch))
		args := make([]interface{}, 0, len(batch)*len(fieldNames))
		argIndex := 1

		for rowIdx, row := range batch {
			// Ensure row has correct number of fields (safety check)
			if len(row) != len(fieldNames) {
				return totalInserted, fmt.Errorf("row %d has %d fields, expected %d", rowIdx, len(row), len(fieldNames))
			}

			placeholders := make([]string, len(row))
			for colIdx, value := range row {
				placeholders[colIdx] = fmt.Sprintf("$%d", argIndex)
				args = append(args, value)
				argIndex++
			}
			valueStrings[rowIdx] = "(" + strings.Join(placeholders, ", ") + ")"
		}

		// Execute INSERT statement
		insertSQL := fmt.Sprintf("INSERT INTO %s %s VALUES %s",
			tableName,
			fieldNamesList,
			strings.Join(valueStrings, ", "))

		result, err := conn.Exec(ctx, insertSQL, args...)
		if err != nil {
			return totalInserted, fmt.Errorf("executing batch INSERT: %w", err)
		}

		totalInserted += int(result.RowsAffected())
	}

	return totalInserted, nil
}

// flush flushes all data buffers
func (i *ParallelInserter) flush(database *Database) error {
	i.bufferMutex.RLock()
	defer i.bufferMutex.RUnlock()

	var errors []error
	flushed := 0

	for tableName, csvBuffer := range i.csvBuffers {
		if csvBuffer.rowCount > 0 {
			err := i.flushTableBuffer(tableName, csvBuffer, database)
			if err != nil {
				errors = append(errors, fmt.Errorf("flushing table %q: %w", tableName, err))
			} else {
				flushed++
			}
		}
	}

	if len(errors) > 0 {
		i.logger.Error("errors during parallel flush",
			zap.Int("error_count", len(errors)),
			zap.Int("successful_flushes", flushed),
		)
		return fmt.Errorf("parallel flush errors: %v", errors)
	}

	if flushed > 0 {
		i.logger.Debug("parallel flush completed",
			zap.Int("tables_flushed", flushed),
		)
	}

	return nil
}

// GetStats returns statistics about the parallel inserter
func (i *ParallelInserter) GetStats() map[string]interface{} {
	i.bufferMutex.RLock()
	defer i.bufferMutex.RUnlock()

	stats := make(map[string]interface{})
	totalRows := 0
	totalBufferSize := 0

	for tableName, buffer := range i.csvBuffers {
		buffer.mutex.Lock()
		stats[fmt.Sprintf("table_%s_rows", tableName)] = buffer.rowCount
		stats[fmt.Sprintf("table_%s_buffer_size", tableName)] = buffer.buffer.Len()
		totalRows += buffer.rowCount
		totalBufferSize += buffer.buffer.Len()
		buffer.mutex.Unlock()
	}

	stats["total_buffered_rows"] = totalRows
	stats["total_buffer_size"] = totalBufferSize
	stats["flush_threshold"] = i.flushThreshold
	stats["max_buffer_size"] = i.maxBufferSize

	return stats
}
