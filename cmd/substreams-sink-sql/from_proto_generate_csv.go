package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/dynamic"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	. "github.com/streamingfast/cli"
	"github.com/streamingfast/cli/sflags"
	"github.com/streamingfast/dstore"
	sink "github.com/streamingfast/substreams-sink"
	sinksql "github.com/streamingfast/substreams-sink-sql"
	"github.com/streamingfast/substreams-sink-sql/db_changes/bundler"
	"github.com/streamingfast/substreams-sink-sql/db_changes/bundler/writer"
	"github.com/streamingfast/substreams-sink-sql/db_changes/db"
	dbproto "github.com/streamingfast/substreams-sink-sql/db_proto/proto"
	"github.com/streamingfast/substreams-sink-sql/db_proto/sql"
	clickhouse "github.com/streamingfast/substreams-sink-sql/db_proto/sql/click_house"
	"github.com/streamingfast/substreams-sink-sql/db_proto/sql/postgres"
	"github.com/streamingfast/substreams-sink-sql/db_proto/sql/risingwave"
	"github.com/streamingfast/substreams-sink-sql/db_proto/sql/schema"
	"github.com/streamingfast/substreams-sink-sql/internal/timefmt"
	pbSchema "github.com/streamingfast/substreams-sink-sql/pb/sf/substreams/sink/sql/schema/v1"
	pbsql "github.com/streamingfast/substreams-sink-sql/pb/sf/substreams/sink/sql/services/v1"
	"github.com/streamingfast/substreams-sink-sql/proto"
	"github.com/streamingfast/substreams-sink-sql/services"
	"github.com/streamingfast/substreams/manifest"
	pbsubstreamsrpc "github.com/streamingfast/substreams/pb/sf/substreams/rpc/v2"
	"go.uber.org/zap"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
)

var fromProtoGenerateCsvCmd = Command(fromProtoGenerateCsvE,
	"from-proto-generate-csv <dsn> <manifest> [module] [start]:[stop]",
	"Generate SQL schema and CSV data exactly as from-proto expects",
	Description(`
		Generates SQL schema definitions and CSV data dumps that are 100% compatible with from-proto mode.
		
		The exported schema.sql file contains the exact DDL statements that from-proto would execute.
		The CSV files are organized by table and contain data in the exact column order expected.
		
		This mode ensures perfect compatibility for operators who need to manage the injection process.
	`),
	RangeArgs(2, 4),
	Flags(func(flags *pflag.FlagSet) {
		// Reuse from-proto flags for consistency
		sink.AddFlagsToSet(flags, ignoreUndoBufferSize{})
		flags.StringP("substreams-endpoint", "e", "", "Substreams gRPC endpoint")
		flags.StringP("start-block", "s", "", "Start block to stream from")
		flags.StringP("stop-block", "t", "0", "Stop block to end stream at")
		flags.Bool("no-constraints", false, "Do not add constraints to schema (matches from-proto behavior)")

		// Export specific flags
		flags.String("schema-output", "./schema.sql", "Path for SQL schema file")
		flags.String("schema-metadata", "./schema.json", "Path for schema metadata")
		flags.String("output-dir", "./csv-output", "Directory for CSV files")
		flags.Uint64("bundle-size", 10000, "Size of output bundle, in blocks")
		flags.String("working-dir", "./workdir", "Working directory")
		flags.Uint64("buffer-max-size", 4*1024*1024, "Memory buffer size for CSV writing")
		// from-proto compatibility: DB uses fixed table name _cursor_
		flags.String("cursors-table", "_cursor_", "Name of the cursors table (from-proto compatibility)")
	}),
)

func fromProtoGenerateCsvE(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()

	// Parse arguments exactly like from-proto
	dsnString := args[0]
	manifestPath := args[1]

	outputModuleName := sink.InferOutputModuleFromPackage
	blockRange := ""

	// Handle optional module name and block range
	if len(args) == 3 {
		// Could be either module name or block range
		arg := args[2]
		if strings.Contains(arg, ":") {
			// It's a block range
			blockRange = arg
		} else {
			// It's a module name
			outputModuleName = arg
		}
	} else if len(args) == 4 {
		// Both module name and block range provided
		outputModuleName = args[2]
		blockRange = args[3]
	}

	// Parse flags
	useConstraints := !sflags.MustGetBool(cmd, "no-constraints")
	schemaOutputPath := sflags.MustGetString(cmd, "schema-output")
	schemaMetadataPath := sflags.MustGetString(cmd, "schema-metadata")
	outputDir := sflags.MustGetString(cmd, "output-dir")
	bundleSize := sflags.MustGetUint64(cmd, "bundle-size")
	workingDir := sflags.MustGetString(cmd, "working-dir")
	bufferMaxSize := sflags.MustGetUint64(cmd, "buffer-max-size")
	cursorsTable := sflags.MustGetString(cmd, "cursors-table")

	// Setup endpoint exactly like from-proto
	endpoint := sflags.MustGetString(cmd, "substreams-endpoint")
	if endpoint == "" {
		network := sflags.MustGetString(cmd, "network")
		if network == "" {
			reader, err := manifest.NewReader(manifestPath)
			if err != nil {
				return fmt.Errorf("setup manifest reader: %w", err)
			}
			pkgBundle, err := reader.Read()
			if err != nil {
				return fmt.Errorf("read manifest: %w", err)
			}
			network = pkgBundle.Package.Network
		}
		var err error
		endpoint, err = manifest.ExtractNetworkEndpoint(network, sflags.MustGetString(cmd, "substreams-endpoint"), zlog)
		if err != nil {
			return err
		}
	}

	// Parse block range exactly like from-proto
	startBlock := sflags.MustGetString(cmd, "start-block")
	endBlock := sflags.MustGetString(cmd, "stop-block")

	// Only build blockRange from flags if it wasn't provided as argument
	if blockRange == "" {
		if startBlock != "" {
			blockRange = startBlock
		}
		blockRange += ":"
		if endBlock != "0" {
			blockRange += endBlock
		}
	}

	// Parse DSN
	dsn, err := db.ParseDSN(dsnString)
	if err != nil {
		return fmt.Errorf("parsing dsn: %w", err)
	}

	// Read manifest and module exactly like from-proto
	spkg, module, _, _, err := sink.ReadManifestAndModuleAndBlockRange(manifestPath, "", nil, outputModuleName, "", false, "", zlog)
	if err != nil {
		return fmt.Errorf("reading manifest: %w", err)
	}

	outputModuleName = module.Name
	outputType := dbproto.ModuleOutputType(spkg, outputModuleName)
	if outputType == "" {
		return fmt.Errorf("could not find output type for module %s", outputModuleName)
	}

	// Extract service exactly like from-proto
	service, err := sinksql.ExtractSinkService(spkg)
	if err != nil {
		service = &pbsql.Service{}
	}

	err = services.Run(service, zlog)
	if err != nil {
		return fmt.Errorf("running service: %w", err)
	}

	// Setup proto files exactly like from-proto
	protoFiles := map[string]*descriptorpb.FileDescriptorProto{}
	for _, file := range spkg.ProtoFiles {
		protoFiles[file.GetName()] = file
	}

	deps, err := dbproto.ResolveDependencies(protoFiles)
	if err != nil {
		return fmt.Errorf("resolving dependencies: %w", err)
	}

	fileDescriptor, err := dbproto.FileDescriptorForOutputType(spkg, err, deps, outputType)
	if err != nil {
		return fmt.Errorf("finding file descriptor for output type %q: %w", outputType, err)
	}

	// Check for proto options exactly like from-proto
	useProtoOption := false
	for _, descriptor := range fileDescriptor.GetDependencies() {
		if descriptor.GetName() == "sf/substreams/sink/sql/schema/v1/schema.proto" {
			useProtoOption = true
		}
	}
	if !useProtoOption {
		useConstraints = false
	}

	// Find root message descriptor exactly like from-proto
	var rootMessageDescriptor *desc.MessageDescriptor
	for _, messageDescriptor := range fileDescriptor.GetMessageTypes() {
		name := messageDescriptor.GetFullyQualifiedName()
		if name == outputType {
			rootMessageDescriptor = messageDescriptor
			break
		}
	}
	if rootMessageDescriptor == nil {
		return fmt.Errorf("message descriptor not found for output type %q", outputType)
	}

	// Create schema exactly like from-proto
	sqlSchema, err := schema.NewSchema(dsn.Schema(), rootMessageDescriptor, useProtoOption, zlog)
	if err != nil {
		return fmt.Errorf("creating schema: %w", err)
	}

	// Create dialect exactly like from-proto
	var dialect sql.Dialect
	switch dsn.Driver() {
	case "postgres":
		dialect, err = postgres.NewDialectPostgres(sqlSchema, zlog)
		if err != nil {
			return fmt.Errorf("creating postgres dialect: %w", err)
		}
	case "risingwave":
		dialect, err = risingwave.NewDialectRisingwave(sqlSchema.Name, sqlSchema.TableRegistry, zlog)
		if err != nil {
			return fmt.Errorf("creating risingwave dialect: %w", err)
		}
	case "clickhouse":
		dialect, err = clickhouse.NewDialectClickHouse(sqlSchema, zlog)
		if err != nil {
			return fmt.Errorf("creating clickhouse dialect: %w", err)
		}
	default:
		return fmt.Errorf("unsupported driver: %s", dsn.Driver())
	}

	// Generate SQL schema
	zlog.Info("generating SQL schema", zap.String("path", schemaOutputPath))
	if err := exportSQLSchema(dialect, sqlSchema, useConstraints, dsn.Driver(), schemaOutputPath); err != nil {
		return fmt.Errorf("exporting SQL schema: %w", err)
	}

	// Generate schema metadata
	zlog.Info("generating schema metadata", zap.String("path", schemaMetadataPath))
	if err := exportSchemaMetadata(sqlSchema, dialect, dsn.Driver(), schemaMetadataPath); err != nil {
		return fmt.Errorf("exporting schema metadata: %w", err)
	}

	// Create base sinker
	baseSink, err := sink.NewFromViper(
		cmd,
		outputType,
		endpoint,
		manifestPath,
		outputModuleName,
		blockRange,
		zlog,
		tracer,
		// We only generate CSVs from final blocks; undos are not applied in this mode
		sink.WithFinalBlocksOnly(),
	)
	if err != nil {
		return fmt.Errorf("new base sinker: %w", err)
	}

	// Create CSV generator
	csvGen, err := newProtoAwareCSVGenerator(
		ctx,
		baseSink,
		sqlSchema,
		dialect,
		rootMessageDescriptor.UnwrapMessage(),
		outputDir,
		workingDir,
		bundleSize,
		bufferMaxSize,
		cursorsTable,
		useProtoOption,
		zlog,
	)
	if err != nil {
		return fmt.Errorf("creating CSV generator: %w", err)
	}

	// Run the CSV generation
	zlog.Info("generating CSV data", zap.String("output_dir", outputDir))
	if err := csvGen.Run(ctx); err != nil {
		return fmt.Errorf("running CSV generator: %w", err)
	}

	zlog.Info("CSV generation completed successfully",
		zap.String("schema", schemaOutputPath),
		zap.String("metadata", schemaMetadataPath),
		zap.String("csv_data", outputDir),
	)

	return nil
}

// exportSQLSchema exports the exact SQL DDL that from-proto would execute
func exportSQLSchema(dialect sql.Dialect, sqlSchema *schema.Schema, useConstraints bool, driver, outputPath string) error {
	// Extract BaseDialect from the dialect
	var baseDialect *sql.BaseDialect
	switch d := dialect.(type) {
	case *postgres.DialectPostgres:
		baseDialect = d.BaseDialect
	case *risingwave.DialectRisingwave:
		baseDialect = d.BaseDialect
	case *clickhouse.DialectClickHouse:
		baseDialect = d.BaseDialect
	default:
		return fmt.Errorf("unsupported dialect type %T", dialect)
	}

	var b strings.Builder

	// Header
	b.WriteString("-- Generated by substreams-sink-sql from-proto-generate-csv\n")
	b.WriteString(fmt.Sprintf("-- Dialect: %s\n", driver))
	b.WriteString(fmt.Sprintf("-- Schema: %s\n", sqlSchema.Name))
	b.WriteString(fmt.Sprintf("-- Generated at: %s\n", time.Now().UTC().Format(time.RFC3339)))
	b.WriteString(fmt.Sprintf("-- Schema hash: %s\n\n", dialect.SchemaHash()))

	// System tables
	if staticSQL := strings.TrimSpace(getStaticSQL(driver, sqlSchema.Name)); staticSQL != "" {
		b.WriteString("-- System Tables\n")
		b.WriteString(staticSQL)
		b.WriteString("\n\n")
	}

	// User tables
	// Sort table names for consistent ordering
	var tableNames []string
	for name := range baseDialect.CreateTableSql {
		tableNames = append(tableNames, name)
	}
	sort.Strings(tableNames)

	if len(tableNames) > 0 {
		b.WriteString("-- User Tables\n")
		for i, tableName := range tableNames {
			createSQL := strings.TrimSpace(baseDialect.CreateTableSql[tableName])
			b.WriteString(prettyFormatCreateTable(createSQL))
			if i < len(tableNames)-1 {
				b.WriteString("\n\n")
			} else {
				b.WriteString("\n\n")
			}
		}
	}

	// Constraints (optional)
	if useConstraints {
		if len(baseDialect.PrimaryKeySql) > 0 || len(baseDialect.UniqueConstraintSql) > 0 || len(baseDialect.ForeignKeySql) > 0 {
			b.WriteString("-- Constraints\n")
		}
		for _, c := range baseDialect.PrimaryKeySql {
			b.WriteString(strings.TrimSpace(c.Sql))
			b.WriteString("\n")
		}
		for _, c := range baseDialect.UniqueConstraintSql {
			b.WriteString(strings.TrimSpace(c.Sql))
			b.WriteString("\n")
		}
		for _, c := range baseDialect.ForeignKeySql {
			b.WriteString(strings.TrimSpace(c.Sql))
			b.WriteString("\n")
		}
		if len(baseDialect.PrimaryKeySql) > 0 || len(baseDialect.UniqueConstraintSql) > 0 || len(baseDialect.ForeignKeySql) > 0 {
			b.WriteString("\n")
		}
	}

	// Seed sink info so from-proto can start without error
	switch driver {
	case "postgres":
		seed := fmt.Sprintf("INSERT INTO \"%s\".\"_sink_info_\" (schema_hash) VALUES ('%s') ON CONFLICT (schema_hash) DO NOTHING;", sqlSchema.Name, dialect.SchemaHash())
		b.WriteString("-- Seed\n")
		b.WriteString(seed)
		b.WriteString("\n")
	case "risingwave":
		// RisingWave: rely on table-level ON CONFLICT policy (DO NOTHING) defined in CREATE TABLE
		seed := fmt.Sprintf(
			"INSERT INTO \"%s\".\"_sink_info_\" (schema_hash) VALUES ('%s');",
			sqlSchema.Name, dialect.SchemaHash(),
		)
		b.WriteString("-- Seed\n")
		b.WriteString(seed)
		b.WriteString("\n")
	}

	// Write to file
	return os.WriteFile(outputPath, []byte(b.String()), 0644)
}

// prettyFormatCreateTable formats a single CREATE TABLE statement so that
// columns/indexes inside the parentheses each appear on their own line.
// It avoids breaking numeric type parameters like NUMERIC(78,0) by only
// splitting on commas at depth 0 within the column list, and skips commas
// inside quoted identifiers.
func prettyFormatCreateTable(sql string) string {
	// Find first opening parenthesis after CREATE TABLE
	idxOpen := strings.Index(sql, "(")
	if idxOpen == -1 {
		return sql
	}

	// Find the matching closing parenthesis for the column list
	depth := 0
	inQuotes := false
	idxClose := -1
	for i := idxOpen; i < len(sql); i++ {
		ch := sql[i]
		if ch == '"' {
			// Toggle double-quote state; basic handling (no escape handling required for our generated SQL)
			inQuotes = !inQuotes
		}
		if inQuotes {
			continue
		}
		if ch == '(' {
			depth++
		} else if ch == ')' {
			depth--
			if depth == 0 {
				idxClose = i
				break
			}
		}
	}
	if idxClose == -1 {
		return sql
	}

	prefix := sql[:idxOpen+1]
	columnsSeg := sql[idxOpen+1 : idxClose]
	suffix := sql[idxClose:]

	// Split columns at top-level commas
	var parts []string
	var cur strings.Builder
	depth = 0
	inQuotes = false
	for i := 0; i < len(columnsSeg); i++ {
		ch := columnsSeg[i]
		if ch == '"' {
			inQuotes = !inQuotes
			cur.WriteByte(ch)
			continue
		}
		if !inQuotes {
			if ch == '(' {
				depth++
			} else if ch == ')' {
				if depth > 0 {
					depth--
				}
			} else if ch == ',' && depth == 0 {
				part := strings.TrimSpace(cur.String())
				if part != "" {
					parts = append(parts, part)
				}
				cur.Reset()
				continue
			}
		}
		cur.WriteByte(ch)
	}
	if s := strings.TrimSpace(cur.String()); s != "" {
		parts = append(parts, s)
	}

	// Rebuild with one part per line
	var b strings.Builder
	b.WriteString(prefix)
	if len(parts) > 0 {
		b.WriteString("\n")
		for i, p := range parts {
			b.WriteString("    ")
			b.WriteString(p)
			if i < len(parts)-1 {
				b.WriteString(",\n")
			} else {
				b.WriteString("\n")
			}
		}
	}
	b.WriteString(suffix)
	return b.String()
}

// getStaticSQL returns the static SQL for system tables
func getStaticSQL(driver, schemaName string) string {
	switch driver {
	case "postgres":
		// Matches db_proto/sql/postgres/dialect.go postgresStaticSql
		return fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS "%s";

CREATE TABLE IF NOT EXISTS "%s"._sink_info_ (
    schema_hash TEXT PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS "%s"._cursor_ (
    name TEXT PRIMARY KEY,
    cursor TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS "%s"._blocks_ (
    number integer,
    hash TEXT NOT NULL,
    timestamp TIMESTAMP NOT NULL
);`, schemaName, schemaName, schemaName, schemaName)

	case "risingwave":
		// Matches db_proto/sql/risingwave/dialect.go risingwaveStaticSql (no ON CONFLICT clauses)
		return fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS "%s";

CREATE TABLE IF NOT EXISTS "%s"._sink_info_ (
    schema_hash VARCHAR PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS "%s"._cursor_ (
    name VARCHAR PRIMARY KEY,
    cursor VARCHAR
);

CREATE TABLE IF NOT EXISTS "%s"._blocks_ (
    number INTEGER PRIMARY KEY,
    hash VARCHAR,
    timestamp TIMESTAMP WITH TIME ZONE
);`, schemaName, schemaName, schemaName, schemaName)

	case "clickhouse":
		// Matches db_proto/sql/click_house/dialect.go staticSqlCreatDatabase + staticSqlCreateBlock
		return fmt.Sprintf(`CREATE DATABASE IF NOT EXISTS %s;

CREATE TABLE IF NOT EXISTS %s._blocks_  (
    number    UInt64,
    hash      text,
    timestamp timestamp,
    version   Int64,
    deleted   bool
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY (toYYYYMM(timestamp))
PRIMARY KEY (number)
ORDER BY (number);`, schemaName, schemaName)

	default:
		return ""
	}
}

// SchemaMetadata represents the exported schema metadata
type SchemaMetadata struct {
	Version        string          `json:"version"`
	GeneratedAt    time.Time       `json:"generated_at"`
	Dialect        string          `json:"dialect"`
	SchemaHash     string          `json:"schema_hash"`
	SchemaName     string          `json:"schema_name"`
	Tables         []TableMetadata `json:"tables"`
	UseConstraints bool            `json:"use_constraints"`
}

type TableMetadata struct {
	Name        string           `json:"name"`
	Columns     []ColumnMetadata `json:"columns"`
	ColumnOrder []string         `json:"column_order"`
	PrimaryKey  *string          `json:"primary_key,omitempty"`
}

type ColumnMetadata struct {
	Name         string  `json:"name"`
	SQLType      string  `json:"sql_type"`
	Position     int     `json:"position"`
	Nullable     bool    `json:"nullable"`
	SemanticType *string `json:"semantic_type,omitempty"`
}

// exportSchemaMetadata exports metadata for validation
func exportSchemaMetadata(sqlSchema *schema.Schema, dialect sql.Dialect, driver string, outputPath string) error {
	metadata := SchemaMetadata{
		Version:     "1.0",
		GeneratedAt: time.Now(),
		Dialect:     driver,
		SchemaHash:  dialect.SchemaHash(),
		SchemaName:  sqlSchema.Name,
		Tables:      []TableMetadata{},
	}

	// Export table metadata
	for _, table := range dialect.GetTables() {
		tm := TableMetadata{
			Name:        table.Name,
			Columns:     []ColumnMetadata{},
			ColumnOrder: []string{},
		}

		// Column order must exactly match CSV generation order
		tm.ColumnOrder = computeCSVColumnOrder(table, dialect)

		// Describe all physical columns (excluding system columns) for metadata
		for i, col := range table.Columns {
			tm.Columns = append(tm.Columns, ColumnMetadata{
				Name:     col.Name,
				SQLType:  getColumnSQLType(col, dialect, driver),
				Position: i,
				Nullable: true,
			})
		}

		if table.PrimaryKey != nil {
			tm.PrimaryKey = &table.PrimaryKey.Name
		}

		metadata.Tables = append(metadata.Tables, tm)
	}

	// Write metadata
	data, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		return fmt.Errorf("marshaling metadata: %w", err)
	}

	return os.WriteFile(outputPath, data, 0644)
}

// computeCSVColumnOrder returns the exact CSV column order for a table
func computeCSVColumnOrder(table *schema.Table, dialect sql.Dialect) []string {
	var cols []string
	bn := dialectBlockNumberName(dialect)
	bt := dialectBlockTimestampName(dialect)
	cols = append(cols, bn)
	cols = append(cols, bt)
	if dialect.UseVersionField() {
		cols = append(cols, sql.DialectFieldVersion)
	}
	if dialect.UseDeletedField() {
		cols = append(cols, sql.DialectFieldDeleted)
	}
	if table.PrimaryKey != nil {
		cols = append(cols, table.PrimaryKey.Name)
	}
	if table.ChildOf != nil {
		cols = append(cols, table.ChildOf.ParentTableField)
	}
	for _, c := range table.Columns {
		// Skip duplicates already added
		if table.PrimaryKey != nil && c.Name == table.PrimaryKey.Name {
			continue
		}
		if table.ChildOf != nil && c.Name == table.ChildOf.ParentTableField {
			continue
		}
		cols = append(cols, c.Name)
	}
	return cols
}

// getColumnSQLType gets the SQL type for a column
func getColumnSQLType(col *schema.Column, dialect sql.Dialect, driver string) string {
	switch driver {
	case "postgres":
		return string(postgres.MapFieldType(col.FieldDescriptor))
	case "clickhouse":
		return string(clickhouse.MapFieldType(col.FieldDescriptor))
	case "risingwave":
		return string(risingwave.MapFieldType(col.FieldDescriptor))
	}
	return "TEXT" // fallback
}

// dialectBlockNumberName returns the exact column name for the block number
// system field for the given dialect (RisingWave uses non-underscored names).
func dialectBlockNumberName(dialect sql.Dialect) string {
	switch dialect.(type) {
	case *risingwave.DialectRisingwave:
		return "block_number"
	default:
		return sql.DialectFieldBlockNumber
	}
}

// dialectBlockTimestampName returns the exact column name for the block timestamp
// system field for the given dialect (RisingWave uses non-underscored names).
func dialectBlockTimestampName(dialect sql.Dialect) string {
	switch dialect.(type) {
	case *risingwave.DialectRisingwave:
		return "block_timestamp"
	default:
		return sql.DialectFieldBlockTimestamp
	}
}

// protoAwareCSVGenerator generates CSV files with the exact structure from-proto expects
type protoAwareCSVGenerator struct {
	sink             *sink.Sinker
	schema           *schema.Schema
	dialect          sql.Dialect
	rootDescriptor   protoreflect.MessageDescriptor
	rootMessage      *dynamic.Message
	tableColumns     map[string][]string
	fieldColumnNames map[fieldColumnKey]string
	columnIndexes    map[string]map[string]int
	rowBufferPools   map[string]*sync.Pool
	outputDir        string
	workingDir       string
	bundleSize       uint64
	bufferSize       uint64
	cursorsTable     string
	bundlers         map[string]*bundler.Bundler
	cursorsStore     dstore.Store
	logger           *zap.Logger
	useProtoOptions  bool

	closeOnce sync.Once
}

// tableRows holds accumulated rows for a table during message traversal
type tableRows struct {
	tableName string
	rows      [][]interface{}
}

type fieldColumnKey struct {
	table string
	field *desc.FieldDescriptor
}

func newProtoAwareCSVGenerator(
	ctx context.Context,
	sink *sink.Sinker,
	sqlSchema *schema.Schema,
	dialect sql.Dialect,
	rootDescriptor protoreflect.MessageDescriptor,
	outputDir string,
	workingDir string,
	bundleSize uint64,
	bufferSize uint64,
	cursorsTable string,
	useProtoOptions bool,
	logger *zap.Logger,
) (*protoAwareCSVGenerator, error) {
	gen := &protoAwareCSVGenerator{
		sink:             sink,
		schema:           sqlSchema,
		dialect:          dialect,
		rootDescriptor:   rootDescriptor,
		tableColumns:     make(map[string][]string),
		fieldColumnNames: make(map[fieldColumnKey]string),
		columnIndexes:    make(map[string]map[string]int),
		rowBufferPools:   make(map[string]*sync.Pool),
		outputDir:        outputDir,
		workingDir:       workingDir,
		bundleSize:       bundleSize,
		bufferSize:       bufferSize,
		cursorsTable:     cursorsTable,
		bundlers:         make(map[string]*bundler.Bundler),
		logger:           logger,
		useProtoOptions:  useProtoOptions,
	}

	if rootDescriptor == nil {
		return nil, fmt.Errorf("root descriptor cannot be nil")
	}

	wrappedRootDesc, err := desc.WrapMessage(rootDescriptor)
	if err != nil {
		return nil, fmt.Errorf("wrapping root message descriptor: %w", err)
	}
	gen.rootMessage = dynamic.NewMessage(wrappedRootDesc)

	// Create output directory
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return nil, fmt.Errorf("creating output directory: %w", err)
	}

	// Create working directory
	if err := os.MkdirAll(workingDir, 0755); err != nil {
		return nil, fmt.Errorf("creating working directory: %w", err)
	}

	// Initialize bundlers for each table
	csvOutputStore, err := dstore.NewStore(outputDir, "csv", "", false)
	if err != nil {
		return nil, fmt.Errorf("creating output store: %w", err)
	}

	blockRange := sink.BlockRange()
	if blockRange == nil || blockRange.EndBlock() == nil {
		return nil, fmt.Errorf("sink must have a stop block defined")
	}

	stopBlock := *blockRange.EndBlock()
	startBlock := blockRange.StartBlock()

	// Create bundlers for each table
	for tableName, table := range sqlSchema.TableRegistry {
		columns := gen.getColumnsForTable(table)
		gen.registerFieldColumns(table)
		gen.ensureRowBufferPool(tableName, len(columns))

		bundlerWriter := writer.NewBufferedIO(
			bufferSize,
			filepath.Join(workingDir, tableName),
			writer.FileTypeCSV,
			logger.With(zap.String("table_name", tableName)),
		)

		subStore, err := csvOutputStore.SubStore(tableName)
		if err != nil {
			return nil, fmt.Errorf("creating substore for table %s: %w", tableName, err)
		}

		// Create CSV header
		header := []byte(strings.Join(columns, ",") + "\n")

		b, err := bundler.New(bundleSize, stopBlock, bundlerWriter, subStore, logger, header)
		if err != nil {
			return nil, fmt.Errorf("creating bundler for table %s: %w", tableName, err)
		}

		if err := b.Start(startBlock); err != nil {
			return nil, fmt.Errorf("starting bundler for table %s: %w", tableName, err)
		}

		b.Launch(ctx)
		gen.bundlers[tableName] = b
	}

	// Prepare cursors store (single last_cursor file, no bundler)
	cursorsStore, err := csvOutputStore.SubStore(cursorsTable)
	if err != nil {
		return nil, fmt.Errorf("creating cursors substore: %w", err)
	}
	gen.cursorsStore = cursorsStore

	return gen, nil
}

// getColumnsForTable returns columns in the exact order from-proto expects
func (g *protoAwareCSVGenerator) getColumnsForTable(table *schema.Table) []string {
	if table == nil {
		return nil
	}

	if g.tableColumns != nil {
		if columns, found := g.tableColumns[table.Name]; found {
			return columns
		}
	}

	columns := g.computeColumnsForTable(table)
	if g.tableColumns != nil {
		g.tableColumns[table.Name] = columns
	}

	if g.columnIndexes != nil {
		if _, exists := g.columnIndexes[table.Name]; !exists {
			index := make(map[string]int, len(columns))
			for i, name := range columns {
				index[name] = i
			}
			g.columnIndexes[table.Name] = index
		}
	}

	return columns
}

func (g *protoAwareCSVGenerator) computeColumnsForTable(table *schema.Table) []string {
	columns := make([]string, 0, len(table.Columns)+5) // include system columns

	// Always add system columns first
	columns = append(columns, dialectBlockNumberName(g.dialect))
	columns = append(columns, dialectBlockTimestampName(g.dialect))

	// Add version/deleted if dialect requires
	if g.dialect.UseVersionField() {
		columns = append(columns, sql.DialectFieldVersion)
	}
	if g.dialect.UseDeletedField() {
		columns = append(columns, sql.DialectFieldDeleted)
	}

	// Add primary key if exists and not already in columns
	primaryKeyAdded := false
	if table.PrimaryKey != nil {
		columns = append(columns, table.PrimaryKey.Name)
		primaryKeyAdded = true
	}

	// Add parent reference column if this is a child table
	if table.ChildOf != nil {
		columns = append(columns, table.ChildOf.ParentTableField)
	}

	// Add other table columns in schema order
	for _, col := range table.Columns {
		// Skip primary key if already added
		if primaryKeyAdded && col.Name == table.PrimaryKey.Name {
			continue
		}
		// Skip parent field if it's also a regular column (shouldn't happen but be safe)
		if table.ChildOf != nil && col.Name == table.ChildOf.ParentTableField {
			continue
		}
		columns = append(columns, col.Name)
	}

	return columns
}

func (g *protoAwareCSVGenerator) registerFieldColumns(table *schema.Table) {
	if g.fieldColumnNames == nil || table == nil {
		return
	}

	for _, col := range table.Columns {
		if col.FieldDescriptor == nil {
			continue
		}
		key := fieldColumnKey{table: table.Name, field: col.FieldDescriptor}
		g.fieldColumnNames[key] = col.Name
	}
}

func (g *protoAwareCSVGenerator) columnNameForField(tableName string, fd *desc.FieldDescriptor) (string, bool) {
	if g.fieldColumnNames == nil || fd == nil {
		return "", false
	}

	name, ok := g.fieldColumnNames[fieldColumnKey{table: tableName, field: fd}]
	return name, ok
}

func (g *protoAwareCSVGenerator) columnIndexForTable(tableName string) map[string]int {
	if g.columnIndexes == nil {
		return nil
	}
	return g.columnIndexes[tableName]
}

func (g *protoAwareCSVGenerator) ensureRowBufferPool(tableName string, size int) {
	if g.rowBufferPools == nil {
		return
	}

	if _, exists := g.rowBufferPools[tableName]; exists {
		return
	}

	g.rowBufferPools[tableName] = &sync.Pool{
		New: func() interface{} {
			return make([]interface{}, size)
		},
	}
}

func (g *protoAwareCSVGenerator) acquireRowBuffer(tableName string, size int) []interface{} {
	if pool, ok := g.rowBufferPools[tableName]; ok {
		buf := pool.Get().([]interface{})
		if cap(buf) < size {
			buf = make([]interface{}, size)
		}
		buf = buf[:size]
		for i := range buf {
			buf[i] = nil
		}
		return buf
	}

	buf := make([]interface{}, size)
	for i := range buf {
		buf[i] = nil
	}
	return buf
}

func (g *protoAwareCSVGenerator) releaseRowBuffer(tableName string, buf []interface{}) {
	for i := range buf {
		buf[i] = nil
	}
	if pool, ok := g.rowBufferPools[tableName]; ok {
		pool.Put(buf)
	}
}

func assignRowValue(row []interface{}, columnIndex map[string]int, column string, value interface{}) {
	if columnIndex == nil {
		return
	}
	idx, ok := columnIndex[column]
	if !ok || idx >= len(row) {
		return
	}
	row[idx] = value
}

// Run streams data and generates CSV files
func (g *protoAwareCSVGenerator) Run(ctx context.Context) error {
	// Ensure we always close bundlers exactly once on termination
	g.sink.OnTerminating(func(err error) {
		g.logger.Info("terminating CSV generation", zap.Error(err))
		g.close()
	})

	// Start from the beginning (no cursor for export mode)
	var cursor *sink.Cursor

	// Run the sinker with our handler
	g.sink.Run(ctx, cursor, g)

	// Extra safety: ensure close after stream ends (idempotent)
	g.close()
	return nil
}

// HandleBlockUndoSignal handles block undo signals (implements sink.SinkerHandler)
func (g *protoAwareCSVGenerator) HandleBlockUndoSignal(ctx context.Context, undo *pbsubstreamsrpc.BlockUndoSignal, cursor *sink.Cursor) error {
	// For CSV export, we don't handle undos since we're generating historical data
	g.logger.Debug("ignoring block undo signal", zap.Uint64("block", undo.LastValidBlock.Number))
	return nil
}

// HandleBlockScopedData processes each block's data (implements sink.SinkerHandler)
func (g *protoAwareCSVGenerator) HandleBlockScopedData(ctx context.Context, data *pbsubstreamsrpc.BlockScopedData, isLive *bool, cursor *sink.Cursor) error {
	block := data.Clock

	g.logger.Debug("processing block",
		zap.Uint64("number", block.Number),
		zap.String("id", block.Id),
	)

	// Process the output data
	output := data.Output
	if output == nil {
		return nil
	}

	mapOutput := output.GetMapOutput()
	if mapOutput == nil || mapOutput.Value == nil {
		return nil
	}

	// Create or reuse dynamic message from our root descriptor
	msg := g.rootMessage
	if msg == nil {
		if g.rootDescriptor == nil {
			return fmt.Errorf("root message descriptor is not configured")
		}
		wrappedRootDesc, err := desc.WrapMessage(g.rootDescriptor)
		if err != nil {
			return fmt.Errorf("wrapping message descriptor: %w", err)
		}
		msg = dynamic.NewMessage(wrappedRootDesc)
		g.rootMessage = msg
	}

	msg.Reset()
	if err := msg.Unmarshal(mapOutput.Value); err != nil {
		return fmt.Errorf("unmarshaling message: %w", err)
	}

	// Walk the message and collect all rows for all tables
	allTableRows, err := g.walkMessageAndCollectRows(msg, block.Number, block.Timestamp.AsTime(), nil)
	if err != nil {
		return fmt.Errorf("walking message: %w", err)
	}

	// Write all collected rows to their respective CSV files
	for _, tr := range allTableRows {
		bundler, exists := g.bundlers[tr.tableName]
		if !exists {
			g.logger.Warn("no bundler for table", zap.String("table", tr.tableName))
			continue
		}

		table, exists := g.schema.TableRegistry[tr.tableName]
		if !exists {
			g.logger.Warn("no table schema found", zap.String("table", tr.tableName))
			continue
		}

		// Ensure header is written once per boundary
		if !bundler.HeaderWritten {
			if _, err := bundler.Writer().Write(bundler.Header); err != nil {
				return fmt.Errorf("writing header for table %s: %w", tr.tableName, err)
			}
			bundler.HeaderWritten = true
		}

		for _, row := range tr.rows {
			csvData := g.formatRowForCSV(row, table)
			if _, err := bundler.Writer().Write(csvData); err != nil {
				g.releaseRowBuffer(tr.tableName, row)
				return fmt.Errorf("writing row to table %s: %w", tr.tableName, err)
			}
			g.releaseRowBuffer(tr.tableName, row)
		}
	}

	// Roll bundlers if needed
	for tableName, b := range g.bundlers {
		if rolled, err := b.Roll(ctx, block.Number); err != nil {
			if err == bundler.ErrStopBlockReached {
				g.logger.Info("stop block reached for table", zap.String("table", tableName))
				continue
			}
			return fmt.Errorf("rolling bundler for table %s: %w", tableName, err)
		} else if rolled {
			g.logger.Debug("rolled bundler", zap.String("table", tableName), zap.Uint64("block", block.Number))
		}
	}

	return nil
}

// walkMessageAndCollectRows mimics WalkMessageDescriptorAndInsertWithDialect but collects rows instead of inserting
func (g *protoAwareCSVGenerator) walkMessageAndCollectRows(dm *dynamic.Message, blockNum uint64, blockTimestamp time.Time, parent *Parent) ([]*tableRows, error) {
	if dm == nil {
		return nil, fmt.Errorf("received a nil message")
	}

	var allRows []*tableRows
	md := dm.GetMessageDescriptor()
	tableInfo := proto.TableInfo(md)

	if tableInfo == nil && !g.useProtoOptions {
		tableInfo = &pbSchema.Table{
			Name: md.GetName(),
		}
	}

	g.logger.Debug("walking message descriptor", zap.String("message_descriptor_name", md.GetName()), zap.Any("table_info", tableInfo))

	releaseAccumulated := func() {
		for _, tr := range allRows {
			for _, row := range tr.rows {
				g.releaseRowBuffer(tr.tableName, row)
			}
		}
	}

	var table *schema.Table
	if tableInfo != nil {
		table = g.dialect.GetTable(tableInfo.Name)
	}

	var (
		tableName   string
		rowValues   []interface{}
		columnIndex map[string]int
		pkValue     interface{}
		pkDefined   bool
	)

	if table != nil {
		tableName = table.Name
		columns := g.getColumnsForTable(table)
		columnIndex = g.columnIndexForTable(tableName)
		if columnIndex == nil {
			columnIndex = make(map[string]int, len(columns))
			for i, name := range columns {
				columnIndex[name] = i
			}
			if g.columnIndexes != nil {
				g.columnIndexes[tableName] = columnIndex
			}
		}
		rowValues = g.acquireRowBuffer(tableName, len(columns))

		assignRowValue(rowValues, columnIndex, dialectBlockNumberName(g.dialect), blockNum)
		assignRowValue(rowValues, columnIndex, dialectBlockTimestampName(g.dialect), blockTimestamp)

		if g.dialect.UseVersionField() {
			assignRowValue(rowValues, columnIndex, sql.DialectFieldVersion, time.Now().UnixNano())
		}
		if g.dialect.UseDeletedField() {
			assignRowValue(rowValues, columnIndex, sql.DialectFieldDeleted, false)
		}

		if table.PrimaryKey != nil {
			pkFieldName := table.PrimaryKey.FieldDescriptor.GetName()
			pkValue = dm.GetFieldByName(pkFieldName)
			if pkValue == nil {
				g.releaseRowBuffer(tableName, rowValues)
				return nil, fmt.Errorf("missing primary key field %q for table %q", pkFieldName, tableInfo.Name)
			}
			assignRowValue(rowValues, columnIndex, table.PrimaryKey.Name, pkValue)
			pkDefined = true
		}

		if parent != nil && table.ChildOf != nil {
			assignRowValue(rowValues, columnIndex, table.ChildOf.ParentTableField, parent.id)
		}
	}

	var childs []*dynamic.Message

	for _, fd := range dm.GetMessageDescriptor().GetFields() {
		if fd.GetOneOf() != nil && !dm.HasField(fd) {
			continue
		}
		if table != nil && table.PrimaryKey != nil && fd == table.PrimaryKey.FieldDescriptor {
			continue
		}

		fv := dm.GetField(fd)
		if v, ok := fv.([]interface{}); ok {
			for _, c := range v {
				fm, ok := c.(*dynamic.Message)
				if !ok {
					if rowValues != nil {
						g.releaseRowBuffer(tableName, rowValues)
					}
					releaseAccumulated()
					return nil, fmt.Errorf("Repeated fields with native values not supported yet in 'from-proto' mode. message %q, field %q", md.GetFullyQualifiedName(), fd.GetName())
				}
				childs = append(childs, fm)
			}
		} else if fm, ok := fv.(*dynamic.Message); ok {
			if fm == nil {
				continue // unused oneOf field
			}
			childs = append(childs, fm)
		} else if rowValues != nil {
			colName := fd.GetName()
			if renamed, ok := g.columnNameForField(tableName, fd); ok {
				colName = renamed
			} else if table != nil {
				for _, c := range table.Columns {
					if c.FieldDescriptor == fd {
						colName = c.Name
						break
					}
				}
			}
			converted, err := sql.NormalizeValue(fd, fv)
			if err != nil {
				g.releaseRowBuffer(tableName, rowValues)
				releaseAccumulated()
				return nil, fmt.Errorf("normalizing field %q: %w", fd.GetName(), err)
			}
			assignRowValue(rowValues, columnIndex, colName, converted)
		}
	}

	var p *Parent

	if tableInfo != nil && table != nil && rowValues != nil {
		if len(childs) > 0 && g.useProtoOptions && table.PrimaryKey == nil {
			g.releaseRowBuffer(tableName, rowValues)
			return nil, fmt.Errorf("table %q has no primary key and has %d associated children table", table.Name, len(childs))
		}

		allRows = append(allRows, &tableRows{
			tableName: tableName,
			rows:      [][]interface{}{rowValues},
		})

		if len(childs) > 0 && g.useProtoOptions {
			id := pkValue
			if !pkDefined && table.PrimaryKey != nil {
				if idx, ok := columnIndex[table.PrimaryKey.Name]; ok && idx < len(rowValues) {
					id = rowValues[idx]
				}
			}
			p = &Parent{
				field: strings.ToLower(md.GetName()),
				id:    id,
			}
		}
	} else if rowValues != nil {
		g.releaseRowBuffer(tableName, rowValues)
	}

	for _, fm := range childs {
		childRows, err := g.walkMessageAndCollectRows(fm, blockNum, blockTimestamp, p)
		if err != nil {
			releaseAccumulated()
			return nil, fmt.Errorf("processing child %q: %w", fm.GetMessageDescriptor().GetFullyQualifiedName(), err)
		}
		allRows = append(allRows, childRows...)
	}

	return allRows, nil
}

// Parent represents a parent relationship
type Parent struct {
	field string
	id    interface{}
}

func (g *protoAwareCSVGenerator) formatTimestamp(t time.Time) string {
	if _, ok := g.dialect.(*risingwave.DialectRisingwave); ok {
		return timefmt.FormatRisingWave(t)
	}
	return t.UTC().Format(time.RFC3339)
}

// formatRowForCSV formats a row for CSV output
func (g *protoAwareCSVGenerator) formatRowForCSV(row []interface{}, table *schema.Table) []byte {
	columns := g.getColumnsForTable(table)
	if len(columns) == 0 {
		return []byte("\n")
	}

	var builder strings.Builder
	// Pre-size builder to avoid repeated growth; assume average 16 bytes per column.
	builder.Grow(len(columns) * 16)

	for i, col := range columns {
		if i > 0 {
			builder.WriteByte(',')
		}
		var cell interface{}
		if i < len(row) {
			cell = row[i]
		}
		builder.WriteString(g.formatValue(cell, col, table))
	}

	builder.WriteByte('\n')
	return []byte(builder.String())
}

// formatValue formats a value for CSV based on its SQL type
func (g *protoAwareCSVGenerator) formatValue(value interface{}, columnName string, table *schema.Table) string {
	if value == nil {
		return ""
	}

	// Handle system columns
	if columnName == sql.DialectFieldBlockNumber || columnName == "block_number" {
		if formatted, ok := formatIntegral(value); ok {
			return formatted
		}
		return fmt.Sprint(value)
	}
	if columnName == sql.DialectFieldBlockTimestamp || columnName == "block_timestamp" {
		if t, ok := value.(time.Time); ok {
			return g.formatTimestamp(t)
		}
	}
	if columnName == sql.DialectFieldVersion {
		if formatted, ok := formatIntegral(value); ok {
			return formatted
		}
		return fmt.Sprint(value)
	}
	if columnName == sql.DialectFieldDeleted {
		if b, ok := value.(bool); ok {
			if b {
				return "true"
			}
			return "false"
		}
	}

	// Handle foreign key columns (parent references)
	if strings.HasSuffix(columnName, "_id") {
		return escapeCSVValue(fmt.Sprint(value))
	}

	// Handle table columns
	var column *schema.Column
	for _, col := range table.Columns {
		if col.Name == columnName {
			column = col
			break
		}
	}

	if column == nil {
		// System column or unknown - escape and return
		return escapeCSVValue(fmt.Sprint(value))
	}

	// Format based on actual value type
	switch v := value.(type) {
	case nil:
		return ""
	case string:
		return escapeCSVValue(v)
	case []byte:
		// Different databases expect different formats for binary data in CSV
		return g.formatBinaryData(v)
	case bool:
		if v {
			return "true"
		}
		return "false"
	case time.Time:
		return g.formatTimestamp(v)
	case *time.Time:
		if v != nil {
			return g.formatTimestamp(*v)
		}
		return ""
	case int:
		return strconv.FormatInt(int64(v), 10)
	case int8:
		return strconv.FormatInt(int64(v), 10)
	case int16:
		return strconv.FormatInt(int64(v), 10)
	case int32:
		return strconv.FormatInt(int64(v), 10)
	case int64:
		return strconv.FormatInt(v, 10)
	case uint:
		return strconv.FormatUint(uint64(v), 10)
	case uint8:
		return strconv.FormatUint(uint64(v), 10)
	case uint16:
		return strconv.FormatUint(uint64(v), 10)
	case uint32:
		return strconv.FormatUint(uint64(v), 10)
	case uint64:
		return strconv.FormatUint(v, 10)
	case float32:
		return strconv.FormatFloat(float64(v), 'g', -1, 32)
	case float64:
		return strconv.FormatFloat(v, 'g', -1, 64)
	case *string:
		if v != nil {
			return escapeCSVValue(*v)
		}
		return ""
	default:
		// Check for nil pointers of any type
		if value == nil || (reflect.ValueOf(value).Kind() == reflect.Ptr && reflect.ValueOf(value).IsNil()) {
			return ""
		}
		// For any other types, convert to string and escape
		return escapeCSVValue(fmt.Sprint(value))
	}
}

// formatBinaryData formats binary data according to the dialect's CSV/COPY format requirements
func (g *protoAwareCSVGenerator) formatBinaryData(data []byte) string {
	// Determine the driver type from the dialect
	var driver string
	switch g.dialect.(type) {
	case *postgres.DialectPostgres:
		driver = "postgres"
	case *clickhouse.DialectClickHouse:
		driver = "clickhouse"
	case *risingwave.DialectRisingwave:
		driver = "risingwave"
	default:
		// Fallback to PostgreSQL format
		driver = "postgres"
	}

	switch driver {
	case "postgres", "risingwave":
		// PostgreSQL and RisingWave COPY CSV format expects \xHEX
		// The backslash is literal in the CSV (not escaped)
		return "\\x" + hex.EncodeToString(data)
	case "clickhouse":
		// ClickHouse CSV imports work better with base64 encoded strings
		// This can be decoded using base64Decode() function in ClickHouse
		return base64.StdEncoding.EncodeToString(data)
	default:
		// Default to PostgreSQL format
		return "\\x" + hex.EncodeToString(data)
	}
}

func formatIntegral(value interface{}) (string, bool) {
	switch v := value.(type) {
	case int:
		return strconv.FormatInt(int64(v), 10), true
	case int8:
		return strconv.FormatInt(int64(v), 10), true
	case int16:
		return strconv.FormatInt(int64(v), 10), true
	case int32:
		return strconv.FormatInt(int64(v), 10), true
	case int64:
		return strconv.FormatInt(v, 10), true
	case uint:
		return strconv.FormatUint(uint64(v), 10), true
	case uint8:
		return strconv.FormatUint(uint64(v), 10), true
	case uint16:
		return strconv.FormatUint(uint64(v), 10), true
	case uint32:
		return strconv.FormatUint(uint64(v), 10), true
	case uint64:
		return strconv.FormatUint(v, 10), true
	default:
		return "", false
	}
}

// escapeCSVValue properly escapes a string value for CSV output
func escapeCSVValue(s string) string {
	// If the string contains comma, quote, newline, or carriage return, it needs to be quoted
	if strings.ContainsAny(s, ",\"\n\r") {
		// Replace quotes with double quotes and wrap in quotes
		s = strings.ReplaceAll(s, "\"", "\"\"")
		return "\"" + s + "\""
	}
	return s
}

// close closes all bundlers
func (g *protoAwareCSVGenerator) close() {
	g.closeOnce.Do(func() {
		var wg sync.WaitGroup
		for tableName, b := range g.bundlers {
			g.logger.Debug("shutting down bundler", zap.String("table", tableName))
			wg.Add(1)
			go func(name string, bund *bundler.Bundler) {
				// Trigger bundler shutdown and wait for it to terminate
				bund.Shutdown(nil)
				<-bund.Terminated()
				g.logger.Debug("bundler terminated", zap.String("table", name))
				wg.Done()
			}(tableName, b)
		}
		wg.Wait()
	})
}

// HandleBlockRangeCompletion ensures a clean shutdown when stop block is reached
func (g *protoAwareCSVGenerator) HandleBlockRangeCompletion(ctx context.Context, cursor *sink.Cursor) error {
	g.logger.Info("substreams ended correctly, reached your stop block", zap.Stringer("last_block_seen", cursor.Block()))

	// Write final cursor file for from-proto compatibility: name,cursor
	if g.cursorsStore != nil {
		var buf bytes.Buffer
		buf.WriteString("name,cursor\n")
		row := fmt.Sprintf("cursor,%s\n", cursor)
		buf.WriteString(row)
		if err := g.cursorsStore.WriteObject(ctx, lastCursorFilename, &buf); err != nil {
			return fmt.Errorf("write last cursor file: %w", err)
		}
	}

	// Finalize: if stop block is not aligned to bundle size, roll once past
	// the current boundary to flush the last partial file before shutdown.
	last := cursor.Block().Num()
	trigger := last - (last % g.bundleSize) + g.bundleSize

	for tableName, b := range g.bundlers {
		if rolled, err := b.Roll(ctx, trigger); err != nil {
			if err != bundler.ErrStopBlockReached {
				g.logger.Warn("final roll failed", zap.String("table", tableName), zap.Error(err))
			}
		} else if rolled {
			g.logger.Debug("finalized boundary on completion", zap.String("table", tableName), zap.Uint64("trigger", trigger))
		}
	}

	// Gracefully close all bundlers before returning
	g.close()
	return nil
}
