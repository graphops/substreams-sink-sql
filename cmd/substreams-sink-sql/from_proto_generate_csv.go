package main

import (
	"context"
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"time"
    "sync"

	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/dynamic"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	. "github.com/streamingfast/cli"
	"github.com/streamingfast/cli/sflags"
	"github.com/streamingfast/dstore"
	sink "github.com/streamingfast/substreams-sink"
	"github.com/streamingfast/substreams-sink-sql/db_changes/bundler"
	"github.com/streamingfast/substreams-sink-sql/db_changes/bundler/writer"
	"github.com/streamingfast/substreams-sink-sql/db_changes/db"
	dbproto "github.com/streamingfast/substreams-sink-sql/db_proto/proto"
	"github.com/streamingfast/substreams-sink-sql/proto"
	"github.com/streamingfast/substreams-sink-sql/db_proto/sql"
	clickhouse "github.com/streamingfast/substreams-sink-sql/db_proto/sql/click_house"
	"github.com/streamingfast/substreams-sink-sql/db_proto/sql/postgres"
	"github.com/streamingfast/substreams-sink-sql/db_proto/sql/risingwave"
	"github.com/streamingfast/substreams-sink-sql/db_proto/sql/schema"
	pbSchema "github.com/streamingfast/substreams-sink-sql/pb/sf/substreams/sink/sql/schema/v1"
	pbsql "github.com/streamingfast/substreams-sink-sql/pb/sf/substreams/sink/sql/services/v1"
	sinksql "github.com/streamingfast/substreams-sink-sql"
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
		flags.String("cursors-table", "cursors", "Name of the cursors table")
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
	var statements []string

	// Add static SQL (system tables)
	staticSQL := getStaticSQL(driver, sqlSchema.Name)
	if staticSQL != "" {
		statements = append(statements, staticSQL)
	}

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

	// Sort table names for consistent ordering
	var tableNames []string
	for name := range baseDialect.CreateTableSql {
		tableNames = append(tableNames, name)
	}
	sort.Strings(tableNames)

	for _, tableName := range tableNames {
		createSQL := baseDialect.CreateTableSql[tableName]
		statements = append(statements, createSQL)
	}

	// Add constraints if enabled
	if useConstraints {
		// Primary keys
		for _, constraint := range baseDialect.PrimaryKeySql {
			statements = append(statements, constraint.Sql)
		}

		// Unique constraints
		for _, constraint := range baseDialect.UniqueConstraintSql {
			statements = append(statements, constraint.Sql)
		}

		// Foreign keys
		for _, constraint := range baseDialect.ForeignKeySql {
			statements = append(statements, constraint.Sql)
		}
	}

	// Write to file
	content := strings.Join(statements, "\n\n") + "\n"
	return os.WriteFile(outputPath, []byte(content), 0644)
}

// getStaticSQL returns the static SQL for system tables
func getStaticSQL(driver, schemaName string) string {
	switch driver {
	case "postgres":
		return fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS "%s";

CREATE TABLE IF NOT EXISTS "%s"."_sink_info_" (
	schema_hash TEXT PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS "%s"."_cursor_" (
	name TEXT PRIMARY KEY,
	cursor TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS "%s"."_blocks_" (
	number integer,
	hash TEXT NOT NULL,
	timestamp TIMESTAMP NOT NULL
);`, schemaName, schemaName, schemaName, schemaName)

	case "risingwave":
		// RisingWave uses similar structure to PostgreSQL
		return fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS "%s";

CREATE TABLE IF NOT EXISTS "%s"."_sink_info_" (
	schema_hash TEXT PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS "%s"."_cursor_" (
	name TEXT PRIMARY KEY,
	cursor TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS "%s"."_blocks_" (
	number integer,
	hash TEXT NOT NULL,
	timestamp TIMESTAMP NOT NULL
);`, schemaName, schemaName, schemaName, schemaName)

	case "clickhouse":
		// ClickHouse has different syntax
		return fmt.Sprintf(`CREATE DATABASE IF NOT EXISTS %s;

CREATE TABLE IF NOT EXISTS %s._sink_info_ (
	schema_hash String
) ENGINE = MergeTree()
ORDER BY schema_hash;

CREATE TABLE IF NOT EXISTS %s._cursor_ (
	name String,
	cursor String
) ENGINE = MergeTree()
ORDER BY name;

CREATE TABLE IF NOT EXISTS %s._blocks_ (
	number UInt32,
	hash String,
	timestamp DateTime
) ENGINE = MergeTree()
ORDER BY number;`, schemaName, schemaName, schemaName, schemaName)

	default:
		return ""
	}
}

// SchemaMetadata represents the exported schema metadata
type SchemaMetadata struct {
	Version       string            `json:"version"`
	GeneratedAt   time.Time         `json:"generated_at"`
	Dialect       string            `json:"dialect"`
	SchemaHash    string            `json:"schema_hash"`
	SchemaName    string            `json:"schema_name"`
	Tables        []TableMetadata   `json:"tables"`
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

		// Always add system columns first
		tm.ColumnOrder = append(tm.ColumnOrder, sql.DialectFieldBlockNumber)
		tm.ColumnOrder = append(tm.ColumnOrder, sql.DialectFieldBlockTimestamp)

		// Add version/deleted fields if dialect uses them
		if dialect.UseVersionField() {
			tm.ColumnOrder = append(tm.ColumnOrder, sql.DialectFieldVersion)
		}
		if dialect.UseDeletedField() {
			tm.ColumnOrder = append(tm.ColumnOrder, sql.DialectFieldDeleted)
		}

		// Add table columns
		for i, col := range table.Columns {
			tm.ColumnOrder = append(tm.ColumnOrder, col.Name)
			tm.Columns = append(tm.Columns, ColumnMetadata{
				Name:     col.Name,
				SQLType:  getColumnSQLType(col, dialect, driver),
				Position: i,
				Nullable: true, // Allow nulls for CSV data
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

// protoAwareCSVGenerator generates CSV files with the exact structure from-proto expects
type protoAwareCSVGenerator struct {
    sink           *sink.Sinker
    schema         *schema.Schema
    dialect        sql.Dialect
	rootDescriptor protoreflect.MessageDescriptor
	outputDir      string
	workingDir     string
	bundleSize     uint64
    bufferSize     uint64
    cursorsTable   string
    bundlers       map[string]*bundler.Bundler
    cursorsStore   dstore.Store
    logger         *zap.Logger
    useProtoOptions bool

    closeOnce sync.Once
}

// tableRows holds accumulated rows for a table during message traversal
type tableRows struct {
	tableName string
	rows      []map[string]interface{}
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
		sink:           sink,
		schema:         sqlSchema,
		dialect:        dialect,
		rootDescriptor: rootDescriptor,
		outputDir:      outputDir,
		workingDir:     workingDir,
		bundleSize:     bundleSize,
		bufferSize:     bufferSize,
		cursorsTable:   cursorsTable,
		bundlers:       make(map[string]*bundler.Bundler),
		logger:         logger,
		useProtoOptions: useProtoOptions,
	}

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
	var columns []string

	// Always add system columns first
	columns = append(columns, sql.DialectFieldBlockNumber)
	columns = append(columns, sql.DialectFieldBlockTimestamp)

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

	// Create dynamic message from our root descriptor
	rootDesc, err := desc.WrapMessage(g.rootDescriptor)
	if err != nil {
		return fmt.Errorf("wrapping message descriptor: %w", err)
	}

	msg := dynamic.NewMessage(rootDesc)
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

		for _, row := range tr.rows {
			csvData := g.formatRowForCSV(row, table)
			if _, err := bundler.Writer().Write(csvData); err != nil {
				return fmt.Errorf("writing row to table %s: %w", tr.tableName, err)
			}
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

	// Build the row for this message
	var fieldValues []interface{}
	fieldValues = append(fieldValues, blockNum)
	fieldValues = append(fieldValues, blockTimestamp)

	primaryKeyOffset := 2
	if g.dialect.UseVersionField() {
		fieldValues = append(fieldValues, time.Now().UnixNano())
		primaryKeyOffset += 1
	}

	if g.dialect.UseDeletedField() {
		fieldValues = append(fieldValues, false)
		primaryKeyOffset += 1
	}

	primaryKey := ""
	if tableInfo != nil {
		if table := g.dialect.GetTable(tableInfo.Name); table != nil {
			if table.PrimaryKey != nil {
				primaryKey = table.PrimaryKey.Name
				pkValue := dm.GetFieldByName(primaryKey)
				if pkValue == nil {
					return nil, fmt.Errorf("missing primary key field %q for table %q", primaryKey, tableInfo.Name)
				}
				fieldValues = append(fieldValues, pkValue)
			}
		}
	}

	if parent != nil {
		fieldValues = append(fieldValues, parent.id)
	}

	var childs []*dynamic.Message
	var fieldNames []string

	// Collect field names in order
	fieldNames = append(fieldNames, sql.DialectFieldBlockNumber)
	fieldNames = append(fieldNames, sql.DialectFieldBlockTimestamp)
	if g.dialect.UseVersionField() {
		fieldNames = append(fieldNames, sql.DialectFieldVersion)
	}
	if g.dialect.UseDeletedField() {
		fieldNames = append(fieldNames, sql.DialectFieldDeleted)
	}
	if primaryKey != "" {
		fieldNames = append(fieldNames, primaryKey)
	}
	// Add parent field name if we have a parent
	// We'll figure out the actual column name later when we have table info
	parentFieldIdx := -1
	if parent != nil {
		parentFieldIdx = len(fieldNames)
		fieldNames = append(fieldNames, "") // Placeholder, will be updated later
	}

	// Walk all known fields
	for _, fd := range dm.GetKnownFields() {
		if fd.GetName() == primaryKey {
			continue
		}
		fv := dm.GetField(fd)
		if v, ok := fv.([]interface{}); ok {
			// Repeated field - handle child messages
			for _, c := range v {
				fm, ok := c.(*dynamic.Message)
				if !ok {
					return nil, fmt.Errorf("Repeated fields with native values not supported yet in 'from-proto' mode. message %q, field %q", md.GetFullyQualifiedName(), fd.GetName())
				}
				childs = append(childs, fm)
			}
		} else if fm, ok := fv.(*dynamic.Message); ok {
			if fm == nil {
				continue //un-used oneOf field
			}
			childs = append(childs, fm) //need to be handled after current message inserted
		} else {
			fieldValues = append(fieldValues, fv)
			fieldNames = append(fieldNames, fd.GetName())
		}
	}

	var p *Parent

	// Create row for this table if it has table info
	if tableInfo != nil {
		table := g.dialect.GetTable(tableInfo.Name)
		if table != nil {
			// Update parent field name if this is a child table
			if parentFieldIdx >= 0 && table.ChildOf != nil {
				fieldNames[parentFieldIdx] = table.ChildOf.ParentTableField
			}

			// Create a map for this row
			row := make(map[string]interface{})
			for i, name := range fieldNames {
				if i < len(fieldValues) && name != "" { // Skip empty placeholders
					row[name] = fieldValues[i]
				}
			}

			allRows = append(allRows, &tableRows{
				tableName: table.Name,
				rows:      []map[string]interface{}{row},
			})

			// Set up parent for child tables
			if len(childs) > 0 && g.useProtoOptions {
				if table.PrimaryKey == nil {
					return nil, fmt.Errorf("table %q has no primary key and has %d associated children table", table.Name, len(childs))
				}
				// Primary key value is always at primaryKeyOffset position in fieldValues
				id := fieldValues[primaryKeyOffset]
				p = &Parent{
					field: strings.ToLower(md.GetName()),
					id:    id,
				}
			}
		}
	}

	// Process all child messages
	for _, fm := range childs {
		childRows, err := g.walkMessageAndCollectRows(fm, blockNum, blockTimestamp, p)
		if err != nil {
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

// formatRowForCSV formats a row for CSV output
func (g *protoAwareCSVGenerator) formatRowForCSV(row map[string]interface{}, table *schema.Table) []byte {
	columns := g.getColumnsForTable(table)
	values := make([]string, len(columns))

	for i, col := range columns {
		value := row[col]
		values[i] = g.formatValue(value, col, table)
	}

	return []byte(strings.Join(values, ",") + "\n")
}

// formatValue formats a value for CSV based on its SQL type
func (g *protoAwareCSVGenerator) formatValue(value interface{}, columnName string, table *schema.Table) string {
	if value == nil {
		return ""
	}

	// Handle system columns
	if columnName == sql.DialectFieldBlockNumber {
		return fmt.Sprintf("%d", value)
	}
	if columnName == sql.DialectFieldBlockTimestamp {
		if t, ok := value.(time.Time); ok {
			return t.Format(time.RFC3339)
		}
	}
	if columnName == sql.DialectFieldVersion {
		return fmt.Sprintf("%d", value)
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
		return escapeCSVValue(fmt.Sprintf("%v", value))
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
		return escapeCSVValue(fmt.Sprintf("%v", value))
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
		return v.Format(time.RFC3339)
	case *time.Time:
		if v != nil {
			return v.Format(time.RFC3339)
		}
		return ""
	case int, int8, int16, int32, int64:
		return fmt.Sprintf("%d", v)
	case uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%d", v)
	case float32, float64:
		return fmt.Sprintf("%g", v) // %g removes trailing zeros
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
		return escapeCSVValue(fmt.Sprintf("%v", value))
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
		return fmt.Sprintf("\\x%x", data)
	case "clickhouse":
		// ClickHouse CSV imports work better with base64 encoded strings
		// This can be decoded using base64Decode() function in ClickHouse
		return base64.StdEncoding.EncodeToString(data)
	default:
		// Default to PostgreSQL format
		return fmt.Sprintf("\\x%x", data)
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

    // Write final cursor file like generate-csv (single file, sorted columns)
    if g.cursorsStore != nil {
        var buf bytes.Buffer
        cols := []string{"block_id", "block_num", "cursor", "id"}
        sort.Strings(cols)
        buf.WriteString(strings.Join(cols, ","))
        buf.WriteString("\n")

        block := cursor.Block()
        // Values must follow the same alphabetical order as columns
        row := fmt.Sprintf("%s,%d,%s,%s\n", block.ID(), block.Num(), cursor, g.sink.OutputModuleHash())
        buf.WriteString(row)

        if err := g.cursorsStore.WriteObject(ctx, lastCursorFilename, &buf); err != nil {
            return fmt.Errorf("write last cursor file: %w", err)
        }
    }

    // Finalize: if stop block is not aligned to bundle size, roll once past
    // the current boundary to flush the last partial file before shutdown.
    last := cursor.Block().Num()
    trigger := last - (last%g.bundleSize) + g.bundleSize

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
