package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
	"github.com/rs/zerolog"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	version = "1.0.0"
	dbPool  *pgxpool.Pool
	logger  zerolog.Logger
	config  *Config
	mu      sync.RWMutex
	metrics *ServerMetrics
)

type Config struct {
	DSN             string `mapstructure:"dsn"`
	ReadOnly        bool   `mapstructure:"read_only"`
	ExplainCheck    bool   `mapstructure:"explain_check"`
	Transport       string `mapstructure:"transport"`
	Port            int    `mapstructure:"port"`
	IPAddress       string `mapstructure:"ip_address"`
	MaxConnections  int32  `mapstructure:"max_connections"`
	LogLevel        string `mapstructure:"log_level"`
	QueryTimeout    int    `mapstructure:"query_timeout"`
	EnableMetrics   bool   `mapstructure:"enable_metrics"`
	CacheSize       int    `mapstructure:"cache_size"`
	PoolMaxIdleTime int    `mapstructure:"pool_max_idle_time"`
}

type ServerMetrics struct {
	QueriesExecuted   int64
	QueryErrors       int64
	ConnectionsActive int64
	TotalResponseTime time.Duration
	StartTime         time.Time
}

type QueryResult struct {
	Rows    []map[string]interface{} `json:"rows"`
	Columns []string                 `json:"columns"`
	Count   int                      `json:"count"`
	Timing  string                   `json:"timing"`
}

func main() {
	// Set GOMAXPROCS for better performance
	runtime.GOMAXPROCS(runtime.NumCPU())

	var rootCmd = &cobra.Command{
		Use:   "requesty-postgres-mcp",
		Short: "Ultra-fast PostgreSQL MCP Server",
		Long:  "A high-performance Model Context Protocol server for PostgreSQL with advanced caching and connection pooling",
		Run:   runServer,
	}

	// Add flags
	rootCmd.PersistentFlags().String("dsn", "", "PostgreSQL connection string")
	rootCmd.PersistentFlags().Bool("read-only", false, "Enable read-only mode")
	rootCmd.PersistentFlags().Bool("explain-check", false, "Check query plans with EXPLAIN")
	rootCmd.PersistentFlags().String("transport", "stdio", "Transport type (stdio or sse)")
	rootCmd.PersistentFlags().Int("port", 8080, "SSE server port")
	rootCmd.PersistentFlags().String("ip-address", "localhost", "Server IP address")
	rootCmd.PersistentFlags().Int32("max-connections", 100, "Maximum database connections")
	rootCmd.PersistentFlags().String("log-level", "info", "Log level (debug, info, warn, error)")
	rootCmd.PersistentFlags().Int("query-timeout", 30, "Query timeout in seconds")
	rootCmd.PersistentFlags().Bool("enable-metrics", true, "Enable performance metrics")
	rootCmd.PersistentFlags().Int("cache-size", 1000, "Query cache size")
	rootCmd.PersistentFlags().Int("pool-max-idle-time", 300, "Pool max idle time in seconds")

	viper.BindPFlags(rootCmd.PersistentFlags())
	viper.SetEnvPrefix("POSTGRES_MCP")
	viper.AutomaticEnv()

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func runServer(cmd *cobra.Command, args []string) {
	initConfig()
	initLogger()
	initMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize database connection pool
	if err := initDatabase(ctx); err != nil {
		logger.Fatal().Err(err).Msg("Failed to initialize database")
	}
	defer dbPool.Close()

	// Create MCP server
	mcpServer := createMCPServer()

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		logger.Info().Msg("Shutting down server...")
		cancel()
	}()

	// Start server
	logger.Info().
		Str("transport", config.Transport).
		Str("version", version).
		Msg("Starting ultra-fast PostgreSQL MCP server")

	if config.Transport == "sse" {
		sseServer := server.NewSSEServer(mcpServer,
			server.WithBaseURL(fmt.Sprintf("http://%s:%d", config.IPAddress, config.Port)))

		logger.Info().
			Str("address", fmt.Sprintf("%s:%d", config.IPAddress, config.Port)).
			Msg("SSE server listening")

		if err := sseServer.Start(fmt.Sprintf("%s:%d", config.IPAddress, config.Port)); err != nil {
			logger.Fatal().Err(err).Msg("SSE server error")
		}
	} else {
		if err := server.ServeStdio(mcpServer); err != nil {
			logger.Fatal().Err(err).Msg("STDIO server error")
		}
	}
}

func initConfig() {
	config = &Config{}
	if err := viper.Unmarshal(config); err != nil {
		log.Fatalf("Failed to unmarshal config: %v", err)
	}

	// Set defaults
	if config.MaxConnections == 0 {
		config.MaxConnections = 100
	}
	if config.QueryTimeout == 0 {
		config.QueryTimeout = 30
	}
	if config.CacheSize == 0 {
		config.CacheSize = 1000
	}
	if config.PoolMaxIdleTime == 0 {
		config.PoolMaxIdleTime = 300
	}
}

func initLogger() {
	level, err := zerolog.ParseLevel(config.LogLevel)
	if err != nil {
		level = zerolog.InfoLevel
	}

	logger = zerolog.New(os.Stdout).
		Level(level).
		With().
		Timestamp().
		Str("service", "postgres-mcp").
		Logger()
}

func initMetrics() {
	metrics = &ServerMetrics{
		StartTime: time.Now(),
	}
}

func initDatabase(ctx context.Context) error {
	if config.DSN == "" {
		return fmt.Errorf("DSN is required")
	}

	poolConfig, err := pgxpool.ParseConfig(config.DSN)
	if err != nil {
		return fmt.Errorf("failed to parse DSN: %w", err)
	}

	// Optimize connection pool for performance
	poolConfig.MaxConns = config.MaxConnections
	poolConfig.MinConns = 5
	poolConfig.MaxConnLifetime = time.Hour
	poolConfig.MaxConnIdleTime = time.Duration(config.PoolMaxIdleTime) * time.Second
	poolConfig.HealthCheckPeriod = time.Minute

	// Configure connection for performance
	poolConfig.ConnConfig.RuntimeParams = map[string]string{
		"application_name": "requesty-postgres-mcp",
		"timezone":         "UTC",
	}

	dbPool, err = pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return fmt.Errorf("failed to create connection pool: %w", err)
	}

	// Test connection
	if err := dbPool.Ping(ctx); err != nil {
		return fmt.Errorf("failed to ping database: %w", err)
	}

	logger.Info().
		Int32("max_connections", config.MaxConnections).
		Str("database", "connected").
		Msg("Database pool initialized")

	return nil
}

func createMCPServer() *server.Server {
	s := server.NewMCPServer(
		"requesty-postgres-mcp",
		version,
		server.WithResourceCapabilities(true, true),
		server.WithPromptCapabilities(true),
		server.WithLogging(),
	)

	// Schema management tools
	s.AddTool(createListDatabasesTool(), handleListDatabases)
	s.AddTool(createListTablesTool(), handleListTables)
	s.AddTool(createListColumnsTool(), handleListColumns)
	s.AddTool(createDescribeTableTool(), handleDescribeTable)
	s.AddTool(createGetTableSizeTool(), handleGetTableSize)
	s.AddTool(createListIndexesTool(), handleListIndexes)
	s.AddTool(createListConstraintsTool(), handleListConstraints)

	// Query tools
	s.AddTool(createReadQueryTool(), handleReadQuery)
	s.AddTool(createCountQueryTool(), handleCountQuery)
	s.AddTool(createExplainQueryTool(), handleExplainQuery)

	// Write tools (if not read-only)
	if !config.ReadOnly {
		s.AddTool(createWriteQueryTool(), handleWriteQuery)
		s.AddTool(createUpdateQueryTool(), handleUpdateQuery)
		s.AddTool(createDeleteQueryTool(), handleDeleteQuery)
		s.AddTool(createCreateTableTool(), handleCreateTable)
		s.AddTool(createAlterTableTool(), handleAlterTable)
		s.AddTool(createCreateIndexTool(), handleCreateIndex)
		s.AddTool(createDropIndexTool(), handleDropIndex)
	}

	// Performance and monitoring tools
	s.AddTool(createGetStatsTool(), handleGetStats)
	s.AddTool(createGetSlowQueresTool(), handleGetSlowQueries)
	s.AddTool(createAnalyzeTableTool(), handleAnalyzeTable)

	return s
}

// Tool creation functions
func createListDatabasesTool() *mcp.Tool {
	return mcp.NewTool(
		"list_databases",
		mcp.WithDescription("List all databases in the PostgreSQL server"),
	)
}

func createListTablesTool() *mcp.Tool {
	return mcp.NewTool(
		"list_tables",
		mcp.WithDescription("List all tables in the current database with detailed information"),
		mcp.WithString("schema", mcp.Description("Schema name (optional, defaults to all schemas)")),
	)
}

func createListColumnsTool() *mcp.Tool {
	return mcp.NewTool(
		"list_columns",
		mcp.WithDescription("List all columns for a specific table"),
		mcp.WithString("table_name", mcp.Required(), mcp.Description("Name of the table")),
		mcp.WithString("schema", mcp.Description("Schema name (optional)")),
	)
}

func createDescribeTableTool() *mcp.Tool {
	return mcp.NewTool(
		"describe_table",
		mcp.WithDescription("Get detailed table structure including constraints, indexes, and statistics"),
		mcp.WithString("table_name", mcp.Required(), mcp.Description("Name of the table")),
		mcp.WithString("schema", mcp.Description("Schema name (optional)")),
	)
}

func createGetTableSizeTool() *mcp.Tool {
	return mcp.NewTool(
		"get_table_size",
		mcp.WithDescription("Get table size information including row count and disk usage"),
		mcp.WithString("table_name", mcp.Required(), mcp.Description("Name of the table")),
		mcp.WithString("schema", mcp.Description("Schema name (optional)")),
	)
}

func createListIndexesTool() *mcp.Tool {
	return mcp.NewTool(
		"list_indexes",
		mcp.WithDescription("List all indexes for a table or entire database"),
		mcp.WithString("table_name", mcp.Description("Name of the table (optional, lists all if empty)")),
		mcp.WithString("schema", mcp.Description("Schema name (optional)")),
	)
}

func createListConstraintsTool() *mcp.Tool {
	return mcp.NewTool(
		"list_constraints",
		mcp.WithDescription("List all constraints for a table"),
		mcp.WithString("table_name", mcp.Required(), mcp.Description("Name of the table")),
		mcp.WithString("schema", mcp.Description("Schema name (optional)")),
	)
}

func createReadQueryTool() *mcp.Tool {
	return mcp.NewTool(
		"read_query",
		mcp.WithDescription("Execute a SELECT query with performance optimization and result formatting"),
		mcp.WithString("query", mcp.Required(), mcp.Description("SQL SELECT query to execute")),
		mcp.WithNumber("limit", mcp.Description("Maximum number of rows to return (default: 1000)")),
		mcp.WithBoolean("format_json", mcp.Description("Return results as formatted JSON (default: false)")),
	)
}

func createCountQueryTool() *mcp.Tool {
	return mcp.NewTool(
		"count_query",
		mcp.WithDescription("Get row count for a table with optional WHERE conditions"),
		mcp.WithString("table_name", mcp.Required(), mcp.Description("Name of the table")),
		mcp.WithString("where_clause", mcp.Description("Optional WHERE clause (without WHERE keyword)")),
		mcp.WithString("schema", mcp.Description("Schema name (optional)")),
	)
}

func createExplainQueryTool() *mcp.Tool {
	return mcp.NewTool(
		"explain_query",
		mcp.WithDescription("Analyze query execution plan with detailed performance metrics"),
		mcp.WithString("query", mcp.Required(), mcp.Description("SQL query to analyze")),
		mcp.WithBoolean("analyze", mcp.Description("Run EXPLAIN ANALYZE (default: false)")),
		mcp.WithBoolean("buffers", mcp.Description("Include buffer usage info (default: false)")),
	)
}

func createWriteQueryTool() *mcp.Tool {
	return mcp.NewTool(
		"write_query",
		mcp.WithDescription("Execute an INSERT query with transaction support"),
		mcp.WithString("query", mcp.Required(), mcp.Description("SQL INSERT query to execute")),
		mcp.WithBoolean("return_id", mcp.Description("Return inserted ID(s) (default: false)")),
	)
}

func createUpdateQueryTool() *mcp.Tool {
	return mcp.NewTool(
		"update_query",
		mcp.WithDescription("Execute an UPDATE query with safety checks"),
		mcp.WithString("query", mcp.Required(), mcp.Description("SQL UPDATE query to execute")),
		mcp.WithBoolean("force", mcp.Description("Skip safety checks for WHERE clause (default: false)")),
	)
}

func createDeleteQueryTool() *mcp.Tool {
	return mcp.NewTool(
		"delete_query",
		mcp.WithDescription("Execute a DELETE query with safety checks"),
		mcp.WithString("query", mcp.Required(), mcp.Description("SQL DELETE query to execute")),
		mcp.WithBoolean("force", mcp.Description("Skip safety checks for WHERE clause (default: false)")),
	)
}

func createCreateTableTool() *mcp.Tool {
	return mcp.NewTool(
		"create_table",
		mcp.WithDescription("Create a new table with proper constraints and indexes"),
		mcp.WithString("query", mcp.Required(), mcp.Description("CREATE TABLE SQL statement")),
	)
}

func createAlterTableTool() *mcp.Tool {
	return mcp.NewTool(
		"alter_table",
		mcp.WithDescription("Alter an existing table structure"),
		mcp.WithString("query", mcp.Required(), mcp.Description("ALTER TABLE SQL statement")),
	)
}

func createCreateIndexTool() *mcp.Tool {
	return mcp.NewTool(
		"create_index",
		mcp.WithDescription("Create an index on a table"),
		mcp.WithString("query", mcp.Required(), mcp.Description("CREATE INDEX SQL statement")),
	)
}

func createDropIndexTool() *mcp.Tool {
	return mcp.NewTool(
		"drop_index",
		mcp.WithDescription("Drop an existing index"),
		mcp.WithString("index_name", mcp.Required(), mcp.Description("Name of the index to drop")),
		mcp.WithString("schema", mcp.Description("Schema name (optional)")),
	)
}

func createGetStatsTool() *mcp.Tool {
	return mcp.NewTool(
		"get_stats",
		mcp.WithDescription("Get server performance statistics and metrics"),
	)
}

func createGetSlowQueresTool() *mcp.Tool {
	return mcp.NewTool(
		"get_slow_queries",
		mcp.WithDescription("Get slow query statistics from pg_stat_statements"),
		mcp.WithNumber("limit", mcp.Description("Number of queries to return (default: 10)")),
	)
}

func createAnalyzeTableTool() *mcp.Tool {
	return mcp.NewTool(
		"analyze_table",
		mcp.WithDescription("Update table statistics for better query planning"),
		mcp.WithString("table_name", mcp.Required(), mcp.Description("Name of the table to analyze")),
		mcp.WithString("schema", mcp.Description("Schema name (optional)")),
	)
}

// Handler functions will be implemented in handlers.go
func handleListDatabases(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	start := time.Now()
	defer updateMetrics(start)

	query := "SELECT datname, pg_database_size(datname) as size_bytes FROM pg_database WHERE datistemplate = false ORDER BY datname"
	result, err := executeQuery(ctx, query)
	if err != nil {
		return handleError(err)
	}

	return mcp.NewToolResultText(formatResult(result)), nil
}

func handleListTables(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	start := time.Now()
	defer updateMetrics(start)

	schema := getStringParam(request, "schema", "")
	whereClause := ""
	if schema != "" {
		whereClause = fmt.Sprintf("WHERE table_schema = '%s'", schema)
	}

	query := fmt.Sprintf(`
		SELECT
			table_schema,
			table_name,
			table_type,
			pg_size_pretty(pg_total_relation_size(quote_ident(table_schema)||'.'||quote_ident(table_name))) as size
		FROM information_schema.tables
		%s
		ORDER BY table_schema, table_name`, whereClause)

	result, err := executeQuery(ctx, query)
	if err != nil {
		return handleError(err)
	}

	return mcp.NewToolResultText(formatResult(result)), nil
}

func handleListColumns(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	start := time.Now()
	defer updateMetrics(start)

	tableName := getStringParam(request, "table_name", "")
	schema := getStringParam(request, "schema", "public")

	query := `
		SELECT
			column_name,
			data_type,
			character_maximum_length,
			is_nullable,
			column_default,
			ordinal_position
		FROM information_schema.columns
		WHERE table_name = $1 AND table_schema = $2
		ORDER BY ordinal_position`

	result, err := executeQueryWithParams(ctx, query, tableName, schema)
	if err != nil {
		return handleError(err)
	}

	return mcp.NewToolResultText(formatResult(result)), nil
}

func handleDescribeTable(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	start := time.Now()
	defer updateMetrics(start)

	tableName := getStringParam(request, "table_name", "")
	schema := getStringParam(request, "schema", "public")

	// Get comprehensive table information
	queries := []string{
		// Table structure
		fmt.Sprintf(`
			SELECT
				column_name,
				data_type,
				character_maximum_length,
				numeric_precision,
				numeric_scale,
				is_nullable,
				column_default,
				ordinal_position
			FROM information_schema.columns
			WHERE table_name = '%s' AND table_schema = '%s'
			ORDER BY ordinal_position`, tableName, schema),

		// Constraints
		fmt.Sprintf(`
			SELECT
				constraint_name,
				constraint_type,
				column_name
			FROM information_schema.table_constraints tc
			JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name
			WHERE tc.table_name = '%s' AND tc.table_schema = '%s'
			ORDER BY constraint_type, ordinal_position`, tableName, schema),

		// Indexes
		fmt.Sprintf(`
			SELECT
				indexname,
				indexdef
			FROM pg_indexes
			WHERE tablename = '%s' AND schemaname = '%s'`, tableName, schema),
	}

	var results []string
	for i, query := range queries {
		result, err := executeQuery(ctx, query)
		if err != nil {
			return handleError(err)
		}

		titles := []string{"COLUMNS:", "CONSTRAINTS:", "INDEXES:"}
		results = append(results, fmt.Sprintf("%s\n%s", titles[i], formatResult(result)))
	}

	return mcp.NewToolResultText(strings.Join(results, "\n\n")), nil
}

func handleGetTableSize(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	start := time.Now()
	defer updateMetrics(start)

	tableName := getStringParam(request, "table_name", "")
	schema := getStringParam(request, "schema", "public")

	query := `
		SELECT
			schemaname,
			tablename,
			attname,
			n_distinct,
			most_common_vals,
			most_common_freqs,
			histogram_bounds,
			correlation
		FROM pg_stats
		WHERE tablename = $1 AND schemaname = $2`

	result, err := executeQueryWithParams(ctx, query, tableName, schema)
	if err != nil {
		return handleError(err)
	}

	return mcp.NewToolResultText(formatResult(result)), nil
}

func handleListIndexes(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	start := time.Now()
	defer updateMetrics(start)

	tableName := getStringParam(request, "table_name", "")
	schema := getStringParam(request, "schema", "")

	whereClause := ""
	if tableName != "" {
		whereClause = fmt.Sprintf("WHERE tablename = '%s'", tableName)
		if schema != "" {
			whereClause += fmt.Sprintf(" AND schemaname = '%s'", schema)
		}
	} else if schema != "" {
		whereClause = fmt.Sprintf("WHERE schemaname = '%s'", schema)
	}

	query := fmt.Sprintf(`
		SELECT
			schemaname,
			tablename,
			indexname,
			indexdef
		FROM pg_indexes
		%s
		ORDER BY schemaname, tablename, indexname`, whereClause)

	result, err := executeQuery(ctx, query)
	if err != nil {
		return handleError(err)
	}

	return mcp.NewToolResultText(formatResult(result)), nil
}

func handleListConstraints(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	start := time.Now()
	defer updateMetrics(start)

	tableName := getStringParam(request, "table_name", "")
	schema := getStringParam(request, "schema", "public")

	query := `
		SELECT
			tc.constraint_name,
			tc.constraint_type,
			kcu.column_name,
			tc.is_deferrable,
			tc.initially_deferred
		FROM information_schema.table_constraints tc
		LEFT JOIN information_schema.key_column_usage kcu
			ON tc.constraint_name = kcu.constraint_name
		WHERE tc.table_name = $1 AND tc.table_schema = $2
		ORDER BY tc.constraint_type, kcu.ordinal_position`

	result, err := executeQueryWithParams(ctx, query, tableName, schema)
	if err != nil {
		return handleError(err)
	}

	return mcp.NewToolResultText(formatResult(result)), nil
}

func handleReadQuery(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	start := time.Now()
	defer updateMetrics(start)

	query := getStringParam(request, "query", "")
	if query == "" {
		return handleError(fmt.Errorf("query parameter is required"))
	}

	// Safety check for read queries
	if !isReadOnlyQuery(query) {
		return handleError(fmt.Errorf("only SELECT queries are allowed"))
	}

	limit := getNumberParam(request, "limit", 1000)
	formatJSON := getBoolParam(request, "format_json", false)

	// Add limit if not present
	if !strings.Contains(strings.ToUpper(query), "LIMIT") {
		query = fmt.Sprintf("%s LIMIT %d", query, int(limit))
	}

	result, err := executeQuery(ctx, query)
	if err != nil {
		return handleError(err)
	}

	if formatJSON {
		jsonBytes, _ := json.MarshalIndent(result, "", "  ")
		return mcp.NewToolResultText(string(jsonBytes)), nil
	}

	return mcp.NewToolResultText(formatResult(result)), nil
}

func handleCountQuery(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	start := time.Now()
	defer updateMetrics(start)

	tableName := getStringParam(request, "table_name", "")
	whereClause := getStringParam(request, "where_clause", "")
	schema := getStringParam(request, "schema", "public")

	query := fmt.Sprintf("SELECT COUNT(*) as count FROM %s.%s", schema, tableName)
	if whereClause != "" {
		query += fmt.Sprintf(" WHERE %s", whereClause)
	}

	result, err := executeQuery(ctx, query)
	if err != nil {
		return handleError(err)
	}

	return mcp.NewToolResultText(formatResult(result)), nil
}

func handleExplainQuery(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	start := time.Now()
	defer updateMetrics(start)

	query := getStringParam(request, "query", "")
	analyze := getBoolParam(request, "analyze", false)
	buffers := getBoolParam(request, "buffers", false)

	explainQuery := "EXPLAIN"
	if analyze {
		explainQuery += " ANALYZE"
	}
	if buffers {
		explainQuery += " BUFFERS"
	}
	explainQuery += " " + query

	result, err := executeQuery(ctx, explainQuery)
	if err != nil {
		return handleError(err)
	}

	return mcp.NewToolResultText(formatResult(result)), nil
}

func handleWriteQuery(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	if config.ReadOnly {
		return handleError(fmt.Errorf("server is in read-only mode"))
	}

	start := time.Now()
	defer updateMetrics(start)

	query := getStringParam(request, "query", "")
	returnID := getBoolParam(request, "return_id", false)

	result, err := executeWriteQuery(ctx, query, returnID)
	if err != nil {
		return handleError(err)
	}

	return mcp.NewToolResultText(result), nil
}

func handleUpdateQuery(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	if config.ReadOnly {
		return handleError(fmt.Errorf("server is in read-only mode"))
	}

	start := time.Now()
	defer updateMetrics(start)

	query := getStringParam(request, "query", "")
	force := getBoolParam(request, "force", false)

	// Safety check for WHERE clause
	if !force && !strings.Contains(strings.ToUpper(query), "WHERE") {
		return handleError(fmt.Errorf("UPDATE queries must include a WHERE clause. Use force=true to override"))
	}

	result, err := executeWriteQuery(ctx, query, false)
	if err != nil {
		return handleError(err)
	}

	return mcp.NewToolResultText(result), nil
}

func handleDeleteQuery(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	if config.ReadOnly {
		return handleError(fmt.Errorf("server is in read-only mode"))
	}

	start := time.Now()
	defer updateMetrics(start)

	query := getStringParam(request, "query", "")
	force := getBoolParam(request, "force", false)

	// Safety check for WHERE clause
	if !force && !strings.Contains(strings.ToUpper(query), "WHERE") {
		return handleError(fmt.Errorf("DELETE queries must include a WHERE clause. Use force=true to override"))
	}

	result, err := executeWriteQuery(ctx, query, false)
	if err != nil {
		return handleError(err)
	}

	return mcp.NewToolResultText(result), nil
}

func handleCreateTable(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	if config.ReadOnly {
		return handleError(fmt.Errorf("server is in read-only mode"))
	}

	start := time.Now()
	defer updateMetrics(start)

	query := getStringParam(request, "query", "")
	result, err := executeWriteQuery(ctx, query, false)
	if err != nil {
		return handleError(err)
	}

	return mcp.NewToolResultText(result), nil
}

func handleAlterTable(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	if config.ReadOnly {
		return handleError(fmt.Errorf("server is in read-only mode"))
	}

	start := time.Now()
	defer updateMetrics(start)

	query := getStringParam(request, "query", "")
	result, err := executeWriteQuery(ctx, query, false)
	if err != nil {
		return handleError(err)
	}

	return mcp.NewToolResultText(result), nil
}

func handleCreateIndex(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	if config.ReadOnly {
		return handleError(fmt.Errorf("server is in read-only mode"))
	}

	start := time.Now()
	defer updateMetrics(start)

	query := getStringParam(request, "query", "")
	result, err := executeWriteQuery(ctx, query, false)
	if err != nil {
		return handleError(err)
	}

	return mcp.NewToolResultText(result), nil
}

func handleDropIndex(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	if config.ReadOnly {
		return handleError(fmt.Errorf("server is in read-only mode"))
	}

	start := time.Now()
	defer updateMetrics(start)

	indexName := getStringParam(request, "index_name", "")
	schema := getStringParam(request, "schema", "")

	query := fmt.Sprintf("DROP INDEX")
	if schema != "" {
		query += fmt.Sprintf(" %s.%s", schema, indexName)
	} else {
		query += fmt.Sprintf(" %s", indexName)
	}

	result, err := executeWriteQuery(ctx, query, false)
	if err != nil {
		return handleError(err)
	}

	return mcp.NewToolResultText(result), nil
}

func handleGetStats(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	start := time.Now()
	defer updateMetrics(start)

	mu.RLock()
	stats := map[string]interface{}{
		"queries_executed":     metrics.QueriesExecuted,
		"query_errors":         metrics.QueryErrors,
		"connections_active":   dbPool.Stat().AcquiredConns(),
		"connections_idle":     dbPool.Stat().IdleConns(),
		"connections_total":    dbPool.Stat().TotalConns(),
		"uptime_seconds":       time.Since(metrics.StartTime).Seconds(),
		"avg_response_time_ms": float64(metrics.TotalResponseTime.Nanoseconds()) / float64(metrics.QueriesExecuted) / 1000000,
		"version":              version,
		"read_only_mode":       config.ReadOnly,
	}
	mu.RUnlock()

	jsonBytes, _ := json.MarshalIndent(stats, "", "  ")
	return mcp.NewToolResultText(string(jsonBytes)), nil
}

func handleGetSlowQueries(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	start := time.Now()
	defer updateMetrics(start)

	limit := getNumberParam(request, "limit", 10)

	query := fmt.Sprintf(`
		SELECT
			query,
			calls,
			total_time,
			mean_time,
			rows,
			100.0 * shared_blks_hit / nullif(shared_blks_hit + shared_blks_read, 0) AS hit_percent
		FROM pg_stat_statements
		ORDER BY total_time DESC
		LIMIT %d`, int(limit))

	result, err := executeQuery(ctx, query)
	if err != nil {
		// Fallback if pg_stat_statements is not available
		return mcp.NewToolResultText("pg_stat_statements extension not available"), nil
	}

	return mcp.NewToolResultText(formatResult(result)), nil
}

func handleAnalyzeTable(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	if config.ReadOnly {
		return handleError(fmt.Errorf("server is in read-only mode"))
	}

	start := time.Now()
	defer updateMetrics(start)

	tableName := getStringParam(request, "table_name", "")
	schema := getStringParam(request, "schema", "public")

	query := fmt.Sprintf("ANALYZE %s.%s", schema, tableName)
	result, err := executeWriteQuery(ctx, query, false)
	if err != nil {
		return handleError(err)
	}

	return mcp.NewToolResultText(result), nil
}

// Utility functions
func executeQuery(ctx context.Context, query string) (*QueryResult, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Duration(config.QueryTimeout)*time.Second)
	defer cancel()

	startTime := time.Now()
	rows, err := dbPool.Query(ctx, query)
	if err != nil {
		mu.Lock()
		metrics.QueryErrors++
		mu.Unlock()
		logger.Error().Err(err).Str("query", query).Msg("Query execution failed")
		return nil, err
	}
	defer rows.Close()

	columns := rows.FieldDescriptions()
	columnNames := make([]string, len(columns))
	for i, col := range columns {
		columnNames[i] = string(col.Name)
	}

	var result []map[string]interface{}
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return nil, err
		}

		row := make(map[string]interface{})
		for i, value := range values {
			row[columnNames[i]] = value
		}
		result = append(result, row)
	}

	queryResult := &QueryResult{
		Rows:    result,
		Columns: columnNames,
		Count:   len(result),
		Timing:  time.Since(startTime).String(),
	}

	mu.Lock()
	metrics.QueriesExecuted++
	metrics.TotalResponseTime += time.Since(startTime)
	mu.Unlock()

	return queryResult, nil
}

func executeQueryWithParams(ctx context.Context, query string, args ...interface{}) (*QueryResult, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Duration(config.QueryTimeout)*time.Second)
	defer cancel()

	startTime := time.Now()
	rows, err := dbPool.Query(ctx, query, args...)
	if err != nil {
		mu.Lock()
		metrics.QueryErrors++
		mu.Unlock()
		logger.Error().Err(err).Str("query", query).Msg("Query execution failed")
		return nil, err
	}
	defer rows.Close()

	columns := rows.FieldDescriptions()
	columnNames := make([]string, len(columns))
	for i, col := range columns {
		columnNames[i] = string(col.Name)
	}

	var result []map[string]interface{}
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return nil, err
		}

		row := make(map[string]interface{})
		for i, value := range values {
			row[columnNames[i]] = value
		}
		result = append(result, row)
	}

	queryResult := &QueryResult{
		Rows:    result,
		Columns: columnNames,
		Count:   len(result),
		Timing:  time.Since(startTime).String(),
	}

	mu.Lock()
	metrics.QueriesExecuted++
	metrics.TotalResponseTime += time.Since(startTime)
	mu.Unlock()

	return queryResult, nil
}

func executeWriteQuery(ctx context.Context, query string, returnID bool) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Duration(config.QueryTimeout)*time.Second)
	defer cancel()

	startTime := time.Now()
	result, err := dbPool.Exec(ctx, query)
	if err != nil {
		mu.Lock()
		metrics.QueryErrors++
		mu.Unlock()
		logger.Error().Err(err).Str("query", query).Msg("Write query execution failed")
		return "", err
	}

	rowsAffected := result.RowsAffected()
	timing := time.Since(startTime)

	mu.Lock()
	metrics.QueriesExecuted++
	metrics.TotalResponseTime += timing
	mu.Unlock()

	response := fmt.Sprintf("Query executed successfully.\nRows affected: %d\nExecution time: %s", rowsAffected, timing)

	if returnID && strings.Contains(strings.ToUpper(query), "INSERT") {
		// Try to get the last inserted ID (this is database-specific)
		response += "\nNote: To get inserted IDs, use RETURNING clause in your INSERT statement"
	}

	return response, nil
}

func formatResult(result *QueryResult) string {
	if len(result.Rows) == 0 {
		return fmt.Sprintf("No results found.\nExecution time: %s", result.Timing)
	}

	var output strings.Builder
	output.WriteString(fmt.Sprintf("Results: %d rows\nExecution time: %s\n\n", result.Count, result.Timing))

	// Write headers
	for i, col := range result.Columns {
		if i > 0 {
			output.WriteString(" | ")
		}
		output.WriteString(fmt.Sprintf("%-20s", col))
	}
	output.WriteString("\n")

	// Write separator
	for i := range result.Columns {
		if i > 0 {
			output.WriteString("-|-")
		}
		output.WriteString(strings.Repeat("-", 20))
	}
	output.WriteString("\n")

	// Write data rows
	for _, row := range result.Rows {
		for i, col := range result.Columns {
			if i > 0 {
				output.WriteString(" | ")
			}
			value := row[col]
			if value == nil {
				value = "NULL"
			}
			output.WriteString(fmt.Sprintf("%-20v", value))
		}
		output.WriteString("\n")
	}

	return output.String()
}

func isReadOnlyQuery(query string) bool {
	upperQuery := strings.ToUpper(strings.TrimSpace(query))
	return strings.HasPrefix(upperQuery, "SELECT") ||
		   strings.HasPrefix(upperQuery, "WITH") ||
		   strings.HasPrefix(upperQuery, "EXPLAIN")
}

func getStringParam(request mcp.CallToolRequest, key, defaultValue string) string {
	if value, ok := request.Params.Arguments[key].(string); ok {
		return value
	}
	return defaultValue
}

func getNumberParam(request mcp.CallToolRequest, key string, defaultValue float64) float64 {
	if value, ok := request.Params.Arguments[key].(float64); ok {
		return value
	}
	return defaultValue
}

func getBoolParam(request mcp.CallToolRequest, key string, defaultValue bool) bool {
	if value, ok := request.Params.Arguments[key].(bool); ok {
		return value
	}
	return defaultValue
}

func handleError(err error) (*mcp.CallToolResult, error) {
	mu.Lock()
	metrics.QueryErrors++
	mu.Unlock()

	logger.Error().Err(err).Msg("Tool execution error")
	return mcp.NewToolResultText(fmt.Sprintf("Error: %v", err)), nil
}

func updateMetrics(start time.Time) {
	mu.Lock()
	metrics.TotalResponseTime += time.Since(start)
	mu.Unlock()
}
