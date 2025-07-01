package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
)

type Handlers struct {
	server *PostgresMCPServer
	mu     sync.RWMutex
}

type QueryResult struct {
	Rows    []map[string]interface{} `json:"rows"`
	Columns []string                 `json:"columns"`
	Count   int                      `json:"count"`
	Timing  string                   `json:"timing"`
}

func NewHandlers(server *PostgresMCPServer) *Handlers {
	return &Handlers{
		server: server,
	}
}

// Tool creation methods
func (h *Handlers) CreateListDatabasesTool() *mcp.Tool {
	return mcp.NewTool(
		"list_databases",
		mcp.WithDescription("List all databases in the PostgreSQL server with size information"),
	)
}

func (h *Handlers) CreateListTablesTool() *mcp.Tool {
	return mcp.NewTool(
		"list_tables",
		mcp.WithDescription("List all tables in the current database with detailed information"),
		mcp.WithString("schema", mcp.Description("Schema name (optional, defaults to all schemas)")),
	)
}

func (h *Handlers) CreateListColumnsTool() *mcp.Tool {
	return mcp.NewTool(
		"list_columns",
		mcp.WithDescription("List all columns for a specific table with data types and constraints"),
		mcp.WithString("table_name", mcp.Required(), mcp.Description("Name of the table")),
		mcp.WithString("schema", mcp.Description("Schema name (optional, defaults to 'public')")),
	)
}

func (h *Handlers) CreateDescribeTableTool() *mcp.Tool {
	return mcp.NewTool(
		"describe_table",
		mcp.WithDescription("Get comprehensive table structure including constraints, indexes, and statistics"),
		mcp.WithString("table_name", mcp.Required(), mcp.Description("Name of the table")),
		mcp.WithString("schema", mcp.Description("Schema name (optional, defaults to 'public')")),
	)
}

func (h *Handlers) CreateGetTableSizeTool() *mcp.Tool {
	return mcp.NewTool(
		"get_table_size",
		mcp.WithDescription("Get table size information including row count and disk usage"),
		mcp.WithString("table_name", mcp.Required(), mcp.Description("Name of the table")),
		mcp.WithString("schema", mcp.Description("Schema name (optional, defaults to 'public')")),
	)
}

func (h *Handlers) CreateListIndexesTool() *mcp.Tool {
	return mcp.NewTool(
		"list_indexes",
		mcp.WithDescription("List all indexes for a table or entire database"),
		mcp.WithString("table_name", mcp.Description("Name of the table (optional, lists all if empty)")),
		mcp.WithString("schema", mcp.Description("Schema name (optional)")),
	)
}

func (h *Handlers) CreateListConstraintsTool() *mcp.Tool {
	return mcp.NewTool(
		"list_constraints",
		mcp.WithDescription("List all constraints for a table"),
		mcp.WithString("table_name", mcp.Required(), mcp.Description("Name of the table")),
		mcp.WithString("schema", mcp.Description("Schema name (optional, defaults to 'public')")),
	)
}

func (h *Handlers) CreateReadQueryTool() *mcp.Tool {
	return mcp.NewTool(
		"read_query",
		mcp.WithDescription("Execute a SELECT query with performance optimization and result formatting"),
		mcp.WithString("query", mcp.Required(), mcp.Description("SQL SELECT query to execute")),
		mcp.WithNumber("limit", mcp.Description("Maximum number of rows to return (default: 1000)")),
		mcp.WithBoolean("format_json", mcp.Description("Return results as formatted JSON (default: false)")),
	)
}

func (h *Handlers) CreateCountQueryTool() *mcp.Tool {
	return mcp.NewTool(
		"count_query",
		mcp.WithDescription("Get row count for a table with optional WHERE conditions"),
		mcp.WithString("table_name", mcp.Required(), mcp.Description("Name of the table")),
		mcp.WithString("where_clause", mcp.Description("Optional WHERE clause (without WHERE keyword)")),
		mcp.WithString("schema", mcp.Description("Schema name (optional, defaults to 'public')")),
	)
}

func (h *Handlers) CreateExplainQueryTool() *mcp.Tool {
	return mcp.NewTool(
		"explain_query",
		mcp.WithDescription("Analyze query execution plan with detailed performance metrics"),
		mcp.WithString("query", mcp.Required(), mcp.Description("SQL query to analyze")),
		mcp.WithBoolean("analyze", mcp.Description("Run EXPLAIN ANALYZE (default: false)")),
		mcp.WithBoolean("buffers", mcp.Description("Include buffer usage info (default: false)")),
	)
}

func (h *Handlers) CreateWriteQueryTool() *mcp.Tool {
	return mcp.NewTool(
		"write_query",
		mcp.WithDescription("Execute an INSERT query with transaction support"),
		mcp.WithString("query", mcp.Required(), mcp.Description("SQL INSERT query to execute")),
		mcp.WithBoolean("return_id", mcp.Description("Return inserted ID(s) (default: false)")),
	)
}

func (h *Handlers) CreateUpdateQueryTool() *mcp.Tool {
	return mcp.NewTool(
		"update_query",
		mcp.WithDescription("Execute an UPDATE query with safety checks"),
		mcp.WithString("query", mcp.Required(), mcp.Description("SQL UPDATE query to execute")),
		mcp.WithBoolean("force", mcp.Description("Skip safety checks for WHERE clause (default: false)")),
	)
}

func (h *Handlers) CreateDeleteQueryTool() *mcp.Tool {
	return mcp.NewTool(
		"delete_query",
		mcp.WithDescription("Execute a DELETE query with safety checks"),
		mcp.WithString("query", mcp.Required(), mcp.Description("SQL DELETE query to execute")),
		mcp.WithBoolean("force", mcp.Description("Skip safety checks for WHERE clause (default: false)")),
	)
}

func (h *Handlers) CreateCreateTableTool() *mcp.Tool {
	return mcp.NewTool(
		"create_table",
		mcp.WithDescription("Create a new table with proper constraints and indexes"),
		mcp.WithString("query", mcp.Required(), mcp.Description("CREATE TABLE SQL statement")),
	)
}

func (h *Handlers) CreateAlterTableTool() *mcp.Tool {
	return mcp.NewTool(
		"alter_table",
		mcp.WithDescription("Alter an existing table structure"),
		mcp.WithString("query", mcp.Required(), mcp.Description("ALTER TABLE SQL statement")),
	)
}

func (h *Handlers) CreateCreateIndexTool() *mcp.Tool {
	return mcp.NewTool(
		"create_index",
		mcp.WithDescription("Create an index on a table"),
		mcp.WithString("query", mcp.Required(), mcp.Description("CREATE INDEX SQL statement")),
	)
}

func (h *Handlers) CreateDropIndexTool() *mcp.Tool {
	return mcp.NewTool(
		"drop_index",
		mcp.WithDescription("Drop an existing index"),
		mcp.WithString("index_name", mcp.Required(), mcp.Description("Name of the index to drop")),
		mcp.WithString("schema", mcp.Description("Schema name (optional)")),
	)
}

func (h *Handlers) CreateGetStatsTool() *mcp.Tool {
	return mcp.NewTool(
		"get_stats",
		mcp.WithDescription("Get server performance statistics and metrics"),
	)
}

func (h *Handlers) CreateGetSlowQueresTool() *mcp.Tool {
	return mcp.NewTool(
		"get_slow_queries",
		mcp.WithDescription("Get slow query statistics from pg_stat_statements"),
		mcp.WithNumber("limit", mcp.Description("Number of queries to return (default: 10)")),
	)
}

func (h *Handlers) CreateAnalyzeTableTool() *mcp.Tool {
	return mcp.NewTool(
		"analyze_table",
		mcp.WithDescription("Update table statistics for better query planning"),
		mcp.WithString("table_name", mcp.Required(), mcp.Description("Name of the table to analyze")),
		mcp.WithString("schema", mcp.Description("Schema name (optional, defaults to 'public')")),
	)
}

// Handler implementations
func (h *Handlers) HandleListDatabases(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	start := time.Now()
	defer h.updateMetrics(start)

	query := "SELECT datname, pg_database_size(datname) as size_bytes, pg_size_pretty(pg_database_size(datname)) as size FROM pg_database WHERE datistemplate = false ORDER BY datname"
	result, err := h.executeQuery(ctx, query)
	if err != nil {
		return h.handleError(err)
	}

	return mcp.NewToolResultText(h.formatResult(result)), nil
}

func (h *Handlers) HandleListTables(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	start := time.Now()
	defer h.updateMetrics(start)

	schema := h.getStringParam(request, "schema", "")
	whereClause := ""
	if schema != "" {
		whereClause = fmt.Sprintf("WHERE table_schema = '%s'", schema)
	}

	query := fmt.Sprintf(`
		SELECT
			table_schema,
			table_name,
			table_type,
			COALESCE(pg_size_pretty(pg_total_relation_size(quote_ident(table_schema)||'.'||quote_ident(table_name))), 'N/A') as size
		FROM information_schema.tables
		%s
		ORDER BY table_schema, table_name`, whereClause)

	result, err := h.executeQuery(ctx, query)
	if err != nil {
		return h.handleError(err)
	}

	return mcp.NewToolResultText(h.formatResult(result)), nil
}

func (h *Handlers) HandleListColumns(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	start := time.Now()
	defer h.updateMetrics(start)

	tableName := h.getStringParam(request, "table_name", "")
	schema := h.getStringParam(request, "schema", "public")

	query := `
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
		WHERE table_name = $1 AND table_schema = $2
		ORDER BY ordinal_position`

	result, err := h.executeQueryWithParams(ctx, query, tableName, schema)
	if err != nil {
		return h.handleError(err)
	}

	return mcp.NewToolResultText(h.formatResult(result)), nil
}

func (h *Handlers) HandleDescribeTable(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	start := time.Now()
	defer h.updateMetrics(start)

	tableName := h.getStringParam(request, "table_name", "")
	schema := h.getStringParam(request, "schema", "public")

	// Multiple queries for comprehensive table description
	var results []string

	// Table structure
	columnQuery := `
		SELECT
			column_name,
			data_type,
			CASE
				WHEN character_maximum_length IS NOT NULL THEN character_maximum_length::text
				WHEN numeric_precision IS NOT NULL THEN numeric_precision::text || ',' || COALESCE(numeric_scale, 0)::text
				ELSE ''
			END as size,
			is_nullable,
			column_default
		FROM information_schema.columns
		WHERE table_name = $1 AND table_schema = $2
		ORDER BY ordinal_position`

	result, err := h.executeQueryWithParams(ctx, columnQuery, tableName, schema)
	if err != nil {
		return h.handleError(err)
	}
	results = append(results, fmt.Sprintf("COLUMNS:\n%s", h.formatResult(result)))

	// Constraints
	constraintQuery := `
		SELECT
			tc.constraint_name,
			tc.constraint_type,
			string_agg(kcu.column_name, ', ' ORDER BY kcu.ordinal_position) as columns
		FROM information_schema.table_constraints tc
		LEFT JOIN information_schema.key_column_usage kcu
			ON tc.constraint_name = kcu.constraint_name
			AND tc.table_schema = kcu.table_schema
		WHERE tc.table_name = $1 AND tc.table_schema = $2
		GROUP BY tc.constraint_name, tc.constraint_type
		ORDER BY tc.constraint_type`

	result, err = h.executeQueryWithParams(ctx, constraintQuery, tableName, schema)
	if err == nil && len(result.Rows) > 0 {
		results = append(results, fmt.Sprintf("CONSTRAINTS:\n%s", h.formatResult(result)))
	}

	// Indexes
	indexQuery := `
		SELECT
			indexname,
			indexdef
		FROM pg_indexes
		WHERE tablename = $1 AND schemaname = $2`

	result, err = h.executeQueryWithParams(ctx, indexQuery, tableName, schema)
	if err == nil && len(result.Rows) > 0 {
		results = append(results, fmt.Sprintf("INDEXES:\n%s", h.formatResult(result)))
	}

	return mcp.NewToolResultText(strings.Join(results, "\n\n")), nil
}

func (h *Handlers) HandleGetTableSize(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	start := time.Now()
	defer h.updateMetrics(start)

	tableName := h.getStringParam(request, "table_name", "")
	schema := h.getStringParam(request, "schema", "public")

	query := `
		SELECT
			schemaname,
			tablename,
			pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as total_size,
			pg_size_pretty(pg_relation_size(schemaname||'.'||tablename)) as table_size,
			pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename) - pg_relation_size(schemaname||'.'||tablename)) as index_size,
			(SELECT COUNT(*) FROM information_schema.tables WHERE table_name = $1 AND table_schema = $2) as row_count_estimate
		FROM pg_tables
		WHERE tablename = $1 AND schemaname = $2`

	result, err := h.executeQueryWithParams(ctx, query, tableName, schema)
	if err != nil {
		return h.handleError(err)
	}

	return mcp.NewToolResultText(h.formatResult(result)), nil
}

func (h *Handlers) HandleListIndexes(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	start := time.Now()
	defer h.updateMetrics(start)

	tableName := h.getStringParam(request, "table_name", "")
	schema := h.getStringParam(request, "schema", "")

	whereClause := ""
	args := []interface{}{}
	argCount := 0

	if tableName != "" {
		argCount++
		whereClause = fmt.Sprintf("WHERE tablename = $%d", argCount)
		args = append(args, tableName)

		if schema != "" {
			argCount++
			whereClause += fmt.Sprintf(" AND schemaname = $%d", argCount)
			args = append(args, schema)
		}
	} else if schema != "" {
		argCount++
		whereClause = fmt.Sprintf("WHERE schemaname = $%d", argCount)
		args = append(args, schema)
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

	var result *QueryResult
	var err error
	if len(args) > 0 {
		result, err = h.executeQueryWithParams(ctx, query, args...)
	} else {
		result, err = h.executeQuery(ctx, query)
	}

	if err != nil {
		return h.handleError(err)
	}

	return mcp.NewToolResultText(h.formatResult(result)), nil
}

func (h *Handlers) HandleListConstraints(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	start := time.Now()
	defer h.updateMetrics(start)

	tableName := h.getStringParam(request, "table_name", "")
	schema := h.getStringParam(request, "schema", "public")

	query := `
		SELECT
			tc.constraint_name,
			tc.constraint_type,
			string_agg(kcu.column_name, ', ' ORDER BY kcu.ordinal_position) as columns,
			tc.is_deferrable,
			tc.initially_deferred
		FROM information_schema.table_constraints tc
		LEFT JOIN information_schema.key_column_usage kcu
			ON tc.constraint_name = kcu.constraint_name
			AND tc.table_schema = kcu.table_schema
		WHERE tc.table_name = $1 AND tc.table_schema = $2
		GROUP BY tc.constraint_name, tc.constraint_type, tc.is_deferrable, tc.initially_deferred
		ORDER BY tc.constraint_type`

	result, err := h.executeQueryWithParams(ctx, query, tableName, schema)
	if err != nil {
		return h.handleError(err)
	}

	return mcp.NewToolResultText(h.formatResult(result)), nil
}

func (h *Handlers) HandleReadQuery(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	start := time.Now()
	defer h.updateMetrics(start)

	query := h.getStringParam(request, "query", "")
	if query == "" {
		return h.handleError(fmt.Errorf("query parameter is required"))
	}

	// Safety check for read queries
	if !h.isReadOnlyQuery(query) {
		return h.handleError(fmt.Errorf("only SELECT queries are allowed in read_query"))
	}

	limit := h.getNumberParam(request, "limit", 1000)
	formatJSON := h.getBoolParam(request, "format_json", false)

	// Add limit if not present
	if !strings.Contains(strings.ToUpper(query), "LIMIT") {
		query = fmt.Sprintf("%s LIMIT %d", query, int(limit))
	}

	result, err := h.executeQuery(ctx, query)
	if err != nil {
		return h.handleError(err)
	}

	if formatJSON {
		jsonBytes, _ := json.MarshalIndent(result, "", "  ")
		return mcp.NewToolResultText(string(jsonBytes)), nil
	}

	return mcp.NewToolResultText(h.formatResult(result)), nil
}

func (h *Handlers) HandleCountQuery(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	start := time.Now()
	defer h.updateMetrics(start)

	tableName := h.getStringParam(request, "table_name", "")
	whereClause := h.getStringParam(request, "where_clause", "")
	schema := h.getStringParam(request, "schema", "public")

	query := fmt.Sprintf("SELECT COUNT(*) as count FROM %s.%s", schema, tableName)
	if whereClause != "" {
		query += fmt.Sprintf(" WHERE %s", whereClause)
	}

	result, err := h.executeQuery(ctx, query)
	if err != nil {
		return h.handleError(err)
	}

	return mcp.NewToolResultText(h.formatResult(result)), nil
}

func (h *Handlers) HandleExplainQuery(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	start := time.Now()
	defer h.updateMetrics(start)

	query := h.getStringParam(request, "query", "")
	if query == "" {
		return h.handleError(fmt.Errorf("query parameter is required"))
	}

	analyze := h.getBoolParam(request, "analyze", false)
	buffers := h.getBoolParam(request, "buffers", false)

	explainQuery := "EXPLAIN"
	if analyze {
		explainQuery += " ANALYZE"
	}
	if buffers {
		explainQuery += " BUFFERS"
	}
	explainQuery += " " + query

	result, err := h.executeQuery(ctx, explainQuery)
	if err != nil {
		return h.handleError(err)
	}

	return mcp.NewToolResultText(h.formatResult(result)), nil
}

// Write operation handlers
func (h *Handlers) HandleWriteQuery(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	if h.server.config.ReadOnly {
		return h.handleError(fmt.Errorf("server is in read-only mode"))
	}

	start := time.Now()
	defer h.updateMetrics(start)

	query := h.getStringParam(request, "query", "")
	if query == "" {
		return h.handleError(fmt.Errorf("query parameter is required"))
	}

	returnID := h.getBoolParam(request, "return_id", false)
	result, err := h.executeWriteQuery(ctx, query, returnID)
	if err != nil {
		return h.handleError(err)
	}

	return mcp.NewToolResultText(result), nil
}

func (h *Handlers) HandleUpdateQuery(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	if h.server.config.ReadOnly {
		return h.handleError(fmt.Errorf("server is in read-only mode"))
	}

	start := time.Now()
	defer h.updateMetrics(start)

	query := h.getStringParam(request, "query", "")
	if query == "" {
		return h.handleError(fmt.Errorf("query parameter is required"))
	}

	force := h.getBoolParam(request, "force", false)

	// Safety check for WHERE clause
	if !force && !strings.Contains(strings.ToUpper(query), "WHERE") {
		return h.handleError(fmt.Errorf("UPDATE queries must include a WHERE clause. Use force=true to override"))
	}

	result, err := h.executeWriteQuery(ctx, query, false)
	if err != nil {
		return h.handleError(err)
	}

	return mcp.NewToolResultText(result), nil
}

func (h *Handlers) HandleDeleteQuery(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	if h.server.config.ReadOnly {
		return h.handleError(fmt.Errorf("server is in read-only mode"))
	}

	start := time.Now()
	defer h.updateMetrics(start)

	query := h.getStringParam(request, "query", "")
	if query == "" {
		return h.handleError(fmt.Errorf("query parameter is required"))
	}

	force := h.getBoolParam(request, "force", false)

	// Safety check for WHERE clause
	if !force && !strings.Contains(strings.ToUpper(query), "WHERE") {
		return h.handleError(fmt.Errorf("DELETE queries must include a WHERE clause. Use force=true to override"))
	}

	result, err := h.executeWriteQuery(ctx, query, false)
	if err != nil {
		return h.handleError(err)
	}

	return mcp.NewToolResultText(result), nil
}

func (h *Handlers) HandleCreateTable(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	if h.server.config.ReadOnly {
		return h.handleError(fmt.Errorf("server is in read-only mode"))
	}

	start := time.Now()
	defer h.updateMetrics(start)

	query := h.getStringParam(request, "query", "")
	if query == "" {
		return h.handleError(fmt.Errorf("query parameter is required"))
	}

	result, err := h.executeWriteQuery(ctx, query, false)
	if err != nil {
		return h.handleError(err)
	}

	return mcp.NewToolResultText(result), nil
}

func (h *Handlers) HandleAlterTable(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	if h.server.config.ReadOnly {
		return h.handleError(fmt.Errorf("server is in read-only mode"))
	}

	start := time.Now()
	defer h.updateMetrics(start)

	query := h.getStringParam(request, "query", "")
	if query == "" {
		return h.handleError(fmt.Errorf("query parameter is required"))
	}

	result, err := h.executeWriteQuery(ctx, query, false)
	if err != nil {
		return h.handleError(err)
	}

	return mcp.NewToolResultText(result), nil
}

func (h *Handlers) HandleCreateIndex(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	if h.server.config.ReadOnly {
		return h.handleError(fmt.Errorf("server is in read-only mode"))
	}

	start := time.Now()
	defer h.updateMetrics(start)

	query := h.getStringParam(request, "query", "")
	if query == "" {
		return h.handleError(fmt.Errorf("query parameter is required"))
	}

	result, err := h.executeWriteQuery(ctx, query, false)
	if err != nil {
		return h.handleError(err)
	}

	return mcp.NewToolResultText(result), nil
}

func (h *Handlers) HandleDropIndex(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	if h.server.config.ReadOnly {
		return h.handleError(fmt.Errorf("server is in read-only mode"))
	}

	start := time.Now()
	defer h.updateMetrics(start)

	indexName := h.getStringParam(request, "index_name", "")
	if indexName == "" {
		return h.handleError(fmt.Errorf("index_name parameter is required"))
	}

	schema := h.getStringParam(request, "schema", "")

	query := "DROP INDEX "
	if schema != "" {
		query += fmt.Sprintf("%s.%s", schema, indexName)
	} else {
		query += indexName
	}

	result, err := h.executeWriteQuery(ctx, query, false)
	if err != nil {
		return h.handleError(err)
	}

	return mcp.NewToolResultText(result), nil
}

func (h *Handlers) HandleGetStats(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	start := time.Now()
	defer h.updateMetrics(start)

	h.mu.RLock()
	stats := map[string]interface{}{
		"queries_executed":     h.server.metrics.QueriesExecuted,
		"query_errors":         h.server.metrics.QueryErrors,
		"connections_active":   h.server.dbPool.Stat().AcquiredConns(),
		"connections_idle":     h.server.dbPool.Stat().IdleConns(),
		"connections_total":    h.server.dbPool.Stat().TotalConns(),
		"uptime_seconds":       time.Since(h.server.metrics.StartTime).Seconds(),
		"version":              version,
		"read_only_mode":       h.server.config.ReadOnly,
		"max_connections":      h.server.config.MaxConnections,
		"query_timeout":        h.server.config.QueryTimeout,
	}

	if h.server.metrics.QueriesExecuted > 0 {
		stats["avg_response_time_ms"] = float64(h.server.metrics.TotalResponseTime.Nanoseconds()) / float64(h.server.metrics.QueriesExecuted) / 1000000
	}
	h.mu.RUnlock()

	jsonBytes, _ := json.MarshalIndent(stats, "", "  ")
	return mcp.NewToolResultText(string(jsonBytes)), nil
}

func (h *Handlers) HandleGetSlowQueries(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	start := time.Now()
	defer h.updateMetrics(start)

	limit := h.getNumberParam(request, "limit", 10)

	query := fmt.Sprintf(`
		SELECT
			query,
			calls,
			total_exec_time,
			mean_exec_time,
			rows,
			100.0 * shared_blks_hit / nullif(shared_blks_hit + shared_blks_read, 0) AS hit_percent
		FROM pg_stat_statements
		ORDER BY total_exec_time DESC
		LIMIT %d`, int(limit))

	result, err := h.executeQuery(ctx, query)
	if err != nil {
		// Fallback if pg_stat_statements is not available
		return mcp.NewToolResultText("pg_stat_statements extension not available"), nil
	}

	return mcp.NewToolResultText(h.formatResult(result)), nil
}

func (h *Handlers) HandleAnalyzeTable(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	if h.server.config.ReadOnly {
		return h.handleError(fmt.Errorf("server is in read-only mode"))
	}

	start := time.Now()
	defer h.updateMetrics(start)

	tableName := h.getStringParam(request, "table_name", "")
	if tableName == "" {
		return h.handleError(fmt.Errorf("table_name parameter is required"))
	}

	schema := h.getStringParam(request, "schema", "public")
	query := fmt.Sprintf("ANALYZE %s.%s", schema, tableName)

	result, err := h.executeWriteQuery(ctx, query, false)
	if err != nil {
		return h.handleError(err)
	}

	return mcp.NewToolResultText(result), nil
}

// Utility functions will be in utils.go
