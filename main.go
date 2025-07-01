package main

import (
	"context"
	"embed"
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"strings"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
	"github.com/nicksnyder/go-i18n/v2/i18n"
	"github.com/pelletier/go-toml/v2"
	"golang.org/x/text/language"
)

//go:embed locales/*
var localeFS embed.FS

const (
	StatementTypeNoExplainCheck = ""
	StatementTypeSelect         = "SELECT"
	StatementTypeInsert         = "INSERT"
	StatementTypeUpdate         = "UPDATE"
	StatementTypeDelete         = "DELETE"
)

var (
	DSN              string
	ReadOnly         bool
	WithExplainCheck bool
	DB               *sqlx.DB
	Transport        string
	IPaddress        string
	Port             int
	Lang             string
)

func main() {
	// Initialize i18n
	bundle := i18n.NewBundle(language.English)
	bundle.RegisterUnmarshalFunc("toml", toml.Unmarshal)

	flag.StringVar(&DSN, "dsn", "", "PostgreSQL DSN")
	flag.BoolVar(&ReadOnly, "read-only", false, "Enable read-only mode")
	flag.BoolVar(&WithExplainCheck, "with-explain-check", false, "Check query plan with EXPLAIN before executing")
	flag.StringVar(&Transport, "t", "stdio", "Transport type (stdio or sse)")
	flag.IntVar(&Port, "port", 8080, "SSE server port")
	flag.StringVar(&IPaddress, "ip", "localhost", "Server IP address")
	flag.StringVar(&Lang, "lang", language.English.String(), "Language code (en/zh-CN/...)")

	flag.Parse()

	langTag, err := language.Parse(Lang)
	if err != nil {
		langTag = language.English
	}

	langFile := fmt.Sprintf("locales/%s/active.%s.toml", langTag.String(), langTag.String())
	if data, err := localeFS.ReadFile(langFile); err == nil {
		bundle.ParseMessageFileBytes(data, langFile)
	} else {
		if enData, err := localeFS.ReadFile("locales/en/active.en.toml"); err == nil {
			bundle.ParseMessageFileBytes(enData, "locales/en/active.en.toml")
		}
	}

	localizer := i18n.NewLocalizer(bundle, langTag.String())
	_ = localizer // Reserved for future localization

	// Create MCP server
	s := server.NewMCPServer(
		"requesty-postgres-mcp",
		"1.0.0",
		server.WithResourceCapabilities(true, true),
		server.WithPromptCapabilities(true),
		server.WithLogging(),
	)

	// Schema Tools
	listDatabaseTool := mcp.NewTool(
		"list_databases",
		mcp.WithDescription("List all databases in the PostgreSQL server"),
	)

	listTableTool := mcp.NewTool(
		"list_tables",
		mcp.WithDescription("List all tables in the current database"),
		mcp.WithString("schema", mcp.Description("Schema name (optional, defaults to all schemas)")),
	)

	listColumnsTool := mcp.NewTool(
		"list_columns",
		mcp.WithDescription("List all columns for a specific table"),
		mcp.WithString("table_name", mcp.Required(), mcp.Description("Name of the table")),
		mcp.WithString("schema", mcp.Description("Schema name (optional, defaults to 'public')")),
	)

	descTableTool := mcp.NewTool(
		"describe_table",
		mcp.WithDescription("Get detailed table structure with constraints and indexes"),
		mcp.WithString("name", mcp.Required(), mcp.Description("Name of the table to describe")),
		mcp.WithString("schema", mcp.Description("Schema name (optional, defaults to 'public')")),
	)

	getTableSizeTool := mcp.NewTool(
		"get_table_size",
		mcp.WithDescription("Get table size and row count information"),
		mcp.WithString("table_name", mcp.Required(), mcp.Description("Name of the table")),
		mcp.WithString("schema", mcp.Description("Schema name (optional, defaults to 'public')")),
	)

	listIndexesTool := mcp.NewTool(
		"list_indexes",
		mcp.WithDescription("List all indexes for a table or database"),
		mcp.WithString("table_name", mcp.Description("Name of the table (optional, lists all if empty)")),
		mcp.WithString("schema", mcp.Description("Schema name (optional)")),
	)

	// Query Tools
	readQueryTool := mcp.NewTool(
		"read_query",
		mcp.WithDescription("Execute a read-only SQL query with safety checks"),
		mcp.WithString("query", mcp.Required(), mcp.Description("SQL SELECT query to execute")),
		mcp.WithNumber("limit", mcp.Description("Maximum rows to return (default: 1000)")),
	)

	explainQueryTool := mcp.NewTool(
		"explain_query",
		mcp.WithDescription("Analyze query execution plan"),
		mcp.WithString("query", mcp.Required(), mcp.Description("SQL query to analyze")),
		mcp.WithBoolean("analyze", mcp.Description("Run EXPLAIN ANALYZE (default: false)")),
	)

	countQueryTool := mcp.NewTool(
		"count_query",
		mcp.WithDescription("Count rows in a table with optional conditions"),
		mcp.WithString("table_name", mcp.Required(), mcp.Description("Name of the table")),
		mcp.WithString("where_clause", mcp.Description("Optional WHERE conditions")),
		mcp.WithString("schema", mcp.Description("Schema name (optional, defaults to 'public')")),
	)

	// Write Tools (only if not read-only)
	var writeQueryTool, updateQueryTool, deleteQueryTool, createTableTool, alterTableTool, createIndexTool mcp.Tool

	if !ReadOnly {
		writeQueryTool = mcp.NewTool(
			"write_query",
			mcp.WithDescription("Execute an INSERT query"),
			mcp.WithString("query", mcp.Required(), mcp.Description("SQL INSERT query to execute")),
		)

		updateQueryTool = mcp.NewTool(
			"update_query",
			mcp.WithDescription("Execute an UPDATE query with WHERE clause validation"),
			mcp.WithString("query", mcp.Required(), mcp.Description("SQL UPDATE query to execute")),
		)

		deleteQueryTool = mcp.NewTool(
			"delete_query",
			mcp.WithDescription("Execute a DELETE query with WHERE clause validation"),
			mcp.WithString("query", mcp.Required(), mcp.Description("SQL DELETE query to execute")),
		)

		createTableTool = mcp.NewTool(
			"create_table",
			mcp.WithDescription("Create a new table"),
			mcp.WithString("query", mcp.Required(), mcp.Description("CREATE TABLE SQL statement")),
		)

		alterTableTool = mcp.NewTool(
			"alter_table",
			mcp.WithDescription("Alter an existing table structure"),
			mcp.WithString("query", mcp.Required(), mcp.Description("ALTER TABLE SQL statement")),
		)

		createIndexTool = mcp.NewTool(
			"create_index",
			mcp.WithDescription("Create an index on a table"),
			mcp.WithString("query", mcp.Required(), mcp.Description("CREATE INDEX SQL statement")),
		)
	}

	// Add tool handlers
	s.AddTool(listDatabaseTool, func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		result, err := HandleQuery("SELECT datname, pg_database_size(datname) as size_bytes, pg_size_pretty(pg_database_size(datname)) as size FROM pg_database WHERE datistemplate = false ORDER BY datname", StatementTypeNoExplainCheck)
		if err != nil {
			return mcp.NewToolResultText(fmt.Sprintf("Error: %v", err)), nil
		}
		return mcp.NewToolResultText(result), nil
	})

	s.AddTool(listTableTool, func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		schema := getStringParam(request, "schema", "")
		query := "SELECT table_schema, table_name, table_type FROM information_schema.tables"
		if schema != "" {
			query += fmt.Sprintf(" WHERE table_schema = '%s'", schema)
		}
		query += " ORDER BY table_schema, table_name"

		result, err := HandleQuery(query, StatementTypeNoExplainCheck)
		if err != nil {
			return mcp.NewToolResultText(fmt.Sprintf("Error: %v", err)), nil
		}
		return mcp.NewToolResultText(result), nil
	})

	s.AddTool(listColumnsTool, func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		tableName := getStringParam(request, "table_name", "")
		schema := getStringParam(request, "schema", "public")

		query := fmt.Sprintf(`
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
			ORDER BY ordinal_position`, tableName, schema)

		result, err := HandleQuery(query, StatementTypeNoExplainCheck)
		if err != nil {
			return mcp.NewToolResultText(fmt.Sprintf("Error: %v", err)), nil
		}
		return mcp.NewToolResultText(result), nil
	})

	s.AddTool(descTableTool, func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		tableName := getStringParam(request, "name", "")
		schema := getStringParam(request, "schema", "public")

		// Get comprehensive table description
		queries := []string{
			// Columns
			fmt.Sprintf(`
				SELECT
					'COLUMN' as type,
					column_name as name,
					data_type || COALESCE('(' || character_maximum_length::text || ')', '') as details,
					CASE WHEN is_nullable = 'NO' THEN 'NOT NULL' ELSE 'NULLABLE' END as constraint_info
				FROM information_schema.columns
				WHERE table_name = '%s' AND table_schema = '%s'
				ORDER BY ordinal_position`, tableName, schema),

			// Constraints
			fmt.Sprintf(`
				SELECT
					'CONSTRAINT' as type,
					tc.constraint_name as name,
					tc.constraint_type as details,
					string_agg(kcu.column_name, ', ') as constraint_info
				FROM information_schema.table_constraints tc
				LEFT JOIN information_schema.key_column_usage kcu
					ON tc.constraint_name = kcu.constraint_name
				WHERE tc.table_name = '%s' AND tc.table_schema = '%s'
				GROUP BY tc.constraint_name, tc.constraint_type`, tableName, schema),

			// Indexes
			fmt.Sprintf(`
				SELECT
					'INDEX' as type,
					indexname as name,
					'' as details,
					indexdef as constraint_info
				FROM pg_indexes
				WHERE tablename = '%s' AND schemaname = '%s'`, tableName, schema),
		}

		var allResults []string
		for _, query := range queries {
			result, err := HandleQuery(query, StatementTypeNoExplainCheck)
			if err == nil && result != "" {
				allResults = append(allResults, result)
			}
		}

		if len(allResults) == 0 {
			return mcp.NewToolResultText("Table not found or no information available"), nil
		}

		return mcp.NewToolResultText(strings.Join(allResults, "\n\n")), nil
	})

	s.AddTool(getTableSizeTool, func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		tableName := getStringParam(request, "table_name", "")
		schema := getStringParam(request, "schema", "public")

		query := fmt.Sprintf(`
			SELECT
				'%s.%s' as table_name,
				pg_size_pretty(pg_total_relation_size('%s.%s')) as total_size,
				pg_size_pretty(pg_relation_size('%s.%s')) as table_size,
				(SELECT COUNT(*) FROM %s.%s) as estimated_rows
		`, schema, tableName, schema, tableName, schema, tableName, schema, tableName)

		result, err := HandleQuery(query, StatementTypeNoExplainCheck)
		if err != nil {
			return mcp.NewToolResultText(fmt.Sprintf("Error: %v", err)), nil
		}
		return mcp.NewToolResultText(result), nil
	})

	s.AddTool(listIndexesTool, func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		tableName := getStringParam(request, "table_name", "")
		schema := getStringParam(request, "schema", "")

		query := "SELECT schemaname, tablename, indexname, indexdef FROM pg_indexes"
		var conditions []string

		if tableName != "" {
			conditions = append(conditions, fmt.Sprintf("tablename = '%s'", tableName))
		}
		if schema != "" {
			conditions = append(conditions, fmt.Sprintf("schemaname = '%s'", schema))
		}

		if len(conditions) > 0 {
			query += " WHERE " + strings.Join(conditions, " AND ")
		}
		query += " ORDER BY schemaname, tablename, indexname"

		result, err := HandleQuery(query, StatementTypeNoExplainCheck)
		if err != nil {
			return mcp.NewToolResultText(fmt.Sprintf("Error: %v", err)), nil
		}
		return mcp.NewToolResultText(result), nil
	})

	s.AddTool(readQueryTool, func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		query := getStringParam(request, "query", "")
		limit := getNumberParam(request, "limit", 1000)

		// Safety check - only allow SELECT queries
		upperQuery := strings.ToUpper(strings.TrimSpace(query))
		if !strings.HasPrefix(upperQuery, "SELECT") && !strings.HasPrefix(upperQuery, "WITH") {
			return mcp.NewToolResultText("Error: Only SELECT queries are allowed"), nil
		}

		// Add limit if not present
		if !strings.Contains(upperQuery, "LIMIT") {
			query = fmt.Sprintf("%s LIMIT %d", query, int(limit))
		}

		result, err := HandleQuery(query, StatementTypeSelect)
		if err != nil {
			return mcp.NewToolResultText(fmt.Sprintf("Error: %v", err)), nil
		}
		return mcp.NewToolResultText(result), nil
	})

	s.AddTool(explainQueryTool, func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		query := getStringParam(request, "query", "")
		analyze := getBoolParam(request, "analyze", false)

		explainQuery := "EXPLAIN"
		if analyze {
			explainQuery += " ANALYZE"
		}
		explainQuery += " " + query

		result, err := HandleQuery(explainQuery, StatementTypeNoExplainCheck)
		if err != nil {
			return mcp.NewToolResultText(fmt.Sprintf("Error: %v", err)), nil
		}
		return mcp.NewToolResultText(result), nil
	})

	s.AddTool(countQueryTool, func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		tableName := getStringParam(request, "table_name", "")
		whereClause := getStringParam(request, "where_clause", "")
		schema := getStringParam(request, "schema", "public")

		query := fmt.Sprintf("SELECT COUNT(*) as count FROM %s.%s", schema, tableName)
		if whereClause != "" {
			query += " WHERE " + whereClause
		}

		result, err := HandleQuery(query, StatementTypeNoExplainCheck)
		if err != nil {
			return mcp.NewToolResultText(fmt.Sprintf("Error: %v", err)), nil
		}
		return mcp.NewToolResultText(result), nil
	})

	// Add write tools if not read-only
	if !ReadOnly && writeQueryTool.Name != "" {
		s.AddTool(writeQueryTool, func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			query := getStringParam(request, "query", "")
			result, err := HandleExec(query, StatementTypeInsert)
			if err != nil {
				return mcp.NewToolResultText(fmt.Sprintf("Error: %v", err)), nil
			}
			return mcp.NewToolResultText(result), nil
		})

		if updateQueryTool.Name != "" {
			s.AddTool(updateQueryTool, func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
				query := getStringParam(request, "query", "")

				// Safety check - require WHERE clause
				if !strings.Contains(strings.ToUpper(query), "WHERE") {
					return mcp.NewToolResultText("Error: UPDATE queries must include a WHERE clause for safety"), nil
				}

				result, err := HandleExec(query, StatementTypeUpdate)
				if err != nil {
					return mcp.NewToolResultText(fmt.Sprintf("Error: %v", err)), nil
				}
				return mcp.NewToolResultText(result), nil
			})
		}

		if deleteQueryTool.Name != "" {
			s.AddTool(deleteQueryTool, func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
				query := getStringParam(request, "query", "")

				// Safety check - require WHERE clause
				if !strings.Contains(strings.ToUpper(query), "WHERE") {
					return mcp.NewToolResultText("Error: DELETE queries must include a WHERE clause for safety"), nil
				}

				result, err := HandleExec(query, StatementTypeDelete)
				if err != nil {
					return mcp.NewToolResultText(fmt.Sprintf("Error: %v", err)), nil
				}
				return mcp.NewToolResultText(result), nil
			})
		}

		if createTableTool.Name != "" {
			s.AddTool(createTableTool, func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
				query := getStringParam(request, "query", "")
				result, err := HandleExec(query, StatementTypeNoExplainCheck)
				if err != nil {
					return mcp.NewToolResultText(fmt.Sprintf("Error: %v", err)), nil
				}
				return mcp.NewToolResultText(result), nil
			})
		}

		if alterTableTool.Name != "" {
			s.AddTool(alterTableTool, func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
				query := getStringParam(request, "query", "")
				result, err := HandleExec(query, StatementTypeNoExplainCheck)
				if err != nil {
					return mcp.NewToolResultText(fmt.Sprintf("Error: %v", err)), nil
				}
				return mcp.NewToolResultText(result), nil
			})
		}

		if createIndexTool.Name != "" {
			s.AddTool(createIndexTool, func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
				query := getStringParam(request, "query", "")
				result, err := HandleExec(query, StatementTypeNoExplainCheck)
				if err != nil {
					return mcp.NewToolResultText(fmt.Sprintf("Error: %v", err)), nil
				}
				return mcp.NewToolResultText(result), nil
			})
		}
	}

	// Start server
	if Transport == "sse" {
		sseServer := server.NewSSEServer(s, server.WithBaseURL(fmt.Sprintf("http://%s:%d", IPaddress, Port)))
		log.Printf("SSE server listening on %s:%d", IPaddress, Port)
		if err := sseServer.Start(fmt.Sprintf("%s:%d", IPaddress, Port)); err != nil {
			log.Fatalf("Server error: %v", err)
		}
	} else {
		if err := server.ServeStdio(s); err != nil {
			log.Fatalf("Server error: %v", err)
		}
	}
}

// Database connection management
func GetDB() (*sqlx.DB, error) {
	if DB != nil {
		return DB, nil
	}

	db, err := sqlx.Connect("pgx", DSN)
	if err != nil {
		return nil, fmt.Errorf("failed to establish database connection: %v", err)
	}

	DB = db
	return DB, nil
}

// Query execution
func HandleQuery(query, expect string) (string, error) {
	result, headers, err := DoQuery(query, expect)
	if err != nil {
		return "", err
	}

	return MapToCSV(result, headers)
}

func DoQuery(query, expect string) ([]map[string]interface{}, []string, error) {
	db, err := GetDB()
	if err != nil {
		return nil, nil, err
	}

	if len(expect) > 0 && WithExplainCheck {
		if err := HandleExplain(query, expect); err != nil {
			return nil, nil, err
		}
	}

	rows, err := db.Queryx(query)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, nil, err
	}

	var result []map[string]interface{}
	for rows.Next() {
		row, err := rows.SliceScan()
		if err != nil {
			return nil, nil, err
		}

		resultRow := map[string]interface{}{}
		for i, col := range cols {
			switch v := row[i].(type) {
			case []byte:
				resultRow[col] = string(v)
			default:
				resultRow[col] = v
			}
		}
		result = append(result, resultRow)
	}

	return result, cols, nil
}

// Execute write operations
func HandleExec(query, expect string) (string, error) {
	db, err := GetDB()
	if err != nil {
		return "", err
	}

	if len(expect) > 0 && WithExplainCheck {
		if err := HandleExplain(query, expect); err != nil {
			return "", err
		}
	}

	result, err := db.Exec(query)
	if err != nil {
		return "", err
	}

	ra, err := result.RowsAffected()
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%d rows affected", ra), nil
}

// EXPLAIN query validation
func HandleExplain(query, expect string) error {
	if !WithExplainCheck {
		return nil
	}

	db, err := GetDB()
	if err != nil {
		return err
	}

	rows, err := db.Queryx(fmt.Sprintf("EXPLAIN %s", query))
	if err != nil {
		return err
	}
	defer rows.Close()

	// For PostgreSQL, just check if EXPLAIN works
	return nil
}

// CSV output formatting
func MapToCSV(m []map[string]interface{}, headers []string) (string, error) {
	var csvBuf strings.Builder
	writer := csv.NewWriter(&csvBuf)

	if err := writer.Write(headers); err != nil {
		return "", fmt.Errorf("failed to write headers: %v", err)
	}

	for _, item := range m {
		row := make([]string, len(headers))
		for i, header := range headers {
			value, exists := item[header]
			if !exists {
				row[i] = ""
			} else {
				row[i] = fmt.Sprintf("%v", value)
			}
		}
		if err := writer.Write(row); err != nil {
			return "", fmt.Errorf("failed to write row: %v", err)
		}
	}

	writer.Flush()
	if err := writer.Error(); err != nil {
		return "", fmt.Errorf("error flushing CSV writer: %v", err)
	}

	return csvBuf.String(), nil
}

// Parameter helpers
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

