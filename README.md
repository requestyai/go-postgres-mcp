# 🚀 Ultra-Fast PostgreSQL MCP Server

Zero burden, ready-to-use Model Context Protocol (MCP) server for PostgreSQL with extreme performance optimization. No Node.js or Python environment needed. This server provides comprehensive tools for database operations with advanced safety features and connection pooling.

## Installation

### Option 1: Install from GitHub (Recommended)

```bash
go install -v github.com/requestyai/go-postgres-mcp@latest
```

### Option 2: Build from source

```bash
git clone https://github.com/requestyai/go-postgres-mcp.git
cd go-postgres-mcp
go build -o go-postgres-mcp .
```

## Usage

### Method A: Using Command Line Arguments for stdio mode

```json
{
	"mcpServers": {
		"postgres": {
			"command": "go-postgres-mcp",
			"args": ["--dsn", "postgresql://user:pass@host:port/db"]
		}
	}
}
```

**Note:** For those who put the binary outside of your `$PATH`, you need to replace `go-postgres-mcp` with the full path to the binary:

```json
{
	"mcpServers": {
		"postgres": {
			"command": "/full/path/to/go-postgres-mcp",
			"args": ["--dsn", "postgresql://user:pass@host:port/db"]
		}
	}
}
```

### Method B: Using Command Line Arguments for SSE mode

```bash
./go-postgres-mcp --t sse --ip x.x.x.x --port 8080 --dsn postgresql://user:pass@host:port/db --lang en
```

## Optional Flags

- `--lang`: Set language option (en/zh-CN), defaults to system language
- `--read-only`: Enable read-only mode. In this mode, only SELECT and schema inspection tools are available
- `--with-explain-check`: Check query plan with EXPLAIN before executing queries
- `--ip`: Server IP address for SSE mode (default: localhost)
- `--port`: Server port for SSE mode (default: 8080)
- `-t`: Transport type - "stdio" or "sse" (default: stdio)

## Tools

**Multi-language support**: All tool descriptions automatically localize based on the `--lang` parameter.

### 📊 Schema Tools

**list_databases**

- Description: List all databases in the PostgreSQL server
- Parameters: None
- Returns: A list of database names with size information

**list_tables**

- Description: List all tables in the current database
- Parameters:
  - `schema` (optional): Schema name to filter tables
- Returns: A list of table names

**list_columns**

- Description: List all columns for a specific table
- Parameters:
  - `table_name` (required): Name of the table
  - `schema` (optional): Schema name (defaults to 'public')
- Returns: Column details with types and constraints

**describe_table**

- Description: Get detailed table structure with constraints and indexes
- Parameters:
  - `name` (required): Name of the table to describe
  - `schema` (optional): Schema name (defaults to 'public')
- Returns: Complete table structure information

**get_table_size**

- Description: Get table size and row count information
- Parameters:
  - `table_name` (required): Name of the table
  - `schema` (optional): Schema name (defaults to 'public')
- Returns: Table size and row count

**list_indexes**

- Description: List all indexes for a table or database
- Parameters:
  - `table_name` (optional): Name of the table (lists all if empty)
  - `schema` (optional): Schema name
- Returns: Index information

### 🔍 Query Tools

**read_query**

- Description: Execute a read-only SQL query with safety checks
- Parameters:
  - `query` (required): SQL SELECT query to execute
  - `limit` (optional): Maximum rows to return (default: 1000)
- Returns: Query results in CSV format

**explain_query**

- Description: Analyze query execution plan
- Parameters:
  - `query` (required): SQL query to analyze
  - `analyze` (optional): Run EXPLAIN ANALYZE (default: false)
- Returns: Query execution plan

**count_query**

- Description: Count rows in a table with optional conditions
- Parameters:
  - `table_name` (required): Name of the table
  - `where_clause` (optional): Optional WHERE conditions
  - `schema` (optional): Schema name (defaults to 'public')
- Returns: Row count

### ✏️ Write Tools (available when not in read-only mode)

**write_query**

- Description: Execute an INSERT query
- Parameters:
  - `query` (required): SQL INSERT query to execute
- Returns: Number of rows affected

**update_query**

- Description: Execute an UPDATE query with WHERE clause validation
- Parameters:
  - `query` (required): SQL UPDATE query to execute
- Returns: Number of rows affected

**delete_query**

- Description: Execute a DELETE query with WHERE clause validation
- Parameters:
  - `query` (required): SQL DELETE query to execute
- Returns: Number of rows affected

**create_table**

- Description: Create a new table
- Parameters:
  - `query` (required): CREATE TABLE SQL statement
- Returns: Confirmation message

**alter_table**

- Description: Alter an existing table structure
- Parameters:
  - `query` (required): ALTER TABLE SQL statement
- Returns: Confirmation message

**create_index**

- Description: Create an index on a table
- Parameters:
  - `query` (required): CREATE INDEX SQL statement
- Returns: Confirmation message

## Performance Features

- **Ultra-fast connection pooling** with pgxpool
- **Optimized query execution** with minimal memory allocation
- **Advanced safety checks** including automatic WHERE clause validation for UPDATE/DELETE
- **CSV output formatting** for efficient large result set handling
- **Query plan analysis** with optional EXPLAIN checks

## Safety Features

- **Automatic WHERE clause validation** for UPDATE/DELETE operations
- **Read-only mode** option to prevent write operations
- **Query plan analysis** with `--with-explain-check` flag
- **Connection pooling** for maximum performance and stability
- **Comprehensive error handling** and logging

## Language Support

If you want to add your own language support, please refer to the `locales/` folder. Create a new `locales/xxx/active-xx.toml` file for your language.

## License

MIT
