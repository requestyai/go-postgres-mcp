package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgxpool/v5"
	"github.com/mark3labs/mcp-go/server"
	"github.com/rs/zerolog"
	"github.com/spf13/viper"
)

type PostgresMCPServer struct {
	config  *Config
	logger  zerolog.Logger
	dbPool  *pgxpool.Pool
	metrics *ServerMetrics
	server  *server.Server
}

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

func NewPostgresMCPServer() *PostgresMCPServer {
	return &PostgresMCPServer{
		metrics: &ServerMetrics{
			StartTime: time.Now(),
		},
	}
}

func (s *PostgresMCPServer) Start() error {
	if err := s.initConfig(); err != nil {
		return fmt.Errorf("failed to initialize config: %w", err)
	}

	s.initLogger()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize database connection pool
	if err := s.initDatabase(ctx); err != nil {
		return fmt.Errorf("failed to initialize database: %w", err)
	}
	defer s.dbPool.Close()

	// Create MCP server
	s.server = s.createMCPServer()

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		s.logger.Info().Msg("Shutting down server...")
		cancel()
	}()

	// Start server
	s.logger.Info().
		Str("transport", s.config.Transport).
		Str("version", version).
		Msg("Starting ultra-fast PostgreSQL MCP server")

	if s.config.Transport == "sse" {
		sseServer := server.NewSSEServer(s.server,
			server.WithBaseURL(fmt.Sprintf("http://%s:%d", s.config.IPAddress, s.config.Port)))

		s.logger.Info().
			Str("address", fmt.Sprintf("%s:%d", s.config.IPAddress, s.config.Port)).
			Msg("SSE server listening")

		return sseServer.Start(fmt.Sprintf("%s:%d", s.config.IPAddress, s.config.Port))
	} else {
		return server.ServeStdio(s.server)
	}
}

func (s *PostgresMCPServer) initConfig() error {
	s.config = &Config{}
	if err := viper.Unmarshal(s.config); err != nil {
		return err
	}

	// Set defaults
	if s.config.MaxConnections == 0 {
		s.config.MaxConnections = 100
	}
	if s.config.QueryTimeout == 0 {
		s.config.QueryTimeout = 30
	}
	if s.config.CacheSize == 0 {
		s.config.CacheSize = 1000
	}
	if s.config.PoolMaxIdleTime == 0 {
		s.config.PoolMaxIdleTime = 300
	}

	return nil
}

func (s *PostgresMCPServer) initLogger() {
	level, err := zerolog.ParseLevel(s.config.LogLevel)
	if err != nil {
		level = zerolog.InfoLevel
	}

	s.logger = zerolog.New(os.Stdout).
		Level(level).
		With().
		Timestamp().
		Str("service", "postgres-mcp").
		Logger()
}

func (s *PostgresMCPServer) initDatabase(ctx context.Context) error {
	if s.config.DSN == "" {
		return fmt.Errorf("DSN is required")
	}

	poolConfig, err := pgxpool.ParseConfig(s.config.DSN)
	if err != nil {
		return fmt.Errorf("failed to parse DSN: %w", err)
	}

	// Optimize connection pool for performance
	poolConfig.MaxConns = s.config.MaxConnections
	poolConfig.MinConns = 5
	poolConfig.MaxConnLifetime = time.Hour
	poolConfig.MaxConnIdleTime = time.Duration(s.config.PoolMaxIdleTime) * time.Second
	poolConfig.HealthCheckPeriod = time.Minute

	// Configure connection for performance
	poolConfig.ConnConfig.RuntimeParams = map[string]string{
		"application_name": "requesty-postgres-mcp",
		"timezone":         "UTC",
	}

	s.dbPool, err = pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return fmt.Errorf("failed to create connection pool: %w", err)
	}

	// Test connection
	if err := s.dbPool.Ping(ctx); err != nil {
		return fmt.Errorf("failed to ping database: %w", err)
	}

	s.logger.Info().
		Int32("max_connections", s.config.MaxConnections).
		Str("database", "connected").
		Msg("Database pool initialized")

	return nil
}

func (s *PostgresMCPServer) createMCPServer() *server.Server {
	mcpServer := server.NewMCPServer(
		"requesty-postgres-mcp",
		version,
		server.WithResourceCapabilities(true, true),
		server.WithPromptCapabilities(true),
		server.WithLogging(),
	)

	// Initialize handlers with server context
	handlers := NewHandlers(s)

	// Schema management tools
	mcpServer.AddTool(handlers.CreateListDatabasesTool(), handlers.HandleListDatabases)
	mcpServer.AddTool(handlers.CreateListTablesTool(), handlers.HandleListTables)
	mcpServer.AddTool(handlers.CreateListColumnsTool(), handlers.HandleListColumns)
	mcpServer.AddTool(handlers.CreateDescribeTableTool(), handlers.HandleDescribeTable)
	mcpServer.AddTool(handlers.CreateGetTableSizeTool(), handlers.HandleGetTableSize)
	mcpServer.AddTool(handlers.CreateListIndexesTool(), handlers.HandleListIndexes)
	mcpServer.AddTool(handlers.CreateListConstraintsTool(), handlers.HandleListConstraints)

	// Query tools
	mcpServer.AddTool(handlers.CreateReadQueryTool(), handlers.HandleReadQuery)
	mcpServer.AddTool(handlers.CreateCountQueryTool(), handlers.HandleCountQuery)
	mcpServer.AddTool(handlers.CreateExplainQueryTool(), handlers.HandleExplainQuery)

	// Write tools (if not read-only)
	if !s.config.ReadOnly {
		mcpServer.AddTool(handlers.CreateWriteQueryTool(), handlers.HandleWriteQuery)
		mcpServer.AddTool(handlers.CreateUpdateQueryTool(), handlers.HandleUpdateQuery)
		mcpServer.AddTool(handlers.CreateDeleteQueryTool(), handlers.HandleDeleteQuery)
		mcpServer.AddTool(handlers.CreateCreateTableTool(), handlers.HandleCreateTable)
		mcpServer.AddTool(handlers.CreateAlterTableTool(), handlers.HandleAlterTable)
		mcpServer.AddTool(handlers.CreateCreateIndexTool(), handlers.HandleCreateIndex)
		mcpServer.AddTool(handlers.CreateDropIndexTool(), handlers.HandleDropIndex)
	}

	// Performance and monitoring tools
	mcpServer.AddTool(handlers.CreateGetStatsTool(), handlers.HandleGetStats)
	mcpServer.AddTool(handlers.CreateGetSlowQueresTool(), handlers.HandleGetSlowQueries)
	mcpServer.AddTool(handlers.CreateAnalyzeTableTool(), handlers.HandleAnalyzeTable)

	return mcpServer
}
