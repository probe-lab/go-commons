package db

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"

	"github.com/uptrace/opentelemetry-go-extra/otelsql"
	semconv "go.opentelemetry.io/otel/semconv/v1.27.0"
)

type PostgresBaseConfig struct {
	Host    string
	Port    int
	User    string
	Pass    string
	SSLMode string
}

func (cfg *PostgresBaseConfig) Validate() error {
	if cfg == nil {
		return fmt.Errorf("config is nil")
	}

	if cfg.Host == "" {
		return fmt.Errorf("host must not be empty")
	}

	if cfg.Port <= 0 {
		return fmt.Errorf("port must be a positive integer")
	}

	if cfg.User == "" {
		return fmt.Errorf("user must not be empty")
	}

	if cfg.Pass == "" {
		return fmt.Errorf("password must not be empty")
	}

	if cfg.SSLMode == "" {
		return fmt.Errorf("sslmode must not be empty")
	}

	return nil
}

type PostgresConfig struct {
	BaseConfig *PostgresBaseConfig
	Database   string
}

func (cfg *PostgresConfig) Validate() error {
	if cfg == nil {
		return fmt.Errorf("config is nil")
	}

	if cfg.Database == "" {
		return fmt.Errorf("database must not be empty")
	}

	return cfg.BaseConfig.Validate()
}

// SourceName returns the keyword/value connection string. The password pair
// is left out when the password is empty, so a server with trust
// authentication works without one.
func (cfg *PostgresConfig) SourceName() string {
	parts := []string{
		"host=" + cfg.BaseConfig.Host,
		fmt.Sprintf("port=%d", cfg.BaseConfig.Port),
		"dbname=" + cfg.Database,
		"user=" + cfg.BaseConfig.User,
	}

	if cfg.BaseConfig.Pass != "" {
		parts = append(parts, "password="+cfg.BaseConfig.Pass)
	}

	parts = append(parts, "sslmode="+cfg.BaseConfig.SSLMode)

	return strings.Join(parts, " ")
}

// OpenAndPing opens the database with the given database/sql driver name,
// wraps the handle with OpenTelemetry instrumentation, and pings it. The
// caller registers the driver: "postgres" for lib/pq, "pgx" for the stdlib
// package of pgx.
func (cfg *PostgresConfig) OpenAndPing(ctx context.Context, driverName string) (*sql.DB, error) {
	slog.Info("Initializing database handle",
		"driver", driverName,
		"host", cfg.BaseConfig.Host,
		"port", cfg.BaseConfig.Port,
		"user", cfg.BaseConfig.User,
		"ssl", cfg.BaseConfig.SSLMode,
		"database", cfg.Database,
	)

	handle, err := otelsql.Open(driverName, cfg.SourceName(),
		otelsql.WithAttributes(semconv.DBSystemPostgreSQL),
	)
	if err != nil {
		return nil, fmt.Errorf("opening %s database: %w", cfg.Database, err)
	}

	otelsql.ReportDBStatsMetrics(handle)

	if err = handle.PingContext(ctx); err != nil {
		_ = handle.Close()
		return nil, fmt.Errorf("pinging database: %w", err)
	}

	return handle, nil
}

type PostgresMultiConfig struct {
	BaseConfig *PostgresBaseConfig
	Databases  []string
}

func (cfg *PostgresMultiConfig) Validate() error {
	if cfg == nil {
		return fmt.Errorf("config is nil")
	}

	if len(cfg.Databases) == 0 {
		return fmt.Errorf("at least one database must be specified")
	}

	for _, db := range cfg.Databases {
		if db == "" {
			return fmt.Errorf("database name must not be empty")
		}
	}

	return cfg.BaseConfig.Validate()
}

func (cfg *PostgresMultiConfig) OpenAndPing(ctx context.Context) ([]*sql.DB, error) {
	slog.Info("Initializing database handles",
		"host", cfg.BaseConfig.Host,
		"port", cfg.BaseConfig.Port,
		"user", cfg.BaseConfig.User,
		"ssl", cfg.BaseConfig.SSLMode,
		"databases", strings.Join(cfg.Databases, ","),
	)

	handles := make([]*sql.DB, len(cfg.Databases))
	for i, database := range cfg.Databases {
		pgCfg := PostgresConfig{
			BaseConfig: cfg.BaseConfig,
			Database:   database,
		}

		handle, err := pgCfg.OpenAndPing(ctx, "postgres")
		if err != nil {
			return handles, err
		}

		handles[i] = handle
	}

	return handles, nil
}
