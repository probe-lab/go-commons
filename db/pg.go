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

func (cfg *PostgresConfig) SourceName() string {
	return fmt.Sprintf(
		"host=%s port=%d dbname=%s user=%s password=%s sslmode=%s",
		cfg.BaseConfig.Host,
		cfg.BaseConfig.Port,
		cfg.Database,
		cfg.BaseConfig.User,
		cfg.BaseConfig.Pass,
		cfg.BaseConfig.SSLMode,
	)
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

		handle, err := otelsql.Open("postgres", pgCfg.SourceName(),
			otelsql.WithAttributes(semconv.DBSystemPostgreSQL),
		)
		if err != nil {
			return handles, fmt.Errorf("opening %s database: %w", database, err)
		}

		otelsql.ReportDBStatsMetrics(handle)

		handles[i] = handle

		// Ping database to verify connection.
		if err = handle.PingContext(ctx); err != nil {
			return handles, fmt.Errorf("pinging database: %w", err)
		}
	}

	return handles, nil
}
