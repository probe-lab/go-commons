package db

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// ClickHouseBaseConfig represents the foundational configuration required to
// establish a connection to a ClickHouse server. It includes basic connection
// parameters such as Host, Port, User, Password, and SSL option. This base
// configuration is used as a building block in more advanced configurations
// like [ClickHouseConfig] and [ClickHouseMultiConfig], which add additional
// settings such as specifying a single database or a list of databases to
// connect to respectively.
type ClickHouseBaseConfig struct {
	Host string
	Port int
	User string
	Pass string
	SSL  bool
}

// Validate checks the [ClickHouseBaseConfig] fields for validity and returns an
// error if any field contains invalid data. It ensures that the Host, Port,
// User, and Pass fields are all properly set.
func (cfg *ClickHouseBaseConfig) Validate() error {
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

	return nil
}

// ClickHouseConfig extends the [ClickHouseBaseConfig] to include a specific
// database in its configuration. It builds upon the base configuration by
// adding a database field, allowing users to connect to a single specific
// ClickHouse database. This structure is essential when distinct database
// connections are required as opposed to general configuration setups.
type ClickHouseConfig struct {
	BaseConfig *ClickHouseBaseConfig
	Database   string
}

// DefaultClickHouseConfig creates a new [ClickHouseConfig] instance with default
// values for the Host, Port, User, and Pass fields. It also sets the SSL field
// to false. This function is useful for populating the command line config with
// default values.
func DefaultClickHouseConfig(name string) *ClickHouseConfig {
	return &ClickHouseConfig{
		BaseConfig: &ClickHouseBaseConfig{
			Host: "127.0.0.1",
			Port: 9400,
			User: name,
			Pass: "password",
			SSL:  false,
		},
		Database: name,
	}
}

// DefaultClickHouseMultiConfig creates a ClickHouseMultiConfig for local use.
// It initializes the base configuration using [DefaultClickHouseConfig] and
// sets the Databases field to a single-element slice containing the given name.
func DefaultClickHouseMultiConfig(name string) *ClickHouseMultiConfig {
	cfg := DefaultClickHouseConfig(name)
	return &ClickHouseMultiConfig{
		BaseConfig: cfg.BaseConfig,
		Databases:  []string{name},
	}
}

// Validate checks the [ClickHouseConfig] fields for validity and returns an
// error if any field contains invalid data. It ensures that the Host, Port,
// User, and Pass fields are all properly set.
func (cfg *ClickHouseConfig) Validate() error {
	if cfg == nil {
		return fmt.Errorf("config is nil")
	}

	if cfg.Database == "" {
		return fmt.Errorf("database must not be empty")
	}

	return cfg.BaseConfig.Validate()
}

// The Options method returns a clickhouse.Options struct which can be
// used to establish a connection with the configured settings, including
// creating authentication details and handling connection contexts with
// SSL support when necessary.
func (cfg *ClickHouseConfig) Options() *clickhouse.Options {
	opts := &clickhouse.Options{
		Addr: []string{
			fmt.Sprintf("%s:%d", cfg.BaseConfig.Host, cfg.BaseConfig.Port),
		},
		Auth: clickhouse.Auth{
			Database: cfg.Database,
			Username: cfg.BaseConfig.User,
			Password: cfg.BaseConfig.Pass,
		},
	}

	if cfg.BaseConfig.SSL {
		opts.TLS = &tls.Config{}
	}

	return opts
}

// ClickHouseMultiConfig extends [ClickHouseBaseConfig] to support multiple
// database connections. It retains the base configuration for the ClickHouse
// server, such as host and user details, while incorporating a slice of database
// names. This allows for specifying connections to multiple ClickHouse databases
// within the same configuration context. It is particularly useful in scenarios
// where an application must interact with various databases simultaneously.
type ClickHouseMultiConfig struct {
	BaseConfig *ClickHouseBaseConfig
	Databases  []string
}

// Validate checks the [ClickHouseMultiConfig] to ensure it is valid and
// returns an error if it is not. This function ensures that the configuration
// is not nil and that at least one database is specified in the Databases slice.
// Additionally, it verifies that none of the database names are empty strings.
// Finally, it validates the [ClickHouseBaseConfig] to ensure host and user
// credentials are set appropriately.
func (cfg *ClickHouseMultiConfig) Validate() error {
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

func (cfg *ClickHouseMultiConfig) Options() []*clickhouse.Options {
	opts := make([]*clickhouse.Options, len(cfg.Databases))

	for i, db := range cfg.Databases {
		chCfg := ClickHouseConfig{
			BaseConfig: cfg.BaseConfig,
			Database:   db,
		}

		opts[i] = chCfg.Options()
	}

	return opts
}

func (cfg *ClickHouseMultiConfig) OpenAndPing(ctx context.Context) ([]driver.Conn, error) {
	conns := make([]driver.Conn, len(cfg.Databases))

	for i, opt := range cfg.Options() {

		slog.With(
			"addr", fmt.Sprintf("%s:%d", cfg.BaseConfig.Host, cfg.BaseConfig.Port),
			"user", opt.Auth.Username,
			"database", opt.Auth.Database,
			"ssl", opt.TLS != nil,
		).Info("Opening clickhouse")

		conn, err := clickhouse.Open(opt)
		if err != nil {
			return conns, fmt.Errorf("open clickhouse (%s@%s): %w", opt.Auth.Username, opt.Auth.Database, err)
		}

		// keep track of the clickhouse client
		conns[i] = conn

		// Ping the ClickHouse client to ensure the connection is valid
		if err = conn.Ping(ctx); err != nil {
			return conns, fmt.Errorf("ping clickhouse (%s@%s): %w", opt.Auth.Username, opt.Auth.Database, err)
		}
	}

	return conns, nil
}
