package db

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net"
	"strconv"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/golang-migrate/migrate/v4"
	mch "github.com/golang-migrate/migrate/v4/database/clickhouse"
	"github.com/golang-migrate/migrate/v4/source/iofs"
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
			Port: 9000,
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
			net.JoinHostPort(cfg.BaseConfig.Host, strconv.Itoa(cfg.BaseConfig.Port)),
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

func (cfg *ClickHouseConfig) OpenAndPing(ctx context.Context) (driver.Conn, error) {
	opt := cfg.Options()

	slog.With(
		"addr", opt.Addr[0],
		"user", opt.Auth.Username,
		"database", opt.Auth.Database,
		"ssl", opt.TLS != nil,
	).Info("Opening clickhouse")

	conn, err := clickhouse.Open(opt)
	if err != nil {
		return nil, fmt.Errorf("open clickhouse (%s@%s): %w", opt.Auth.Username, opt.Auth.Database, err)
	}

	// Ping the ClickHouse client to ensure the connection is valid
	if err = conn.Ping(ctx); err != nil {
		return nil, fmt.Errorf("ping clickhouse (%s@%s): %w", opt.Auth.Username, opt.Auth.Database, err)
	}

	return conn, nil
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

// Configs returns a slice of [ClickHouseConfig] instances, each with its own
// database name. This function is useful for iterating over the databases in
// a [ClickHouseMultiConfig] instance.
func (cfg *ClickHouseMultiConfig) Configs() []*ClickHouseConfig {
	configs := make([]*ClickHouseConfig, len(cfg.Databases))

	for i, db := range cfg.Databases {
		configs[i] = &ClickHouseConfig{
			BaseConfig: cfg.BaseConfig,
			Database:   db,
		}
	}

	return configs
}

// OpenAndPing opens and pings all of the configured databases in the
// [ClickHouseMultiConfig] instance. It returns a slice of [driver.Conn]
// instances, one for each database. This function is useful for establishing
// multiple connections to multiple databases at once.
func (cfg *ClickHouseMultiConfig) OpenAndPing(ctx context.Context) ([]driver.Conn, error) {
	conns := make([]driver.Conn, len(cfg.Databases))
	for i, c := range cfg.Configs() {
		conn, err := c.OpenAndPing(ctx)
		if err != nil {
			return conns, err
		}

		conns[i] = conn
	}

	return conns, nil
}

// ClickHouseMigrationsConfig represents the configuration options for
// ClickHouse migrations.
type ClickHouseMigrationsConfig struct {
	ClusterName            string
	MigrationsTable        string
	MigrationsTableEngine  string
	MultiStatementEnabled  bool
	MultiStatementMaxSize  int
	ReplicatedTableEngines bool
}

// DefaultClickHouseMigrationsConfig creates a new ClickHouseMigrationsConfig
// instance with default values. This function is useful for populating the
// command line config with default values.
func DefaultClickHouseMigrationsConfig() *ClickHouseMigrationsConfig {
	return &ClickHouseMigrationsConfig{
		ClusterName:            "",
		MigrationsTable:        mch.DefaultMigrationsTable,
		MigrationsTableEngine:  mch.DefaultMigrationsTableEngine,
		MultiStatementEnabled:  false,
		MultiStatementMaxSize:  mch.DefaultMultiStatementMaxSize,
		ReplicatedTableEngines: false,
	}
}

// Apply applies the migrations in the given filesystem to the given ClickHouse
// database. It returns an error if any migrations fail to apply. If
// ReplicatedTableEngines is set to false, it will replace all occurrences of
// "Replicated" with the empty string and replace "allow_experimental_json_type"
// with "enable_json_type" in the migrations. This is necessary because the
// migrations are otherwise not compatible with a local docker Clickhouse
// instance.
func (cfg *ClickHouseMigrationsConfig) Apply(opt *clickhouse.Options, migrations fs.ReadDirFS) error {
	db := clickhouse.OpenDB(opt)
	mdriver, err := mch.WithInstance(db, &mch.Config{
		DatabaseName:          opt.Auth.Database,
		ClusterName:           cfg.ClusterName,
		MigrationsTable:       cfg.MigrationsTable,
		MigrationsTableEngine: cfg.MigrationsTableEngine,
		MultiStatementEnabled: cfg.MultiStatementEnabled,
		MultiStatementMaxSize: cfg.MultiStatementMaxSize,
	})
	if err != nil {
		return fmt.Errorf("create migrate driver: %w", err)
	}

	if !cfg.ReplicatedTableEngines {
		migrations = &replacingFS{ReadDirFS: migrations, old: "Replicated", new: ""}
		migrations = &replacingFS{ReadDirFS: migrations, old: "allow_experimental_json_type", new: "enable_json_type"}
	}

	migrationsDir, err := iofs.New(migrations, "migrations")
	if err != nil {
		return fmt.Errorf("create iofs migrations source: %w", err)
	}

	m, err := migrate.NewWithInstance("iofs", migrationsDir, opt.Auth.Database, mdriver)
	if err != nil {
		return fmt.Errorf("create migrate instance: %w", err)
	}

	beforeVersion, _, err := m.Version()
	if errors.Is(err, migrate.ErrNilVersion) {
		slog.Info("Clean database - no migrations applied yet")
	} else if err != nil {
		return fmt.Errorf("get current migration version: %w", err)
	}

	// apply migrations
	err = m.Up()

	if errors.Is(err, migrate.ErrNoChange) {
		slog.Debug("No migrations to apply")
	} else if err != nil {
		return err
	} else {
		afterVersion, _, err := m.Version()
		if err != nil {
			return fmt.Errorf("get current migration version: %w", err)
		}
		slog.Info(fmt.Sprintf("Applied %d migrations to version %d", afterVersion-beforeVersion, afterVersion))
	}

	return nil
}

// replacingFS is a wrapper around an fs.FS that replaces all occurrences of
// the old string with the new string.
type replacingFS struct {
	fs.ReadDirFS
	old, new string
}

func (t *replacingFS) Open(name string) (fs.File, error) {
	f, err := t.ReadDirFS.Open(name)
	return &replacingFile{File: f, old: t.old, new: t.new}, err
}

// replacingFile is a wrapper around an fs.File that replaces all occurrences
// of the old string with the new string.
type replacingFile struct {
	fs.File
	reader   io.Reader
	old, new string
}

func (t *replacingFile) Read(p []byte) (int, error) {
	if t.reader == nil {
		content, err := io.ReadAll(t.File)
		if err != nil {
			return 0, err
		}
		modified := bytes.ReplaceAll(content, []byte(t.old), []byte(t.new))
		t.reader = bytes.NewReader(modified)
	}

	return t.reader.Read(p)
}
