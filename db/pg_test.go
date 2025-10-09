package db

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

var (
	validPostgresBaseCfgFn = func() *PostgresBaseConfig {
		return &PostgresBaseConfig{
			Host:    "localhost",
			Port:    9440,
			User:    "default",
			Pass:    "password",
			SSLMode: "require",
		}
	}

	validPostgresCfgFn = func() *PostgresConfig {
		return &PostgresConfig{
			BaseConfig: validPostgresBaseCfgFn(),
			Database:   "database",
		}
	}

	validPostgresMultiCfgFn = func() *PostgresMultiConfig {
		return &PostgresMultiConfig{
			BaseConfig: validPostgresBaseCfgFn(),
			Databases:  []string{"database1", "database2"},
		}
	}
)

func TestPostgresBaseConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		cfgFn   func() *PostgresBaseConfig
		wantErr bool
	}{
		{
			name:    "validPostgres",
			cfgFn:   validPostgresBaseCfgFn,
			wantErr: false,
		},
		{
			name:    "nil",
			cfgFn:   func() *PostgresBaseConfig { return nil },
			wantErr: true,
		},
		{
			name: "no host",
			cfgFn: func() *PostgresBaseConfig {
				cfg := validPostgresBaseCfgFn()
				cfg.Host = ""
				return cfg
			},
			wantErr: true,
		},
		{
			name: "no port",
			cfgFn: func() *PostgresBaseConfig {
				cfg := validPostgresBaseCfgFn()
				cfg.Port = 0
				return cfg
			},
			wantErr: true,
		},
		{
			name: "negative port",
			cfgFn: func() *PostgresBaseConfig {
				cfg := validPostgresBaseCfgFn()
				cfg.Port = -1
				return cfg
			},
			wantErr: true,
		},
		{
			name: "no user",
			cfgFn: func() *PostgresBaseConfig {
				cfg := validPostgresBaseCfgFn()
				cfg.User = ""
				return cfg
			},
			wantErr: true,
		},
		{
			name: "no password",
			cfgFn: func() *PostgresBaseConfig {
				cfg := validPostgresBaseCfgFn()
				cfg.Pass = ""
				return cfg
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.cfgFn().Validate(); (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestPostgresConfig_Options(t *testing.T) {
	cfg := validPostgresCfgFn()
	assert.Equal(t, "host=localhost port=9440 dbname=database user=default password=password sslmode=require", cfg.SourceName())
}

func TestPostgresConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		cfgFn   func() *PostgresConfig
		wantErr bool
	}{
		{
			name:    "validPostgres",
			cfgFn:   validPostgresCfgFn,
			wantErr: false,
		},
		{
			name:    "nil",
			cfgFn:   func() *PostgresConfig { return nil },
			wantErr: true,
		},
		{
			name: "empty db",
			cfgFn: func() *PostgresConfig {
				cfg := validPostgresCfgFn()
				cfg.Database = ""
				return cfg
			},
			wantErr: true,
		},
		{
			name: "invalidPostgres base config",
			cfgFn: func() *PostgresConfig {
				baseCfg := validPostgresBaseCfgFn()
				baseCfg.Host = ""
				cfg := validPostgresCfgFn()
				cfg.BaseConfig = baseCfg
				return cfg
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.cfgFn().Validate(); (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestPostgresMultiConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		cfgFn   func() *PostgresMultiConfig
		wantErr bool
	}{
		{
			name:    "validPostgres",
			cfgFn:   validPostgresMultiCfgFn,
			wantErr: false,
		},
		{
			name:    "nil",
			cfgFn:   func() *PostgresMultiConfig { return nil },
			wantErr: true,
		},
		{
			name: "no databases",
			cfgFn: func() *PostgresMultiConfig {
				cfg := validPostgresMultiCfgFn()
				cfg.Databases = []string{}
				return cfg
			},
			wantErr: true,
		},
		{
			name: "empty database",
			cfgFn: func() *PostgresMultiConfig {
				cfg := validPostgresMultiCfgFn()
				cfg.Databases = []string{"", "database2"}
				return cfg
			},
			wantErr: true,
		},
		{
			name: "invalidPostgres base config",
			cfgFn: func() *PostgresMultiConfig {
				baseCfg := validPostgresBaseCfgFn()
				baseCfg.Host = ""
				cfg := validPostgresMultiCfgFn()
				cfg.BaseConfig = baseCfg
				return cfg
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.cfgFn().Validate(); (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestPostgresMultiConfig_Options(t *testing.T) {
	cfg := validPostgresMultiCfgFn()
	for _, database := range cfg.Databases {
		pgCfg := PostgresConfig{
			BaseConfig: cfg.BaseConfig,
			Database:   database,
		}
		expected := "host=localhost port=9440 dbname=database user=default password=password sslmode=require"
		expected = strings.Replace(expected, "database", database, 1)
		assert.Equal(t, expected, pgCfg.SourceName())
	}
}
