package cli

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v3"

	"github.com/probe-lab/go-commons/db"
)

var (
	validBaseCfgFn = func() *db.ClickHouseBaseConfig {
		return &db.ClickHouseBaseConfig{
			Host: "localhost",
			Port: 9440,
			User: "default",
			Pass: "password",
			SSL:  true,
		}
	}

	validCfgFn = func() *db.ClickHouseConfig {
		return &db.ClickHouseConfig{
			BaseConfig: validBaseCfgFn(),
			Database:   "database",
		}
	}

	validMultiCfgFn = func() *db.ClickHouseMultiConfig {
		return &db.ClickHouseMultiConfig{
			BaseConfig: validBaseCfgFn(),
			Databases:  []string{"database1", "database2"},
		}
	}
)

func TestClickHouseBaseConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		cfgFn   func() *db.ClickHouseBaseConfig
		wantErr bool
	}{
		{
			name:    "valid",
			cfgFn:   validBaseCfgFn,
			wantErr: false,
		},
		{
			name:    "nil",
			cfgFn:   func() *db.ClickHouseBaseConfig { return nil },
			wantErr: true,
		},
		{
			name: "no host",
			cfgFn: func() *db.ClickHouseBaseConfig {
				cfg := validBaseCfgFn()
				cfg.Host = ""
				return cfg
			},
			wantErr: true,
		},
		{
			name: "no port",
			cfgFn: func() *db.ClickHouseBaseConfig {
				cfg := validBaseCfgFn()
				cfg.Port = 0
				return cfg
			},
			wantErr: true,
		},
		{
			name: "negative port",
			cfgFn: func() *db.ClickHouseBaseConfig {
				cfg := validBaseCfgFn()
				cfg.Port = -1
				return cfg
			},
			wantErr: true,
		},
		{
			name: "no user",
			cfgFn: func() *db.ClickHouseBaseConfig {
				cfg := validBaseCfgFn()
				cfg.User = ""
				return cfg
			},
			wantErr: true,
		},
		{
			name: "no password",
			cfgFn: func() *db.ClickHouseBaseConfig {
				cfg := validBaseCfgFn()
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

func TestDatabaseFlagsEnvPrefix(t *testing.T) {
	tests := []struct {
		prefix string
		want   string
	}{
		{prefix: "nebula", want: "NEBULA_"},
		{prefix: "NEBULA_", want: "NEBULA_"},
		{prefix: "", want: ""},
	}

	for _, tt := range tests {
		t.Run(tt.prefix, func(t *testing.T) {
			var flags []cli.Flag
			flags = append(flags, ClickHouseFlags(tt.prefix, db.DefaultClickHouseConfig("test"))...)
			flags = append(flags, ClickHouseMultiFlags(tt.prefix, db.DefaultClickHouseMultiConfig("test"))...)
			flags = append(flags, ClickHouseBaseFlags(tt.prefix, validBaseCfgFn())...)
			flags = append(flags, ClickHouseMigrationsFlags(tt.prefix, db.DefaultClickHouseMigrationsConfig())...)
			flags = append(flags, PostgresFlags(tt.prefix, &db.PostgresConfig{BaseConfig: &db.PostgresBaseConfig{}})...)
			flags = append(flags, PostgresMultiFlags(tt.prefix, &db.PostgresMultiConfig{BaseConfig: &db.PostgresBaseConfig{}})...)

			for _, f := range flags {
				for _, env := range f.(interface{ GetEnvVars() []string }).GetEnvVars() {
					rest, ok := strings.CutPrefix(env, tt.want)
					assert.True(t, ok && (strings.HasPrefix(rest, "CLICKHOUSE_") || strings.HasPrefix(rest, "POSTGRES_")), env)
				}
			}
		})
	}
}

func Test_clickHouseBaseFlags(t *testing.T) {
	envPrefix := "TEST_"
	flags := ClickHouseBaseFlags(envPrefix, validBaseCfgFn())
	assert.NotEmpty(t, flags)
}

func TestClickHouseConfig_Options(t *testing.T) {
	cfg := validCfgFn()
	cfg.BaseConfig.SSL = false
	opts := cfg.Options()
	assert.Nil(t, opts.TLS)
	require.Len(t, opts.Addr, 1)
	assert.Equal(t, fmt.Sprintf("%s:%d", cfg.BaseConfig.Host, cfg.BaseConfig.Port), opts.Addr[0])
	assert.Equal(t, cfg.BaseConfig.User, opts.Auth.Username)
	assert.Equal(t, cfg.Database, opts.Auth.Database)
	assert.Equal(t, cfg.BaseConfig.Pass, opts.Auth.Password)

	cfg.BaseConfig.SSL = true
	assert.NotNil(t, cfg.Options().TLS)
}

func TestClickHouseFlags(t *testing.T) {
	envPrefix := "TEST_"
	flags := ClickHouseFlags(envPrefix, validCfgFn())
	assert.NotEmpty(t, flags)
}

func TestClickHouseConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		cfgFn   func() *db.ClickHouseConfig
		wantErr bool
	}{
		{
			name:    "valid",
			cfgFn:   validCfgFn,
			wantErr: false,
		},
		{
			name:    "nil",
			cfgFn:   func() *db.ClickHouseConfig { return nil },
			wantErr: true,
		},
		{
			name: "empty db",
			cfgFn: func() *db.ClickHouseConfig {
				cfg := validCfgFn()
				cfg.Database = ""
				return cfg
			},
			wantErr: true,
		},
		{
			name: "invalid base config",
			cfgFn: func() *db.ClickHouseConfig {
				baseCfg := validBaseCfgFn()
				baseCfg.Host = ""
				cfg := validCfgFn()
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

func TestClickHouseMultiConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		cfgFn   func() *db.ClickHouseMultiConfig
		wantErr bool
	}{
		{
			name:    "valid",
			cfgFn:   validMultiCfgFn,
			wantErr: false,
		},
		{
			name:    "nil",
			cfgFn:   func() *db.ClickHouseMultiConfig { return nil },
			wantErr: true,
		},
		{
			name: "no databases",
			cfgFn: func() *db.ClickHouseMultiConfig {
				cfg := validMultiCfgFn()
				cfg.Databases = []string{}
				return cfg
			},
			wantErr: true,
		},
		{
			name: "empty database",
			cfgFn: func() *db.ClickHouseMultiConfig {
				cfg := validMultiCfgFn()
				cfg.Databases = []string{"", "database2"}
				return cfg
			},
			wantErr: true,
		},
		{
			name: "invalid base config",
			cfgFn: func() *db.ClickHouseMultiConfig {
				baseCfg := validBaseCfgFn()
				baseCfg.Host = ""
				cfg := validMultiCfgFn()
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

func TestClickHouseMultiFlags(t *testing.T) {
	envPrefix := "TEST_"
	flags := ClickHouseMultiFlags(envPrefix, validMultiCfgFn())
	assert.NotEmpty(t, flags)
}
