package db

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	validClickHouseBaseCfgFn = func() *ClickHouseBaseConfig {
		return &ClickHouseBaseConfig{
			Host: "localhost",
			Port: 9440,
			User: "default",
			Pass: "password",
			SSL:  true,
		}
	}

	validClickHouseCfgFn = func() *ClickHouseConfig {
		return &ClickHouseConfig{
			BaseConfig: validClickHouseBaseCfgFn(),
			Database:   "database",
		}
	}

	validClickHouseMultiCfgFn = func() *ClickHouseMultiConfig {
		return &ClickHouseMultiConfig{
			BaseConfig: validClickHouseBaseCfgFn(),
			Databases:  []string{"database1", "database2"},
		}
	}
)

func TestClickHouseBaseConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		cfgFn   func() *ClickHouseBaseConfig
		wantErr bool
	}{
		{
			name:    "validClickHouse",
			cfgFn:   validClickHouseBaseCfgFn,
			wantErr: false,
		},
		{
			name:    "nil",
			cfgFn:   func() *ClickHouseBaseConfig { return nil },
			wantErr: true,
		},
		{
			name: "no host",
			cfgFn: func() *ClickHouseBaseConfig {
				cfg := validClickHouseBaseCfgFn()
				cfg.Host = ""
				return cfg
			},
			wantErr: true,
		},
		{
			name: "no port",
			cfgFn: func() *ClickHouseBaseConfig {
				cfg := validClickHouseBaseCfgFn()
				cfg.Port = 0
				return cfg
			},
			wantErr: true,
		},
		{
			name: "negative port",
			cfgFn: func() *ClickHouseBaseConfig {
				cfg := validClickHouseBaseCfgFn()
				cfg.Port = -1
				return cfg
			},
			wantErr: true,
		},
		{
			name: "no user",
			cfgFn: func() *ClickHouseBaseConfig {
				cfg := validClickHouseBaseCfgFn()
				cfg.User = ""
				return cfg
			},
			wantErr: true,
		},
		{
			name: "no password",
			cfgFn: func() *ClickHouseBaseConfig {
				cfg := validClickHouseBaseCfgFn()
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

func TestClickHouseConfig_Options(t *testing.T) {
	cfg := validClickHouseCfgFn()
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

func TestClickHouseConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		cfgFn   func() *ClickHouseConfig
		wantErr bool
	}{
		{
			name:    "validClickHouse",
			cfgFn:   validClickHouseCfgFn,
			wantErr: false,
		},
		{
			name:    "nil",
			cfgFn:   func() *ClickHouseConfig { return nil },
			wantErr: true,
		},
		{
			name: "empty db",
			cfgFn: func() *ClickHouseConfig {
				cfg := validClickHouseCfgFn()
				cfg.Database = ""
				return cfg
			},
			wantErr: true,
		},
		{
			name: "invalidClickHouse base config",
			cfgFn: func() *ClickHouseConfig {
				baseCfg := validClickHouseBaseCfgFn()
				baseCfg.Host = ""
				cfg := validClickHouseCfgFn()
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
		cfgFn   func() *ClickHouseMultiConfig
		wantErr bool
	}{
		{
			name:    "validClickHouse",
			cfgFn:   validClickHouseMultiCfgFn,
			wantErr: false,
		},
		{
			name:    "nil",
			cfgFn:   func() *ClickHouseMultiConfig { return nil },
			wantErr: true,
		},
		{
			name: "no databases",
			cfgFn: func() *ClickHouseMultiConfig {
				cfg := validClickHouseMultiCfgFn()
				cfg.Databases = []string{}
				return cfg
			},
			wantErr: true,
		},
		{
			name: "empty database",
			cfgFn: func() *ClickHouseMultiConfig {
				cfg := validClickHouseMultiCfgFn()
				cfg.Databases = []string{"", "database2"}
				return cfg
			},
			wantErr: true,
		},
		{
			name: "invalidClickHouse base config",
			cfgFn: func() *ClickHouseMultiConfig {
				baseCfg := validClickHouseBaseCfgFn()
				baseCfg.Host = ""
				cfg := validClickHouseMultiCfgFn()
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

func TestClickHouseMultiConfig_Configs(t *testing.T) {
	cfg := validClickHouseMultiCfgFn()
	cfgs := cfg.Configs()
	assert.Len(t, cfgs, len(cfg.Databases))

	for i, c := range cfgs {
		opts := c.Options()
		assert.NotNil(t, c.BaseConfig)
		require.Len(t, opts.Addr, 1)
		assert.Equal(t, fmt.Sprintf("%s:%d", cfg.BaseConfig.Host, cfg.BaseConfig.Port), opts.Addr[0])
		assert.Equal(t, cfg.BaseConfig.User, opts.Auth.Username)
		assert.Equal(t, cfg.Databases[i], opts.Auth.Database)
		assert.Equal(t, cfg.BaseConfig.Pass, opts.Auth.Password)
	}
}
