package log

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefaultConfigValidates(t *testing.T) {
	require.NoError(t, DefaultConfig().Validate())
}

func TestNewLogger(t *testing.T) {
	for _, format := range []string{"console", "text", "json"} {
		t.Run(format, func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.Format = format
			l, err := NewLogger(cfg)
			require.NoError(t, err)
			assert.NotNil(t, l)
		})
	}
}

func TestNewLoggerRejects(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
		want string
	}{
		{name: "format", cfg: Config{Level: "info", Format: "bogus"}, want: "unsupported log format"},
		{name: "level", cfg: Config{Level: "loud", Format: "console"}, want: "unknown log level"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewLogger(&tt.cfg)
			require.ErrorContains(t, err, tt.want)
		})
	}
}
