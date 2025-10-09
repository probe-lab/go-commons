package log

import (
	"fmt"
	"log/slog"
	"os"
)

type Config struct {
	Level  string
	Format string
	Source bool
}

func DefaultConfig() *Config {
	return &Config{
		Level:  "info",
		Format: "text",
		Source: false,
	}
}

// New configures a structured logger based on the given configuration. If any
// of the configuration parameters do not match expected values, it returns an
// error.
func New(cfg *Config) (*slog.Logger, error) {
	// parse log level
	var logLevel slog.Level
	if err := logLevel.UnmarshalText([]byte(cfg.Level)); err != nil {
		return nil, fmt.Errorf("unknown log level %s: %w", cfg.Level, err)
	}

	// parse log format
	var h slog.Handler
	switch cfg.Format {
	case "text":
		h = slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
			AddSource: cfg.Source,
			Level:     logLevel,
		})
	case "json":
		h = slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
			AddSource: cfg.Source,
			Level:     logLevel,
		})
	default:
		return nil, fmt.Errorf("unsupported log format: %s", cfg.Format)
	}

	// wrap the base handler into our custom one so that we can enrich
	// log information with custom fields extracted from the log context.
	wrapped := &handler{Handler: h}

	return slog.New(wrapped), nil
}
