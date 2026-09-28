package log

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/lmittmann/tint"
)

// Config configures the logger built by NewLogger.
type Config struct {
	// Level is the minimum level to log: debug, info, warn, or error.
	Level string
	// Format selects the handler: console (one line per record, level in
	// color when stderr is a terminal), text (slog key=value), or json.
	Format string
	// Source adds the file and line of the log call to every record.
	Source bool
}

func DefaultConfig() *Config {
	return &Config{
		Level:  "info",
		Format: "console",
		Source: false,
	}
}

// Validate checks that the level and the format are known.
func (c *Config) Validate() error {
	if _, err := c.level(); err != nil {
		return err
	}

	switch c.Format {
	case "console", "text", "json":
		return nil
	default:
		return fmt.Errorf("unsupported log format %q (console, text, json)", c.Format)
	}
}

func (c *Config) level() (slog.Level, error) {
	var l slog.Level
	if err := l.UnmarshalText([]byte(c.Level)); err != nil {
		return 0, fmt.Errorf("unknown log level %q: %w", c.Level, err)
	}
	return l, nil
}

// NewLogger configures a structured logger based on the given configuration. If any
// of the configuration parameters do not match expected values, it returns an
// error.
func NewLogger(cfg *Config) (*slog.Logger, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	logLevel, _ := cfg.level()

	var h slog.Handler
	switch cfg.Format {
	case "console":
		// Colors only reach a terminal. Files, pipes, and journals get the
		// same lines without escape codes, and NO_COLOR turns them off too.
		h = tint.NewHandler(os.Stderr, &tint.Options{
			AddSource: cfg.Source,
			Level:     logLevel,
			NoColor:   !isTerminal(os.Stderr) || os.Getenv("NO_COLOR") != "",
		})
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
	}

	// wrap the base handler into our custom one so that we can enrich
	// log information with custom fields extracted from the log context.
	wrapped := &handler{Handler: h}

	return slog.New(wrapped), nil
}

// isTerminal reports whether f is a character device, which is what a
// terminal is and a file or pipe is not.
func isTerminal(f *os.File) bool {
	fi, err := f.Stat()
	return err == nil && fi.Mode()&os.ModeCharDevice != 0
}

// SetGlobaLogger applies the given configuration to the global slog.SetGlobal
func SetGlobalLogger(cfg *Config) error {
	logger, err := NewLogger(cfg)
	if err != nil {
		return err
	}
	slog.SetDefault(logger)
	return nil
}

func Defer(fn func() error, errMsg string) {
	if err := fn(); err != nil {
		slog.Warn(errMsg, "err", err)
	}
}
