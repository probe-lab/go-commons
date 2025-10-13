package cli

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"runtime/debug"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/urfave/cli/v3"

	"github.com/probe-lab/go-commons/log"
	"github.com/probe-lab/go-commons/tele"
)

const (
	flagCategoryDatabase  = "Database Configuration:"
	flagCategoryLogging   = "Logging Configuration:"
	flagCategoryTelemetry = "Telemetry Configuration:"
)

type RootCommand struct {
	cmd *cli.Command
	cfg *RootCommandConfig
}

type RootCommandConfig struct {
	BuildInfo     *BuildInfo
	Log           *log.Config
	Metrics       *tele.MetricsConfig
	Trace         *tele.TraceConfig
	ShutdownGrace time.Duration
	EnvPrefix     string
	AWSRegion     string

	metricsShutdown func(ctx context.Context) error
	tracesShutdown  func(ctx context.Context) error
}

func NewRootCommand(cmd *cli.Command) (*RootCommand, *RootCommandConfig) {
	cfg := &RootCommandConfig{
		BuildInfo:     buildInfo(),
		Log:           log.DefaultConfig(),
		Metrics:       tele.DefaultMetricsConfig(cmd.Name),
		Trace:         tele.DefaultTraceConfig(),
		ShutdownGrace: 30 * time.Second,
		EnvPrefix:     buildEnvPrefix(cmd.Name),
		AWSRegion:     "",

		metricsShutdown: func(ctx context.Context) error { return nil },
		tracesShutdown:  func(ctx context.Context) error { return nil },
	}

	shortCommit := cfg.BuildInfo.ShortCommit()
	if cfg.BuildInfo.Dirty {
		shortCommit += "+dirty"
	}

	if cmd.Version == "" {
		cmd.Version = shortCommit
	} else {
		cmd.Version += "-" + shortCommit
	}

	cmd.Flags = append(cmd.Flags, []cli.Flag{
		&cli.StringFlag{
			Name:        "log.level",
			Sources:     cli.EnvVars(cfg.EnvPrefix + "LOG_LEVEL"),
			Usage:       "Sets an explicit logging level: debug, info, warn, error.",
			Destination: &cfg.Log.Level,
			Value:       cfg.Log.Level,
			Category:    flagCategoryLogging,
		},
		&cli.StringFlag{
			Name:        "log.format",
			Sources:     cli.EnvVars(cfg.EnvPrefix + "LOG_FORMAT"),
			Usage:       "Sets the format to output the log statements in: text, json",
			Destination: &cfg.Log.Format,
			Value:       cfg.Log.Format,
			Category:    flagCategoryLogging,
		},
		&cli.BoolFlag{
			Name:        "log.source",
			Sources:     cli.EnvVars(cfg.EnvPrefix + "LOG_SOURCE"),
			Usage:       "Compute the source code position of a log statement and add a SourceKey attribute to the output.",
			Destination: &cfg.Log.Source,
			Value:       cfg.Log.Source,
			Category:    flagCategoryLogging,
		},
		&cli.BoolFlag{
			Name:        "metrics.enabled",
			Sources:     cli.EnvVars(cfg.EnvPrefix + "METRICS_ENABLED"),
			Usage:       "Whether to expose metrics information",
			Destination: &cfg.Metrics.Enabled,
			Value:       cfg.Metrics.Enabled,
			Category:    flagCategoryTelemetry,
		},
		&cli.StringFlag{
			Name:        "metrics.host",
			Sources:     cli.EnvVars(cfg.EnvPrefix + "METRICS_HOST"),
			Usage:       "Which network interface should the metrics endpoint bind to",
			Value:       cfg.Metrics.Host,
			Destination: &cfg.Metrics.Host,
			Category:    flagCategoryTelemetry,
		},
		&cli.IntFlag{
			Name:        "metrics.port",
			Sources:     cli.EnvVars(cfg.EnvPrefix + "METRICS_PORT"),
			Usage:       "On which port should the metrics endpoint listen",
			Value:       cfg.Metrics.Port,
			Destination: &cfg.Metrics.Port,
			Category:    flagCategoryTelemetry,
		},
		&cli.StringFlag{
			Name:        "metrics.path",
			Sources:     cli.EnvVars(cfg.EnvPrefix + "METRICS_PATH"),
			Usage:       "On which path should the metrics endpoint listen",
			Value:       cfg.Metrics.Path,
			Destination: &cfg.Metrics.Path,
			Category:    flagCategoryTelemetry,
		},
		&cli.BoolFlag{
			Name:        "tracing.enabled",
			Sources:     cli.EnvVars(cfg.EnvPrefix + "TRACING_ENABLED"),
			Usage:       "Whether to emit trace data",
			Destination: &cfg.Trace.Enabled,
			Value:       cfg.Trace.Enabled,
			Category:    flagCategoryTelemetry,
		},
		&cli.DurationFlag{
			Name:        "shutdown.grace",
			Sources:     cli.EnvVars(cfg.EnvPrefix + "SHUTDOWN_GRACE"),
			Usage:       "How long to wait for the application to gracefully shutdown.",
			Value:       cfg.ShutdownGrace,
			Destination: &cfg.ShutdownGrace,
			Hidden:      true,
		},
		&cli.StringFlag{
			Name:        "aws.region",
			Sources:     cli.EnvVars("AWS_REGION"),
			Usage:       "On which path should the metrics endpoint listen",
			Value:       cfg.Metrics.Path,
			Destination: &cfg.Metrics.Path,
			Category:    flagCategoryTelemetry,
		},
	}...)

	rootCmd := &RootCommand{
		cmd: cmd,
		cfg: cfg,
	}

	oldBefore := rootCmd.cmd.Before
	rootCmd.cmd.Before = func(ctx context.Context, c *cli.Command) (context.Context, error) {
		if err := rootCmd.before(ctx, c); err != nil {
			return ctx, err
		}

		if oldBefore == nil {
			return ctx, nil
		}

		return oldBefore(ctx, c)
	}

	oldAfter := rootCmd.cmd.After
	rootCmd.cmd.After = func(ctx context.Context, c *cli.Command) error {
		if err := rootCmd.after(ctx, c); err != nil {
			return err
		}

		if oldAfter == nil {
			return nil
		}

		return oldAfter(ctx, c)
	}

	return rootCmd, cfg
}

func (r *RootCommand) before(ctx context.Context, c *cli.Command) error {
	// configure logger
	slogger, err := log.New(r.cfg.Log)
	if err != nil {
		return fmt.Errorf("create logger: %w", err)
	}

	// use initialized logger for everything
	slog.SetDefault(slogger)

	slog.Debug("Starting " + r.cmd.Name + "...")

	// print all environment variables
	debugPrintEnvVars()

	// initialize metrics server - don't prohibit startup
	r.cfg.metricsShutdown, err = tele.ServeMetrics(r.cfg.Metrics)
	if err != nil {
		slog.Warn("failed to start metrics server", "err", err)
	}

	// initialize trace exporter - don't prohibit startup
	r.cfg.tracesShutdown, err = tele.InitTraceProvider(ctx, r.cmd.Name, r.cfg.Trace)
	if err != nil {
		slog.Warn("failed to start exporting traces", "err", err)
	}

	return nil
}

func (r *RootCommand) Run() error {
	ctx, cancel := signalContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	return r.cmd.Run(ctx, os.Args)
}

func (r *RootCommand) RunWithContext(ctx context.Context) error {
	// the main application context
	ctx, cancel := signalContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	return r.cmd.Run(ctx, os.Args)
}

func (r *RootCommand) RunWithContextAndArgs(ctx context.Context, args []string) error {
	// the main application context
	ctx, cancel := signalContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	return r.cmd.Run(ctx, args)
}

func (r *RootCommand) after(ctx context.Context, c *cli.Command) error {
	defer slog.Debug("Stopped " + r.cmd.Name + " service.")

	// use a new context as the application context might have been canceled.
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), r.cfg.ShutdownGrace)
	defer shutdownCancel()

	if err := r.cfg.metricsShutdown(shutdownCtx); err != nil {
		slog.Warn("failed to shutdown metrics server", "err", err)
	}

	if err := r.cfg.tracesShutdown(shutdownCtx); err != nil {
		slog.Warn("failed to shutdown traces exporter", "err", err)
	}

	return nil
}

// signalContext returns a context that gets canceled when the application
// receives a termination signal. We are not using [signal.NotifyContext]
// because when the context is canceled, we cannot differentiate between a
// regular shutdown and actually receiving a signal. This would make the log
// message below misleading.
func signalContext(ctx context.Context, signals ...os.Signal) (context.Context, context.CancelFunc) {
	sigs := make(chan os.Signal, 1)
	ctx, cancel := context.WithCancel(ctx)

	signal.Notify(sigs, signals...)
	go func() {
		defer cancel()
		defer signal.Stop(sigs)

		select {
		case <-ctx.Done():
		case sig := <-sigs:
			slog.Info("Received termination signal - Stopping...", "signal", sig.String())
		}
	}()

	return ctx, cancel
}

// debugPrintEnvVars logs all environment variables at debug level.
// Redacts values of variables containing the string "password".
func debugPrintEnvVars() {
	slog.Debug("Environment variables:")
	for _, kv := range os.Environ() {
		parts := strings.Split(kv, "=")
		if len(parts) != 2 {
			slog.Debug(kv)
			continue
		}

		if !strings.Contains(strings.ToLower(parts[0]), "password") {
			slog.Debug(kv)
			continue
		}

		redacted := "*****"
		if parts[1] == "" {
			redacted = ""
		}

		slog.Debug(strings.Join([]string{parts[0], redacted}, "="))
	}
}

func buildEnvPrefix(name string) string {
	prefix := strings.ToUpper(name)
	if !strings.HasSuffix(prefix, "_") {
		prefix += "_"
	}
	return prefix
}

type BuildInfo struct {
	Commit string
	Dirty  bool
}

func (bi *BuildInfo) ShortCommit() string {
	shortCommit := bi.Commit
	if len(shortCommit) > 8 {
		shortCommit = shortCommit[:8]
	}
	return shortCommit
}

func buildInfo() *BuildInfo {
	bi := &BuildInfo{}
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, setting := range info.Settings {
			switch setting.Key {
			case "vcs.revision":
				bi.Commit = setting.Value

			case "vcs.modified":
				dirty, err := strconv.ParseBool(setting.Value)
				if err != nil {
					panic(err)
				}
				bi.Dirty = dirty
			}
		}
	}

	return bi
}
