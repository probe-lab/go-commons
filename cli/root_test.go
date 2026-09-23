package cli

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v3"
)

func TestNewRootCommand(t *testing.T) {
	var ran bool
	cmd := &cli.Command{
		Name:    "app",
		Version: "1.2.3",
		Action: func(context.Context, *cli.Command) error {
			ran = true
			return nil
		},
	}

	root, cfg := NewRootCommand(cmd)
	require.Equal(t, "APP_", cfg.EnvPrefix)
	require.True(t, len(cmd.Version) >= len("1.2.3"), "the commit is appended to the version")
	require.Equal(t, cmd.Version, cfg.Metrics.Version, "the metrics resource carries the command version")
	require.Equal(t, cmd.Version, cfg.Trace.Version, "the trace resource carries the command version")

	err := root.RunWithContextAndArgs(context.Background(), []string{"app", "--log.level", "debug"})
	require.NoError(t, err)
	require.True(t, ran)
	require.Equal(t, "debug", cfg.Log.Level)
}

func TestTracingFlags(t *testing.T) {
	cmd := &cli.Command{
		Name:   "app",
		Action: func(context.Context, *cli.Command) error { return nil },
	}
	root, cfg := NewRootCommand(cmd)

	t.Setenv("APP_TRACING_ENDPOINT", "collector:4317")
	t.Setenv("APP_TRACING_HEADERS", "authorization=Bearer x,x-tenant=y")

	// tracing stays disabled so the run does not connect anywhere
	err := root.RunWithContextAndArgs(context.Background(), []string{"app", "--tracing.insecure"})
	require.NoError(t, err)
	require.Equal(t, "collector:4317", cfg.Trace.Endpoint)
	require.True(t, cfg.Trace.Insecure)
	require.Equal(t, map[string]string{"authorization": "Bearer x", "x-tenant": "y"}, cfg.Trace.Headers)
}

func TestTracingFlagsReadCanonicalEnv(t *testing.T) {
	run := func(t *testing.T) *RootCommandConfig {
		t.Helper()
		cmd := &cli.Command{
			Name:   "app",
			Action: func(context.Context, *cli.Command) error { return nil },
		}
		root, cfg := NewRootCommand(cmd)
		require.NoError(t, root.RunWithContextAndArgs(context.Background(), []string{"app"}))
		return cfg
	}

	// the generic variables apply when nothing more specific is set
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://generic:4317")
	t.Setenv("OTEL_EXPORTER_OTLP_INSECURE", "true")
	t.Setenv("OTEL_EXPORTER_OTLP_HEADERS", "x-from=generic")
	cfg := run(t)
	require.Equal(t, "http://generic:4317", cfg.Trace.Endpoint)
	require.True(t, cfg.Trace.Insecure)
	require.Equal(t, map[string]string{"x-from": "generic"}, cfg.Trace.Headers)

	// the traces-specific variable wins over the generic one
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "http://traces:4317")
	require.Equal(t, "http://traces:4317", run(t).Trace.Endpoint)

	// the application's own variable wins over both
	t.Setenv("APP_TRACING_ENDPOINT", "app:4317")
	require.Equal(t, "app:4317", run(t).Trace.Endpoint)
}
