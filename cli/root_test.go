package cli

import (
	"context"
	"errors"
	"net"
	"runtime/debug"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v3"
	"go.opentelemetry.io/otel"
	colmetricpb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	"google.golang.org/grpc"

	"github.com/probe-lab/go-commons/tele"
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
	require.Equal(t, "1.2.3", cmd.Version, "the version stays as the command declared it")
	require.Equal(t, cmd.Version, cfg.Metrics.Version, "the metrics resource carries the command version")
	require.Equal(t, cmd.Version, cfg.Trace.Version, "the trace resource carries the command version")

	err := root.RunWithContextAndArgs(context.Background(), []string{"app", "--log.level", "debug"})
	require.NoError(t, err)
	require.True(t, ran)
	require.Equal(t, "debug", cfg.Log.Level)
}

func TestNewRootCommandModuleVersion(t *testing.T) {
	cmd := &cli.Command{Name: "app"}
	_, cfg := NewRootCommand(cmd)

	info, ok := debug.ReadBuildInfo()
	require.True(t, ok)
	require.Equal(t, info.Main.Version, cfg.BuildInfo.Version)
	require.Equal(t, info.Main.Version, cmd.Version, "without a declared version the command reports the module version")
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

func TestMetricsFlagsReadCanonicalEnv(t *testing.T) {
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

	cfg := run(t)
	require.Equal(t, "localhost", cfg.Metrics.Host)
	require.Equal(t, 6060, cfg.Metrics.Port, "the default stays even though the spec says 9464")

	t.Setenv("OTEL_EXPORTER_PROMETHEUS_HOST", "0.0.0.0")
	t.Setenv("OTEL_EXPORTER_PROMETHEUS_PORT", "9464")
	cfg = run(t)
	require.Equal(t, "0.0.0.0", cfg.Metrics.Host)
	require.Equal(t, 9464, cfg.Metrics.Port)

	// the application's own variable wins
	t.Setenv("APP_METRICS_PORT", "7070")
	require.Equal(t, 7070, run(t).Metrics.Port)
}

func TestRunSurvivesATakenMetricsPort(t *testing.T) {
	// A health command run beside the server finds the metrics port taken.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = ln.Close() }()

	cmd := &cli.Command{
		Name:   "app",
		Action: func(context.Context, *cli.Command) error { return nil },
	}
	root, _ := NewRootCommand(cmd)
	port := strconv.Itoa(ln.Addr().(*net.TCPAddr).Port)
	args := []string{"app", "--metrics.enabled", "--metrics.host", "127.0.0.1", "--metrics.port", port}
	require.NoError(t, root.RunWithContextAndArgs(context.Background(), args))
}

func TestMetricsPushFlagsReadEnv(t *testing.T) {
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

	cfg := run(t)
	require.Equal(t, tele.MetricsExporterPrometheus, cfg.Metrics.Exporter)
	require.Zero(t, cfg.Metrics.Interval, "the SDK picks the interval")

	// metrics stay disabled so the runs do not connect anywhere
	t.Setenv("OTEL_METRICS_EXPORTER", "otlp")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://generic:4317")
	t.Setenv("OTEL_EXPORTER_OTLP_INSECURE", "true")
	t.Setenv("OTEL_EXPORTER_OTLP_HEADERS", "x-from=generic")
	cfg = run(t)
	require.Equal(t, tele.MetricsExporterOTLP, cfg.Metrics.Exporter)
	require.Equal(t, "http://generic:4317", cfg.Metrics.Endpoint)
	require.True(t, cfg.Metrics.Insecure)
	require.Equal(t, map[string]string{"x-from": "generic"}, cfg.Metrics.Headers)

	// the metrics-specific variable wins over the generic one
	t.Setenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", "http://metrics:4317")
	require.Equal(t, "http://metrics:4317", run(t).Metrics.Endpoint)

	// the application's own variables win over both
	t.Setenv("APP_METRICS_EXPORTER", "prometheus")
	t.Setenv("APP_METRICS_OTLP_ENDPOINT", "alloy:4317")
	t.Setenv("APP_METRICS_OTLP_INTERVAL", "15s")
	cfg = run(t)
	require.Equal(t, tele.MetricsExporterPrometheus, cfg.Metrics.Exporter)
	require.Equal(t, "alloy:4317", cfg.Metrics.Endpoint)
	require.Equal(t, 15*time.Second, cfg.Metrics.Interval)
}

// metricsCollector is an in-process OTLP metrics receiver.
type metricsCollector struct {
	colmetricpb.UnimplementedMetricsServiceServer

	mu       sync.Mutex
	requests []*colmetricpb.ExportMetricsServiceRequest
}

func (c *metricsCollector) Export(_ context.Context, req *colmetricpb.ExportMetricsServiceRequest) (*colmetricpb.ExportMetricsServiceResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.requests = append(c.requests, req)
	return &colmetricpb.ExportMetricsServiceResponse{}, nil
}

// sum returns the value of the named counter in the latest push.
func (c *metricsCollector) sum(t *testing.T, name string) int64 {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	require.NotEmpty(t, c.requests)
	for _, rm := range c.requests[len(c.requests)-1].GetResourceMetrics() {
		for _, sm := range rm.GetScopeMetrics() {
			for _, m := range sm.GetMetrics() {
				if m.GetName() == name {
					return m.GetSum().GetDataPoints()[0].GetAsInt()
				}
			}
		}
	}
	require.Failf(t, "metric not pushed", "%s", name)
	return 0
}

func TestShortRunPushesItsMetrics(t *testing.T) {
	for _, actionErr := range []error{nil, errors.New("crawl failed")} {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		c := &metricsCollector{}
		srv := grpc.NewServer()
		colmetricpb.RegisterMetricsServiceServer(srv, c)
		go func() { _ = srv.Serve(ln) }()
		t.Cleanup(srv.Stop)

		cmd := &cli.Command{
			Name: "app",
			Action: func(ctx context.Context, _ *cli.Command) error {
				runs, err := otel.Meter("scope").Int64Counter("runs")
				require.NoError(t, err)
				runs.Add(ctx, 1)
				return actionErr
			},
		}
		root, _ := NewRootCommand(cmd)
		args := []string{
			"app",
			"--metrics.enabled",
			"--metrics.exporter", "otlp",
			"--metrics.otlp.endpoint", ln.Addr().String(),
			"--metrics.otlp.insecure",
			"--metrics.otlp.interval", "1h", // the run ends long before the first push
		}
		err = root.RunWithContextAndArgs(context.Background(), args)
		require.ErrorIs(t, err, actionErr)

		require.Equal(t, int64(1), c.sum(t, "app_runs"), "the run's value is pushed on exit (action error: %v)", actionErr)
	}
}
