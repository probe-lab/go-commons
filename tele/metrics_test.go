package tele

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/probe-lab/ecs-exporter/enacollector"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	colmetricpb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	metricpb "go.opentelemetry.io/proto/otlp/metrics/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func TestServeMetrics(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := ln.Addr().(*net.TCPAddr).Port
	require.NoError(t, ln.Close())

	cfg := DefaultMetricsConfig("test")
	cfg.Enabled = true
	cfg.Host = "127.0.0.1"
	cfg.Port = port
	cfg.Version = "v"

	shutdown, err := ServeMetrics(cfg)
	require.NoError(t, err)
	defer func() { require.NoError(t, shutdown(context.Background())) }()

	ctx := context.Background()
	meter := otel.GetMeterProvider().Meter("scope")
	hist, err := meter.Float64Histogram("x", metric.WithUnit("s"), metric.WithExplicitBucketBoundaries(0.005, 1))
	require.NoError(t, err)
	counter, err := meter.Int64Counter("y")
	require.NoError(t, err)
	hist.Record(ctx, 0.001)
	counter.Add(ctx, 3)

	base := "http://" + net.JoinHostPort(cfg.Host, strconv.Itoa(port))
	res, err := http.Get(base + cfg.Path) //nolint:noctx // test only
	require.NoError(t, err)
	body, err := io.ReadAll(res.Body)
	require.NoError(t, err)
	require.NoError(t, res.Body.Close())
	text := string(body)

	require.Contains(t, text, "test_x_seconds_bucket{")
	require.Contains(t, text, `le="0.005"`)
	require.Contains(t, text, "test_y_total{")
	require.Contains(t, text, `service_version="v"`)
	require.Contains(t, text, "go_goroutines")

	res, err = http.Get(base + "/healthz") //nolint:noctx // test only
	require.NoError(t, err)
	require.NoError(t, res.Body.Close())
	require.Equal(t, http.StatusOK, res.StatusCode)
}

func TestServeMetricsReturnsBindError(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = ln.Close() }()

	cfg := DefaultMetricsConfig("test")
	cfg.Enabled = true
	cfg.Host = "127.0.0.1"
	cfg.Port = ln.Addr().(*net.TCPAddr).Port

	_, err = ServeMetrics(cfg)
	require.ErrorContains(t, err, "listen on")
}

func TestRegisterAWSCollectors(t *testing.T) {
	require.NoError(t, registerAWSCollectors())
	require.NoError(t, registerAWSCollectors(), "a second call keeps the registered collectors")

	err := prometheus.DefaultRegisterer.Register(enacollector.NewCollector(slog.Default()))
	require.ErrorAs(t, err, &prometheus.AlreadyRegisteredError{}, "the ENA collector is registered outside of ECS too")
}

func TestServeMetricsDisabled(t *testing.T) {
	shutdown, err := ServeMetrics(DefaultMetricsConfig("test"))
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, shutdown(ctx))
}

// metricsCollector is an in-process OTLP metrics receiver.
type metricsCollector struct {
	colmetricpb.UnimplementedMetricsServiceServer

	mu       sync.Mutex
	requests []*colmetricpb.ExportMetricsServiceRequest
	headers  []metadata.MD
}

func (c *metricsCollector) Export(ctx context.Context, req *colmetricpb.ExportMetricsServiceRequest) (*colmetricpb.ExportMetricsServiceResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	md, _ := metadata.FromIncomingContext(ctx)
	c.headers = append(c.headers, md)
	c.requests = append(c.requests, req)
	return &colmetricpb.ExportMetricsServiceResponse{}, nil
}

func (c *metricsCollector) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.requests)
}

func startMetricsCollector(t *testing.T) (*metricsCollector, string) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	c := &metricsCollector{}
	srv := grpc.NewServer()
	colmetricpb.RegisterMetricsServiceServer(srv, c)
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(srv.Stop)

	return c, ln.Addr().String()
}

// pushedMetrics indexes the metrics of a push request by name.
func pushedMetrics(req *colmetricpb.ExportMetricsServiceRequest) map[string]*metricpb.Metric {
	metrics := map[string]*metricpb.Metric{}
	for _, rm := range req.GetResourceMetrics() {
		for _, sm := range rm.GetScopeMetrics() {
			for _, m := range sm.GetMetrics() {
				metrics[m.GetName()] = m
			}
		}
	}
	return metrics
}

func resourceAttrs(req *colmetricpb.ExportMetricsServiceRequest) map[string]string {
	attrs := map[string]string{}
	for _, kv := range req.GetResourceMetrics()[0].GetResource().GetAttributes() {
		attrs[kv.GetKey()] = kv.GetValue().GetStringValue()
	}
	return attrs
}

func TestMetricsConfigValidate(t *testing.T) {
	require.NoError(t, DefaultMetricsConfig("test").Validate())
	require.NoError(t, (&MetricsConfig{}).Validate(), "an empty exporter is prometheus")

	var nilCfg *MetricsConfig
	require.Error(t, nilCfg.Validate())

	require.ErrorContains(t, (&MetricsConfig{Exporter: "none"}).Validate(), "unknown exporter")
	require.NoError(t, (&MetricsConfig{Exporter: MetricsExporterOTLP, Endpoint: "alloy:4317"}).Validate())
	require.Error(t, (&MetricsConfig{Exporter: MetricsExporterOTLP, Endpoint: "alloy"}).Validate())
	require.Error(t, (&MetricsConfig{Exporter: MetricsExporterOTLP, Interval: -time.Second}).Validate())

	// the push fields do not matter to the prometheus exporter
	require.NoError(t, (&MetricsConfig{Exporter: MetricsExporterPrometheus, Endpoint: "alloy"}).Validate())
}

func TestServeMetricsRejectsUnknownExporter(t *testing.T) {
	cfg := DefaultMetricsConfig("test")
	cfg.Enabled = true
	cfg.Exporter = "none"

	_, err := ServeMetrics(cfg)
	require.ErrorContains(t, err, "unknown exporter")
}

func TestServeMetricsPushesOnShutdown(t *testing.T) {
	c, addr := startMetricsCollector(t)

	cfg := DefaultMetricsConfig("test")
	cfg.Enabled = true
	cfg.Exporter = MetricsExporterOTLP
	cfg.Endpoint = addr
	cfg.Insecure = true
	cfg.Headers = map[string]string{"x-tenant": "probelab"}
	cfg.Interval = time.Hour // only the shutdown pushes within the test
	cfg.Version = "v"

	shutdown, err := ServeMetrics(cfg)
	require.NoError(t, err)

	ctx := context.Background()
	meter := otel.GetMeterProvider().Meter("scope")
	hist, err := meter.Float64Histogram("x", metric.WithUnit("s"), metric.WithExplicitBucketBoundaries(0.005, 1))
	require.NoError(t, err)
	counter, err := meter.Int64Counter("y")
	require.NoError(t, err)
	hist.Record(ctx, 0.001)
	counter.Add(ctx, 3)

	require.Zero(t, c.count(), "nothing is pushed before the interval elapses")

	shutdownCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	require.NoError(t, shutdown(shutdownCtx))

	c.mu.Lock()
	defer c.mu.Unlock()
	require.Len(t, c.requests, 1, "shutdown pushes the last values")
	require.Equal(t, []string{"probelab"}, c.headers[0].Get("x-tenant"))

	attrs := resourceAttrs(c.requests[0])
	require.Equal(t, "test", attrs["service.name"])
	require.Equal(t, "v", attrs["service.version"])
	require.NotEmpty(t, attrs["service.instance.id"])

	metrics := pushedMetrics(c.requests[0])

	x := metrics["test_x"]
	require.NotNil(t, x, "instrument names carry the namespace")
	require.Equal(t, "s", x.GetUnit())
	require.Equal(t, []float64{0.005, 1}, x.GetHistogram().GetDataPoints()[0].GetExplicitBounds())

	y := metrics["test_y"]
	require.NotNil(t, y)
	require.Equal(t, int64(3), y.GetSum().GetDataPoints()[0].GetAsInt())

	require.Contains(t, metrics, "go_goroutines", "metrics of the default prometheus registry are pushed too")
}

func TestServeMetricsPushesEveryInterval(t *testing.T) {
	c, addr := startMetricsCollector(t)

	// a deployment can pin the instance
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "service.instance.id=task-1")

	cfg := DefaultMetricsConfig("test")
	cfg.Enabled = true
	cfg.Exporter = MetricsExporterOTLP
	cfg.Endpoint = "http://" + addr
	cfg.Interval = 50 * time.Millisecond

	shutdown, err := ServeMetrics(cfg)
	require.NoError(t, err)
	defer func() { require.NoError(t, shutdown(context.Background())) }()

	require.Eventually(t, func() bool { return c.count() >= 2 }, 5*time.Second, 10*time.Millisecond)

	c.mu.Lock()
	defer c.mu.Unlock()
	require.Equal(t, "task-1", resourceAttrs(c.requests[0])["service.instance.id"])
}
