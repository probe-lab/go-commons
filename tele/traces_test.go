package tele

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// collector is an in-process OTLP trace receiver.
type collector struct {
	coltracepb.UnimplementedTraceServiceServer

	mu       sync.Mutex
	requests []*coltracepb.ExportTraceServiceRequest
	headers  []metadata.MD
}

func (c *collector) Export(ctx context.Context, req *coltracepb.ExportTraceServiceRequest) (*coltracepb.ExportTraceServiceResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	md, _ := metadata.FromIncomingContext(ctx)
	c.headers = append(c.headers, md)
	c.requests = append(c.requests, req)
	return &coltracepb.ExportTraceServiceResponse{}, nil
}

func startCollector(t *testing.T) (*collector, string) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	c := &collector{}
	srv := grpc.NewServer()
	coltracepb.RegisterTraceServiceServer(srv, c)
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(srv.Stop)

	return c, ln.Addr().String()
}

func TestTraceConfigValidate(t *testing.T) {
	require.NoError(t, DefaultTraceConfig().Validate())

	var nilCfg *TraceConfig
	require.Error(t, nilCfg.Validate())

	for _, ep := range []string{"collector:4317", "127.0.0.1:4317", "http://collector:4317", "https://collector:4317/v1/traces"} {
		require.NoError(t, (&TraceConfig{Endpoint: ep}).Validate(), ep)
	}
	for _, ep := range []string{"collector", "http://", "://x", "a:b:c"} {
		require.Error(t, (&TraceConfig{Endpoint: ep}).Validate(), ep)
	}
}

func TestInitTraceProviderDisabled(t *testing.T) {
	shutdown, err := InitTraceProvider(context.Background(), "test", DefaultTraceConfig())
	require.NoError(t, err)
	require.NoError(t, shutdown(context.Background()))
}

func TestInitTraceProviderConfigWinsOverEnv(t *testing.T) {
	c, addr := startCollector(t)

	// the canonical variables point somewhere unreachable; the config
	// must take precedence.
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "https://127.0.0.1:1")
	t.Setenv("OTEL_EXPORTER_OTLP_HEADERS", "x-from=env")
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "deployment.environment=test")

	cfg := &TraceConfig{
		Enabled:  true,
		Version:  "v",
		Endpoint: addr,
		Insecure: true,
		Headers:  map[string]string{"x-from": "config"},
	}

	ctx := context.Background()
	shutdown, err := InitTraceProvider(ctx, "test", cfg)
	require.NoError(t, err)

	_, span := otel.Tracer("scope").Start(ctx, "op")
	span.End()

	// shutdown flushes the batch processor
	shutdownCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	require.NoError(t, shutdown(shutdownCtx))

	c.mu.Lock()
	defer c.mu.Unlock()
	require.Len(t, c.requests, 1)
	require.Equal(t, []string{"config"}, c.headers[0].Get("x-from"))

	rs := c.requests[0].GetResourceSpans()
	require.Len(t, rs, 1)
	attrs := map[string]string{}
	for _, kv := range rs[0].GetResource().GetAttributes() {
		attrs[kv.GetKey()] = kv.GetValue().GetStringValue()
	}
	require.Equal(t, "test", attrs["service.name"])
	require.Equal(t, "v", attrs["service.version"])
	require.Equal(t, "test", attrs["deployment.environment"])
	require.Equal(t, "op", rs[0].GetScopeSpans()[0].GetSpans()[0].GetName())
}

func TestInitTraceProviderUsesEnv(t *testing.T) {
	c, addr := startCollector(t)

	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://"+addr)
	t.Setenv("OTEL_EXPORTER_OTLP_HEADERS", "x-from=env")

	cfg := DefaultTraceConfig()
	cfg.Enabled = true

	ctx := context.Background()
	shutdown, err := InitTraceProvider(ctx, "test", cfg)
	require.NoError(t, err)

	_, span := otel.Tracer("scope").Start(ctx, "op")
	span.End()

	shutdownCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	require.NoError(t, shutdown(shutdownCtx))

	c.mu.Lock()
	defer c.mu.Unlock()
	require.Len(t, c.requests, 1)
	require.Equal(t, []string{"env"}, c.headers[0].Get("x-from"))
}
