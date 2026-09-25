package tele

import (
	"context"
	"io"
	"net"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
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

func TestServeMetricsDisabled(t *testing.T) {
	shutdown, err := ServeMetrics(DefaultMetricsConfig("test"))
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, shutdown(ctx))
}
