package tele

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"

	"github.com/probe-lab/ecs-exporter/ecscollector"
	"github.com/probe-lab/ecs-exporter/ecsmetadata"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	promexp "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

type MetricsConfig struct {
	Enabled bool
	Host    string
	Port    int
	Path    string
	Name    string
	Version string
}

func DefaultMetricsConfig(name string) *MetricsConfig {
	return &MetricsConfig{
		Enabled: false,
		Host:    "localhost",
		Port:    6060,
		Path:    "/metrics",
		Name:    name,
	}
}

// ServeMetrics installs the OpenTelemetry meter provider and serves the
// Prometheus endpoint, a health check, and pprof on Host:Port. It binds the
// listener before registering anything, so an address that cannot be bound
// is an error and leaves the global state untouched. When the configuration
// is disabled, a no-op provider is installed.
func ServeMetrics(cfg *MetricsConfig) (func(ctx context.Context) error, error) {
	if !cfg.Enabled {
		provider := noop.NewMeterProvider()
		otel.SetMeterProvider(provider)
		return func(ctx context.Context) error { return nil }, nil
	}

	addr := fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)
	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("listen on %s: %w", addr, err)
	}

	provider, err := initMeterProvider(cfg)
	if err != nil {
		_ = ln.Close()
		return nil, fmt.Errorf("new meter provider: %w", err)
	}

	otel.SetMeterProvider(provider)

	mux := http.NewServeMux()

	mux.Handle(cfg.Path, promhttp.Handler())
	mux.HandleFunc("/healthz", healthzHandler)
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	srv := &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	slogger := slog.With("addr", ln.Addr().String())

	go func() {
		slogger.Info("Starting metrics server")
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slogger.Error("Metrics server stopped", "err", err)
		}
	}()

	shutdownFunc := func(ctx context.Context) error {
		slogger.Info("Shutting down metrics server")
		if err := srv.Shutdown(ctx); err != nil {
			slogger.Warn("Failed to shut down metrics server", "err", err)
		}

		slog.Debug("Shutting down meter provider")
		return provider.Shutdown(ctx)
	}

	return shutdownFunc, nil
}

func healthzHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}

func initMeterProvider(cfg *MetricsConfig) (*sdkmetric.MeterProvider, error) {
	// initialize AWS Elastic Container Service collector and register it with
	// the default prometheus registry. If we are not running in an ECS
	// environment, don't do anything.
	client, err := ecsmetadata.NewClientFromEnvironment()
	if err == nil {
		slog.Debug("Registering ECS collector")
		collector := ecscollector.NewCollector(client, slog.Default())
		if err := prometheus.DefaultRegisterer.Register(collector); err != nil {
			return nil, fmt.Errorf("register collector: %w", err)
		}
	}

	// initialize the prometheus exporter
	exporter, err := promexp.New(promexp.WithNamespace(cfg.Name))
	if err != nil {
		return nil, fmt.Errorf("new prometheus exporter: %w", err)
	}

	// build common resource information
	res, err := newResource(cfg.Name, cfg.Version)
	if err != nil {
		return nil, fmt.Errorf("new metrics resource: %w", err)
	}

	// construct meter provider
	provider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(exporter), // the exporter reads from the meter provider
		sdkmetric.WithResource(res),
	)

	return provider, nil
}
