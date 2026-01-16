package tele

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/pprof"

	"github.com/probe-lab/ecs-exporter/ecscollector"
	"github.com/probe-lab/ecs-exporter/ecsmetadata"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	promexp "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

type MetricsConfig struct {
	Enabled bool
	Host    string
	Port    int
	Path    string
	Name    string
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

func ServeMetrics(cfg *MetricsConfig) (func(ctx context.Context) error, error) {
	if !cfg.Enabled {
		provider := noop.NewMeterProvider()
		otel.SetMeterProvider(provider)
		return func(ctx context.Context) error { return nil }, nil
	}

	provider, providerShutdownFn, err := initMeterProvider(cfg.Name)
	if err != nil {
		return nil, fmt.Errorf("new meter provider: %w", err)
	}

	otel.SetMeterProvider(provider)

	mux := http.NewServeMux()

	mux.Handle(cfg.Path, promhttp.Handler())
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	addr := fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)
	srv := &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	slogger := slog.With("addr", addr)

	go func() {
		slogger.Info("Starting metrics server")
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slogger.Error("Failed starting metrics server", "err", err)
		}
	}()

	shutdownFunc := func(ctx context.Context) error {
		slogger.Info("Shutting down metrics server")
		if err := srv.Shutdown(ctx); err != nil {
			slogger.Warn("Failed to shut down metrics server", "err", err)
		}

		return providerShutdownFn(ctx)
	}

	return shutdownFunc, nil
}

func initMeterProvider(name string) (metric.MeterProvider, func(ctx context.Context) error, error) {
	// initialize AWS Elastic Container Service collector and register it with
	// the default prometheus registry. If we are not running in a prometheus
	// environment, don't do anything.
	client, err := ecsmetadata.NewClientFromEnvironment()
	if err == nil {
		slog.Debug("Registering ECS collector")
		collector := ecscollector.NewCollector(client, slog.Default())
		if err := prometheus.DefaultRegisterer.Register(collector); err != nil {
			return nil, nil, fmt.Errorf("register collector: %w", err)
		}
	}

	// initialize the prometheus exporter
	exporter, err := promexp.New(promexp.WithNamespace(name))
	if err != nil {
		return nil, nil, fmt.Errorf("new prometheus exporter: %w", err)
	}

	// build common resource information
	res, err := newResource(name)
	if err != nil {
		return nil, nil, fmt.Errorf("new metrics resource: %w", err)
	}

	// construct meter provider
	provider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(exporter), // the exporter reads from the meter provider
		sdkmetric.WithResource(res),
	)

	shutdownFunc := func(ctx context.Context) error {
		slog.Debug("Shutting down prometheus exporter")
		if err := exporter.Shutdown(ctx); err != nil {
			slog.Warn("Failed to shut down metrics server", "err", err)
		}
		return nil
	}

	return provider, shutdownFunc, nil
}
