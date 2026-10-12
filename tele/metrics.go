package tele

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"time"

	"github.com/google/uuid"
	"github.com/probe-lab/ecs-exporter/ecscollector"
	"github.com/probe-lab/ecs-exporter/ecsmetadata"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	prombridge "go.opentelemetry.io/contrib/bridges/prometheus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	promexp "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	semconv "go.opentelemetry.io/otel/semconv/v1.27.0"
)

const (
	// MetricsExporterPrometheus serves metrics on Host:Port/Path for a
	// scraper to collect.
	MetricsExporterPrometheus = "prometheus"
	// MetricsExporterOTLP pushes metrics over OTLP gRPC to Endpoint every
	// Interval and once more on shutdown. Use it for short-lived processes
	// that a scraper would miss.
	MetricsExporterOTLP = "otlp"
)

type MetricsConfig struct {
	Enabled bool
	// Exporter selects how metrics leave the process:
	// MetricsExporterPrometheus (also when empty) or MetricsExporterOTLP.
	Exporter string

	// Host, Port and Path locate the scrape endpoint of the prometheus
	// exporter.
	Host string
	Port int
	Path string

	// Endpoint is the collector the otlp exporter pushes to, either
	// "host:port" or a URL such as "http://alloy:4317". A URL with the
	// http scheme implies Insecure. Empty leaves it to the exporter, which
	// reads OTEL_EXPORTER_OTLP_METRICS_ENDPOINT and
	// OTEL_EXPORTER_OTLP_ENDPOINT and falls back to localhost:4317.
	Endpoint string
	// Insecure pushes over plain gRPC without TLS.
	Insecure bool
	// Headers are sent with every push.
	Headers map[string]string
	// Interval is the time between pushes. Zero leaves it to the SDK, which
	// reads OTEL_METRIC_EXPORT_INTERVAL and falls back to 60s.
	Interval time.Duration

	Name    string
	Version string
}

func DefaultMetricsConfig(name string) *MetricsConfig {
	return &MetricsConfig{
		Enabled:  false,
		Exporter: MetricsExporterPrometheus,
		Host:     "localhost",
		Port:     6060,
		Path:     "/metrics",
		Name:     name,
	}
}

// Validate checks the fields of the selected exporter.
func (cfg *MetricsConfig) Validate() error {
	if cfg == nil {
		return fmt.Errorf("metrics config is nil")
	}

	switch cfg.Exporter {
	case "", MetricsExporterPrometheus:
	case MetricsExporterOTLP:
		if err := validateEndpoint(cfg.Endpoint); err != nil {
			return err
		}
		if cfg.Interval < 0 {
			return fmt.Errorf("interval %s is negative", cfg.Interval)
		}
	default:
		return fmt.Errorf("unknown exporter %q, want %q or %q", cfg.Exporter, MetricsExporterPrometheus, MetricsExporterOTLP)
	}

	return nil
}

// ServeMetrics installs the OpenTelemetry meter provider and exports through
// cfg.Exporter.
//
// The prometheus exporter serves the Prometheus endpoint, a health check,
// and pprof on Host:Port. It binds the listener before registering
// anything, so an address that cannot be bound is an error and leaves the
// global state untouched.
//
// The otlp exporter pushes every Interval, and the returned shutdown
// function pushes once more, so a process that exits between two pushes
// still delivers its last values. Each process gets a random
// service.instance.id, which Prometheus receives as the instance label;
// OTEL_RESOURCE_ATTRIBUTES can set it instead.
//
// When the configuration is disabled, a no-op provider is installed.
func ServeMetrics(cfg *MetricsConfig) (func(ctx context.Context) error, error) {
	if !cfg.Enabled {
		provider := noop.NewMeterProvider()
		otel.SetMeterProvider(provider)
		return func(ctx context.Context) error { return nil }, nil
	}

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("validate metrics config: %w", err)
	}

	if cfg.Exporter == MetricsExporterOTLP {
		return pushMetrics(cfg)
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
	if err := registerECSCollector(); err != nil {
		return nil, err
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

// pushMetrics installs a meter provider that pushes over OTLP gRPC instead
// of serving an endpoint.
func pushMetrics(cfg *MetricsConfig) (func(ctx context.Context) error, error) {
	if err := registerECSCollector(); err != nil {
		return nil, err
	}

	exporter, err := otlpmetricgrpc.New(context.Background(), metricExporterOptions(cfg)...)
	if err != nil {
		return nil, fmt.Errorf("new otlp metrics exporter: %w", err)
	}

	// A scraper labels each target with its address. A push has none, so
	// without an instance id concurrent processes would write to the same
	// series.
	res, err := newResource(cfg.Name, cfg.Version, semconv.ServiceInstanceID(uuid.NewString()))
	if err != nil {
		return nil, fmt.Errorf("new metrics resource: %w", err)
	}

	// The default prometheus registry holds the Go runtime, process, and
	// ECS collectors that the scrape endpoint serves; the bridge pushes
	// them too.
	readerOpts := []sdkmetric.PeriodicReaderOption{
		sdkmetric.WithProducer(prombridge.NewMetricProducer()),
	}
	if cfg.Interval > 0 {
		readerOpts = append(readerOpts, sdkmetric.WithInterval(cfg.Interval))
	}

	providerOpts := []sdkmetric.Option{
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(exporter, readerOpts...)),
		sdkmetric.WithResource(res),
	}
	if cfg.Name != "" {
		providerOpts = append(providerOpts, sdkmetric.WithView(namespaceView(cfg.Name)))
	}

	provider := sdkmetric.NewMeterProvider(providerOpts...)

	otel.SetMeterProvider(provider)

	slog.Info("Pushing metrics", "endpoint", cfg.Endpoint)

	shutdownFunc := func(ctx context.Context) error {
		// shutting down the reader pushes what was recorded since the last
		// interval.
		slog.Debug("Pushing final metrics and shutting down meter provider")
		return provider.Shutdown(ctx)
	}

	return shutdownFunc, nil
}

// metricExporterOptions translates the set fields of cfg into exporter
// options. Unset fields produce no option and keep the exporter's defaults.
func metricExporterOptions(cfg *MetricsConfig) []otlpmetricgrpc.Option {
	var opts []otlpmetricgrpc.Option

	if cfg.Endpoint != "" {
		if isURL(cfg.Endpoint) {
			opts = append(opts, otlpmetricgrpc.WithEndpointURL(cfg.Endpoint))
		} else {
			opts = append(opts, otlpmetricgrpc.WithEndpoint(cfg.Endpoint))
		}
	}

	if cfg.Insecure {
		opts = append(opts, otlpmetricgrpc.WithInsecure())
	}

	if len(cfg.Headers) > 0 {
		opts = append(opts, otlpmetricgrpc.WithHeaders(cfg.Headers))
	}

	return opts
}

// namespaceView prefixes every instrument name with ns, so that a pushed
// metric reaches Prometheus under the name the prometheus exporter gives it
// with promexp.WithNamespace, for example test_x_seconds.
func namespaceView(ns string) sdkmetric.View {
	return func(i sdkmetric.Instrument) (sdkmetric.Stream, bool) {
		return sdkmetric.Stream{
			Name:        ns + "_" + i.Name,
			Description: i.Description,
			Unit:        i.Unit,
		}, true
	}
}

// registerECSCollector registers the AWS Elastic Container Service
// collector with the default prometheus registry. Outside of ECS it does
// nothing.
func registerECSCollector() error {
	client, err := ecsmetadata.NewClientFromEnvironment()
	if err != nil {
		// not running in ECS
		return nil
	}

	slog.Debug("Registering ECS collector")
	collector := ecscollector.NewCollector(client, slog.Default())
	if err := prometheus.DefaultRegisterer.Register(collector); err != nil {
		return fmt.Errorf("register collector: %w", err)
	}

	return nil
}
