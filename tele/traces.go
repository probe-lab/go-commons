package tele

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

// TraceConfig configures the OTLP gRPC trace exporter. A zero field leaves
// the choice to the canonical OpenTelemetry environment variables
// (OTEL_EXPORTER_OTLP_ENDPOINT, OTEL_EXPORTER_OTLP_INSECURE,
// OTEL_EXPORTER_OTLP_HEADERS, OTEL_TRACES_SAMPLER, ...) and their defaults.
// A field that is set takes precedence over the environment.
type TraceConfig struct {
	// Enabled turns tracing on. Without it a no-op provider is installed.
	Enabled bool
	// Version is recorded as service.version on the resource.
	Version string
	// Endpoint is the collector to send spans to, either "host:port" or a
	// URL such as "http://collector:4317". A URL with the http scheme
	// implies Insecure.
	Endpoint string
	// Insecure sends spans over plain gRPC without TLS.
	Insecure bool
	// Headers are sent with every export request, for example an
	// authorization token for a hosted collector.
	Headers map[string]string
}

func DefaultTraceConfig() *TraceConfig {
	return &TraceConfig{
		Enabled: false,
	}
}

func (cfg *TraceConfig) Validate() error {
	if cfg == nil {
		return fmt.Errorf("trace config is nil")
	}

	if cfg.Endpoint == "" {
		return nil
	}

	if isURL(cfg.Endpoint) {
		u, err := url.Parse(cfg.Endpoint)
		if err != nil {
			return fmt.Errorf("parse endpoint %q: %w", cfg.Endpoint, err)
		}
		if u.Host == "" {
			return fmt.Errorf("endpoint %q has no host", cfg.Endpoint)
		}
		return nil
	}

	if _, _, err := net.SplitHostPort(cfg.Endpoint); err != nil {
		return fmt.Errorf("endpoint %q is not host:port or a URL: %w", cfg.Endpoint, err)
	}

	return nil
}

func isURL(s string) bool {
	return strings.Contains(s, "://")
}

// InitTraceProvider installs the W3C propagators and, when cfg is enabled,
// a tracer provider that exports spans over OTLP gRPC. Exporter options
// come from cfg where set and from the OTEL_EXPORTER_OTLP_* environment
// otherwise. The sampler is the SDK default (parent based, sample all)
// unless OTEL_TRACES_SAMPLER says otherwise.
func InitTraceProvider(ctx context.Context, name string, cfg *TraceConfig) (func(ctx context.Context) error, error) {
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("validate trace config: %w", err)
	}

	if !cfg.Enabled {
		provider := noop.NewTracerProvider()
		otel.SetTracerProvider(provider)
		return func(ctx context.Context) error { return nil }, nil
	}

	res, err := newResource(name, cfg.Version)
	if err != nil {
		return nil, fmt.Errorf("failed to create otel trace provider resource: %w", err)
	}

	exporter, err := otlptracegrpc.New(ctx, exporterOptions(cfg)...)
	if err != nil {
		return nil, fmt.Errorf("failed to create trace exporter: %w", err)
	}

	// using a batch span processor to aggregate spans before export.
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithResource(res),
		sdktrace.WithSpanProcessor(sdktrace.NewBatchSpanProcessor(exporter)),
	)

	otel.SetTracerProvider(provider)

	shutdownFunc := func(ctx context.Context) error {
		slog.Debug("Shutting down traces provider")
		return provider.Shutdown(ctx)
	}

	return shutdownFunc, nil
}

// exporterOptions translates the set fields of cfg into exporter options.
// Unset fields produce no option, so the exporter falls back to the
// environment for them.
func exporterOptions(cfg *TraceConfig) []otlptracegrpc.Option {
	var opts []otlptracegrpc.Option

	if cfg.Endpoint != "" {
		if isURL(cfg.Endpoint) {
			opts = append(opts, otlptracegrpc.WithEndpointURL(cfg.Endpoint))
		} else {
			opts = append(opts, otlptracegrpc.WithEndpoint(cfg.Endpoint))
		}
	}

	if cfg.Insecure {
		opts = append(opts, otlptracegrpc.WithInsecure())
	}

	if len(cfg.Headers) > 0 {
		opts = append(opts, otlptracegrpc.WithHeaders(cfg.Headers))
	}

	return opts
}
