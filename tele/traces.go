package tele

import (
	"context"
	"fmt"
	"log/slog"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

type TraceConfig struct {
	Enabled bool
}

func DefaultTraceConfig() *TraceConfig {
	return &TraceConfig{
		Enabled: false,
	}
}

func InitTraceProvider(ctx context.Context, name string, cfg *TraceConfig) (func(ctx context.Context) error, error) {
	if !cfg.Enabled {
		provider := noop.NewTracerProvider()
		otel.SetTracerProvider(provider)
		return func(ctx context.Context) error { return nil }, nil
	}

	res, err := newResource(name)
	if err != nil {
		return nil, fmt.Errorf("failed to create otel trace provider resource: %w", err)
	}

	exporter, err := otlptracegrpc.New(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create trace exporter: %w", err)
	}

	// using a batch span processor to aggregate spans before export.
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithResource(res),
		sdktrace.WithSpanProcessor(sdktrace.NewBatchSpanProcessor(exporter)),
	)

	otel.SetTracerProvider(provider)

	shutdownFunc := func(ctx context.Context) error {
		slog.Debug("Shutting down traces provider")
		return exporter.Shutdown(ctx)
	}

	return shutdownFunc, nil
}
