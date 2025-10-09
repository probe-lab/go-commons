package tele

import (
	"context"

	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.27.0"
)

func newResource(serviceName string) (*resource.Resource, error) {
	return resource.New(context.TODO(),
		resource.WithAttributes(
			semconv.ServiceName(serviceName),
		),
	)
}
