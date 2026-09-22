package tele

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.27.0"
)

// newResource describes the service. version is recorded as
// service.version when it is not empty.
func newResource(serviceName, version string) (*resource.Resource, error) {
	attrs := []attribute.KeyValue{semconv.ServiceName(serviceName)}
	if version != "" {
		attrs = append(attrs, semconv.ServiceVersion(version))
	}
	return resource.New(context.TODO(), resource.WithAttributes(attrs...))
}
