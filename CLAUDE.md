# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Common Commands

### Testing
- Run all tests: `just test` or `go test ./...`
- Run tests with coverage: `just test-coverage`
- Run tests for specific package: `just test-pkg [package-name]` or `go test ./[package-name]`
- Run benchmarks: `just bench`
- Generate coverage report: `just coverage`

### Build and Quality
- Build all packages: `just build` or `go build ./...`
- Check for syntax errors: `just check` or `go vet ./...`
- Format code: `just fmt`
- Run linter: `just lint` (requires golangci-lint)
- Run all quality checks: `just quality`

### Development
- Tidy dependencies: `just tidy`
- Update dependencies: `just update`
- Clean build artifacts: `just clean`
- Security audit: `just audit` (requires nancy)
- Check outdated dependencies: `just outdated`
- Full CI pipeline: `just ci`

## Project Architecture

This is a Go commons library (`github.com/probe-lab/go-commons`) containing reusable components for ProbeLab applications. The library is structured into the following main packages:

### Package Structure

**cli/**: Command-line interface utilities
- `cli/root.go`: Core CLI framework using urfave/cli/v3 with built-in telemetry, logging, and graceful shutdown
- `cli/pg.go`: PostgreSQL CLI configuration flags and setup
- `cli/ch.go`: ClickHouse CLI configuration flags and setup
- `cli/health.go`: Health check CLI utilities

**db/**: Database connectivity and configuration
- `db/pg.go`: PostgreSQL connection management with OpenTelemetry integration
- `db/ch.go`: ClickHouse connection management with automatic migrations support
- `db/mapping.go`: Database field mapping utilities
- Supports both single and multi-database configurations

**http/**: HTTP utilities
- `http/client.go`: HTTP client with one user agent, timeout, body size cap, redirect cap, and optional global rate limit; `ClientConfig` has `Validate()`, the transport is wrapped by otelhttp for traces and metrics
- `http/server.go`: `ListenAndServe(ctx, srv, grace)` binds first, serves, and shuts down gracefully when the context ends
- `http/resp.go`: Standardized JSON response structures with error handling
- `http/io.go`: HTTP I/O utilities
- `http/mw.go`: HTTP middleware components

**log/**: Structured logging
- `log/log.go`: slog-based structured logging; `--log.format` is `console` (default; one line per record with the level in color when stderr is a terminal and `NO_COLOR` is unset), `text`, or `json`; `Config` has `Validate()`
- `log/handlers.go`: Custom log handlers with context enrichment

**tele/**: Telemetry and observability
- `tele/metrics.go`: Prometheus metrics configuration and serving on the default registry (scrape only; `Version` on the resource; binds before registering anything so a taken port is an error); the `--metrics.host` and `--metrics.port` root flags also read `OTEL_EXPORTER_PROMETHEUS_HOST` and `OTEL_EXPORTER_PROMETHEUS_PORT` after `<NAME>_METRICS_*`
- `tele/traces.go`: Distributed tracing setup with OTLP gRPC export; `TraceConfig` (`Endpoint`, `Insecure`, `Headers`, `Validate()`); the `--tracing.*` root flags read `<NAME>_TRACING_*`, then `OTEL_EXPORTER_OTLP_TRACES_*`, then `OTEL_EXPORTER_OTLP_*`; the sampler follows `OTEL_TRACES_SAMPLER`
- `tele/tele.go`: OpenTelemetry resource creation; includes `OTEL_RESOURCE_ATTRIBUTES`, the service name and version given in code win

**grpc/**: gRPC server utilities
- `grpc/server.go`: gRPC server with OpenTelemetry, health checks, panic recovery, and rate limiting

**ptr/**: Pointer utilities
- `ptr/ptr.go`: Generic `From[T]` function that returns `nil` for zero values, otherwise a pointer to the value

### Key Design Patterns

**Configuration-First Approach**: Each package provides default configuration constructors (e.g., `DefaultClickHouseConfig()`, `DefaultMetricsConfig()`) and validation methods.

**OpenTelemetry Integration**: All database, HTTP, and gRPC components include built-in OpenTelemetry instrumentation for metrics and tracing.

**Graceful Shutdown**: CLI framework handles signal-based graceful shutdown with configurable timeout periods.

**Multi-Database Support**: Database packages support connecting to multiple databases simultaneously with shared base configurations.

**Standardized Error Handling**: HTTP package provides consistent error response structures across services.

## Dependencies

Key external dependencies:
- **urfave/cli/v3**: Command-line interface framework
- **ClickHouse/clickhouse-go/v2**: ClickHouse database driver
- **OpenTelemetry**: Comprehensive observability (metrics, traces, logs)
- **Prometheus**: Metrics collection and export
- **grpc-ecosystem/go-grpc-middleware/v2**: gRPC middleware for logging and recovery
- **golang-migrate/migrate/v4**: Database migration support for ClickHouse

## Development Notes

- Uses Go 1.25.1
- Build info is automatically extracted from VCS during compilation
- Environment variable configuration follows `{SERVICE_NAME}_{CONFIG_OPTION}` pattern
- Supports both local development and clustered ClickHouse deployments