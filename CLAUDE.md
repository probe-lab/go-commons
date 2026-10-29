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
- `cli/root.go`: Core CLI framework using urfave/cli/v3 with built-in telemetry, logging, and graceful shutdown; a command that declares no `Version` reports the main module version Go records at build time (`v1.2.3` at a tag, a pseudo-version after it, `+dirty` for local changes; v2 and up need a `/vN` module path)
- `cli/banner.go`: on start, the root command writes its name in the ANSI Shadow figlet font and "powered by ProbeLab · <year>" to stderr; `--noBanner` (`<NAME>_NO_BANNER`) turns it off, and help and shell completion requests skip it
- `cli/pg.go`: PostgreSQL CLI configuration flags and setup
- `cli/ch.go`: ClickHouse CLI configuration flags and setup
- `cli/health.go`: Health check CLI utilities

**db/**: Database connectivity and configuration
- `db/pg.go`: PostgreSQL connection management with OpenTelemetry integration; `PostgresMigrationsConfig.Apply` runs golang-migrate on one pooled connection and leaves the handle open
- `db/ch.go`: ClickHouse connection management with automatic migrations support; both migration configs read their files from `Dir` (default `migrations`)
- `db/ch_batch.go`: generic `BatchInserter` for ClickHouse
- Integration tests skip unless `GO_COMMONS_TEST_POSTGRES_DSN` or `GO_COMMONS_TEST_CLICKHOUSE_ADDR` (plus `_USER`, `_PASSWORD`, `_DATABASE`) is set
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
- `tele/metrics.go`: metrics configuration (`MetricsConfig`, `Validate()`) and export, selected by `Exporter` / `--metrics.exporter` (also `OTEL_METRICS_EXPORTER`):
  - `prometheus` (default) serves the default registry for scraping (`Version` on the resource; binds before registering anything so a taken port is an error); the `--metrics.host` and `--metrics.port` root flags also read `OTEL_EXPORTER_PROMETHEUS_HOST` and `OTEL_EXPORTER_PROMETHEUS_PORT` after `<NAME>_METRICS_*`
  - both register ecs-exporter's ENA collector (AWS network allowance counters, nothing on hosts without ENA interfaces) with the default Prometheus registry, and its ECS collector when running in ECS
  - `otlp` pushes over OTLP gRPC every `--metrics.otlp.interval` (zero: `OTEL_METRIC_EXPORT_INTERVAL` or 60s) and once more on shutdown, for short-lived runs a scraper would miss; the `--metrics.otlp.*` flags read `<NAME>_METRICS_OTLP_*`, then `OTEL_EXPORTER_OTLP_METRICS_*`, then `OTEL_EXPORTER_OTLP_*`; instrument names get the `<name>_` prefix so they reach Prometheus under the scrape names, the default Prometheus registry is pushed through the bridge, and each process gets a random `service.instance.id` (overridable via `OTEL_RESOURCE_ATTRIBUTES`)
- `tele/traces.go`: Distributed tracing setup with OTLP gRPC export; `TraceConfig` (`Endpoint`, `Insecure`, `Headers`, `Validate()`); the `--tracing.*` root flags read `<NAME>_TRACING_*`, then `OTEL_EXPORTER_OTLP_TRACES_*`, then `OTEL_EXPORTER_OTLP_*`; the sampler follows `OTEL_TRACES_SAMPLER`
- `tele/tele.go`: OpenTelemetry resource creation; includes `OTEL_RESOURCE_ATTRIBUTES`, the service name and version given in code win

**grpc/**: gRPC server utilities
- `grpc/server.go`: gRPC server with OpenTelemetry, health checks, panic recovery, and rate limiting

**p2p/**: Well-known peer-to-peer networks
- `p2p/config.go`: `GetBootstrapConfig(project, network)` returns bootstrappers and protocol IDs; Bitcoin-family protocol IDs are the network magic in hex, their bootstrappers are DNS seeds; HyperDHT bootstrappers use the dht-rpc `ip@host:port` form, BitTorrent bootstrappers the `host:port` form; TRON bootstrappers the `ip:port` form, with the p2p network ID in decimal as the protocol ID (`11111` on mainnet, `201910292` on `tron/nile`); Tor bootstrappers are the `host:port` DirPorts of the directory authorities; the Ethereum discv4 bootstrappers mix ENRs and enode URLs, as geth's list does; the Filecoin lists have `/dnsaddr` entries without a peer ID, which `maddr.ResolveDNSAddr` expands; `bitcoin/lightning` is the Lightning Network on Bitcoin mainnet, its bootstrappers use the `node_id@host:port` form and its protocol ID is the BOLT 7 chain hash in hex
- `p2p/bootstrap.go`: the bootstrap lists; this is the source of truth for Nebula

**maddr/**: Multiaddress helpers (parse, filter public or private, merge, extract IP, resolve a `/dnsaddr` entry one level into the listed addresses that end in `/p2p/`)

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
- **golang-migrate/migrate/v4**: Database migration support for ClickHouse and PostgreSQL

## Development Notes

- Uses Go 1.25.1
- Build info is automatically extracted from VCS during compilation
- Environment variable configuration follows `{SERVICE_NAME}_{CONFIG_OPTION}` pattern
- Supports both local development and clustered ClickHouse deployments