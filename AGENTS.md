# go-commons

ProbeLab repository. Follow the `probelab` skill: it holds the company rules. Load it before you start any task here.

Shared Go library (`github.com/probe-lab/go-commons`) for ProbeLab tools: CLI setup, logging, telemetry, database access, HTTP and gRPC servers, and well-known p2p network data. Every Go measurement tool of ProbeLab builds on it. The repo is public.

## Layout

- `cli/` — root command on urfave/cli/v3 with telemetry, logging, banner and graceful shutdown; Postgres, ClickHouse and health flags
- `db/` — Postgres and ClickHouse connections with OpenTelemetry, migrations (golang-migrate, files from `Dir`, default `migrations`), generic `BatchInserter`
- `http/` — client (one user agent, timeout, body and redirect caps, optional rate limit), server with graceful shutdown, JSON responses, middleware
- `log/` — slog setup; `--log.format` is `console` (default), `text` or `json`
- `tele/` — metrics (`prometheus` default, or `otlp` push), traces (OTLP gRPC), OpenTelemetry resource
- `grpc/` — gRPC server with OpenTelemetry, health checks, panic recovery, rate limiting
- `p2p/` — `GetBootstrapConfig(project, network)`: bootstrappers and protocol IDs per network; `bootstrap.go` is the source of truth for Nebula
- `maddr/` — multiaddr helpers, including one-level `/dnsaddr` resolution
- `ptr/` — generic `From[T]` pointer helper

## Commands

- `just build`, `just test`, `just lint` (gofumpt, `go fix`, `go vet`, staticcheck), `just format` (gofumpt and `go fix`), `just ci` (the checks CI runs)
- Integration tests skip unless `GO_COMMONS_TEST_POSTGRES_DSN`, or `GO_COMMONS_TEST_CLICKHOUSE_ADDR` with `_USER`, `_PASSWORD` and `_DATABASE`, is set. See `.env.example`.

## Conventions

- Each package has a default config constructor (`DefaultClickHouseConfig()`, `DefaultMetricsConfig()`) and a `Validate()` method.
- Environment variables follow `<NAME>_<OPTION>`; the prefix comes from the command name. Telemetry flags also read the standard `OTEL_*` variables after the prefixed ones.
- A command with no `Version` reports the main module version Go records at build time. v2 and up need a `/vN` module path.
- This is a library: keep exported APIs stable and tell dependents about breaking changes in the pull request.

## Related repositories

- Every ProbeLab Go tool (nebula, tiros, parsec, hermes and others) imports this module.

## Owner

@probe-lab/probelab-core
