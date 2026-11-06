# ProbeLab Go Commons

[![Go Reference](https://pkg.go.dev/badge/github.com/probe-lab/go-commons.svg)](https://pkg.go.dev/github.com/probe-lab/go-commons)
[![Go Report Card](https://goreportcard.com/badge/github.com/probe-lab/go-commons)](https://goreportcard.com/report/github.com/probe-lab/go-commons)

A comprehensive Go library providing reusable components for ProbeLab applications. This library offers standardized patterns for CLI applications, database connectivity, HTTP services, logging, telemetry, and gRPC servers with built-in observability.

## Quickstart

```sh
go get github.com/probe-lab/go-commons@latest
```

Import the packages you need, for example `github.com/probe-lab/go-commons/cli` for the root command of a tool. Read the API with `go doc github.com/probe-lab/go-commons/<package>`.

| Package | Provides |
|---|---|
| `cli` | Root command with logging, telemetry, banner and graceful shutdown; Postgres, ClickHouse and health flags |
| `db` | Postgres and ClickHouse connections, migrations, batch inserts |
| `http` | HTTP client, server, middleware, JSON responses |
| `grpc` | gRPC server with health checks, panic recovery and rate limiting |
| `log` | Structured logging on slog |
| `tele` | Metrics and traces with OpenTelemetry |
| `p2p`, `maddr` | Bootstrap lists and protocol IDs per network; multiaddr helpers |
| `ptr` | Pointer helper |

## Configuration

The root command in `cli` adds these flags to a tool. Each flag also reads `<NAME>_<FLAG>` from the environment, where `<NAME>` is the command name in upper case; the telemetry flags then fall back to the standard `OTEL_*` variables.

| Flag | Default | Description |
|---|---|---|
| `--log.level` | `info` | `debug`, `info`, `warn` or `error` |
| `--log.format` | `console` | `console`, `text` or `json` |
| `--noBanner` | `false` | Turns off the start banner |
| `--metrics.enabled` | `false` | Exposes metrics |
| `--metrics.exporter` | `prometheus` | `prometheus` serves for scraping, `otlp` pushes |
| `--metrics.host`, `--metrics.port`, `--metrics.path` | `localhost`, `6060`, `/metrics` | Where the Prometheus endpoint listens |
| `--tracing.enabled` | `false` | Emits traces over OTLP gRPC |
| `--tracing.endpoint` | `localhost:4317` | OTLP collector for traces |
| `--shutdown.grace` | `30s` | Time to wait for a graceful shutdown |

The flags `--log.source`, `--metrics.otlp.*`, `--tracing.insecure`, `--tracing.headers` and `--aws.region` are listed by `--help`. The integration tests read the variables in `.env.example`.

## Metrics

The library creates these instruments when a tool uses the matching component. Names are as in code; the Prometheus exporter adds the `<name>_` prefix and unit suffixes. There is no dashboard for the library itself.

| Instrument | Type | Unit | Component |
|---|---|---|---|
| `requests` | counter | none | `http.MiddlewareMetric` |
| `in_flight` | up-down counter | none | `http.MiddlewareMetric` |
| `grpc_req_panics_recovered_total` | counter | none | `grpc` server |
| `batch_inserter.rows_flushed` | counter | none | `db.BatchInserter` |
| `batch_inserter.rows_dropped` | counter | none | `db.BatchInserter` |
| `batch_inserter.flush_duration` | histogram | s | `db.BatchInserter` |
| `batch_inserter.flush_size` | histogram | none | `db.BatchInserter` |

## Development

| Recipe | Does |
|---|---|
| `just build` | Builds every package |
| `just test` | Runs the tests; extra arguments go to `go test` |
| `just lint` | Checks gofumpt formatting, `go fix`, `go vet` and staticcheck |
| `just format` | Formats with gofumpt and applies `go fix` |
| `just ci` | Runs `lint`, `build` and `test` |

Integration tests for Postgres and ClickHouse skip unless the variables in `.env.example` are set.

## Deployment

Nothing is deployed. This is a library: a tool picks up a change when it updates its `go-commons` version.
