# Pierre

[![CI](https://github.com/gleicon/pierre/actions/workflows/ci.yml/badge.svg)](https://github.com/gleicon/pierre/actions/workflows/ci.yml)
[![OSV-Scanner](https://github.com/gleicon/pierre/actions/workflows/osv-scanner.yml/badge.svg)](https://github.com/gleicon/pierre/actions/workflows/osv-scanner.yml)
[![crates.io](https://img.shields.io/crates/v/pierre.svg)](https://crates.io/crates/pierre)
[![docs.rs](https://docs.rs/pierre/badge.svg)](https://docs.rs/pierre)

<img src="assets/pierre-mascot.jpg" alt="Pierre" width="400">

A single-binary log indexer, in Rust, built on [edgestore](https://github.com/gleicon/edgestore) (an
embedded Rust KV/LSM engine developed alongside it). No separate service, no RPC, no network hop
between ingest, storage, full-text search, and aggregation — one process, one deployable unit.

Real full-text search on recent data, cheap tiered storage for old data, and aggregations
computed once instead of rescanned on every read — for teams running ~100 req/s of log volume
who don't want to inherit Elasticsearch/Splunk's operational weight, or Loki's lack of real
search on message content.

See [`SPEC.md`](SPEC.md) for the full functional/non-functional requirements and acceptance
criteria, [`STATUS.md`](STATUS.md) for what's built and verified vs. still missing, and
[`DECISIONS.md`](DECISIONS.md) for the reasoning behind non-obvious choices.

## What it does

- **Ingest**: a Loki-push-compatible HTTP endpoint (real collectors — Promtail, Alloy, Vector,
  Fluent Bit — work with just an endpoint change, no pipeline changes), an Elasticsearch
  `_bulk`-compatible endpoint (Filebeat, Logstash, Fluent Bit's ES output, Vector's ES sink —
  same zero-collector-change story), syslog RFC5424 over UDP and TCP (appliances and legacy
  systems that never leave), OTLP logs over gRPC and HTTP (the real upstream protobuf schema,
  a genuine `tonic` gRPC service — where new OpenTelemetry deployments already start), and a
  native framed binary protocol.
- **Query**: time-range + field-selector reads, BM25 full-text search, pre-computed aggregations
  (exact count, cardinality, top-k, quantile), and a Loki-compatible `query_range` subset so
  Grafana's Loki datasource works against it.
- **Storage**: hot (WAL/memtable) → warm (local BM25-indexed segments) → archived (S3 or
  filesystem, configurable), with TTL-based retention and optional local-disk pruning after
  archival.
- **Metrics**: `GET /metrics` (Prometheus text exposition format) — ingest rate and the
  rollup/textindex drop counters, same auth as the rest of the query API.

## Quickstart

```sh
cargo build --release
cp pierre.toml my-pierre.toml   # edit data_dir / listen addrs / fields as needed
./target/release/pierre my-pierre.toml
```

The config file path is the only CLI argument; it defaults to `pierre.toml` in the current
directory if omitted. See the comments in [`pierre.toml`](pierre.toml) for every field.

Once running, three HTTP/TCP surfaces come up on the addresses configured in `pierre.toml`:

- `native_listen_addr` — the native ingest protocol (unauthenticated by design, see
  [`DECISIONS.md`](DECISIONS.md)).
- `loki_listen_addr` — `/loki/api/v1/push` (ingest) and `/loki/api/v1/query_range` (query).
- `query_listen_addr` — `/query/logs`, `/query/search`, `/query/aggregate`.

The Loki and native-query surfaces share one bearer-token auth middleware, off by default
(`auth_tokens = []`). Turn it on before exposing Pierre beyond a fully trusted network — see
[`RUNBOOK.md`](RUNBOOK.md) for a concrete walkthrough.

## Deploying somewhere real

[`RUNBOOK.md`](RUNBOOK.md) walks through standing Pierre up on a VM with a real collector
tee'd into it: generating and setting an auth token, watching the live ingest-rate/drop-counter
log line, and what to check before pointing real traffic at it.

## Building a release

Cross-compiled release binaries are built with [`cargo-dist`](https://opensource.axo.dev/cargo-dist/)
— see `dist-workspace.toml`. `cargo dist build` locally, or push a `v*` tag to trigger the CI
release workflow.

## Development

```sh
cargo build --tests
cargo test
cargo clippy --all-targets
```

Pierre depends on `edgestore`/`edgestore-tokio`/`edgestore-repl`/`edgestore-tier`, published on
crates.io — a solo `pierre` checkout builds standalone, no sibling repository needed.
