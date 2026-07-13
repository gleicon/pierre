# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2026-07-13

First tagged release. All v1 functional requirements in `SPEC.md` are built and verified —
see `STATUS.md` for the full picture of what's done vs. still missing, and `DECISIONS.md` for
the reasoning behind non-obvious choices.

### Added

- Ingest via a Loki-compatible push API (JSON and real Loki protobuf+snappy wire format,
  dispatched on Content-Type) and a native framed binary protocol.
- Synchronous typed field extraction and Drain-style log templating at ingest.
- Query surface: `/query/logs` (selector + time range), `/query/search` (BM25 full-text),
  `/query/aggregate` (pre-computed sketches), and a Loki-compatible `query_range` subset for
  Grafana's Loki datasource.
- Pre-computed, algebraically mergeable aggregations (exact counter, HyperLogLog, Space-Saving
  top-K, DDSketch) at minute/hour/day/month granularity, declared in `pierre.toml` with no code
  changes.
- Two-tier local storage (hot WAL/memtable → warm BM25-indexed segments) with archival to a
  config-selectable `RemoteStore` (filesystem or S3), TTL-based retention via deathtime-cohort
  compaction, and optional local-disk pruning after archival.
- Flush-triggered immediate archiving — a freshly-flushed segment gets archived without
  waiting for the next archive-interval tick.
- Static bearer-token auth (`Authorization: Bearer <token>`), shared across all three HTTP
  surfaces, off by default.
- Hardening on unauthenticated/pre-auth input paths: a capped snappy decompression claim on
  Loki push, a capped native frame length, and a capped native connection count — all found by
  `/ds-security-review` and verified against a real running binary, not just unit tests.
- A periodic (~5s) ingest-rate/drop-counter log line for live visibility while pushing load —
  not a metrics endpoint, just enough signal to watch a demo/pilot in real time.
- `README.md`, `RUNBOOK.md` (concrete steps for a VM demo/pitch deployment), `DECISIONS.md`,
  `SPEC.md`, `STATUS.md`, `PLAN.md`.
