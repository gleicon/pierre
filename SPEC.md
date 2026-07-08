# Specification: Pierre

> **Status (v1.0):** Spec verified against edgestore's actual code (not README/roadmap claims) across versions 1.0.7–1.1.3. Several rounds of edgestore bugs were found and fixed upstream during this process (BM25 query-time O(N) collapse, write-amplification, a crash-recovery gap, two `S3RemoteStore` runtime-lifecycle panics) — all confirmed resolved. **Real tiering now exists and is adopted**: edgestore 1.1.0 shipped `edgestore-tier`/`TieredEngine` (point-lookup read-through: local miss → fetch matching archived segment from `RemoteStore` → import → retry), verified against a live LocalStack S3, not just filesystem. Pierre migrated `Storage` from `AsyncEngine` to a new `AsyncTieredEngine` (built this session, in `edgestore-tokio`, feature `tier`) with a Pierre-specific `fetch_archived_overlapping()` for range-aware selective rehydration. One caveat survives: `range()`/`prefix()` scans remain **local-only** by `TieredEngine`'s own design — only single-key `get()` reads through — so NFR-6's "cost dominated by object storage" still doesn't fully hold for Pierre's scan-heavy query model (see Known Limitations #L1, now scoped narrower than before). edgestore 1.1.3 also shipped `ImmutableEngine` (read-only, in-memory, real K-way merge across segments for range/prefix) — the right primitive for ad-hoc cold-range queries without permanently growing local disk, noted as clearly-scoped future work, not yet wired into `query.rs`. Tracked in **Known Limitations** below, not scattered through the requirements — check that section before re-litigating anything edgestore-related. Full blow-by-blow audit history has been trimmed from this document; the git history of this file has it if needed.

## Problem

Teams running ~100 req/s of log volume are forced onto log platforms built for 1000x that scale (Elasticsearch, OpenSearch, Splunk) and inherit their operational weight — clusters, shards, JVMs, ILM — or onto Loki, which is cheap but has no real search on message content. Nobody serves the middle: real full-text search on recent data, cheap tiered storage for old data, and aggregations computed once instead of rescanned on every read — all in a single deployable unit.

edgestore gives Pierre a real, tested foundation for ingest durability, full-text/aggregation-adjacent primitives, and (as of 1.1.0) real tiering (WAL/LSM/BM25/TTL-cohort-compaction/`TieredEngine` read-through all exist and work as designed, verified at the code level, the S3 and tiering pieces against a live LocalStack container). The one remaining gap: tiering's read-through only covers single-key `get()` — `range()`/`prefix()` scans stay local-only by `TieredEngine`'s own design, which matters because Pierre's entire query model is scan-based, not point-lookup-based. Cold-tier index stripping (drop the inverted index on archival) also remains unbuilt (see Known Limitations #L1).

## Scope

**In scope**
- Single static binary log indexer built on the edgestore embedded storage engine.
- Ingest via Loki push API and a native binary protocol.
- Structured field extraction and Drain-style log templating at ingest (inline, synchronous — cheap).
- Two-tier local storage in v1: hot (WAL/memtable) → warm (local BM25-indexed segments), aged by policy. Warm segments are archived via `TieredEngine` to a config-selectable `RemoteStore` (`FilesystemRemoteStore` or `S3RemoteStore`, both real, verified against LocalStack) — single-key point lookups (`get`) read through to the archive on a local miss; range/prefix scans (Pierre's actual query path) remain local-only, so this bounds durability risk more than it bounds local storage cost. Pierre owns the flush and archive policies; edgestore does not do either automatically.
- BM25 full-text indexing performed **asynchronously**, batched off the ingest hot path — same architectural pattern as rollups.
- Pre-computed, algebraically mergeable aggregations (exact counters, HyperLogLog, Space-Saving/CMS, DDSketch) at minute→hour→day→month granularity.
- Retention as a first-class write-time property via TTL and deathtime-cohort compaction (verified matching edgestore's real implementation).
- Query surface: label/field selectors, time range, line filter (grep-equivalent), and aggregation reads from pre-computed sketches.

**Out of scope (v1)**
- **Cold-tier index stripping** (drop BM25 inverted index, keep only a lightweight filter on archived data) — requires upstream edgestore work (tiering lifecycle hooks), not yet implemented. See Appendix A, ENG-5; Known Limitations #L1.
- Full LogQL: no `rate()`, `sum by()`, or arbitrary metric-query planner.
- Collector protocols beyond Loki push and native: no Fluent Forward, syslog, OTLP.
- Horizontal clustering, rebalancing, or consensus (partitioning/sharding data across nodes). Read replication — whole-dataset copies for query scale/HA via pull-only anti-entropy, no partition ownership or consensus — is a separate axis and not excluded by this line; see PLAN.md's "Read scaling" entry (blocked on edgestore `REPL-01`, not a v1 scope decision).
- Multi-tenant isolation and RBAC.
- Template similarity search / HNSW-based clustering (edgestore has a real HNSW vector engine already — unused by Pierre v1, candidate for v2 per original PRD).

## Users

- **App operator running a single-service or small-fleet workload**: needs to ship logs from an existing collector (Promtail, Alloy, Vector, Fluent Bit) and get real full-text search plus dashboards, without adopting cluster infrastructure.
- **Pierre's own shipper client**: needs a low-overhead native ingest path directly into the storage engine.
- **Dashboard/Explore user (via Grafana)**: needs filter-and-range queries and standard aggregations (p50/p95/p99, top-K, unique counts, exact counts) to return fast without rescanning raw data, and accepts that full-text search on the most recent few seconds of data may lag slightly behind ingest (async indexing).

## Functional Requirements

FR-1: The system shall run as a single static binary with no external service dependencies in the data path.

FR-2: The system shall accept log writes via a Loki-compatible push API endpoint.

FR-3: The system shall accept log writes via a native framed binary protocol.

FR-4: The system shall normalize all ingested records, regardless of listener, into one internal record type before further processing.

FR-5: The system shall extract a configurable set of typed fields (e.g. `level`, `status`, `trace_id`, `latency_ms`) from each ingested line into a columnar side-index, synchronously at ingest.

FR-6: The system shall compute a Drain-style `template_id` for each ingested line synchronously at ingest, and make it available to downstream full-text, rollup, and future similarity consumers.

FR-7: The system shall store all ingested records durably via edgestore's WAL before acknowledging the write. All writes shall be serialized through a single-writer lock around the edgestore `Engine` handle (edgestore's `Engine` is not internally thread-safe by design; concurrency is the caller's responsibility).

FR-8: The system shall age data from hot (WAL/memtable) to warm (local BM25-indexed segments) on a configured flush policy, and separately archive warm segments via `edgestore-tier`'s `TieredEngine` (config-selectable `RemoteStore`: `FilesystemRemoteStore` or `S3RemoteStore`) for durability/DR and single-key read-through. Archiving does **not** let Pierre prune local segments and keep serving *scan* (range/prefix) queries for them — only `get()` reads through on a local miss; local segments are otherwise removed only by TTL/cohort-compaction expiry (FR-21). This is a constraint of `TieredEngine`'s own design (see Known Limitations #L1), not a Pierre choice.

FR-9: The system shall encode time into the storage key prefix such that a time-range query scan can select the tiers to touch without a separate time index.

FR-10: The system shall perform BM25 indexing of ingested lines **asynchronously**, batched off the synchronous ingest write path — mirroring the rollup async pattern (FR-16-18), keeping non-zero synchronous CPU work (tokenization, scoring structures) off the ack path.

FR-11: The system shall bound the in-memory resident size of any single per-namespace BM25 merged index by aligning BM25 index boundaries with time-bucketed rotation (new index per bucket), so full-index in-memory caching does not grow unboundedly with total retained data; queries spanning multiple time buckets merge results across per-bucket indexes. (Bucket-size default depends on Known Limitations #L2.)

FR-12: The system shall archive warm-tier segments via `edgestore-tier`'s `TieredEngine::archive_segments`, with the concrete `RemoteStore` backend (`FilesystemRemoteStore` or `S3RemoteStore`) selected by `pierre.toml` configuration — both real, tested implementations (S3 verified against a live LocalStack container, not just compiled). Archived-segment metadata is persisted by Pierre to a plain sidecar file (`archived_segments.json`), not through the KV engine itself — an earlier version that persisted this via `put_with_ttl` created a self-perpetuating feedback loop (the bookkeeping write became new flushable data, archived again, forever), found via an actual multi-tick test run and fixed (see FR-8).

FR-13: The system shall answer selector queries (label/field predicates plus time range) against the columnar index, restricting reads to tiers overlapping the time range.

FR-14: The system shall answer line-filter (grep-equivalent) queries using BM25 against the relevant time-bucketed segment(s), for any data still resident on local disk within the configured retention window (backup status to `RemoteStore` has no bearing on queryability — see FR-8).

FR-15: The system shall maintain pre-computed aggregations per configured field, updated as data arrives, using a structure selected by field cardinality/type:
FR-15.a: exact counter for low-cardinality categorical fields.
FR-15.b: HyperLogLog for high-cardinality uniqueness fields.
FR-15.c: Space-Saving/Count-Min Sketch for unbounded frequency (top-K) fields.
FR-15.d: DDSketch for numeric fields requiring relative-error quantiles.

FR-16: The system shall maintain aggregation tiers at minute, hour, day, and month granularity, where each coarser tier is computed as an algebraic merge of the tier below it, never a recomputation from raw data.

FR-17: The system shall serve aggregation query reads exclusively from pre-computed sketches, never by rescanning raw log lines.

FR-18: The system shall process rollup field extraction asynchronously off the ingest write path, via a bounded channel.

FR-19: When the rollup channel is full, the system shall drop the rollup contribution and increment a counter, without blocking or failing the ingest write.

FR-20: The system shall flush the live minute-granularity rollup bucket to durable storage on wall-clock-aligned minute boundaries, with a bounded grace window for late data.

FR-21: The system shall apply a configurable TTL to raw log data and to each rollup tier independently, with retention enforced via write-time cohorting rather than a periodic scan-and-delete job (matches edgestore's real deathtime-cohort compactor).

FR-22: The system shall expose rollup configuration (field, kind, parameters, granularities) via a declarative config file, requiring no code changes to add or modify a rollup.

FR-23: The system shall call edgestore's `flush()` on a bounded interval (default target: every ≤5s) to bound the crash-recovery rebuild cost for BM25 merged indexes (see Known Limitations #L2 for why this matters).

## Non-Functional Requirements

NFR-1: Latency: full-text query over a single warm-tier time-bucket index shall return in single-digit milliseconds at the target load in NFR-2. Queries spanning many time buckets degrade proportionally to bucket count merged, not corpus size within a bucket (see FR-11).

NFR-2: Scale: the system shall sustain **10,000 req/s** ingest (100x the 100 req/s baseline) on a single modest node with operating headroom remaining. Practical ceiling is expected to be HTTP/JSON parsing overhead and the single-writer lock (FR-7), not the storage/indexing engine — see Known Limitations #L3 for why this number is still a hypothesis, not a committed target.

NFR-3: Search-visibility lag: a log line shall become full-text searchable within the configured async-indexing delay (default target ≤10s, FR-10) after ingest acknowledgment, not immediately. Structured field/selector queries (FR-13) are not subject to this lag.

NFR-4: Availability: rollup data loss on crash is bounded to at most the unflushed live minute bucket (~1 minute) per rollup tier, approximate-by-design and inconsequential to correctness beyond that window; raw log durability is unaffected by rollup crashes. BM25 search-index crash behavior is governed separately by NFR-8.

NFR-5: Data retention: retention policy is configurable per data class (raw logs, per rollup tier) via TTL; coarse rollup tiers (month) may be retained effectively indefinitely given their bounded size.

NFR-6: Cost: steady-state operating cost is still dominated by **local disk**, not compute, for Pierre's actual query pattern — S3 archiving (when configured) gives real single-key durability/read-through, but Pierre's scan-based queries (range/prefix, i.e. essentially all of them) get no benefit from it, since `TieredEngine` only reads through on `get()`. The original PRD's "cost dominated by object storage" framing does **not** fully hold in v1; a real fix (`ImmutableEngine`-based ad-hoc cold-range queries) exists upstream but isn't wired in yet (Known Limitations #L1).

NFR-7: Compatibility: the system shall accept input from Promtail, Alloy, Vector, and Fluent Bit's Loki output with only an endpoint/URL change to the existing collector config — no pipeline changes.

NFR-8: BM25 crash recovery: no full-text data is permanently lost on crash — a stale index sidecar is detected and automatically rebuilt from durable raw records on engine restart (self-healing, verified against edgestore's own crash-recovery test). The cost is a recovery-time pause proportional to the affected namespace's total raw record count (full rebuild, not incremental gap-replay) — see Known Limitations #L2.

## Interfaces

- **Loki push API** (HTTP): ingest endpoint compatible with Promtail/Alloy/Vector/Fluent Bit Loki output.
- **Native binary protocol**: framed batch ingest for Pierre's own shipper, writing directly into edgestore's write path.
- **Query API — Loki-compatible subset** (HTTP): label/field selectors, time range, line filter; targets what Grafana Explore and typical dashboards generate.
- **Query API — Native** (HTTP): text search, field filters, and aggregation reads over pre-computed sketches.
- **Config file** (`pierre.toml`): declarative rollup definitions (field, kind, granularities, parameters) and tier/retention policy.
- **External system**: edgestore (embedded Rust storage engine) — WAL, memtable, LSM segments with deathtime-cohort compaction, BM25 inverted index, xor filter (uniform per-segment, not tier-specific), `put_with_ttl`. Not a separate service; linked into the same binary. Pierre's `Storage` wraps `edgestore-tokio`'s `AsyncTieredEngine` (built this session), not the plain `AsyncEngine` — surface used: `get/put/put_with_ttl/delete/range/prefix/flush/flush_to_segments/list_segment_metas/index_text/search_text/archive_segments/archived_segments/register_archived`.
- **External system — archive/tiering**: `edgestore-tier`'s `TieredEngine` (via `AsyncTieredEngine`), config-selectable `RemoteStore` backend — `FilesystemRemoteStore` (local disk, content-addressed) or `S3RemoteStore` (real `aws-sdk-s3`, verified against a live LocalStack container). Both are library dependencies linked into Pierre's binary — `edgestore-repl`/`edgestore-tier` are used as crates, not run as a separate process, preserving FR-1's single-binary requirement. Read-through only for single-key `get()` — see FR-8.
- **External system, only when S3 is configured**: an S3-compatible object store (AWS S3, or any endpoint accepting `force_path_style` — LocalStack/MinIO verified). Optional at runtime; `Storage::open` defaults to a local-disk archive under `{data_dir}/_archive` with no external dependency when S3 isn't configured.

## Constraints

- Language/runtime: Rust, compiled to a single static binary.
- Storage engine: edgestore (existing embedded database) — Pierre does not implement its own WAL, LSM, or compaction; it configures and consumes edgestore.
- edgestore's `Engine` is single-writer and not internally thread-safe. Pierre's storage layer wraps it via `edgestore-tokio`'s `AsyncTieredEngine`: `Arc<RwLock<TieredEngine>>`, writes serialized through the write lock, reads (which take `&self`) only needing a read lock, calls pushed through `spawn_blocking` to avoid blocking the async runtime.
- Rollup crates: `hyperloglogplus` (fixed-seed `DefaultHasher`, not `RandomState` — required for merge/cross-restart determinism), `sketches-ddsketch`; Space-Saving implemented directly as a bounded evicting map. All verified in `src/rollup/`.
- Tiering/archive crates: `edgestore-tier` and `edgestore-repl` as path dependencies (libraries, not subprocesses), `s3` feature enabled for `S3RemoteStore`. `edgestore-tokio`'s `tier` feature enables `AsyncTieredEngine`.
- Archived-segment bookkeeping is persisted to a plain sidecar file (`archived_segments.json`), never through the KV engine itself — see FR-12 for why.
- Loki push accepts both the real Loki protobuf+raw-snappy wire format (the collector default — Promtail, Alloy, Vector, Fluent Bit all send this unless explicitly configured otherwise) and plain JSON, dispatched on Content-Type. Wire schema in `proto/logproto.proto` (prost-generated), a deliberately gogoproto-stripped copy of Loki's real `pkg/push/push.proto` — field numbers/types kept identical for wire compatibility, Go-specific codegen annotations dropped (irrelevant to wire format).
- `GET /query/search` (and `textindex::search()` generally) rejects a time range spanning more than `MAX_BUCKETS_PER_SEARCH` (200) BM25 buckets at the configured bucket duration, with a 400 explaining how to narrow the range — protects against enumerating an unbounded number of bucket namespaces (a wide-open `start=0, end=i64::MAX` range would otherwise enumerate ~2.56 million buckets and hang the server). Found via this project's own real-collector end-to-end test.
- Forbidden approaches: no message broker (e.g. Kafka) between ingest and storage — edgestore's WAL is the durability boundary; no LogQL metric-query planner (`rate()`, `sum by()`); no horizontal clustering/consensus in v1; no multi-tenant RBAC in v1.
- edgestore's own status documentation (README feature matrix, ARCHITECTURE.md, roadmap) has a history of overclaiming (the S3 claim was false for several releases before 1.0.12 made it true) — verify any new capability claim against code, ideally against a real integration test, before Pierre's spec depends on it. See Known Limitations #L3.

## Acceptance Criteria

AC-1: Given the binary is started with a config file and no other services running, when it accepts a write, then the write succeeds with no external service calls in the data path.
AC-2: Given a Promtail/Alloy/Vector/Fluent Bit collector configured to push to Pierre's Loki endpoint, when logs are shipped, then Pierre ingests them with no collector pipeline changes beyond the endpoint URL. **Verified against a real `grafana/promtail:latest` container** (`tests/e2e/run_promtail_e2e.sh`) — this is what caught the protobuf/snappy wire-format gap below; a synthetic JSON push alone had not (and could not have) caught it.
AC-3: Given a batch sent via the native protocol, when ingested, then records land in edgestore's WAL and are queryable via the native query API's structured/selector path immediately (not subject to BM25 indexing lag).
AC-4: Given an ingested line, when processed, then its configured typed fields are present in the columnar side-index and its `template_id` is computed and retrievable, synchronously, before write acknowledgment.
AC-5: Given a warm-tier segment past the archive policy threshold, when the archive policy runs, then the segment's bytes are present in the configured `RemoteStore` (`FilesystemRemoteStore` or `S3RemoteStore`, per `pierre.toml`) and retrievable there — for the S3 backend, verified against a real LocalStack container (upload/download/list/delete round-trip, plus an independent download-and-compare against the local bytes) — while the segment also remains queryable locally (archiving does not remove it), and archive metadata survives a restart via the sidecar file.
AC-6: Given a range/prefix query (Pierre's actual query path), when executed, then it is served entirely from local segments — `RemoteStore` is never read for scans, only for single-key `get()` misses (true by `TieredEngine`'s own design, verified in `edgestore-tokio`'s test suite).
AC-7: Given a full-text line-filter query against a single time-bucketed index under NFR-2's load, when executed, then the response returns in single-digit milliseconds at p99, regardless of total retained data volume (only the queried bucket's corpus size matters).
AC-8: Given a rollup configured with kind `exact`, `hll`, `topk`, or `ddsketch` at multiple granularities, when data arrives, then live minute buckets update without blocking ingest writes, and hour/day/month tiers reflect algebraic merges of persisted lower tiers (not raw rescans).
AC-9: Given the rollup channel is saturated, when a new rollup contribution is submitted, then it is dropped, the drop counter increments, and the ingest write path proceeds without added latency or failure.
AC-10: Given the process crashes with an unflushed live minute rollup bucket, when it restarts, then at most that one minute's rollup contribution is lost, and all raw logs and structured-field data from before the crash remain queryable.
AC-10.a: Given the process crashes with BM25-indexed documents added since the last `flush()` call, when it restarts, then the stale index sidecar is detected and automatically rebuilt from raw records, so all documents (including those added after the last flush) are present in full-text search results. The rebuild cost scales with the namespace's total raw record count (Known Limitations #L2).
AC-11: Given a rollup tier's TTL has elapsed, when compaction runs, then the expired rollup data is removed without rewriting unrelated live data in the same segment (deathtime-cohort compaction).
AC-12: Given an aggregation query (e.g. p99 latency by endpoint over 7 days), when executed, then the response is served by merging pre-computed sketches only, with response time independent of raw log volume in that window.
AC-13: Given `pierre.toml` is edited to add or change a rollup definition, when the process restarts, then the new rollup configuration takes effect with no code changes.
AC-14: Given a sustained ingest load at NFR-2's 10,000 req/s target with BM25 indexing running asynchronously, when measured end-to-end (HTTP → WAL → ack), then ingest acknowledgment latency does not depend on BM25 indexing throughput.

**Coverage**

- FR-1 → AC-1
- FR-2 → AC-2
- FR-3 → AC-3
- FR-4 → AC-3, AC-4
- FR-5 → AC-4
- FR-6 → AC-4
- FR-7 → AC-1, AC-3, AC-10, AC-14
- FR-8 → AC-5, AC-6
- FR-9 → AC-6
- FR-10 → AC-10.a, AC-14
- FR-11 → AC-7
- FR-12 → AC-5
- FR-13 → AC-6
- FR-14 → AC-7
- FR-15.a/b/c/d → AC-8
- FR-16 → AC-8, AC-12
- FR-17 → AC-12
- FR-18 → AC-8, AC-9
- FR-19 → AC-9
- FR-20 → AC-10
- FR-21 → AC-11
- FR-22 → AC-13
- FR-23 → AC-10.a

## Known Limitations — edgestore-derived (revisit later)

These are constraints on Pierre imposed by edgestore's current state, not Pierre design choices. Each has a stable ID for cross-reference. Re-check against edgestore's code (not its docs) before assuming any of these have changed.

**#L1 — MOSTLY RESOLVED. `TieredEngine::range()`/`prefix()` are still local-only by edgestore's own design, but Pierre now has a separate ephemeral cold-query path that closes the practical gap; cold-tier index stripping remains unavailable.**
`edgestore-tier`'s `TieredEngine` does real point-lookup read-through (verified against live LocalStack S3), and Pierre uses it (`AsyncTieredEngine`). `TieredEngine::range()`/`prefix()` themselves are still local-only — that hasn't changed upstream. What's new: Pierre built its own range-query path around this, `Storage::query_archived_range()` — downloads just the archived segments overlapping a query's time range, builds an ephemeral `edgestore::ImmutableEngine` from their bytes, queries it, discards it. Verified with a `Storage` instance holding zero local data of its own: correct results, zero new local `.dat` files. Required two small, mechanical additions to `edgestore-tier`/`edgestore-tokio` (`download_segment` — byte fetch without importing, unlike `fetch_segment`). Residual gap, still real: edgestore applies its xor filter uniformly to every segment with no mechanism to selectively drop the BM25 inverted index on archival (upstream tiering lifecycle hooks, Appendix A ENG-5, still not implemented) — archiving still doesn't reduce local disk pressure for data Pierre keeps locally; it only adds this alternate path for data that was never kept, or has since been pruned, locally. Pierre does not currently prune local segments after archiving, so in today's deployment this path is provably correct but not yet load-bearing — it becomes load-bearing the day local pruning ships.

**#L2 — RESOLVED, root cause found and fixed upstream (not just mitigated).**
Original finding: crash-recovery rebuild time itself was always fast (10K docs → 13ms reopen; 100K docs → ~150ms reopen) — never the bottleneck. But indexing throughput *while accumulating in one namespace* collapsed from ~13,000 docs/sec at 10K documents to ~750 docs/sec at 100K — a ~18x degradation. The `textindex_bucket_duration_secs` default cut (3600s → 300s, earlier this session) was a *mitigation* — bounding how large a single bucket could get before rotating — not a fix for the underlying cause.

**Actual root cause, found by tracing the code, not guessing:** `Engine::index_text` (`engine.rs`) unconditionally called `InvertedIndex::remove_document(key)` before every `add_document`, even for a document never indexed before. `remove_document` scans every posting in every term — O(total index size) — regardless of whether the doc_id was ever present. For Pierre's real workload (each log line gets a unique key, essentially never re-indexed), this was pure waste on every single call, scaling with however much had already been indexed: effectively O(n²) total work to index n documents.

**Fix, in two rounds** (`edgestore/src/text/bloom.rs`, new module): added a Bloom filter (`InvertedIndex::doc_bloom`) checked before `remove_document` — zero false negatives means it's always safe to skip the scan when the filter says "definitely not present." First version used a *fixed* capacity (~200K), which turned out to degrade silently and just as badly once exceeded (false-positive rate climbs toward saturation, `remove_document` starts firing again for most "new" documents) — measured as a 45.77x slowdown on the last 10% of a 300K-document run versus the first 10%, an almost-invisible regression since nothing errors or warns. Fixed properly by making the filter capacity-adaptive: it doubles (rebuilding from `postings`, the existing source of truth for which doc_ids exist) whenever it saturates — the same amortized-O(1)-per-insert analysis that makes `Vec::push` cheap despite occasional reallocation.

**Measured improvement** (same hardware, `examples/bench_crash_recovery.rs` + `examples/bench_index_text_progress.rs`):
- 10K docs: 750ms (13,321 docs/sec) → ~130-154ms (65,000-77,500 docs/sec) — ~5-6x faster.
- 100K docs: 134.4s (744 docs/sec) → ~1.5-1.8s (54,000-67,000 docs/sec) — **~73-90x faster**.
- 1M docs: never completed in any prior run (killed after 30-40+ min without finishing) → **18.48s clean/isolated** (~54,000 docs/sec average, completely flat across 20 checkpoints — no degradation trend at all). A qualitative shift from "impractical at this scale" to "trivially fast."

**Consequence for FR-11's bucket-duration default:** the 300s `textindex_bucket_duration_secs` default is no longer load-bearing *for this specific reason* — the underlying O(n²) cost it was bounding is gone. It still has independent justifications (crash-recovery rebuild time, per-bucket memory footprint, cross-bucket query merge cost) and reverting it wasn't done reflexively; treat this as a candidate to revisit with fresh reasoning, not an automatic rollback.

**Follow-up opportunities identified, not chased this round (lower priority, real workload doesn't need them yet):**
1. `remove_document` itself is still O(total index size) when it *does* fire (a genuine re-index or delete) — the bloom filter only lets new documents skip the call, it doesn't speed up updates to existing ones. A `doc_id → terms` reverse index would fix that specifically; not needed for Pierre's append-only log workload, but the natural next step if an update-heavy workload ever depends on this engine.
2. `index_text` calls `self.get(&text_ns, TEXT_INDEX_KEY)` and conditionally deserializes on *every* call (`engine.rs`), then discards the result via `entry(...).or_insert(...)` whenever the namespace is already cached in memory (the common case after the first call) — `or_insert` evaluates eagerly, so this KV lookup/deserialize happens even when its result is thrown away. Small compared to the bug just fixed, but a real, easy win (`or_insert_with`) for a follow-up pass.

**#L3 — RESOLVED (measured, `examples/bench_ingest_throughput.rs`).** Real end-to-end ingest (native TCP protocol → normalize → WAL → ack, 50 concurrent connections, 5s sustained): **34,490 req/s achieved**, comfortably clearing the 10,000 req/s NFR-2 target. This is a genuine measurement of Pierre's actual ingest path, not edgestore's in-process engine-only numbers the target was originally derived from. NFR-2 is no longer a hypothesis.

**#L4 — edgestore's own status/roadmap docs have a track record of overclaiming; verify capability claims against code, and against a real integration test where possible.**
The S3 claim was the standing example for several releases (README/ARCHITECTURE.md said "done" while `remote_store.rs`'s own comment said "future phase" and no SDK dependency existed) — resolved in 1.0.12, where the claim finally became true, verified independently against a live LocalStack container rather than taken on the changelog's word. A separate, smaller instance: a fix was briefly recorded under the wrong version number in `CHANGELOG.md` (since corrected). Standing habit for any *future* edgestore capability Pierre considers depending on (e.g. the existing HNSW/vector engine, if v2 template-similarity work picks it up): check the code, run a real test against it, don't stop at the docs.

**#L5 — RESOLVED (edgestore-repl 1.1.0).** `S3RemoteStore` had two real runtime-lifecycle bugs, found by actually using it against LocalStack, not by reading: (1) `S3RemoteStore::new()` called `Runtime::block_on()` directly in its constructor, panicking from async context; (2) it owned an `Arc<Runtime>` unconditionally, so dropping it from async context also panicked. Fixed properly in 1.1.0, verified in code and by re-running the LocalStack test with Pierre's workaround removed (still passes): the struct now holds `owned_runtime: Option<Arc<Runtime>>`, and the constructor checks `Handle::try_current()` — if already inside a runtime (Pierre's case, always, via `spawn_blocking`), it reuses the ambient handle through `block_in_place` and stores `None`, so there is no owned runtime to ever panic on drop. Pierre's `mem::forget` leak workaround has been removed from `backup/mod.rs` — no longer needed. (1.1.0's own changelog entry was empty, same pattern as before — this was verified against code and a real test, not taken on the version bump alone.)

**Resolved, kept for record (not action items):**
- BM25 query-time O(N) collapse (merged incremental index) and BM25 write-amplification on every indexed document (in-memory-until-flush) — both confirmed fixed in code and no longer constrain Pierre's design.
- **Object-storage (S3) as a real backend** — verified against a live LocalStack container: all upload/download/list/delete/idempotent-upload tests pass for real, not just compile.
- **Real tiering adopted** (see #L1 above for the residual scan-vs-point-lookup gap) — `Storage` migrated to `AsyncTieredEngine`; `Storage::open` defaults to a local-disk archive with no external dependency, `Storage::open_with_remote` for explicit S3 config.
- **A real self-perpetuating feedback-loop bug**, found by Pierre's own testing (not edgestore's): persisting archived-segment bookkeeping via `put_with_ttl` through the same engine being archived caused every archive pass to generate new flushable data about itself, forever, even at zero ingest (5 segments from 1 real event in one test run). Fixed by moving that bookkeeping to a plain sidecar file outside the KV engine.
- **Query API built and verified end-to-end** — Pierre had zero query HTTP surface before this round (only direct library calls in tests). Added: Native query API (`/query/logs`, `/query/search`, `/query/aggregate`) and a deliberate LogQL subset (`/loki/api/v1/query_range`, `{label="value"} |= "text"`). All verified via real HTTP requests through the actual axum routers, not just the underlying library functions.
- **Deathtime-cohort compaction verified through Pierre's own `Storage`** (FR-21/AC-11), not just trusted from edgestore's own tests: a short-TTL record's cohort is removed by `compact_once()` while a long-TTL neighbor in a different cohort survives untouched.
- **ImmutableEngine-based ephemeral cold-range queries, built and verified**: a `Storage` instance with zero local data answers a time-range query purely from downloaded archived segments, with zero permanent local disk growth (asserted via `.dat` file count before/after) — the deferred item from the previous revision, now real.
- **LocalStack S3 performance and consistency, measured for real**: archive (upload) throughput 3K–210K records/sec depending on segment size; cold-query (download + ephemeral query) 15K–414K records/sec; 20 concurrent upload/download round-trips, 0 failures/mismatches.
- **NFR-2/NFR-3 (#L2/#L3) both resolved with real measurements** — see their entries above. #L2 in particular surfaced a genuine new finding (in-memory BM25 index growth isn't bounded by flushing alone) that changes the `textindex_bucket_duration_secs` default recommendation.

## Open Questions (Pierre-scoped, not edgestore-derived)

1. Concrete p99 target for aggregation reads — still unstated numerically.
2. What is the expected on-disk/memory footprint budget per rollup tier and per BM25 time-bucket segment ("bounded by config, not by traffic")?
3. Is there an auth/access-control story for v1, or is the query API assumed to sit behind a trusted network boundary? Worth being explicit rather than silent on this.
4. Versioning/compatibility guarantee for `pierre.toml` schema across releases?
