# Pierre — Status

A snapshot of what exists, what's been verified, and what's genuinely still missing.
Detailed checklists live in `PLAN.md`; the formal spec and Known Limitations live in
`SPEC.md`. This document is the synthesized summary of both, current as of this round
of work.

## What Pierre is

A single-binary log indexer, in Rust, built on top of `edgestore` (an embedded
Rust KV/LSM engine developed alongside it in a sibling repo). All of edgestore's
crates (`edgestore`, `edgestore-tokio`, `edgestore-tier`, `edgestore-repl`) are linked
into the one Pierre binary as library dependencies — no separate service, no RPC, no
network hop between them.

## What's built and verified

### Ingest
- Native binary protocol (length-prefixed frames) and a Loki-push-compatible HTTP
  endpoint, both normalizing into the same `Record` type and committing through one
  shared `ingest::commit` path.
- Typed field extraction and Drain-style template IDs computed synchronously, before ack.
- Measured end-to-end throughput (real TCP, real WAL, real ack — not an in-process
  engine benchmark): **34,490 req/s** at 50 concurrent connections, well past the
  10,000 req/s target.

### Query
- `GET /query/logs` — time-range + field-selector reads.
- `GET /query/search` — BM25 line-filter, resolves each hit back to its full record.
- `GET /query/aggregate` — reads exclusively from merged rollup sketches (count,
  cardinality, top-k, quantile), never a raw rescan.
- `GET /loki/api/v1/query_range` — a deliberate LogQL subset
  (`{label="value"} |= "text"`), Loki-shaped JSON response so Grafana's Loki
  datasource can parse it. Full LogQL is explicitly out of scope.
- All four verified against real HTTP requests, including a real curl/urllib run
  against the compiled, running binary — not just library-level test calls.
- **Real collector interop verified against an actual `grafana/promtail:latest`
  container** (Docker, `tests/e2e/run_promtail_e2e.sh`), not just synthetic requests.
  Found and fixed two real bugs: (1) Pierre's push endpoint only accepted JSON, but
  real collectors default to Loki's actual protobuf+raw-snappy wire format — every
  real Promtail batch got a 415. Fixed with a real prost-generated decoder
  (`src/lokiproto.rs`, schema pulled from Loki's own `pkg/push/push.proto`) and
  Content-Type dispatch. (2) `GET /query/search` could hang the server on a wide-open
  time range (enumerates every BM25 bucket index in range, not just ones with data)
  — fixed with an upfront bucket-count guard (400, not a hang).

### Rollups
- Exact-counter, HyperLogLog, Space-Saving (top-K), DDSketch — all four sketch
  kinds, algebraic minute→hour→day→month merge, independent TTL per tier.
- Declared entirely in `pierre.toml`, no code changes to add a new rolled-up field.

### Full-text search (BM25)
- Time-bucketed per-namespace indexes (bounds in-memory index size).
- Crash-then-restart leaves nothing permanently unsearchable — verified with a real
  crash simulation (abort mid-session, drop the last reference, reopen fresh).

### Tiering, backup, retention
- Real range/prefix read-through tiering — as of edgestore 1.1.4, `TieredEngine::
  range()`/`prefix()` (not just `get()`) transparently merge local data with an
  ephemeral, no-import view of overlapping archived segments. Pierre's own bespoke
  workaround (`query_archived_range`) was deleted once this shipped upstream — one
  code path instead of two, pinned against regression by
  `tests/archived_range_readthrough.rs`.
- Backup to filesystem or real S3 (verified against live LocalStack, not mocked).
- Archived-segment metadata survives restarts via a plain sidecar file — deliberately
  *not* through the KV engine itself (an earlier version that did caused a
  self-perpetuating feedback loop: bookkeeping write → new segment → archived →
  more bookkeeping → forever, at zero real ingest. Found by running the system, not
  by review. Fixed.). Now also tracks an explicit archive timestamp per segment.
- Deathtime-cohort compaction, now exercised by a real Pierre-side test: a short-TTL
  record's cohort is removed; a long-TTL neighbor in a different cohort survives
  untouched.
- **Local segment pruning** — after a segment has been archived for at least
  `local_retention_secs` (opt-in, off by default), the backup worker deletes its
  local files, reclaiming disk. Safe because of the read-through above. Verified
  end-to-end: flush → archive → confirmed still local → confirmed pruned after the
  grace period → confirmed still queryable via archived read-through
  (`tests/local_pruning.rs`). This is the piece that makes tiering actually save
  local disk; before this round, Pierre archived forever and never reclaimed space.
- **Static bearer-token auth** — `Authorization: Bearer <token>` checked against a
  configured list, one shared middleware across all three HTTP surfaces (Loki push,
  Loki query_range, native query API). Off by default (empty list). Deliberately not
  federated auth — matches what real collectors already send by default, not a new
  scheme, and not a claim of real access control beyond small/trusted-network use.
- **Flush-triggered immediate archiving** — as of edgestore 1.3.0's
  `with_on_segment_flushed` (wired through `AsyncTieredEngine::flush_notify()`), the
  backup worker's archive pass races a `Notify` against its own interval tick, so a
  segment that just flushed — explicit or edgestore's own auto-flush-on-put — gets
  archived immediately instead of sitting local-only for up to `archive_interval_secs`.
  Verified with `archive_interval` set to 3600s and the test still observing the
  segment archived within 300ms (`tests/backup_filesystem.rs`).
- **Unauthenticated-listener hardening (`/ds-security-review` findings, all fixed
  this round)** — three DoS gaps closed on the surfaces that carry untrusted input
  before or without auth: (1) Loki push's protobuf+snappy decoder now checks the
  claimed decompressed size against a 64MB cap (`lokiproto.rs`) before allocating,
  not after — `snap`'s own decoder only rejects claims above ~4GB, reachable from a
  request body of a few bytes (`tests/lokiproto_decompression_bomb.rs`). (2) The
  native protocol's client-supplied length prefix is capped at 16MB
  (`listener/native.rs`) and rejected before the payload buffer is allocated — this
  protocol is deliberately unauthenticated (no existing client convention to
  authenticate against), so an unbounded prefix was an unauthenticated
  memory-exhaustion DoS (`tests/native_ingest_roundtrip.rs`). (3) The native
  listener now caps concurrent connections at 1024 via a semaphore, closing
  connections over the cap immediately rather than letting them each pin a
  socket/fd and a transient allocation — verified at a test capacity of 4 to avoid
  tripping a low `ulimit -n` (`listener::native::tests`, in `native.rs`).

### Measured, not assumed
- BM25 crash-recovery rebuild: fast at every scale tested, never the bottleneck
  (~14ms at 10K docs, ~150-210ms at 100K docs).
- **Real finding, root-caused and fixed, not just mitigated**: indexing throughput
  inside one still-open BM25 bucket used to collapse ~18x between 10K and 100K
  documents. Traced the actual cause: `Engine::index_text` unconditionally scanned
  the *entire* existing index (`remove_document`) before every insert, even for
  documents never seen before — O(n²) total work for Pierre's unique-key log
  workload. Fixed with a capacity-adaptive Bloom filter existence check
  (`edgestore/src/text/bloom.rs`) that skips the scan when a document is definitely
  new. A first attempt used a *fixed* capacity, which degraded silently and just as
  badly once exceeded (a 45.77x slowdown found and caught before shipping) — fixed
  properly by doubling capacity on saturation, amortized O(1) per insert.
  **Result**: 10K docs ~5-6x faster, 100K docs ~73-90x faster, and 1M docs — which
  never completed in any prior run — now indexes in **18.48s clean/isolated**, flat
  throughput the whole way. `textindex_bucket_duration_secs`'s 300s default is no
  longer load-bearing for this specific reason (though it still has other reasons to
  stay short).
- LocalStack S3: 3K–210K records/sec archive (upload), 15K–414K records/sec cold-query
  (download); 20/20 concurrent round-trips byte-exact, zero failures.

## What's still missing

**Two small, understood inefficiencies in the BM25 indexing path, found while fixing
the bug above, not chased further this round** (documented in SPEC.md #L2): (1)
`remove_document` is still O(index size) when it *does* fire (genuine re-index or
delete, not a new document) — irrelevant for Pierre's append-only logs, would matter
for an update-heavy workload; a `doc_id → terms` reverse index would fix it. (2)
`index_text` does a KV lookup + potential deserialize on every call regardless of
whether the in-memory cache already has the answer, because `HashMap::entry(...)
.or_insert(...)` evaluates its argument eagerly — an easy `or_insert_with` fix, just
not done yet.

**Three of SPEC.md's four open questions were resolved as documentation/formula
decisions, not numbers or code** (DECISIONS.md): aggregation p99 target is a stated
goal (sub-50ms) not yet benchmarked; per-tier footprint budget is a documented
formula, not a fixed MB figure; config schema versioning is explicitly deferred until
an actual breaking change needs it. None of these are blocking anything today.

**edgestore's cold-segment BM25/xor-filter lifecycle hook is built and wired, but has
a known durability gap — found by testing it, not assumed.** `TieredEngine::
with_text_stripping`/`archive_segments` (upstream) and Pierre's
`strip_text_index_after_archive` config knob (this round, `Storage::open_with_options`
→ `AsyncTieredEngine::open_with_options`) are both fully wired and the segment-level
rewrite is confirmed correct (`text_index_stripped=true`, record count drops). But
`tests/text_index_stripping.rs` found that stripping isn't durable across a restart:
edgestore's WAL rotation is purely size-based (64MB default), unrelated to
flush/strip events, so the original WAL entry for a stripped write is usually still
present and gets replayed straight back into the memtable on reopen — silently
undoing the strip. Pierre's knob defaults to `false` and is documented with this
warning; not recommended to enable until the upstream WAL/strip interaction is fixed.

**Not started at all:** anything past what's in `PLAN.md`/`DECISIONS.md` — there's no
vector/similarity search (HNSW was looked at only as a future-idea reference, never
adopted), no clustering/HA story, no multi-tenant isolation, no metrics/observability
surface for Pierre's own health (dogfooding its own ingest for itself is not wired up).
