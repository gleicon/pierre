# Decisions

Resolved during the `/ds-grill-me` session on `PLAN.md`, one entry per decision.
Implementation status noted per entry where it's since been built.

## Path to first real-world exposure: demo/pitch on a production or QA VM (2026-07-12 `/ds-grill-me` session)

**Question:** Pierre is pre-production with all v1 FRs built and STATUS.md's known gaps documented, but no explicit "what does done mean" criteria existed. What are the objectives, what counts as complete, and when does development stop in favor of using it?

**Decision:** The target isn't "finish PLAN.md's backlog" — it's a single, narrow-window demo/pitch: the Pierre binary handed off to run on a VM inside a production or QA context (not the author's own machine), tee'd from an *existing* real log pipeline (Promtail/Alloy/Vector/Fluent Bit already flowing somewhere), pushed with real volume to see how far it goes, in service of a positive first impression. This reframes "done" away from full feature completeness and toward closing the specific gaps that could visibly embarrass that one event — the same logic that made pass 6 of that session's `/ds-quality-gate-mode` run skip the data-review pass when nothing in scope touched it: judge against what's actually load-bearing for the event, not a generic checklist.

**Why:** A narrow, single-shot opportunity window changes the risk calculus entirely from open-ended hardening. The deployment is additive (a tee off an existing pipeline, not a cutover), which bounds blast radius if Pierre falls over — the existing destination is unaffected. But "shared VM inside production/QA, real data, not physically driven by the author" raises the bar on exactly three things that a purely local/trusted-network demo wouldn't need: auth (see below), operability without hand-holding (a runbook), and *some* live signal while pushing volume (see below) — while leaving everything else (replica mode, cold-tier BM25 stripping, multi-tenant RBAC, vector search) correctly out of scope, exactly as SPEC.md's "Out of scope (v1)" already states.

**Resolved sub-decisions:**
- **Observability for "how far we go":** a periodic (~5s) `log::info!` line reusing the existing worker-loop pattern (`backup::spawn`/`rollup::spawn`), reporting ingest rate plus the two counters that already exist but are unexposed anywhere (`RollupHandle::dropped_count()`, `TextIndexHandle::dropped_count()`). Explicitly **not** a TUI or a `/metrics` HTTP endpoint — those are real scope (new dependency, render loop, or a new API surface) for an event days away, when the counters and the `log` crate already exist and just need wiring.
  **Superseded (2026-07-13, after the demo-prep window):** with the release/CI work done, `GET /metrics` was added (`src/listener/query_api.rs`) exposing the same three counters in Prometheus text exposition format, on the same auth-gated query API surface — no new dependency (hand-formatted text, matching this codebase's existing preference for small hand-rolled things over pulling in a crate for a handful of lines), verified against a real running binary (`pierre_ingest_records_total` reflecting real ingested records). Still deliberately minimal — no histograms, no per-endpoint counts, no dashboard.
- **Auth:** turn on the existing bearer-token middleware (`src/auth.rs`, off by default) for this deployment. A shared production/QA-network VM is not "trusted network only," which is the explicit condition the feature's off-by-default posture assumed (STATUS.md: "not a claim of real access control beyond small/trusted-network use"). Must be documented and easy to set up — a filled-in `pierre.toml` example, not just "the feature exists."
- **Backup/retention:** `BackupConfig::None` (already the default) — explicitly ephemeral, VM-local only. No new durable copy of production/QA log data gets created by a pitch tool; that's a compliance conversation nobody planned for. If the VM is torn down after, the data goes with it.
- **Deployment mechanics:** a short runbook is in scope even though the author expects to drive it themselves — a narrow window is exactly the wrong time to be reconstructing config field names from memory or debugging a misconfigured collector endpoint live.

**Status: in progress.** README/runbook, `.gitignore` review, and a release pipeline (see next entry) are being built directly off this session.

## Release packaging: cargo-dist over goreleaser, edgestore as a real crates.io dependency

**Question:** The user asked for a "goreleaser file" for versioned releases starting at a `0.1.0` tag, plus a synced CHANGELOG. Two real technical snags surfaced before implementing as literally requested: (1) goreleaser is Go-native (`go build`-shaped; Rust support is newer/uncommon), and (2) `Cargo.toml` depended on `edgestore`/`edgestore-tokio`/`edgestore-repl`/`edgestore-tier` via sibling-directory `path = "../edgestore/..."`, which only resolves on a machine with both repos checked out side by side — incompatible with "give the pierre binary" as a standalone artifact handoff.

**Decision:** Use `cargo-dist` (the Rust-native equivalent of what goreleaser does for Go: cross-compiled release binaries, GitHub Releases, checksums, install script) instead of goreleaser. Switch the four `edgestore*` dependencies from path deps to versioned crates.io dependencies (`1.3.0`, confirmed published and matching what Pierre already codes against — the flush-notify feature from this session's earlier hardening work is itself an edgestore-1.3.0 feature). Treat edgestore as a real foreign dependency going forward — Pierre's own build no longer assumes direct access to edgestore's development checkout.

**Why:** goreleaser would work but fights its Go-shaped defaults the whole way for a Cargo workspace; cargo-dist is purpose-built for exactly this shape of project (single Rust binary, multi-platform release artifacts) and needs far less custom configuration. The path-dependency switch isn't just a release-tooling nicety — it's a correctness fix for the stated goal: a "give the pierre binary" handoff to a VM that has never seen `../edgestore/` cannot build from source under the old `Cargo.toml` at all, only run a binary built elsewhere; switching to crates.io deps means `cargo build --release` (or a cargo-dist-driven CI build) works from a solo `pierre` checkout, which is the more robust artifact-production story regardless of who builds it.

**Blocker found executing this:** crates.io's published `edgestore*` `1.3.0` predates `flush_notify()` (which Pierre's `backup` worker already depends on, from earlier this session) and the `get`/`range`/`prefix` lock-split — both are real, built, but sitting under `## [Unreleased]` in edgestore's own CHANGELOG.md, never version-bumped or published; the lock-split isn't even committed yet in edgestore's working tree. This is edgestore's own release-discipline gap, not a Pierre problem to route around by re-implementing anything. Filed as `edgestore/.planning/BACKLOG.md` `REL-01`, written as a real technical request (not an assumption of automatic acceptance — edgestore is a generic embedded-DB library, and the lock-split changes locking behavior for every caller, so it deserves review on its own merits). Not committing, publishing, or pushing anything in the edgestore repo — that's explicitly the author's call, on their own timeline.

**Interim:** `Cargo.toml` reverted to path dependencies (`../edgestore/...`) so Pierre's own build/demo prep isn't blocked on an external publish. The 0.1.0 release binary gets built from a machine with both repos checked out (the author's dev machine or a CI runner they control) — which still satisfies "give the pierre binary" as a pre-built artifact handoff; it just means the *build* isn't yet reproducible from a solo `pierre` checkout. Revisit the crates.io switch once `REL-01` ships.

**Update — `REL-01` shipped same session.** edgestore's `Cargo.toml` bumped to `1.4.0` (commit `1c3721d`, "feat: v1.4.0 — lock-split read path, flush_notify, get_needs_archived_fetch"), and all four crates (`edgestore`, `edgestore-tokio`, `edgestore-repl`, `edgestore-tier`) are published on crates.io at `1.4.0` — confirmed directly against the crates.io API, not assumed from a version string. Pierre's `Cargo.toml` switched back to real version dependencies (`"1.4.0"`, no path deps), full test suite green (28 lib unit tests + all integration suites), and `dist build` succeeds. "Give the pierre binary" now genuinely means "buildable from a solo `pierre` checkout" — the generated GitHub Actions release workflow (`cargo-dist`) will work on hosted runners with no access to a local `edgestore` checkout, since the dependency is real crates.io now, not a sibling-directory path.

**Status: done.**

## Storage key generation: monotonic counter → per-record randomness

**Question:** `Storage`'s record key was `timestamp_ns (8 bytes) || seq (8 bytes)`, where `seq` was a per-process `AtomicU64` reset to 0 on every restart. Real risk: two records at the same `timestamp_ns` with a similarly-low `seq` across different process lifetimes (e.g. a backfill replaying identical recorded timestamps) could collide and silently overwrite each other under normal KV last-write-wins semantics.

**Decision:** Replace the monotonic counter with fresh per-record randomness — `encode_key(timestamp_ns, rand::random::<u64>())` instead of an incrementing atomic. UUIDv7-*inspired*, not literal UUIDv7: kept Pierre's own nanosecond timestamp leading the key (UUIDv7 uses a 48-bit millisecond timestamp, which would have been a real ordering-precision regression for a log system where same-millisecond events need correct relative order), but adopted UUIDv7's actual approach to the trailing bits — fresh randomness generated per record, not a value seeded once at startup and then incremented (which was an earlier, weaker version of this same idea: it would have given each *process* independent collision odds, but fresh-per-record gives each *record* independent odds, a stronger property for the same cost).

**Why not just seed a counter once at startup:** that fixes the exact "restart at 0" bug but still means every record within one process shares one collision surface (the process's random seed) rather than getting its own. True per-record randomness, at the cost of one RNG call per commit (added `rand` crate — chosen over the OS-syscall-per-call alternative like `getrandom`, since `rand`'s thread-local RNG seeds from the OS once and then runs a fast, non-syscall PRNG for every subsequent call), gives a stronger guarantee for negligible extra cost — confirmed via `examples/bench_ingest_throughput.rs`: 34,490 → 32,866 req/s, within normal run-to-run variance, not a measurable regression.

**Verified:** `tests/key_collision.rs` — 1000 records committed at the exact same `timestamp_ns` all get distinct keys and all remain independently retrievable (no silent overwrite).

## Auth model for v1

**Question:** Every ingest/query endpoint is unauthenticated. What's the story for v1?

**Decision:** No federated/full auth in v1. Add a config-based static token list Pierre checks per request, using whatever real collector clients already send by default (not inventing a new scheme) — `Authorization: Bearer <token>`, matching Promtail's/Alloy's/Vector's existing `bearer_token` client config field. Applied to all three HTTP surfaces (Loki push, Loki query_range, native query API). The native TCP protocol stays unauthenticated for now — no existing client convention to match there, lower priority.

Document clearly: this is not federated auth, it's meant for small/test/trusted-network deployments; users needing real secret-manager-backed auth should ask, to prioritize which providers get built later.

**Why:** A real auth story (API key rotation, mTLS certs, OAuth/IdP) cuts against Pierre's single-static-binary, no-external-dependency pitch. But zero auth at all kills the "point my existing Loki/OpenSearch collector at Pierre to try it" on-ramp — those tools already expect to send *some* credential, and getting rejected outright (or worse, wide open) breaks the first impression. A static token list is the minimum that satisfies "the protocol already expects this," without committing to infrastructure Pierre doesn't want to own.

**Status: built.** `src/auth.rs`, `auth_tokens` in `pierre.toml`, verified by `tests/auth.rs` and a real running-binary check (401/401/200 for missing/wrong/correct token).

## Mechanism and scope for the token check

**Decision:** `Authorization: Bearer <token>` only (no Basic auth) via one shared axum middleware layer, applied uniformly to Loki push, Loki query_range, and the native query API.

**Why:** Bearer is what Promtail/Alloy/Vector's Loki sinks already support out of the box — zero collector-side surprises. One code path is simpler than also supporting Basic, and nothing so far has demanded it.

## Local segment pruning — build now, and how it's driven

**Decision:** Build it now. After a segment is archived, wait a configurable grace period, then delete its local `.dat`/`.idx`/`.xf`/`.meta` files. Driven from `backup::spawn`'s *existing* worker task as a third tick arm (alongside flush/archive), not a new thread and not on the ingest hot path. The "is this segment due for pruning" check is a pure, thread-independent predicate function, factored into a small `pierre::retention` module.

Pruning logic stays `RemoteStore`-agnostic (works identically whether the archive backend is filesystem, S3, or a future NFS-style store) since archiving already abstracts over that via the `RemoteStore` trait — no special-casing by backend.

**Why:** edgestore 1.1.4 made `TieredEngine::range()`/`prefix()` do real archived read-through by default (confirmed via `tests/archived_range_readthrough.rs`), which removes the main risk that made pruning unsafe before (queries silently missing pruned data). Pruning is the step that actually makes tiering save local disk — today Pierre archives forever but never reclaims local space. The retention check must not run per-ingest (latency risk) or assume a dedicated thread exists (config-dependent) — reusing `backup::spawn`'s already-running loop avoids both.

**Status: built.** Required a new edgestore primitive (`Engine::prune_local_segment`, mirroring the already-existing `replace_segment`/`strip_text_index` pattern) plumbed through `TieredEngine`/`AsyncTieredEngine`/`Storage`. `ArchivedSegmentRecord` (`backup/mod.rs`) now tracks an explicit `archived_at` timestamp per segment. `local_retention_secs`/`local_prune_interval_secs` in `pierre.toml`, opt-in (`None` by default). Verified end-to-end by `tests/local_pruning.rs`: flush → archive → confirmed still-local before grace period → confirmed pruned after → confirmed still queryable via archived read-through.

## Shared lifecycle/retention pattern — how far to generalize

**Question:** Multiple concerns ("prune local segment N seconds after archiving," and a future "strip BM25/xor-filter from a cold segment" once edgestore ships it) all have the same "wait for an event, then act" shape. Does this need a general scheduler?

**Decision:** A small, focused `pierre::retention` module (a due/predicate primitive, not a full generic GC framework or unified scheduler). Explicitly *not* merging the three existing worker loops (`backup::spawn`, `rollup::spawn`, `textindex::spawn`) into one — they're simple, working, tested, and not causing real duplication pain today.

**Why:** Matches the project's YAGNI bar held throughout — no structure without a requirement demanding it. The convergent pattern is real (spotted correctly), but the fix is a shared primitive the next concern can reuse, not a refactor of working code for tidiness.

**Status: built.** `src/retention.rs` — `is_due(event_time, grace_period)`, unit tested, driven from `backup::spawn`'s existing loop for local pruning.

## Aggregation read p99 latency target

**Decision:** Sub-50ms p99 for `GET /query/aggregate`.

**Why:** The workload is one rollup tier, a handful of persisted sketch buckets fetched and merged in memory, no raw log scan — comparable to a single KV read plus cheap in-memory merge. Matches the actual cost shape rather than picking an arbitrary round number; still needs a real benchmark to confirm (not measured yet, unlike NFR-2/#L2/#L3 which now have real numbers).

## Per-tier memory/disk footprint budget

**Decision:** Document the bounding knobs and the formula (e.g. "number of live buckets × per-bucket sketch size, where bucket count is governed by TTL/bucket-duration config"), not a fixed MB number.

**Why:** Footprint already scales with config an operator controls (bucket duration, rollup TTL, Space-Saving capacity, DDSketch bucket count) — asserting a specific number would depend on their field cardinality anyway and could be misleading. Documenting the formula lets an operator compute their own budget from their own config, which is more honest than a default that doesn't generalize.

## pierre.toml schema versioning

**Decision:** Defer. No version field for now.

**Why:** Every config field added this session (`query_listen_addr`, `cohort_window_secs`, `hot_to_warm_flush_interval_secs`, etc.) was additive with a sane default — zero breakage so far. A version field is speculative machinery for a compatibility problem that doesn't exist yet; add it when an actual breaking change forces a migration story, not before.

## Incomplete 1M-record BM25 benchmark

**Original decision:** Leave it as-is, don't re-run. (No response within 60s on this question — proceeded with the recommended default per this session's established pattern.)

**Superseded:** the user later asked to re-run it and root-cause the degradation properly ("fix it right for good"). That investigation found the actual bug (see below) — the 14x/18x degradation wasn't an inherent property of the BM25 engine at all, it was a specific, fixable algorithmic mistake. The original "leave it, we already have enough to act on" reasoning was sound *given the fix available at the time* (a bucket-duration mitigation); it stopped being the right call once a real fix became possible.

## Root-cause and fix: BM25 indexing throughput collapse (SPEC.md #L2)

**Investigation:** `Engine::index_text` unconditionally called `remove_document` (an O(total index size) scan over every posting in every term) before every `add_document`, even for documents never indexed before. For Pierre's real workload — every log line gets a unique key, essentially never re-indexed — this scan was 100% wasted work on every single call, scaling with however much had already been indexed. That's O(n²) total work to index n documents, not the O(n) the design intended.

**Decision:** Fix upstream with a Bloom filter existence check (zero false negatives — always safe to skip the scan when it says "definitely not present"), not a workaround on Pierre's side. Reused the project's existing preference for hand-rolled simple structures (matching Space-Saving) rather than adding a crate — a plain `HashSet<Vec<u8>>` was considered and rejected as heavier than necessary (full-key heap allocation per entry) for a check that only needs "possibly present" with zero false negatives.

**A first attempt used a fixed capacity** (~200K entries) and was caught before shipping: it degrades silently and just as badly once actual usage exceeds that capacity (false-positive rate climbs toward saturation, `remove_document` fires again for most "new" documents) — measured as a 45.77x slowdown at 1.5x over capacity, with no error or warning of any kind. Corrected to a capacity-adaptive design: the filter tracks its own saturation and `InvertedIndex` doubles it (rebuilding from `postings`, the already-existing source of truth for which doc_ids exist) whenever needed — amortized O(1) per insert, the same analysis that makes `Vec::push` cheap.

**Why the xor filter (already used elsewhere in edgestore) wasn't reused:** xor filters need a fixed, fully-known key set to construct — no cheap incremental insert. `InvertedIndex` grows one document at a time; rebuilding an xor filter on every insert would reintroduce the exact cost being eliminated. Bloom filter's incremental-insert support is why it fits this specific spot, even though xor filter is the better primitive everywhere else in the codebase.

**Verified:** 10K docs ~5-6x faster, 100K docs ~73-90x faster, 1M docs (never completed in any prior run) now indexes in 18.48s clean/isolated with flat throughput throughout. Two follow-up inefficiencies noted but deliberately not chased (documented in SPEC.md #L2): `remove_document`'s cost for genuine re-indexes/deletes (irrelevant for Pierre's append-only workload), and an eager KV lookup in `index_text` that could be made lazy.

**Note on debugging process:** a "stuck" appearance during a background benchmark run (12+ minutes with no output) turned out to be CPU contention from an unrelated Claude Code process on the same machine, not a bug — confirmed by running the same workload in isolation (18.48s) and by watching memory continue to grow (real progress) rather than stay flat (a real stall). Worth remembering: absence of output during a long-running background task is not itself evidence of a hang if the tool only prints at completion.

## Pierre's bespoke `query_archived_range` — keep or delete

**Decision:** Delete it (and its `logs_key_bounds` helper), along with `tests/cold_query.rs`. Replace with an evergreen regression test (`tests/archived_range_readthrough.rs`) asserting the plain `Storage::range()` reads through to archived-only data with zero local disk growth — a test that must fail if a future edgestore version ever regresses this behavior back to local-only.

**Why:** edgestore 1.1.4's built-in `range()`/`prefix()` read-through (real `ImmutableEngine`-backed ephemeral merge, local wins ties) is strictly more general than Pierre's bespoke version (which never covered `prefix()`) and is now what local segment pruning depends on anyway. Keeping both would mean two ways to reach the same data with different call conventions, for no benefit.

**Status: done.**
