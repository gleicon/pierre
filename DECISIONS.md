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

## PRD v0.2 amendment — grill-me session (2026-07-25)

Resolving the open questions and ambiguities in `pierre-prd-v02-amendment.md` (agent-facing retrieval, hybrid semantic search, migration surfaces, zero-page ops). One entry per resolved decision below; each flagged as buildable in Pierre's own repo now vs. blocked on new edgestore engineering work (ENG-7 through ENG-12 in the PRD's appendix) that falls under the "don't touch edgestore, communicate instead" standing instruction from earlier this session.

### Interview scope

**Question:** Should this session resolve product/priority decisions across the whole PRD, including the parts blocked on edgestore work, or stay scoped to what's buildable in Pierre today?

**Decision:** Whole PRD. Flag each resolved decision as buildable-now vs. blocked-on-edgestore.

**Why:** Cheap to resolve as conversation now; expensive to re-run this same interview later once edgestore work lands. Keeps the plan coherent even though execution will be staged.

### Sequencing: start M1 now, hold M2/M3 on edgestore

**Question:** Block C (ES `_bulk`, OTLP, syslog ingest — the PRD's M1) has no edgestore dependency at all. Most of Block A (MCP server) and all of Block B (hybrid retrieval) need new edgestore work (ENG-7–12) first. Start M1 in Pierre now, or hold everything until edgestore's timeline is clearer?

**Decision:** Start M1 (Block C) in Pierre now, independent of edgestore's timeline.

**Why:** Real value — more shippers can point at Pierre with zero collector change — that depends on nothing unresolved. Matches the PRD's own sequencing.

### edgestore stays generic — no Pierre-specific coupling, ever

**Question:** How should edgestore feature requests and bugs surfaced by this PRD (ENG-7 through ENG-12) be handled, given edgestore is meant to be a reusable building block, not Pierre's private engine?

**Decision:** edgestore is never modified to be Pierre-specific. Any bug found or feature needed (including the six ENG-7–12 items this PRD requires) gets written up as clear text and handed to the user directly — he decides if/how/when to build it in edgestore's own repo, on his own timeline. No edits, no backlog entries, no issues filed, no draft code — not even as a starting point — in the edgestore repository from this project's sessions.

**Why:** edgestore is used by other things beyond Pierre; letting one downstream consumer's PRD shape its design directly (even for a well-motivated feature) risks exactly the coupling a shared building block must avoid. This generalizes the earlier "communicate bugs, don't fixate" rule to feature requests too.

### Embedding generation placement (PRD Open Question 1)

**Question:** Generate embeddings inline at ingest (adds latency + a runtime dependency to the hot path) or asynchronously post-compaction (recent data briefly keyword-only)?

**Decision:** Async, reusing the exact worker-channel pattern `src/rollup/mod.rs` and `src/textindex/mod.rs` already use (bounded channel, drop-and-count on overflow, non-blocking to ingest). The embedding worker is new Pierre-side application code — a new consumer of the ingest fan-out, same shape as rollup/textindex today. Vector *storage* (ENG-7) is edgestore's job; *generating* the embedding and handing it off stays Pierre's, matching the existing split.

**Why:** Matches the PRD's own stated lean ("the more defensible default") and Pierre's established architecture — no new pattern invented, no edgestore write-path change needed for this part.

### Default embedding model (PRD Open Question 4)

**Question:** Multilingual, or English-first/smaller with multilingual opt-in?

**Decision:** `multilingual-e5-small`, matching `vectoria`'s (sibling project, same author) existing default. Not a fresh size/speed trade-off — e5-small is already the small/fast tier, so multilingual doesn't cost extra there.

**Condition, not yet satisfied:** async placement (previous entry) only bounds *ingest*-path latency. Query-time embedding — embedding the search query itself before a vector lookup — is unavoidably inline on the read path, and that's exactly what the PRD's own success criteria bind (agent investigation loop within one turn budget, cold-tier p95 < 3s). This choice is provisional until query-time embedding latency is actually measured on representative hardware — matching this project's own "measured, not assumed" discipline (see STATUS.md) — not assumed acceptable just because vectoria uses it for a different workload shape (product catalogs, not ad-hoc agent queries).

**Why:** Reuses a validated choice instead of a fresh evaluation, but doesn't let that reuse paper over an unmeasured latency risk on Pierre's actual query pattern.

### OSS/commercial boundary (PRD Open Question 3)

**Question:** Where's the line between free/OSS and paid? The PRD's own guess: org-level features (SSO, RBAC, multi-tenant isolation, retention governance), not any retrieval capability.

**Decision:** Confirmed, with a sharper framing than the PRD's own phrasing: the line is *who absorbs the administrative/management overhead of running Pierre across an organization* (SSO integration, RBAC administration, multi-tenant isolation, retention governance workflows), not gated retrieval capability. MCP server, semantic search, and migration surfaces stay fully OSS — none of that is ever paywalled.

**Why:** Matches the PRD's own positioning (Section 00.2: self-hosting is the moat; gating retrieval features would undercut the "why not a managed retrieval API" pitch in Section 01) and extends Block D's "zero-page infrastructure ops" premise to a parallel "zero-page organizational ops" commercial value prop — same underlying idea (Pierre/the vendor absorbs operational burden), applied to org administration instead of infra operation.

### Cold-tier semantic search (PRD Open Question 2)

**Decision:** Confirmed as the PRD stated — embeddings dropped at cold tier for storage economics; a quantized cold vector index is an explicit v0.3 research question, not a v0.2 commitment.

### `mcpfier` and `vectoria` reuse scope

**Question:** Does A-1 (MCP server) depend on `mcpfier`, and does Block B depend on `vectoria` as a real crate dependency?

**Decision:** Neither is a dependency. `mcpfier` is a separate Go project, too broad/heavy for what Pierre's MCP surface actually needs — implement the MCP server directly in Pierre (Rust) instead of pulling it in. `vectoria` gets mined for techniques, smart ideas, and implementation patterns (RRF fusion approach, identifier-aware tokenizer, embedding pipeline design) — not a crate dependency, since it's a full standalone product tuned for a different domain (ecommerce catalogs: CTR feedback, multi-index namespaces) that doesn't fit logs.

**Why:** Avoids coupling Pierre's release cadence and dependency surface to two unrelated products (one a different language entirely). Matches the "keep dependencies justified" discipline already established in this codebase (SPEC.md's dependency-audit constraints).

### Success criteria bindingness

**Question:** Are the PRD's 6 numeric M4 gates (10×–20× cost, 30-day zero-intervention soak, <5min setup, agent-loop-within-one-turn, ≥25% semantic recall lift, cold-tier p95 <3s) locked commitments or directional targets?

**Decision:** Locked: the 30-day zero-intervention soak (binary outcome, explicitly a marketing asset per the PRD) and cold-tier p95 <3s (a real user-facing latency promise). Directional: cost multiplier range, semantic recall lift, and setup time — real numbers worth measuring, not release-blocking if missed by a small margin (22× instead of 20×, 23% instead of 25%).

**Why:** Distinguishes objectively binary/user-facing promises from continuous metrics where a near-miss shouldn't stall shipping.

### Block D-1 ("no knobs that can be set wrong") vs. existing pierre.toml tunables

**Question:** Pierre's current `pierre.toml` already exposes explicit tunables (flush intervals, cohort windows, bucket durations, TTLs — the same config `RUNBOOK.md` walks through for the demo deployment). D-1 wants self-tuning from observed volume/disk instead. Remove/deprecate the existing tunables in favor of a smaller policy-only config, or keep them as overrides with self-tuning only supplying defaults when unset?

**Decision:** Keep them as explicit overrides. Self-tuning supplies sensible defaults when a value is unset; an operator who wants to override still can.

**Why:** A hard removal breaks every existing deployment (including the just-completed real demo deployment this config was written for) and turns a design improvement into a breaking migration. Matches D-1's actual spirit — "no knob a novice has to get right," not "no knob at all for an operator who knows what they want."

**Status: done.**

## M1 execution — scope calls made while implementing (2026-07-25)

Three real implementation decisions surfaced while actually building ES `_bulk`, syslog RFC5424, and OTLP logs (Block C, greenlit in the PRD grill-me session above). None were worth a separate interview round — each was a bounded, reversible engineering call, not a product-direction question — but are recorded here since they're the kind of thing worth knowing before touching this code again.

**OTLP gRPC port default is 4327, not the OTel-conventional 4317.** Pierre's native protocol already claims 4317 in the same process (`native_listen_addr`), so OTLP gRPC couldn't reuse it — two listeners can't bind the same port in one process. Not a real-world problem: OTel exporters always require an explicit endpoint configuration anyway (no SDK "magically" finds 4317 without being told), so an operator pointing a real OTel SDK/Collector at Pierre sets the endpoint either way.

**OTLP/JSON is a deliberate scope cut, not an oversight.** Only `POST /v1/logs` with `Content-Type: application/x-protobuf` is accepted; a JSON body gets a clear 415. OTLP/JSON needs its own field-casing (lowerCamelCase) and bytes-encoding (base64) rules distinct from what `prost`'s generated types carry via `serde_json`'s default derive — real additional work for the less common of OTLP/HTTP's two content types (most OTLP/HTTP exporters default to protobuf). Revisit if a real user needs it.

**tonic 0.14 forced a project-wide `prost` 0.13→0.14 bump.** tonic's gRPC codegen (`tonic-prost-build`) requires `prost`/`prost-types` 0.14; Pierre's existing Loki proto compilation (`prost-build`, unrelated to gRPC) got bumped alongside it for consistency. Verified this doesn't conflict with edgestore's own dependency resolution (`cargo build` succeeded end-to-end with no version conflicts) before writing any new code on top of it.

**Status: done — es_bulk, syslog, otlp all shipped and verified against real running binaries (real gRPC client, real curl protobuf POST, real UDP/TCP sockets, real NDJSON).**

## M2 execution — MCP server, edgestore 1.5.0 async-wrapping gap (2026-07-25)

**Bumped Pierre to edgestore/edgestore-tokio/edgestore-repl/edgestore-tier 1.5.0 before building the MCP server.** The upstream "Pierre guide" documents (pasted into this session) confirmed ENG-7 (query-cost stats), ENG-9 (per-query scan budgets), ENG-11 (BM25 snippet extraction), and ENG-12 (vector storage) shipped as real engine APIs at 1.5.0 — while ENG-8 (RRF fusion) and ENG-10 (template extraction) are deliberately caller-side patterns, not engine APIs, and Pierre already implements its own template extraction (`src/template.rs`).

**Finding: none of ENG-7/9/11/12 are usable from the MCP server yet, despite being real 1.5.0 engine APIs.** `edgestore-tokio::AsyncTieredEngine` — the only handle `Storage` (`src/storage.rs`) ever talks to — wraps just the pre-1.5.0 method set (`get`/`put`/`range`/`prefix`/`index_text`/`search_text`/etc.); it has zero async wrappers for `*_with_stats`, `*_budgeted`, `search_text_with_snippets`, or the vector read/write APIs. Verified by reading the actual local cargo registry source for `edgestore-tokio-1.5.0`, not assumed from a changelog. This is the same async-wrapping gap every previous edgestore feature needed closed before Pierre could use it (`put_with_ttl`, `index_text`, local-segment read-through — each needed its own `edgestore-tokio` addition first).

**Decision:** Build the MCP server scoped to what's already async-wrapped (`search_text`, `range`/`prefix`, `get`), and have each tool be explicit in its own response about what it can't yet report, rather than blocking the whole feature on the gap or faking the missing numbers. `search_logs`'s `considered` field is documented as "an honest floor, not a real byte count" (no ENG-7 query-cost stats); `get_context`'s context window is a fixed ±10min rather than adaptive; message truncation is plain truncation, not ENG-11's real position-based snippet extraction.

**Why:** Per the standing rule (this file, PRD grill-me session) — edgestore stays hands-off; report a bug/feature gap as text to its owner, never touch edgestore's repo or fixate on unblocking it here. The gap doesn't block a real, useful MCP server today; it only means some PRD A-1 response fields are honest placeholders until `edgestore-tokio` grows the corresponding wrappers.

**Reported (text only, to edgestore's owner, not written to any repo):** `edgestore-tokio::AsyncTieredEngine` at 1.5.0 needs async wrappers for `*_with_stats`, `*_budgeted`, `search_text_with_snippets`, and the vector read/write APIs before Pierre (or any tokio-based consumer) can use ENG-7/9/11/12.

**Status: done — MCP server (`search_logs`, `get_context`, `list_streams`, `aggregate`, `find_anomalies`) shipped on `rmcp`, Streamable HTTP transport at `/mcp`, verified via 6 integration tests using a real `rmcp` client and against a real running binary (curl-driven MCP handshake + real `_bulk`-ingested data round-tripping through `search_logs`).

## Pre-production hardening pass (2026-07-25/26)

Requested directly: "fix all fragilities and potential bugs/deadlocks. pierre is not in full production yet, this is the time." A full read of every `src/` file (not just the current branch's diff), looking specifically for panics, overflow, races/deadlocks, and auth gaps reachable from untrusted input. No lock-based concurrency exists anywhere in Pierre's own code (`grep -rn "Mutex\|RwLock" src/` — empty; every worker uses a bounded `mpsc` channel plus atomics instead), so the deadlock risk was structurally already low; the real findings were panics and an auth bypass.

**Fixed — `Storage`'s key encoding broke ordering for negative timestamps (`src/storage.rs`).** `encode_key` cast `timestamp_ns as u64` directly; a negative timestamp wraps into the *upper* half of `u64`, sorting after every non-negative timestamp instead of before it. Reachable from `GET /query/logs?start=<negative>` (parsed straight from the query string, no lower bound) and from any MCP tool taking `start_ns`/`end_ns` — a negative `start` silently returned wrong (usually empty) results instead of an error. Fixed at the source with the standard sign-bit-flip bias, not by clamping every call site — this is a genuine on-disk key-format change, acceptable now specifically because there's no real production data yet to migrate. Proven with a test spanning `i64::MIN` to `i64::MAX`, and against a real running binary (a query with `start_ns = -1e12` now correctly returns the real record in range instead of nothing).

**Fixed — a real, reproducible crash/hang in `src/mcp.rs`'s `hex_decode`.** The original implementation sliced the caller-supplied `doc_id` at fixed byte offsets (`&s[i..i+2]`); a `doc_id` containing a multi-byte UTF-8 character at the wrong position (e.g. `"€a"`, 4 bytes, passes the even-length check) panics with "byte index N is not a char boundary." Confirmed with a standalone repro, then confirmed the consequence is worse than a caught panic: calling `get_context` with that `doc_id` through a real `rmcp` client *hung the test indefinitely* rather than returning an error — `rmcp`'s `StreamableHttpService` dispatches each tool call through its own internal task, so the panic never reaches anything that could turn it into a clean response. Fixed by rewriting `hex_decode` to work on raw bytes (`s.as_bytes()`, casting each `u8` to `char` for `to_digit(16)`) instead of `str` slicing — sidesteps UTF-8 boundaries entirely, a non-hex byte just fails to parse like any other invalid input. Verified two ways: a regression test asserting the malformed-input call returns a clean `Result` and the connection stays usable afterward, and a real curl-driven MCP call against the running binary.

**Fixed — OTLP gRPC and OTLP/HTTP silently ignored `pierre.toml`'s `auth_tokens` entirely.** Every other HTTP/gRPC ingest surface (Loki, ES `_bulk`, MCP) wires in the shared `AuthTokens` check; OTLP's `serve_grpc`/`router`/`serve_http` never took an `AuthTokens` parameter at all — an operator who configured a token expecting it to lock down *every* surface had this one wide open regardless, with no documented rationale (unlike the native/syslog protocols, which are deliberately, explicitly unauthenticated). Fixed: added a `tonic::service::Interceptor` for gRPC (checks the same `authorization` value out of gRPC metadata) and the standard `crate::auth::layer` for HTTP, both built on a new shared `AuthTokens::check_header` so the "off when unconfigured, else require a matching bearer token" rule has exactly one implementation across every transport shape. Verified with 2 new integration tests (`tests/otlp_ingest.rs`) proving a missing/wrong token is rejected and a correct one is accepted, on both transports, against a real running binary.

**Added — `tower_http::catch_panic::CatchPanicLayer` in the shared `auth::layer`.** Without it, a panic inside a plain axum handler (Loki, ES `_bulk`, OTLP/HTTP, `/query/*`) aborts the whole HTTP/1.1 connection instead of returning a clean 500 — defense-in-depth against the *next* untrusted-input bug of the same shape as the `hex_decode` one, not just this one instance. Verified it actually works with a unit test (a deliberately panicking handler wrapped in `auth::layer` comes back as a clean 500). Also verified, empirically, what it does *not* cover: reintroducing the `hex_decode` bug with this layer already in place still hung the test — `rmcp`'s tool-call dispatch happens off the future this layer wraps, so MCP's real protection has to be "don't panic on untrusted input" at the source, not this layer. Documented honestly in `auth.rs` rather than leaving an unverified claim.

**Fixed — several `saturating_sub`/`saturating_add` gaps on client-supplied `i64` nanosecond arithmetic**, each reachable from an unauthenticated-by-default surface and each silently wrapping to a nonsense value in a release build (or panicking in a debug build) rather than erroring cleanly: `aggregate::merged_sketch`'s span computation, `mcp.rs`'s `find_anomalies` span/baseline computation, `mcp.rs`'s `get_context` window (`anchor.timestamp_ns` is itself client-supplied at ingest time, with no bounds check on any ingest surface), and `lokiproto.rs`'s protobuf timestamp decode (`seconds * 1_000_000_000 + nanos` could overflow on an adversarial `seconds`).

**Fixed — one failed BM25 bucket no longer fails an entire multi-bucket `textindex::search`.** Every background worker in this codebase already logs-and-continues on an individual failure (`rollup::worker::merge_up`, `backup::archive_new_segments`); the read-path search loop was the one place that still propagated a single bucket's error with `?`, killing results for the whole requested time range.

**Found, reported here, not fixed — needs a product decision, not a local patch:**
- **No idempotency on partial-batch-commit failure** (native, Loki, ES `_bulk`, OTLP): if record N of a batch fails to commit, everything before N already landed, but the whole batch gets NACKed — a client that retries the whole batch on NACK (a common, reasonable shipper behavior) will duplicate the already-committed records. No idempotency key exists anywhere in Pierre's wire formats to de-duplicate on retry. This is a protocol-design gap across every ingest surface, not a bug in one of them.
- **ES `_bulk`'s `update` action** (`wire_record_from_doc`) reads the document line's top-level object directly as the record's fields. Real Elasticsearch `update` actions wrap the actual document under a `"doc"` key (`{"doc": {...}, "doc_as_upsert": true}`); Pierre would extract `doc`/`doc_as_upsert` as if they were real fields instead of the nested document's real content. Low real-world impact — log shippers overwhelmingly use `index`, not `update` — not fixed pending confirmation it's worth the scope.
- **`backup::archive_new_segments`** updates its in-memory `archived_hashes`/`all_archived` before persisting them to `archived_segments.json`; if the upload succeeds but the metadata write fails, a restart before the next successful write re-archives those segments (wasted upload, not data loss — the grace-period/idempotent-prune design already tolerates this). Left as-is: the alternative (persist-then-upload) risks the opposite failure mode (metadata claims archived, upload never happened), which is worse.

**Status: done for everything marked Fixed above — full suite (`cargo test`, `cargo clippy --all-targets`, `cargo fmt -- --check`) clean, verified against a real running binary for every fix with an external-facing effect.**

## Idiom/duplication follow-up pass (`/ds-quality-gate`, 2026-07-26)

A second pass, scoped to the same "not in production yet" mandate but focused on Rust idioms: `unsafe` (none exists anywhere in Pierre's own code), hand-rolled reimplementations of stdlib/crate functionality, and type-conversion correctness.

**`hex_decode`'s fix above has since been superseded.** The byte-based rewrite cited two entries up ("casting each `u8` to `char`") was itself replaced with the `hex` crate — already resolved in `Cargo.lock` (pulled in transitively via `edgestore-repl`'s AWS SDK chain regardless of the `s3` feature flag, so this is zero net new compiled dependency), and its `decode`/`encode` operate on raw bytes the same way, verified by reading the crate's actual `from_hex`/`decode` source rather than assumed. This also deleted a second, independently hand-rolled hex encoder in `otlpproto.rs` (`mod hex { pub fn encode ... }`) that had drifted to claim it wasn't "a general-purpose encoding need elsewhere in Pierre" — false the moment `mcp.rs` needed the same thing.

**Consolidated two other duplicated primitives**, both real single-source-of-truth violations, not just style: `jiff::Timestamp::now().as_nanosecond() as i64` ("now" in epoch nanoseconds) was copy-pasted in three files, plus a *fourth*, independently-implemented version in `rollup::worker` built on `std::time::SystemTime` instead of `jiff` — now one `crate::clock::now_ns()`. And the sign-bit-flip bias `Storage::encode_key` uses to make negative timestamps sort correctly (see the ordering-bug entry above) existed as a second, undiscovered copy of the *exact same bug* in `rollup::worker::rollup_key` (`bucket_start_ns as u64`, no bias) — unreachable in practice since `bucket_start_ns` only ever comes from real wall-clock reads, but a latent duplicate of a bug that's already been fixed once. Extracted to `crate::keycodec::{order_preserving_ns, decode_order_preserving_ns}`, used by both `Storage` and the rollup worker, with its own round-trip test spanning `i64::MIN` to `i64::MAX`.

**Fixed on request — `otlpproto.rs`'s `time_unix_nano as i64` cast.** Flagged as a soft finding (a same-width cast, never panics/UB, unlike the arithmetic-overflow bugs fixed above) but the user asked for it anyway: a wire `u64` past `i64::MAX` now falls through the existing time/observed-time/now fallback chain via `i64::try_from(...).ok()` instead of silently reinterpreting as a negative timestamp. Two new tests cover single- and both-fields-out-of-range.

**Status: done — full suite/clippy/fmt clean (42 lib tests, up from 38 before this pass).**

## `/ds-security-review` pass (2026-07-26)

Full `src/` scope (no branch diff exists — everything is on `main`). Ruled out clean: no `unsafe` anywhere, no command-injection surface, no regex (no ReDoS), `serde_json`'s recursion limit is on by default so untrusted NDJSON in `es_bulk.rs` can't stack-overflow the parser, every ingest surface already has an explicit size cap, auth now gates every HTTP/gRPC surface (fixed in the hardening pass above).

**Fixed — internal error strings leaked to callers.** `query_api.rs` (4 sites), `es_bulk.rs`, `loki.rs`, `otlp.rs`'s gRPC path, and `mcp.rs`'s shared `internal_err` helper all returned a raw `anyhow`/`Status` error's `Display` text directly in the HTTP/gRPC/MCP response body. `anyhow` chains can carry I/O error text (potentially including `data_dir` filesystem paths) or internal engine error strings. Fixed: each site now logs the real error server-side (`log::warn!`) and returns a fixed `"internal error"` to the caller. Left one message intentionally verbatim — `search_handler`'s "narrow the time range" bail from `textindex::search`'s bucket-count guard, which is client-fixable input guidance, not internal detail.

**Fixed — `RUNBOOK.md` never mentioned TLS.** The runbook's own worked example (step 4) points a real collector's `bearer_token` config at a plain `http://` Pierre URL, and step 3 explicitly covers the non-co-located (`0.0.0.0`) case where that traffic crosses a real network segment — the original "Path to first real-world exposure" decision (above, 2026-07-12) reasoned through auth but never through transport confidentiality, so the token was documented to travel in cleartext on exactly the network ("production or QA... not your own machine") where an attacker is realistically present. Added a note after the auth-token step: put a TLS-terminating reverse proxy in front of Pierre for anything other than loopback-to-loopback.

**Not fixed — bearer-token comparison isn't constant-time** (`auth.rs`, `HashSet::contains`). Theoretical timing side channel, heavily dampened by hashing before lookup, and the auth model is already documented as non-adversarial-grade for small/trusted-network deployments. Noted for completeness, not worth the change.

**Status: done — full suite/clippy/fmt clean, no test asserted on the removed raw error text.**

**Superseded (same day, on request):** fixed anyway. `AuthTokens::is_valid` now compares the provided token against *every* configured token via `[u8]::ct_eq` (`subtle`, already resolved in the dependency tree via the RustCrypto chain — zero net new compiled crate), never short-circuiting on a match, instead of `HashSet::contains`. A length mismatch still short-circuits (token length isn't the secret, only content is — standard practice, not a shortcut back to the leak this closes). Verified: a new unit test covers the full functional matrix (auth off, right token among several, wrong token, prefix/suffix near-misses, empty token, missing header) and a real running binary confirms the right token gets 200 and the wrong one gets 401.
