use serde::Deserialize;
use std::path::Path;

use crate::backup::BackupConfig;
use crate::rollup::RollupKind;

#[derive(Debug, Clone, Deserialize)]
pub struct RollupDef {
    pub field: String,
    pub kind: RollupKind,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct PierreConfig {
    pub data_dir: String,
    pub native_listen_addr: String,
    pub loki_listen_addr: String,
    pub query_listen_addr: String,
    pub fields: Vec<String>,
    /// Declarative rollup definitions (FR-22) — add a field/kind pair, no code change.
    pub rollup: Vec<RollupDef>,
    /// TTL for persisted minute buckets, seconds.
    pub rollup_minute_ttl_secs: u32,
    /// Backup destination for warm segments (FR-8/FR-12) — backup/DR only, not tiering.
    pub backup: BackupConfig,
    /// How often the memtable flushes to a new warm segment, seconds (hot→warm, FR-8).
    pub hot_to_warm_flush_interval_secs: u64,
    /// How often new warm segments are archived to the backup destination, seconds.
    pub archive_interval_secs: u64,
    /// Deathtime-cohort compaction bucket size, seconds (FR-21). Smaller windows
    /// expire TTL'd data sooner after it dies, in exchange for more/smaller cohorts.
    pub cohort_window_secs: u64,
    /// BM25 index bucket window, seconds — bounds each namespace's corpus size (FR-11).
    /// Default is 300s (5 min), not edgestore's inherited 1-hour assumption: measured
    /// indexing throughput collapses from ~10K docs/sec at 10K documents in one
    /// namespace to ~700 docs/sec at 100K (SPEC.md #L2) because the in-memory merged
    /// index isn't cleared by `flush()`, only persisted — it keeps growing for the
    /// whole bucket lifetime. A short bucket window bounds how large that in-memory
    /// index can get before rotating to a fresh one.
    pub textindex_bucket_duration_secs: u64,
    /// How often the BM25 index is flushed to disk, seconds — bounds crash-recovery
    /// rebuild cost (FR-23), default matches the ≤5s target from SPEC.md.
    pub textindex_flush_interval_secs: u64,
    /// Static bearer tokens checked against `Authorization: Bearer <token>` on every
    /// HTTP endpoint (Loki push, Loki query_range, native query API). Empty (the
    /// default) means auth is off entirely — matches every deployment before this
    /// existed. Deliberately not federated auth: no rotation, no per-client scoping,
    /// no expiry — fine for small/trusted-network/test deployments (DECISIONS.md).
    pub auth_tokens: Vec<String>,
    /// Grace period after archiving before a segment's local copy is deleted, seconds.
    /// `None` (the default) disables local pruning entirely — Pierre keeps archiving
    /// forever without ever reclaiming local disk, matching every deployment before
    /// this existed. Safe to enable because `Storage::range()`/`prefix()` read
    /// through to archived data (DECISIONS.md "Local segment pruning").
    pub local_retention_secs: Option<u64>,
    /// How often to check for segments due for local pruning, seconds.
    pub local_prune_interval_secs: u64,
    /// Strip a segment's embedded BM25/full-text records from the *local* copy
    /// immediately after it's archived (the archived copy is untouched) — a finer-
    /// grained disk reclaim than waiting for the whole segment to qualify for local
    /// pruning. Off by default.
    ///
    /// KNOWN GAP (found via `tests/text_index_stripping.rs`, not assumed): stripping
    /// correctly rewrites the segment file, but edgestore's WAL rotation is purely
    /// size-based (64MB default) and unrelated to flush/strip events, so the
    /// *original* WAL entry for a stripped write is usually still present. A restart
    /// replays it straight back into the memtable via `recover_from_wal`, silently
    /// undoing the strip. This is the opposite of what `with_text_stripping`'s own
    /// doc promises. Do not enable this expecting durable disk savings until the
    /// upstream WAL/strip interaction is fixed.
    pub strip_text_index_after_archive: bool,
}

impl Default for PierreConfig {
    fn default() -> Self {
        PierreConfig {
            data_dir: "./pierre-data".to_string(),
            native_listen_addr: "127.0.0.1:4317".to_string(),
            loki_listen_addr: "127.0.0.1:3100".to_string(),
            query_listen_addr: "127.0.0.1:3101".to_string(),
            rollup: vec![
                RollupDef { field: "level".to_string(), kind: RollupKind::Exact },
                RollupDef { field: "status".to_string(), kind: RollupKind::Exact },
            ],
            rollup_minute_ttl_secs: 3600,
            backup: BackupConfig::None,
            hot_to_warm_flush_interval_secs: 300,
            archive_interval_secs: 300,
            cohort_window_secs: 3600,
            textindex_bucket_duration_secs: 300,
            textindex_flush_interval_secs: 5,
            auth_tokens: vec![],
            local_retention_secs: None,
            local_prune_interval_secs: 300,
            strip_text_index_after_archive: false,
            fields: vec![
                "level".to_string(),
                "status".to_string(),
                "trace_id".to_string(),
                "latency_ms".to_string(),
            ],
        }
    }
}

impl PierreConfig {
    /// Loads config from `path`, falling back to defaults if the file doesn't exist.
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        if !path.exists() {
            return Ok(PierreConfig::default());
        }
        let text = std::fs::read_to_string(path)?;
        Ok(toml::from_str(&text)?)
    }
}
