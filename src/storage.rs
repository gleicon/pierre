use std::collections::HashMap;
use std::path::{Path, PathBuf};

use edgestore::types::SegmentMeta;
use edgestore::{EdgestoreConfig, RemoteStore, TextSearchResult};
use edgestore_repl::FilesystemRemoteStore;
use edgestore_tier::ArchivedSegment;
use edgestore_tokio::AsyncTieredEngine;

use crate::record::Record;

const LOGS_NS: &[u8] = b"logs";

/// Pierre's storage layer: encodes time into the key so a range scan is a time-range
/// query, and holds the single edgestore handle everything else in Pierre goes through.
///
/// Always tiered (`AsyncTieredEngine`), even without an explicit backup config —
/// `open()` defaults to a local-disk archive under `{data_dir}/_archive`, so every
/// deployment gets real archival with no external dependency; `open_with_remote`
/// lets the caller (main.rs, per `pierre.toml`) swap in S3 instead.
pub struct Storage {
    engine: AsyncTieredEngine,
    data_dir: PathBuf,
}

/// edgestore's own default (`EdgestoreConfig::new`) — kept as a named constant so
/// `open`'s local-disk-archive default and `open_with_cohort_window`'s explicit
/// default agree without repeating the magic number.
const DEFAULT_COHORT_WINDOW_SECS: u64 = 3600;

impl Storage {
    pub async fn open(data_dir: &Path) -> anyhow::Result<Self> {
        std::fs::create_dir_all(data_dir)?;
        let archive_dir = data_dir.join("_archive");
        std::fs::create_dir_all(&archive_dir)?;
        let remote = FilesystemRemoteStore::new(archive_dir)
            .map_err(|e| anyhow::anyhow!("failed to build default local archive store: {e}"))?;
        Self::open_with_remote(data_dir, Box::new(remote)).await
    }

    /// Same as `open`, but with an explicit `RemoteStore` backend (e.g. S3 per config)
    /// instead of the local-disk default.
    pub async fn open_with_remote(
        data_dir: &Path,
        remote: Box<dyn RemoteStore>,
    ) -> anyhow::Result<Self> {
        Self::open_with_options(data_dir, remote, DEFAULT_COHORT_WINDOW_SECS, false).await
    }

    /// Full control over construction — `cohort_window_secs` sizes the deathtime-cohort
    /// compaction bucket (FR-21): how finely TTL-expiry granularity is bucketed for
    /// compaction. Smaller windows expire data sooner after TTL but create more, smaller
    /// cohorts; edgestore's own default is 1 hour.
    ///
    /// `strip_text_index_after_archive` enables edgestore's `TieredEngine::
    /// with_text_stripping`: once a segment is archived, its embedded BM25 records
    /// are stripped from the *local* copy (the archived copy is untouched) to reclaim
    /// disk without waiting for the whole segment to qualify for local pruning. Off
    /// by default — the trade-off (documented on `with_text_stripping`) is that a
    /// stripped segment's search history can't be reconstructed by crash-recovery
    /// rebuild if the merged index sidecar hadn't already flushed past it.
    pub async fn open_with_options(
        data_dir: &Path,
        remote: Box<dyn RemoteStore>,
        cohort_window_secs: u64,
        strip_text_index_after_archive: bool,
    ) -> anyhow::Result<Self> {
        std::fs::create_dir_all(data_dir)?;
        let mut config = EdgestoreConfig::new(data_dir);
        config.cohort_window_secs = cohort_window_secs;
        let engine = AsyncTieredEngine::open_with_options(
            config,
            remote,
            false,
            strip_text_index_after_archive,
        )
        .await?;
        Ok(Storage {
            engine,
            data_dir: data_dir.to_path_buf(),
        })
    }

    /// Commits a record durably. Key is `timestamp_ns (8 bytes BE) || random (8 bytes)`
    /// — UUIDv7-inspired (leading sortable timestamp + trailing per-record randomness),
    /// but keeping Pierre's own nanosecond timestamp precision rather than UUIDv7's
    /// 48-bit millisecond field, since log events legitimately need correct relative
    /// ordering within the same millisecond. The trailing 8 bytes are freshly random
    /// per record (not a counter seeded once at startup) — each record gets
    /// independent collision odds, rather than sharing one process-lifetime seed.
    /// A monotonic counter reset to 0 on every restart was the previous design; that
    /// meant two records at the same timestamp_ns with a similarly-low counter value
    /// across different process lifetimes (e.g. a backfill replaying identical
    /// recorded timestamps) could collide and silently overwrite each other under
    /// normal KV last-write-wins semantics.
    ///
    /// Byte order still matches chronological order (timestamp_ns leads), so a
    /// byte-range scan over the timestamp prefix is still a time-range query (FR-9).
    /// Returns the generated key so callers (e.g. the BM25 indexer) can correlate a
    /// search hit back to this exact record.
    pub async fn commit(&self, record: &Record) -> anyhow::Result<Vec<u8>> {
        let key = encode_key(record.timestamp_ns, rand::random::<u64>());
        let value = serde_json::to_vec(record)?;
        self.engine.put(LOGS_NS, &key, &value).await?;
        Ok(key)
    }

    /// Returns all records with `start_ns <= timestamp_ns < end_ns`, in chronological order.
    /// Reads through to archived segments overlapping the range as of edgestore 1.1.4
    /// (`TieredEngine::prefix()` merges local data with an ephemeral, no-import
    /// `ImmutableEngine` view of overlapping archived segments — see SPEC.md #L1 and
    /// `tests/archived_range_readthrough.rs`, which pins this behavior against
    /// regression). Requires the archived-segment registry to be populated first
    /// (`register_archived`/`archive_segments`) — read-through only knows about
    /// segments it's been told are archived, it doesn't discover them on its own.
    pub async fn range(&self, start_ns: i64, end_ns: i64) -> anyhow::Result<Vec<Record>> {
        let all = self.engine.prefix(LOGS_NS, &[]).await?;
        let start_key = encode_key(start_ns, 0);
        let end_key = encode_key(end_ns, 0);
        let mut out = Vec::new();
        for (key, value) in all {
            if key >= start_key && key < end_key {
                out.push(serde_json::from_slice::<Record>(&value)?);
            }
        }
        out.sort_by_key(|r| r.timestamp_ns);
        Ok(out)
    }

    /// Same as `range()`, but keeps each record's own storage key alongside it — the
    /// MCP server's `search_logs`/`get_context` tools (`src/mcp.rs`) need a durable
    /// `doc_id` to hand back to the caller and later resolve again, which plain
    /// `range()`/`query::select()` never needed (their callers only ever wanted the
    /// record content). A separate narrow method rather than widening `range()`'s
    /// existing return shape, which `/query/logs` and other callers already depend on.
    pub async fn range_with_keys(
        &self,
        start_ns: i64,
        end_ns: i64,
    ) -> anyhow::Result<Vec<(Vec<u8>, Record)>> {
        let all = self.engine.prefix(LOGS_NS, &[]).await?;
        let start_key = encode_key(start_ns, 0);
        let end_key = encode_key(end_ns, 0);
        let mut out = Vec::new();
        for (key, value) in all {
            if key >= start_key && key < end_key {
                out.push((key.clone(), serde_json::from_slice::<Record>(&value)?));
            }
        }
        out.sort_by_key(|(_, r)| r.timestamp_ns);
        Ok(out)
    }

    /// Fetches a single log record by its exact storage key — used to resolve a BM25
    /// search hit's `doc_id` (which is this same key) back to the full record.
    pub async fn get_record(&self, key: &[u8]) -> anyhow::Result<Option<Record>> {
        match self.engine.get(LOGS_NS, key).await? {
            Some(bytes) => Ok(Some(serde_json::from_slice(&bytes)?)),
            None => Ok(None),
        }
    }

    pub async fn flush(&self) -> anyhow::Result<()> {
        self.engine.flush().await?;
        Ok(())
    }

    /// Generic namespaced write with TTL — used by rollup tiers (FR-21) to persist
    /// via write-time cohorting instead of a periodic scan-and-delete job.
    pub async fn put_with_ttl(
        &self,
        ns: &[u8],
        key: &[u8],
        value: &[u8],
        ttl_secs: u32,
    ) -> anyhow::Result<()> {
        self.engine.put_with_ttl(ns, key, value, ttl_secs).await?;
        Ok(())
    }

    /// Generic namespaced read, used by rollup/textindex to read back persisted state.
    /// Note: unlike plain `AsyncEngine::get`, this reads through to the remote archive
    /// on a local miss (the one place `AsyncTieredEngine` diverges from local-only).
    pub async fn get(&self, ns: &[u8], key: &[u8]) -> anyhow::Result<Option<Vec<u8>>> {
        Ok(self.engine.get(ns, key).await?)
    }

    /// Generic namespaced prefix scan, used by rollup tier-merge reads. Reads through
    /// to archived segments overlapping the prefix range (see `range()`'s doc).
    pub async fn prefix(
        &self,
        ns: &[u8],
        prefix: &[u8],
    ) -> anyhow::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        Ok(self.engine.prefix(ns, prefix).await?)
    }

    /// Flushes the memtable to a new immutable, BM25-indexed segment file (hot→warm, FR-8).
    pub async fn flush_to_segments(&self) -> anyhow::Result<SegmentMeta> {
        Ok(self.engine.flush_to_segments().await?)
    }

    /// Local segment metadata (id, hash, key bounds) — the input `archive_segments` needs.
    pub async fn list_segment_metas(&self) -> Vec<SegmentMeta> {
        self.engine.list_segment_metas().await
    }

    /// Runs one deathtime-cohort compaction pass (FR-21): fully-expired cohorts are
    /// dropped outright; partially-expired cohorts are rewritten with only live
    /// records relocated — a segment with any still-live record is never dropped
    /// wholesale, only trimmed.
    pub async fn compact_once(&self) -> anyhow::Result<edgestore::CompactionStats> {
        Ok(self.engine.compact_once().await?)
    }

    /// Removes one segment from local storage only (files + manifest entry) — does
    /// not touch the remote archive. Safe only once the segment is durably archived
    /// (see `pierre::retention`, which gates this on a grace period past archiving)
    /// and relies on `range()`/`prefix()`'s archived read-through to keep queries
    /// correct afterward (`tests/archived_range_readthrough.rs` pins that behavior).
    pub async fn prune_local_segment(&self, segment_id: u64) -> anyhow::Result<()> {
        Ok(self.engine.prune_local_segment(segment_id).await?)
    }

    /// Uploads the given local segments to the configured remote store and records
    /// them as archived (backup/DR — see SPEC.md #L1, this does not prune local files
    /// or reduce local disk usage).
    pub async fn archive_segments(&self, metas: Vec<SegmentMeta>) -> anyhow::Result<()> {
        Ok(self.engine.archive_segments(metas).await?)
    }

    /// Currently-known archived segments (this session plus anything re-registered
    /// on open from persisted metadata).
    pub async fn archived_segments(&self) -> Vec<ArchivedSegment> {
        self.engine.archived_segments().await
    }

    /// Re-registers previously-archived segments (e.g. read back from Pierre's own
    /// persisted metadata namespace on startup) without re-uploading them.
    pub async fn register_archived(&self, segments: Vec<ArchivedSegment>) {
        self.engine.register_archived(segments).await
    }

    /// Indexes a document for BM25 full-text search under the given namespace (FR-10).
    pub async fn index_text(&self, ns: &[u8], key: &[u8], text: &str) -> anyhow::Result<()> {
        self.engine
            .index_text(ns, key, text, HashMap::new())
            .await?;
        Ok(())
    }

    /// Searches a single BM25 namespace for the top-`k` matches (FR-14). Local only.
    pub async fn search_text(
        &self,
        ns: &[u8],
        query: &str,
        k: usize,
    ) -> anyhow::Result<Vec<TextSearchResult>> {
        Ok(self.engine.search_text(ns, query, k).await?)
    }

    /// The local data directory — e.g. for locating segment files directly, or the
    /// `backup` module's archived-segment metadata sidecar file.
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    /// A handle that resolves the instant any local segment is flushed (explicit
    /// `flush_to_segments` or edgestore's own auto-flush-on-put). The `backup`
    /// worker races this against its archive-interval tick so a freshly-flushed
    /// segment gets archived immediately instead of waiting up to the full
    /// interval — see `backup::spawn`.
    pub fn flush_notify(&self) -> std::sync::Arc<tokio::sync::Notify> {
        self.engine.flush_notify()
    }
}

/// `suffix` is either fresh per-record randomness (real record keys, see `commit()`)
/// or a literal `0` (range-query boundary construction, see `range()` — the minimum
/// possible key at that timestamp, not a real record's key).
///
/// See `crate::keycodec` for why the timestamp goes through
/// `order_preserving_ns` rather than a plain `as u64` cast — `/query/logs`
/// takes `start`/`end` straight from a client-supplied query param, no lower
/// bound, and a negative `start` used to silently return wrong results
/// instead of an error.
fn encode_key(timestamp_ns: i64, suffix: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(16);
    key.extend_from_slice(&crate::keycodec::order_preserving_ns(timestamp_ns).to_be_bytes());
    key.extend_from_slice(&suffix.to_be_bytes());
    key
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_order_matches_time_order() {
        let a = encode_key(100, 0);
        let b = encode_key(200, 0);
        assert!(a < b, "later timestamp must sort after earlier one");
    }

    #[test]
    fn key_order_disambiguates_same_timestamp_by_seq() {
        let a = encode_key(100, 0);
        let b = encode_key(100, 1);
        assert!(a < b);
    }

    #[test]
    fn key_order_holds_across_negative_and_positive_timestamps() {
        // A plain `timestamp_ns as u64` cast (the pre-fix encoding) wraps negative
        // values into the *upper* half of u64, sorting them after every positive
        // timestamp — inverted. The bias-flip must keep every case in real order.
        let min = encode_key(i64::MIN, 0);
        let negative = encode_key(-1_000_000_000, 0);
        let zero = encode_key(0, 0);
        let positive = encode_key(1_000_000_000, 0);
        let max = encode_key(i64::MAX, 0);
        assert!(min < negative);
        assert!(negative < zero);
        assert!(zero < positive);
        assert!(positive < max);
    }

    #[test]
    fn random_suffixes_are_practically_always_distinct() {
        // Not a formal guarantee (still probabilistic) — a sanity check that
        // rand::random::<u64>() isn't somehow degenerate (e.g. always zero).
        let suffixes: std::collections::HashSet<u64> =
            (0..100_000).map(|_| rand::random::<u64>()).collect();
        assert_eq!(
            suffixes.len(),
            100_000,
            "100K random u64s should not collide with each other"
        );
    }
}
