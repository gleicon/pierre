use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use edgestore::types::SegmentMeta;
use edgestore::RemoteStore;
use edgestore_repl::FilesystemRemoteStore;
#[cfg(feature = "s3")]
use edgestore_repl::S3RemoteStore;
use edgestore_tier::ArchivedSegment;
use serde::{Deserialize, Serialize};

use crate::storage::Storage;

/// Config-selected backup destination. **Backup only** — there is no read-through
/// path for range/prefix scans (see SPEC.md #L1), so this does not reduce local disk
/// usage; it exists for durability/DR. `None` means "use `Storage::open`'s local-disk
/// default archive" rather than "no archiving at all" — every Pierre deployment gets
/// real archival with no external dependency unless S3 is explicitly configured.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(tag = "backend", rename_all = "lowercase")]
pub enum BackupConfig {
    #[default]
    None,
    Filesystem {
        path: String,
    },
    S3 {
        bucket: String,
        prefix: Option<String>,
        endpoint: Option<String>,
    },
}

/// Builds the explicit `RemoteStore` for `Storage::open_with_remote`. Not called for
/// `BackupConfig::None` — use plain `Storage::open` instead in that case.
pub async fn build_remote_store(config: BackupConfig) -> anyhow::Result<Box<dyn RemoteStore>> {
    match config {
        BackupConfig::None => Err(anyhow::anyhow!(
            "BackupConfig::None has no explicit remote store; use Storage::open's local-disk default instead"
        )),
        BackupConfig::Filesystem { path } => tokio::task::spawn_blocking(move || {
            std::fs::create_dir_all(&path)?;
            let store = FilesystemRemoteStore::new(std::path::PathBuf::from(path))
                .map_err(|e| anyhow::anyhow!("failed to build FilesystemRemoteStore: {e}"))?;
            Ok(Box::new(store) as Box<dyn RemoteStore>)
        })
        .await
        .map_err(|e| anyhow::anyhow!("remote store init task panicked: {e}"))?,
        #[cfg(feature = "s3")]
        BackupConfig::S3 { bucket, prefix, endpoint } => tokio::task::spawn_blocking(move || {
            // S3RemoteStore::new() must run where blocking is allowed (edgestore-repl
            // 1.1.0+ uses block_in_place internally when called from async context).
            let store = S3RemoteStore::new(&bucket, prefix.as_deref(), endpoint.as_deref())
                .map_err(|e| anyhow::anyhow!("failed to build S3RemoteStore: {e}"))?;
            Ok(Box::new(store) as Box<dyn RemoteStore>)
        })
        .await
        .map_err(|e| anyhow::anyhow!("remote store init task panicked: {e}"))?,
        #[cfg(not(feature = "s3"))]
        BackupConfig::S3 { .. } => {
            Err(anyhow::anyhow!("backup.backend = \"s3\" requires building pierre with `--features s3`"))
        }
    }
}

/// One archived segment's metadata plus when it was archived. The timestamp is
/// Pierre's own bookkeeping — edgestore's `SegmentMeta` has no such field — and is
/// what `pierre::retention::is_due` needs to decide when local pruning is safe.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArchivedSegmentRecord {
    pub meta: SegmentMeta,
    archived_at_unix_secs: i64,
}

impl ArchivedSegmentRecord {
    pub fn new(meta: SegmentMeta) -> Self {
        let archived_at_unix_secs = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs() as i64;
        ArchivedSegmentRecord { meta, archived_at_unix_secs }
    }

    fn archived_at(&self) -> SystemTime {
        UNIX_EPOCH + Duration::from_secs(self.archived_at_unix_secs.max(0) as u64)
    }
}

/// Filename for persisted archived-segment metadata, one JSON array, sibling to the
/// segment files. **Deliberately not stored through `Storage`/the KV engine**: an
/// earlier version persisted this as regular `put_with_ttl` records, which created a
/// self-perpetuating feedback loop — each bookkeeping write became new memtable data,
/// which the next flush turned into a new segment, which archiving then archived
/// (writing more bookkeeping about it), forever, even at total ingest idle. A plain
/// file has no such cycle: writing it doesn't create flushable engine data.
const ARCHIVED_META_FILENAME: &str = "archived_segments.json";

fn archived_meta_path(data_dir: &Path) -> PathBuf {
    data_dir.join(ARCHIVED_META_FILENAME)
}

/// Reads back the persisted archived-segment metadata. Returns an empty list if
/// nothing has been archived yet (not an error).
pub async fn load_archived_meta(data_dir: &Path) -> Vec<ArchivedSegmentRecord> {
    match tokio::fs::read(archived_meta_path(data_dir)).await {
        Ok(bytes) => serde_json::from_slice(&bytes).unwrap_or_default(),
        Err(_) => Vec::new(),
    }
}

async fn save_archived_meta(data_dir: &Path, records: &[ArchivedSegmentRecord]) -> anyhow::Result<()> {
    let bytes = serde_json::to_vec(records)?;
    tokio::fs::write(archived_meta_path(data_dir), bytes).await?;
    Ok(())
}

/// Spawns the tiering worker: periodically flushes the memtable to a new warm segment
/// (FR-8 hot→warm), periodically archives any not-yet-archived segment (FR-8/FR-12
/// backup, not tiering — see SPEC.md #L1), and — if `local_retention` is configured —
/// periodically prunes local segment files once they've been archived for at least
/// that long (DECISIONS.md "Local segment pruning"). Pruning is safe because
/// `Storage::range()`/`prefix()` read through to archived data (edgestore 1.1.4,
/// `tests/archived_range_readthrough.rs` pins this against regression) — a pruned
/// segment's data stays reachable, just no longer local.
///
/// All three concerns share this one worker task/tick loop rather than each getting
/// their own thread (DECISIONS.md "Shared lifecycle/retention pattern") — the
/// due-for-pruning check itself (`pierre::retention::is_due`) is a pure function,
/// independent of any thread; this loop is just where it's driven from.
///
/// The archive pass also races against `storage.flush_notify()` (edgestore 1.3.0's
/// `with_on_segment_flushed`, wired through `AsyncTieredEngine`) alongside its own
/// interval tick, so a segment that just landed — via this worker's own flush_tick
/// or edgestore's auto-flush-on-put once `memtable_max_bytes` is exceeded — gets
/// archived immediately instead of sitting local-only for up to `archive_interval`.
pub async fn spawn(
    storage: Arc<Storage>,
    flush_interval: Duration,
    archive_interval: Duration,
    local_retention: Option<Duration>,
    prune_interval: Duration,
) -> tokio::task::JoinHandle<()> {
    let data_dir = storage.data_dir().to_path_buf();
    let prior = load_archived_meta(&data_dir).await;
    let mut archived_hashes: HashSet<[u8; 32]> = HashSet::new();
    let mut all_archived: Vec<ArchivedSegmentRecord> = Vec::with_capacity(prior.len());
    let mut restored = Vec::with_capacity(prior.len());
    for record in prior {
        if let Ok(hash) = <[u8; 32]>::try_from(record.meta.segment_hash.as_slice()) {
            archived_hashes.insert(hash);
            restored.push(ArchivedSegment { hash, min_key: record.meta.min_key.clone(), max_key: record.meta.max_key.clone() });
            all_archived.push(record);
        }
    }
    if !restored.is_empty() {
        storage.register_archived(restored).await;
    }

    let flush_notify = storage.flush_notify();

    tokio::spawn(async move {
        let mut flush_tick = tokio::time::interval(flush_interval);
        let mut archive_tick = tokio::time::interval(archive_interval);
        let mut prune_tick = tokio::time::interval(prune_interval);
        // First tick of each fires immediately; consume it (see textindex::spawn's
        // identical fix for the same tokio::time::interval gotcha).
        flush_tick.tick().await;
        archive_tick.tick().await;
        prune_tick.tick().await;

        loop {
            tokio::select! {
                _ = flush_tick.tick() => {
                    if let Err(e) = storage.flush_to_segments().await {
                        log::warn!("hot->warm flush failed: {e}");
                    }
                }
                _ = archive_tick.tick() => {
                    if let Err(e) = archive_new_segments(&storage, &data_dir, &mut archived_hashes, &mut all_archived).await {
                        log::warn!("segment archive pass failed: {e}");
                    }
                }
                // Reacts to a real flush the instant it happens rather than
                // waiting for archive_tick — notify_one's stored-permit semantics
                // mean a flush that lands between loop iterations isn't missed.
                _ = flush_notify.notified() => {
                    if let Err(e) = archive_new_segments(&storage, &data_dir, &mut archived_hashes, &mut all_archived).await {
                        log::warn!("segment archive pass failed (flush-triggered): {e}");
                    }
                }
                _ = prune_tick.tick() => {
                    if let Some(grace_period) = local_retention {
                        prune_local_segments(&storage, &all_archived, grace_period).await;
                    }
                }
            }
        }
    })
}

async fn archive_new_segments(
    storage: &Storage,
    data_dir: &Path,
    archived_hashes: &mut HashSet<[u8; 32]>,
    all_archived: &mut Vec<ArchivedSegmentRecord>,
) -> anyhow::Result<()> {
    let metas = storage.list_segment_metas().await;
    let new_metas: Vec<SegmentMeta> = metas
        .into_iter()
        .filter(|m| {
            <[u8; 32]>::try_from(m.segment_hash.as_slice())
                .map(|hash| !archived_hashes.contains(&hash))
                .unwrap_or(false)
        })
        .collect();
    if new_metas.is_empty() {
        return Ok(());
    }

    storage.archive_segments(new_metas.clone()).await?;

    for meta in &new_metas {
        if let Ok(hash) = <[u8; 32]>::try_from(meta.segment_hash.as_slice()) {
            archived_hashes.insert(hash);
        }
    }
    all_archived.extend(new_metas.into_iter().map(ArchivedSegmentRecord::new));
    save_archived_meta(data_dir, all_archived).await?;
    Ok(())
}

/// Deletes local files for segments that have been archived for at least
/// `grace_period`. Idempotent and safe to call repeatedly — pruning an
/// already-pruned segment is a no-op (`Engine::prune_local_segment`), so there's no
/// need to track a separate "already pruned" flag. Errors on an individual segment
/// are logged and skipped, not fatal to the pass.
async fn prune_local_segments(storage: &Storage, all_archived: &[ArchivedSegmentRecord], grace_period: Duration) {
    for record in all_archived {
        if !crate::retention::is_due(record.archived_at(), grace_period) {
            continue;
        }
        if let Err(e) = storage.prune_local_segment(record.meta.segment_id).await {
            log::warn!("failed to prune local segment {}: {e}", record.meta.segment_id);
        }
    }
}
