use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;

use super::sketch::FieldSketch;
use super::{RollupKind, RollupSample};
use crate::storage::Storage;

pub const ROLLUP_MINUTE_NS: &[u8] = b"rollup_minute";
pub const ROLLUP_HOUR_NS: &[u8] = b"rollup_hour";
pub const ROLLUP_DAY_NS: &[u8] = b"rollup_day";
pub const ROLLUP_MONTH_NS: &[u8] = b"rollup_month";

/// One sketch per configured field for the currently-live bucket window.
type Bucket = HashMap<String, FieldSketch>;

/// Wall-clock tick interval and TTL for each rollup tier. Durations are independent
/// of granularity naming so tests can use short intervals for all four tiers.
#[derive(Debug, Clone)]
pub struct TierConfig {
    pub minute_duration: Duration,
    pub hour_duration: Duration,
    pub day_duration: Duration,
    pub month_duration: Duration,
    pub minute_ttl_secs: u32,
    pub hour_ttl_secs: u32,
    pub day_ttl_secs: u32,
    pub month_ttl_secs: u32,
}

impl TierConfig {
    /// Production defaults matching the PRD: minute sketches live hours, hour
    /// sketches live weeks, day/month tiers are tiny and kept effectively forever.
    pub fn production_defaults() -> Self {
        TierConfig {
            minute_duration: Duration::from_secs(60),
            hour_duration: Duration::from_secs(3600),
            day_duration: Duration::from_secs(86_400),
            month_duration: Duration::from_secs(30 * 86_400),
            minute_ttl_secs: 3600,             // 1 hour
            hour_ttl_secs: 7 * 86_400,         // 1 week
            day_ttl_secs: 90 * 86_400,         // 90 days
            month_ttl_secs: 10 * 365 * 86_400, // ~10 years — effectively forever
        }
    }
}

pub async fn run(
    mut receiver: mpsc::Receiver<RollupSample>,
    storage: Arc<Storage>,
    field_kinds: HashMap<String, RollupKind>,
    tiers: TierConfig,
) {
    let mut bucket: Bucket = HashMap::new();
    let now = crate::clock::now_ns();
    let mut minute_window_start = now;
    let mut hour_window_start = now;
    let mut day_window_start = now;
    let mut month_window_start = now;

    let mut minute_tick = tokio::time::interval(tiers.minute_duration);
    let mut hour_tick = tokio::time::interval(tiers.hour_duration);
    let mut day_tick = tokio::time::interval(tiers.day_duration);
    let mut month_tick = tokio::time::interval(tiers.month_duration);
    // First tick of each fires immediately; consume it so the interval is real.
    minute_tick.tick().await;
    hour_tick.tick().await;
    day_tick.tick().await;
    month_tick.tick().await;

    loop {
        tokio::select! {
            sample = receiver.recv() => {
                match sample {
                    Some(sample) => {
                        if let Some(kind) = field_kinds.get(&sample.field).copied() {
                            let entry = bucket.entry(sample.field.clone()).or_insert_with(|| FieldSketch::new_for_kind(kind));
                            entry.observe(&sample.value);
                        }
                    }
                    None => return, // all senders dropped
                }
            }
            _ = minute_tick.tick() => {
                if !bucket.is_empty() {
                    if let Err(e) = persist_bucket(&storage, ROLLUP_MINUTE_NS, &bucket, minute_window_start, tiers.minute_ttl_secs).await {
                        log::warn!("failed to persist minute rollup bucket: {e}");
                    }
                }
                bucket = HashMap::new();
                minute_window_start = crate::clock::now_ns();
            }
            _ = hour_tick.tick() => {
                let end = crate::clock::now_ns();
                if let Err(e) = merge_up(&storage, ROLLUP_MINUTE_NS, ROLLUP_HOUR_NS, hour_window_start, end, tiers.hour_ttl_secs).await {
                    log::warn!("failed to merge minute rollups into hour tier: {e}");
                }
                hour_window_start = end;
            }
            _ = day_tick.tick() => {
                let end = crate::clock::now_ns();
                if let Err(e) = merge_up(&storage, ROLLUP_HOUR_NS, ROLLUP_DAY_NS, day_window_start, end, tiers.day_ttl_secs).await {
                    log::warn!("failed to merge hour rollups into day tier: {e}");
                }
                day_window_start = end;
            }
            _ = month_tick.tick() => {
                let end = crate::clock::now_ns();
                if let Err(e) = merge_up(&storage, ROLLUP_DAY_NS, ROLLUP_MONTH_NS, month_window_start, end, tiers.month_ttl_secs).await {
                    log::warn!("failed to merge day rollups into month tier: {e}");
                }
                month_window_start = end;
            }
        }
    }
}

async fn persist_bucket(
    storage: &Storage,
    ns: &[u8],
    bucket: &Bucket,
    bucket_start_ns: i64,
    ttl_secs: u32,
) -> anyhow::Result<()> {
    for (field, sketch) in bucket {
        let key = rollup_key(bucket_start_ns, field);
        let value = sketch.to_bytes()?;
        storage.put_with_ttl(ns, &key, &value, ttl_secs).await?;
    }
    Ok(())
}

/// Merges every already-persisted bucket in `from_ns` whose window falls within
/// `[window_start_ns, window_end_ns)` into one coarser bucket per field in `to_ns`.
/// This folds pre-aggregated sketches (FR-16) — it never rescans raw log lines.
pub async fn merge_up(
    storage: &Storage,
    from_ns: &[u8],
    to_ns: &[u8],
    window_start_ns: i64,
    window_end_ns: i64,
    ttl_secs: u32,
) -> anyhow::Result<()> {
    let all = storage.prefix(from_ns, &[]).await?;
    let mut merged: Bucket = HashMap::new();

    for (key, value) in all {
        let Some((bucket_start_ns, field)) = decode_rollup_key(&key) else {
            continue;
        };
        if bucket_start_ns < window_start_ns || bucket_start_ns >= window_end_ns {
            continue;
        }
        let sketch = FieldSketch::from_bytes(&value)?;
        match merged.get_mut(&field) {
            Some(existing) => existing.merge_from(&sketch)?,
            None => {
                merged.insert(field, sketch);
            }
        }
    }

    for (field, sketch) in &merged {
        let key = rollup_key(window_start_ns, field);
        let bytes = sketch.to_bytes()?;
        storage.put_with_ttl(to_ns, &key, &bytes, ttl_secs).await?;
    }
    Ok(())
}

/// `bucket_start_ns (8 bytes BE, order-preserving — see `crate::keycodec`) ||
/// field name` — sortable by time, scoped by field. `bucket_start_ns` only
/// ever comes from `crate::clock::now_ns()`, always positive in practice, but
/// going through the same encoding `Storage`'s own keys use means there's one
/// implementation of "signed timestamp, sortable as bytes" instead of two that
/// could silently drift apart.
pub fn rollup_key(bucket_start_ns: i64, field: &str) -> Vec<u8> {
    let mut key = crate::keycodec::order_preserving_ns(bucket_start_ns)
        .to_be_bytes()
        .to_vec();
    key.extend_from_slice(field.as_bytes());
    key
}

pub fn decode_rollup_key(key: &[u8]) -> Option<(i64, String)> {
    if key.len() < 8 {
        return None;
    }
    let bucket_start_ns =
        crate::keycodec::decode_order_preserving_ns(u64::from_be_bytes(key[0..8].try_into().ok()?));
    let field = String::from_utf8(key[8..].to_vec()).ok()?;
    Some((bucket_start_ns, field))
}
