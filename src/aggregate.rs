use crate::rollup::sketch::FieldSketch;
use crate::rollup::worker::{
    decode_rollup_key, ROLLUP_DAY_NS, ROLLUP_HOUR_NS, ROLLUP_MINUTE_NS, ROLLUP_MONTH_NS,
};
use crate::storage::Storage;

const HOUR_NS: i64 = 3_600_000_000_000;
const DAY_NS: i64 = 24 * HOUR_NS;
const MONTH_NS: i64 = 31 * DAY_NS;

/// Picks the single rollup tier whose granularity best matches the query span, then
/// merges every persisted bucket for `field` in that tier overlapping `[start_ns,
/// end_ns)`. Reading exactly one tier — never multiple — avoids double-counting
/// between a tier and its own already-merged parent (a minute bucket and the hour
/// bucket it was folded into can coexist until the minute tier's TTL expires it).
/// FR-17: served from pre-computed sketches only, never a raw rescan.
pub async fn merged_sketch(
    storage: &Storage,
    field: &str,
    start_ns: i64,
    end_ns: i64,
) -> anyhow::Result<Option<FieldSketch>> {
    let span = end_ns - start_ns;
    let ns: &[u8] = if span <= HOUR_NS {
        ROLLUP_MINUTE_NS
    } else if span <= DAY_NS {
        ROLLUP_HOUR_NS
    } else if span <= MONTH_NS {
        ROLLUP_DAY_NS
    } else {
        ROLLUP_MONTH_NS
    };

    let all = storage.prefix(ns, &[]).await?;
    let mut merged: Option<FieldSketch> = None;
    for (key, value) in all {
        let Some((bucket_start_ns, bucket_field)) = decode_rollup_key(&key) else {
            continue;
        };
        if bucket_field != field || bucket_start_ns < start_ns || bucket_start_ns >= end_ns {
            continue;
        }
        let sketch = FieldSketch::from_bytes(&value)?;
        match &mut merged {
            Some(existing) => existing.merge_from(&sketch)?,
            None => merged = Some(sketch),
        }
    }
    Ok(merged)
}
