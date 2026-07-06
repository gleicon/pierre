use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use pierre::record::WireRecord;
use pierre::rollup::sketch::FieldSketch;
use pierre::rollup::worker::{TierConfig, ROLLUP_HOUR_NS, ROLLUP_MINUTE_NS};
use pierre::rollup::RollupKind;
use pierre::storage::Storage;

const LONG: Duration = Duration::from_secs(3600);

fn tiers_with_minute(minute_duration: Duration) -> TierConfig {
    TierConfig {
        minute_duration,
        hour_duration: LONG,
        day_duration: LONG,
        month_duration: LONG,
        minute_ttl_secs: 3600,
        hour_ttl_secs: 3600,
        day_ttl_secs: 3600,
        month_ttl_secs: 3600,
    }
}

fn exact_counts(bytes: &[u8]) -> BTreeMap<String, u64> {
    match FieldSketch::from_bytes(bytes).unwrap() {
        FieldSketch::Exact(counts) => counts.into_iter().collect(),
        _ => panic!("expected an Exact sketch"),
    }
}

#[tokio::test]
async fn exact_rollup_persists_minute_bucket_and_does_not_block_ingest() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(Storage::open(dir.path()).await.unwrap());
    let allowed_fields = vec!["level".to_string()];
    let field_kinds = HashMap::from([("level".to_string(), RollupKind::Exact)]);

    let bucket_duration = Duration::from_millis(100);
    let rollup = pierre::rollup::spawn(storage.clone(), field_kinds, tiers_with_minute(bucket_duration));

    for level in ["error", "error", "info"] {
        let mut fields = BTreeMap::new();
        fields.insert("level".to_string(), level.to_string());
        let wire = WireRecord {
            timestamp_ns: 1,
            message: "x".to_string(),
            fields,
        };
        pierre::ingest::commit(&storage, wire, &allowed_fields, Some(&rollup), None).await.unwrap();
    }

    assert_eq!(rollup.dropped_count(), 0, "channel has ample capacity; nothing should be dropped");

    // Wait past the first bucket boundary so the worker flushes it.
    tokio::time::sleep(bucket_duration * 3).await;

    let all = storage.prefix(ROLLUP_MINUTE_NS, &[]).await.unwrap();
    assert!(!all.is_empty(), "a minute bucket should have been persisted");

    let counts = exact_counts(&all[0].1);
    assert_eq!(counts.get("error"), Some(&2));
    assert_eq!(counts.get("info"), Some(&1));
}

#[tokio::test]
async fn full_rollup_channel_drops_and_counts_instead_of_blocking() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(Storage::open(dir.path()).await.unwrap());
    let field_kinds = HashMap::from([("level".to_string(), RollupKind::Exact)]);

    // A long bucket duration so the worker never drains while we flood it.
    let rollup = pierre::rollup::spawn(storage, field_kinds, tiers_with_minute(LONG));

    // Channel capacity is 1024; sending far more than that must drop, not block or panic.
    for _ in 0..5000 {
        rollup.record("level".to_string(), "spam".to_string());
    }

    assert!(rollup.dropped_count() > 0, "overflowing the bounded channel must increment the drop counter");
}

#[tokio::test]
async fn minute_buckets_merge_into_hour_tier_algebraically() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(Storage::open(dir.path()).await.unwrap());
    let allowed_fields = vec!["level".to_string()];
    let field_kinds = HashMap::from([("level".to_string(), RollupKind::Exact)]);

    let minute_duration = Duration::from_millis(80);
    let tiers = TierConfig {
        minute_duration,
        hour_duration: Duration::from_millis(250),
        day_duration: LONG,
        month_duration: LONG,
        minute_ttl_secs: 3600,
        hour_ttl_secs: 3600,
        day_ttl_secs: 3600,
        month_ttl_secs: 3600,
    };
    let rollup = pierre::rollup::spawn(storage.clone(), field_kinds, tiers);

    // Two minute buckets' worth of samples, spaced so both flush before the hour tick.
    for _ in 0..2 {
        let mut fields = BTreeMap::new();
        fields.insert("level".to_string(), "error".to_string());
        let wire = WireRecord { timestamp_ns: 1, message: "x".to_string(), fields };
        pierre::ingest::commit(&storage, wire, &allowed_fields, Some(&rollup), None).await.unwrap();
        tokio::time::sleep(minute_duration).await;
    }

    // Wait for the hour tick to fire and merge whatever minute buckets exist so far.
    tokio::time::sleep(Duration::from_millis(300)).await;

    let hour_buckets = storage.prefix(ROLLUP_HOUR_NS, &[]).await.unwrap();
    assert!(!hour_buckets.is_empty(), "hour tier should contain a merged bucket");

    let total: u64 = hour_buckets
        .iter()
        .map(|(_, value)| exact_counts(value).get("error").copied().unwrap_or(0))
        .sum();
    assert_eq!(total, 2, "hour tier must reflect the sum of the merged minute buckets, not a recomputation");
}

#[tokio::test]
async fn topk_rollup_survives_noise_through_the_full_ingest_path() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(Storage::open(dir.path()).await.unwrap());
    let allowed_fields = vec!["path".to_string()];
    let field_kinds = HashMap::from([("path".to_string(), RollupKind::TopK)]);

    let bucket_duration = Duration::from_millis(100);
    let rollup = pierre::rollup::spawn(storage.clone(), field_kinds, tiers_with_minute(bucket_duration));

    // One heavily-hit path plus 50 one-off noise paths — capacity is 20, so the
    // heavy hitter must survive eviction pressure from the noise.
    for _ in 0..200 {
        let mut fields = BTreeMap::new();
        fields.insert("path".to_string(), "/api/orders".to_string());
        let wire = WireRecord { timestamp_ns: 1, message: "x".to_string(), fields };
        pierre::ingest::commit(&storage, wire, &allowed_fields, Some(&rollup), None).await.unwrap();
    }
    for i in 0..50 {
        let mut fields = BTreeMap::new();
        fields.insert("path".to_string(), format!("/noise/{i}"));
        let wire = WireRecord { timestamp_ns: 1, message: "x".to_string(), fields };
        pierre::ingest::commit(&storage, wire, &allowed_fields, Some(&rollup), None).await.unwrap();
    }

    tokio::time::sleep(bucket_duration * 3).await;

    let all = storage.prefix(ROLLUP_MINUTE_NS, &[]).await.unwrap();
    assert!(!all.is_empty());

    let sketch = FieldSketch::from_bytes(&all[0].1).unwrap();
    let top = sketch.top_k(1).unwrap();
    assert_eq!(top[0], ("/api/orders".to_string(), 200), "the true heavy hitter must survive 50 one-off noise paths");
}

#[tokio::test]
async fn ddsketch_rollup_estimates_p99_through_the_full_ingest_path() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(Storage::open(dir.path()).await.unwrap());
    let allowed_fields = vec!["latency_ms".to_string()];
    let field_kinds = HashMap::from([("latency_ms".to_string(), RollupKind::DDSketch)]);

    let bucket_duration = Duration::from_millis(100);
    let rollup = pierre::rollup::spawn(storage.clone(), field_kinds, tiers_with_minute(bucket_duration));

    for i in 1..=1000 {
        let mut fields = BTreeMap::new();
        fields.insert("latency_ms".to_string(), i.to_string());
        let wire = WireRecord { timestamp_ns: 1, message: "x".to_string(), fields };
        pierre::ingest::commit(&storage, wire, &allowed_fields, Some(&rollup), None).await.unwrap();
    }

    tokio::time::sleep(bucket_duration * 3).await;

    let all = storage.prefix(ROLLUP_MINUTE_NS, &[]).await.unwrap();
    assert!(!all.is_empty());

    let sketch = FieldSketch::from_bytes(&all[0].1).unwrap();
    let p99 = sketch.quantile(0.99).unwrap();
    assert!((980.0..1000.0).contains(&p99), "p99 estimate {p99} should be within ~1% of the true p99 (990)");
}

#[tokio::test]
async fn hll_rollup_estimates_distinct_values_through_the_full_ingest_path() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(Storage::open(dir.path()).await.unwrap());
    let allowed_fields = vec!["user_id".to_string()];
    let field_kinds = HashMap::from([("user_id".to_string(), RollupKind::Hll)]);

    let bucket_duration = Duration::from_millis(100);
    let rollup = pierre::rollup::spawn(storage.clone(), field_kinds, tiers_with_minute(bucket_duration));

    // 200 distinct users, each appearing twice — cardinality should read ~200, not 400.
    for i in 0..200 {
        for _ in 0..2 {
            let mut fields = BTreeMap::new();
            fields.insert("user_id".to_string(), format!("user-{i}"));
            let wire = WireRecord { timestamp_ns: 1, message: "x".to_string(), fields };
            pierre::ingest::commit(&storage, wire, &allowed_fields, Some(&rollup), None).await.unwrap();
        }
    }

    tokio::time::sleep(bucket_duration * 3).await;

    let all = storage.prefix(ROLLUP_MINUTE_NS, &[]).await.unwrap();
    assert!(!all.is_empty());

    let mut sketch = FieldSketch::from_bytes(&all[0].1).unwrap();
    let estimate = sketch.hll_estimate().unwrap();
    assert!((170.0..230.0).contains(&estimate), "estimate {estimate} should be close to 200 distinct users, not 400");
}
