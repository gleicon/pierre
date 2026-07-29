use std::collections::BTreeMap;
use std::time::Duration;

use pierre::record::WireRecord;
use pierre::storage::Storage;

/// Proves local segment pruning end to end (DECISIONS.md "Local segment pruning"):
/// after archiving, once the configured grace period has passed, the real
/// `backup::spawn` worker deletes the local segment file — and the data stays
/// queryable afterward purely through archived read-through (edgestore 1.1.4).
/// This is the test that makes pruning "safe" a checked fact, not an assumption.
#[tokio::test]
async fn backup_worker_prunes_local_segment_after_grace_period_and_data_stays_queryable() {
    let dir = tempfile::tempdir().unwrap();
    let remote_dir = tempfile::tempdir().unwrap();
    let remote =
        edgestore_repl::FilesystemRemoteStore::new(remote_dir.path().to_path_buf()).unwrap();
    let storage = std::sync::Arc::new(
        Storage::open_with_remote(dir.path(), Box::new(remote))
            .await
            .unwrap(),
    );

    let mut fields = BTreeMap::new();
    fields.insert("level".to_string(), "error".to_string());
    let wire = WireRecord {
        timestamp_ns: 1_000_000_000,
        message: "prune me after archiving".to_string(),
        fields,
    };
    pierre::ingest::commit(&storage, wire, &["level".to_string()], None, None, None)
        .await
        .unwrap();

    // Flush/archive fast so the segment is archived well within the first
    // checkpoint; grace period is set with a wide margin from that checkpoint so
    // there's no timing overlap between "confirmed archived, not yet prunable" and
    // "prunable" under slower/parallel test-run scheduling.
    let _worker = pierre::backup::spawn(
        storage.clone(),
        Duration::from_millis(50),
        Duration::from_millis(50),
        Some(Duration::from_millis(1500)), // grace period
        Duration::from_millis(100),        // prune check interval
    )
    .await;

    // Let flush + archive happen, and confirm the .dat file exists locally first —
    // otherwise a "pruned" assertion later would be vacuously true. Well short of
    // the 1500ms grace period, so this can't race with pruning.
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        count_dat_files(dir.path()),
        1,
        "segment should be flushed and archived, but not yet prunable (grace period not elapsed)"
    );

    // Wait comfortably past the grace period for the prune tick to catch it.
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert_eq!(
        count_dat_files(dir.path()),
        0,
        "local segment file should be deleted once its grace period has elapsed"
    );

    // Data must still be queryable — read-through to the archived copy.
    let results = storage.range(0, 2_000_000_000).await.unwrap();
    assert_eq!(
        results.len(),
        1,
        "pruned segment's data must still be reachable via archived read-through"
    );
    assert_eq!(results[0].message, "prune me after archiving");
}

fn count_dat_files(dir: &std::path::Path) -> usize {
    std::fs::read_dir(dir)
        .map(|entries| {
            entries
                .flatten()
                .filter(|e| e.path().extension().is_some_and(|ext| ext == "dat"))
                .count()
        })
        .unwrap_or(0)
}
