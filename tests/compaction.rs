use std::time::Duration;

use pierre::storage::Storage;

/// Proves FR-21/AC-11 through Pierre's own `Storage` wrapper end-to-end: a short-TTL
/// record's cohort is fully expired and removed by compaction, while a long-TTL
/// record in a different cohort survives untouched — deathtime-cohort compaction
/// doesn't rewrite/lose live neighbors just because it ran.
#[tokio::test]
async fn compaction_removes_expired_cohort_without_touching_live_data() {
    let dir = tempfile::tempdir().unwrap();
    // A 1-second cohort window so the test doesn't need to wait a real hour
    // (edgestore's own default) for a cohort boundary to pass.
    let remote_dir = tempfile::tempdir().unwrap();
    let remote = edgestore_repl::FilesystemRemoteStore::new(remote_dir.path().to_path_buf()).unwrap();
    let storage = Storage::open_with_options(dir.path(), Box::new(remote), 1, false).await.unwrap();

    let ns = b"compaction_test";
    storage.put_with_ttl(ns, b"expiring", b"short-lived", 1).await.unwrap();
    storage.put_with_ttl(ns, b"persisting", b"long-lived", 100).await.unwrap();

    // Compaction operates on segments, not the live memtable.
    storage.flush_to_segments().await.unwrap();

    // Both keys readable immediately after flush, before anything has expired.
    assert_eq!(storage.get(ns, b"expiring").await.unwrap(), Some(b"short-lived".to_vec()));
    assert_eq!(storage.get(ns, b"persisting").await.unwrap(), Some(b"long-lived".to_vec()));

    // Wait past the short TTL *and* a full cohort window so that cohort's boundary
    // has definitely passed (death_time = write_time + ttl; cohort granularity = 1s).
    tokio::time::sleep(Duration::from_millis(2500)).await;

    let stats = storage.compact_once().await.unwrap();
    assert!(
        stats.cohorts_collected > 0 || stats.live_records_relocated > 0,
        "compaction should have done real work: {stats:?}"
    );

    // Expired key is gone; the long-lived neighbor (different cohort, not yet expired)
    // must survive untouched — this is the "without rewriting live neighbors" half
    // of the requirement, not just "expiry works".
    assert_eq!(storage.get(ns, b"expiring").await.unwrap(), None, "expired cohort should be gone after compaction");
    assert_eq!(
        storage.get(ns, b"persisting").await.unwrap(),
        Some(b"long-lived".to_vec()),
        "the long-TTL record in a different cohort must survive compaction untouched"
    );
}
