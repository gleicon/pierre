use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use pierre::record::WireRecord;
use pierre::storage::Storage;

#[tokio::test]
async fn indexed_line_is_searchable_after_the_indexer_catches_up() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(Storage::open(dir.path()).await.unwrap());
    let allowed_fields = vec!["level".to_string()];

    let bucket_duration = Duration::from_secs(3600);
    let flush_interval = Duration::from_millis(50);
    let (textindex, _worker) =
        pierre::textindex::spawn(storage.clone(), bucket_duration, flush_interval);

    let wire = WireRecord {
        timestamp_ns: 1_000_000_000,
        message: "request 500 failed after 42ms".to_string(),
        fields: BTreeMap::new(),
    };
    pierre::ingest::commit(
        &storage,
        wire,
        &allowed_fields,
        None,
        Some(&textindex),
        None,
    )
    .await
    .unwrap();

    // Indexing is async — give the worker a moment to process the sample.
    tokio::time::sleep(Duration::from_millis(100)).await;

    let results =
        pierre::textindex::search(&storage, 0, 2_000_000_000, bucket_duration, "failed", 10)
            .await
            .unwrap();
    assert_eq!(results.len(), 1);

    let none = pierre::textindex::search(
        &storage,
        0,
        2_000_000_000,
        bucket_duration,
        "nonexistentword",
        10,
    )
    .await
    .unwrap();
    assert!(none.is_empty());
}

#[tokio::test]
async fn ingest_never_blocks_on_a_full_textindex_channel() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(Storage::open(dir.path()).await.unwrap());

    // A huge bucket/flush duration so the worker never drains while we flood it.
    let (textindex, _worker) = pierre::textindex::spawn(
        storage.clone(),
        Duration::from_secs(3600),
        Duration::from_secs(3600),
    );

    for _ in 0..5000 {
        textindex.record(b"key".to_vec(), "some log line".to_string(), 1);
    }

    assert!(
        textindex.dropped_count() > 0,
        "overflowing the bounded channel must increment the drop counter"
    );
}

#[tokio::test]
async fn search_merges_results_across_time_buckets() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(Storage::open(dir.path()).await.unwrap());
    let allowed_fields = vec!["level".to_string()];

    // Small bucket window so two records land in two different buckets.
    let bucket_duration = Duration::from_secs(60);
    let flush_interval = Duration::from_millis(50);
    let (textindex, _worker) =
        pierre::textindex::spawn(storage.clone(), bucket_duration, flush_interval);

    for (i, ts) in [(0, 0i64), (1, 120_000_000_000)].into_iter() {
        let wire = WireRecord {
            timestamp_ns: ts,
            message: format!("marker-line-{i} needle"),
            fields: BTreeMap::new(),
        };
        pierre::ingest::commit(
            &storage,
            wire,
            &allowed_fields,
            None,
            Some(&textindex),
            None,
        )
        .await
        .unwrap();
    }

    tokio::time::sleep(Duration::from_millis(150)).await;

    let results =
        pierre::textindex::search(&storage, 0, 200_000_000_000, bucket_duration, "needle", 10)
            .await
            .unwrap();
    assert_eq!(
        results.len(),
        2,
        "search must merge hits from both time buckets"
    );
}

/// Proves BM25 crash-recovery completeness (SPEC.md NFR-8): a document indexed after
/// the last `flush()`, followed by a real crash (process-equivalent: drop every handle
/// to the engine without a clean shutdown), must still be searchable once storage is
/// reopened. This isn't testing edgestore's self-healing rebuild in isolation (already
/// verified upstream) — it's testing that Pierre's own textindex module doesn't do
/// anything that would defeat it (e.g. hold state elsewhere that doesn't survive restart).
#[tokio::test]
async fn crash_after_partial_flush_still_leaves_both_documents_searchable() {
    let dir = tempfile::tempdir().unwrap();
    let bucket_duration = Duration::from_secs(3600); // both docs land in the same bucket
    let allowed_fields = vec!["level".to_string()];

    {
        let storage = Arc::new(Storage::open(dir.path()).await.unwrap());
        // Long flush interval — we trigger flush() manually to control exactly what's
        // persisted before the "crash", rather than racing a timer.
        let (textindex, worker) =
            pierre::textindex::spawn(storage.clone(), bucket_duration, Duration::from_secs(3600));

        let doc1 = WireRecord {
            timestamp_ns: 1_000_000_000,
            message: "zzzalpha flushed before crash".to_string(),
            fields: BTreeMap::new(),
        };
        pierre::ingest::commit(
            &storage,
            doc1,
            &allowed_fields,
            None,
            Some(&textindex),
            None,
        )
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await; // let the worker index it
        storage.flush().await.unwrap(); // persist the sidecar with doc1 only

        let doc2 = WireRecord {
            timestamp_ns: 1_100_000_000,
            message: "zzzbeta indexed after last flush".to_string(),
            fields: BTreeMap::new(),
        };
        pierre::ingest::commit(
            &storage,
            doc2,
            &allowed_fields,
            None,
            Some(&textindex),
            None,
        )
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await; // in-memory only — never flushed

        // Simulate a crash: kill the background worker (it holds its own Arc<Storage>
        // clone forever otherwise) and drop our own handle, with no clean shutdown call.
        worker.abort();
        let _ = worker.await;
        drop(storage);
    }

    // Reopen fresh, as a real restart would. edgestore's rebuild_text_indices() must
    // detect the stale sidecar (LSN watermark behind doc2's write) and rebuild from
    // durable raw records automatically.
    let recovered = Arc::new(Storage::open(dir.path()).await.unwrap());

    let alpha = pierre::textindex::search(
        &recovered,
        0,
        2_000_000_000,
        bucket_duration,
        "zzzalpha",
        10,
    )
    .await
    .unwrap();
    assert_eq!(
        alpha.len(),
        1,
        "the flushed-before-crash document must survive"
    );

    let beta =
        pierre::textindex::search(&recovered, 0, 2_000_000_000, bucket_duration, "zzzbeta", 10)
            .await
            .unwrap();
    assert_eq!(
        beta.len(),
        1,
        "the document indexed after the last flush must still be searchable post-crash — not permanently lost"
    );
}
