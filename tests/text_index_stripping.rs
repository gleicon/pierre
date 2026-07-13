use std::collections::BTreeMap;

use pierre::record::WireRecord;
use pierre::storage::Storage;

/// Documents a real, upstream correctness gap in edgestore's `with_text_stripping`
/// (found by this test, not assumed): stripping rewrites the *segment* file
/// correctly (confirmed — `text_index_stripped=true`, record count drops), but does
/// **not** rotate/truncate the WAL. `flush_to_segments`'s own WAL rotation is purely
/// size-based (64MB default), unrelated to flush/strip events. So the *original* WAL
/// entry for the stripped write is usually still present, and a restart's
/// `recover_from_wal` replays it straight back into the memtable — silently
/// resurrecting exactly the data that was just stripped from disk. This is the
/// opposite of what `with_text_stripping`'s own doc comment promises (framed as "a
/// restart can't reconstruct a stripped segment" — the observed behavior is that it
/// *does*, via WAL replay, not via the segment).
///
/// This test asserts the **current actual** behavior (stripping isn't durable
/// across a restart), not the aspirational one — so it will *fail* the day this gets
/// fixed upstream, which is the point: that's the signal to flip
/// `strip_text_index_after_archive`'s default and update this test.
#[tokio::test]
async fn stripping_is_not_yet_durable_across_a_restart_known_upstream_gap() {
    let dir = tempfile::tempdir().unwrap();
    let remote_dir = tempfile::tempdir().unwrap();

    {
        let remote =
            edgestore_repl::FilesystemRemoteStore::new(remote_dir.path().to_path_buf()).unwrap();
        let storage = Storage::open_with_options(dir.path(), Box::new(remote), 3600, true)
            .await
            .unwrap();

        let mut fields = BTreeMap::new();
        fields.insert("level".to_string(), "error".to_string());
        let wire = WireRecord {
            timestamp_ns: 1_000_000_000,
            message: "payment gateway timeout".to_string(),
            fields,
        };
        pierre::ingest::commit(&storage, wire, &["level".to_string()], None, None)
            .await
            .unwrap();
        storage
            .index_text(b"text_bucket_0", b"doc-1", "payment gateway timeout")
            .await
            .unwrap();

        storage.flush().await.unwrap();
        let meta = storage.flush_to_segments().await.unwrap();
        storage.archive_segments(vec![meta]).await.unwrap();

        // The segment itself is correctly stripped — this part of the feature works.
        let metas = storage.list_segment_metas().await;
        assert!(
            metas[0].text_index_stripped,
            "segment should be marked stripped"
        );
        assert_eq!(
            metas[0].record_count, 1,
            "the __text__ record should be gone from the segment (log record only remains)"
        );
    } // storage dropped — simulates a process exit.

    // Reopen against the same local directory and the same remote — a real restart.
    let remote =
        edgestore_repl::FilesystemRemoteStore::new(remote_dir.path().to_path_buf()).unwrap();
    let reopened = Storage::open_with_options(dir.path(), Box::new(remote), 3600, true)
        .await
        .unwrap();

    // KNOWN GAP: this *should* be empty (the segment was stripped) but isn't, because
    // WAL replay on reopen resurrects the original pre-strip write into the memtable.
    let hits_after = reopened
        .search_text(b"text_bucket_0", "payment", 10)
        .await
        .unwrap();
    assert_eq!(
        hits_after.len(),
        1,
        "KNOWN GAP: WAL replay resurrects stripped data on restart — see this test's doc comment"
    );

    // The raw log record is unaffected either way (never touched by stripping).
    let logs_after = reopened.range(0, 2_000_000_000).await.unwrap();
    assert_eq!(
        logs_after.len(),
        1,
        "raw log records must survive text-index stripping untouched"
    );
    assert_eq!(logs_after[0].message, "payment gateway timeout");
}
