use std::collections::BTreeMap;

use pierre::record::WireRecord;
use pierre::storage::Storage;

/// Pins a load-bearing edgestore capability against regression: `TieredEngine::
/// range()`/`prefix()` read through to archived segments overlapping the query
/// (introduced in edgestore 1.1.4; SPEC.md #L1). Pierre depends on this directly —
/// it deleted its own bespoke `query_archived_range` workaround in favor of it, and
/// local segment pruning (deleting a local segment after archiving) is only safe
/// because of this behavior. If a future edgestore version regresses `range()`/
/// `prefix()` back to local-only, this test must fail.
#[tokio::test]
async fn plain_range_reads_through_to_archived_only_data() {
    let source_dir = tempfile::tempdir().unwrap();
    let remote_dir = tempfile::tempdir().unwrap();

    {
        let remote = edgestore_repl::FilesystemRemoteStore::new(remote_dir.path().to_path_buf()).unwrap();
        let storage = Storage::open_with_remote(source_dir.path(), Box::new(remote)).await.unwrap();

        let mut fields = BTreeMap::new();
        fields.insert("level".to_string(), "error".to_string());
        let wire = WireRecord { timestamp_ns: 1_000_000_000, message: "archived-only record".to_string(), fields };
        pierre::ingest::commit(&storage, wire, &["level".to_string()], None, None).await.unwrap();

        let meta = storage.flush_to_segments().await.unwrap();
        storage.archive_segments(vec![meta.clone()]).await.unwrap();

        let record = pierre::backup::ArchivedSegmentRecord::new(meta);
        let bytes = serde_json::to_vec(&vec![record]).unwrap();
        tokio::fs::write(source_dir.path().join("archived_segments.json"), bytes).await.unwrap();
    }

    // Fresh Storage, zero local data, same remote. Register the archived segments
    // the same way backup::spawn's restore-on-startup logic does (TieredEngine's
    // read-through — old get()-only and new range()/prefix() alike — needs to know
    // which segments are archived; it doesn't auto-discover them from the remote
    // store). No query_archived_range call — just the plain method every caller uses.
    let fresh_dir = tempfile::tempdir().unwrap();
    let remote_b = edgestore_repl::FilesystemRemoteStore::new(remote_dir.path().to_path_buf()).unwrap();
    let fresh_storage = Storage::open_with_remote(fresh_dir.path(), Box::new(remote_b)).await.unwrap();

    let archived_records = pierre::backup::load_archived_meta(source_dir.path()).await;
    let restored = archived_records
        .into_iter()
        .map(|r| edgestore_tier::ArchivedSegment {
            hash: <[u8; 32]>::try_from(r.meta.segment_hash.as_slice()).unwrap(),
            min_key: r.meta.min_key,
            max_key: r.meta.max_key,
        })
        .collect();
    fresh_storage.register_archived(restored).await;

    let dat_files_before = count_dat_files(fresh_dir.path());

    let results = fresh_storage.range(0, 2_000_000_000).await.unwrap();
    assert_eq!(results.len(), 1, "plain range() should read through to archived data with no bespoke workaround needed");
    assert_eq!(results[0].message, "archived-only record");

    let dat_files_after = count_dat_files(fresh_dir.path());
    assert_eq!(
        dat_files_before, dat_files_after,
        "read-through must stay ephemeral (no new local segment files) — this is what makes local segment pruning safe"
    );
}

fn count_dat_files(dir: &std::path::Path) -> usize {
    std::fs::read_dir(dir)
        .map(|entries| entries.flatten().filter(|e| e.path().extension().is_some_and(|ext| ext == "dat")).count())
        .unwrap_or(0)
}
