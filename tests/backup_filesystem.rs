use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use pierre::backup::BackupConfig;
use pierre::record::WireRecord;
use pierre::storage::Storage;

#[tokio::test]
async fn warm_segment_is_backed_up_to_filesystem_remote_store() {
    let data_dir = tempfile::tempdir().unwrap();
    let backup_dir = tempfile::tempdir().unwrap();
    let backup_config = BackupConfig::Filesystem { path: backup_dir.path().to_string_lossy().to_string() };
    let remote = pierre::backup::build_remote_store(backup_config).await.unwrap();
    let storage = Arc::new(Storage::open_with_remote(data_dir.path(), remote).await.unwrap());

    // Write something so flush_to_segments has data to flush.
    let mut fields = BTreeMap::new();
    fields.insert("level".to_string(), "info".to_string());
    let wire = WireRecord { timestamp_ns: 1, message: "hello".to_string(), fields };
    pierre::ingest::commit(&storage, wire, &["level".to_string()], None, None).await.unwrap();

    let flush_interval = Duration::from_millis(50);
    let archive_interval = Duration::from_millis(80);
    let _worker = pierre::backup::spawn(storage.clone(), flush_interval, archive_interval, None, std::time::Duration::from_secs(3600)).await;

    // Wait for at least one flush tick (hot->warm) and one archive tick.
    tokio::time::sleep(Duration::from_millis(400)).await;

    let manifest = storage.list_segment_metas().await;
    assert!(!manifest.is_empty(), "flush_to_segments should have produced at least one warm segment");

    // FilesystemRemoteStore is content-addressed by BLAKE3 hash — confirm the segment's
    // hash shows up as a file somewhere under the backup directory.
    let backed_up_hashes: Vec<String> = walk_hex_filenames(backup_dir.path());
    for meta in &manifest {
        let hex = meta.segment_hash.iter().map(|b| format!("{b:02x}")).collect::<String>();
        assert!(
            backed_up_hashes.iter().any(|f| f.contains(&hex)),
            "segment {hex} should have been backed up under {}",
            backup_dir.path().display()
        );
    }
}

/// Proves the flush_notify wiring (edgestore 1.3.0's `with_on_segment_flushed`,
/// see `backup::spawn`'s doc): with `archive_interval` set far longer than this
/// test waits, a segment must still get archived promptly — because the flush
/// itself wakes the archive pass, not because the interval elapsed.
#[tokio::test]
async fn segment_is_archived_immediately_on_flush_not_only_on_archive_interval() {
    let data_dir = tempfile::tempdir().unwrap();
    let backup_dir = tempfile::tempdir().unwrap();
    let backup_config = BackupConfig::Filesystem { path: backup_dir.path().to_string_lossy().to_string() };
    let remote = pierre::backup::build_remote_store(backup_config).await.unwrap();
    let storage = Arc::new(Storage::open_with_remote(data_dir.path(), remote).await.unwrap());

    let mut fields = BTreeMap::new();
    fields.insert("level".to_string(), "info".to_string());
    let wire = WireRecord { timestamp_ns: 1, message: "hello".to_string(), fields };
    pierre::ingest::commit(&storage, wire, &["level".to_string()], None, None).await.unwrap();

    let flush_interval = Duration::from_millis(50);
    // Far longer than this test waits — if archiving only happened on this
    // interval's own tick, the assertion below would fail.
    let archive_interval = Duration::from_secs(3600);
    let _worker = pierre::backup::spawn(storage.clone(), flush_interval, archive_interval, None, Duration::from_secs(3600)).await;

    // Long enough for one flush_tick, short enough that archive_interval's own
    // 3600s tick cannot possibly have fired.
    tokio::time::sleep(Duration::from_millis(300)).await;

    let manifest = storage.list_segment_metas().await;
    assert!(!manifest.is_empty(), "flush_to_segments should have produced at least one warm segment");

    let backed_up_hashes: Vec<String> = walk_hex_filenames(backup_dir.path());
    for meta in &manifest {
        let hex = meta.segment_hash.iter().map(|b| format!("{b:02x}")).collect::<String>();
        assert!(
            backed_up_hashes.iter().any(|f| f.contains(&hex)),
            "segment {hex} should have been archived promptly via flush_notify, not only on archive_interval's own tick"
        );
    }
}

fn walk_hex_filenames(dir: &std::path::Path) -> Vec<String> {
    let mut out = Vec::new();
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                out.extend(walk_hex_filenames(&path));
            } else if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                out.push(name.to_string());
            }
        }
    }
    out
}
