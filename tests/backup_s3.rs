#![cfg(feature = "s3")]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use edgestore::RemoteStore;
use edgestore_repl::S3RemoteStore;
use pierre::backup::BackupConfig;
use pierre::record::WireRecord;
use pierre::storage::Storage;

// S3RemoteStore's blocking bridge uses `block_in_place`, which requires the
// multi-threaded runtime flavor (the default for `#[tokio::main]`, but not for
// `#[tokio::test]`) — hence the explicit flavor here.
#[tokio::test(flavor = "multi_thread")]
async fn warm_segment_is_backed_up_to_s3() {
    let Ok(endpoint) = std::env::var("EDGESTORE_S3_ENDPOINT_URL") else {
        eprintln!("skip: EDGESTORE_S3_ENDPOINT_URL not set");
        return;
    };
    let bucket = std::env::var("EDGESTORE_S3_BUCKET").unwrap_or_else(|_| "pierre-test".to_string());

    let data_dir = tempfile::tempdir().unwrap();
    let prefix = "pierre-backup-test/";
    let backup_config = BackupConfig::S3 {
        bucket: bucket.clone(),
        prefix: Some(prefix.to_string()),
        endpoint: Some(endpoint.clone()),
    };
    let remote = pierre::backup::build_remote_store(backup_config).await.unwrap();
    let storage = Arc::new(Storage::open_with_remote(data_dir.path(), remote).await.unwrap());

    let mut fields = BTreeMap::new();
    fields.insert("level".to_string(), "info".to_string());
    let wire = WireRecord { timestamp_ns: 1, message: "hello from pierre".to_string(), fields };
    pierre::ingest::commit(&storage, wire, &["level".to_string()], None, None).await.unwrap();

    let flush_interval = Duration::from_millis(50);
    let archive_interval = Duration::from_millis(80);
    let _worker = pierre::backup::spawn(storage.clone(), flush_interval, archive_interval, None, std::time::Duration::from_secs(3600)).await;

    tokio::time::sleep(Duration::from_millis(500)).await;

    let manifest = storage.list_segment_metas().await;
    assert!(!manifest.is_empty(), "flush_to_segments should have produced at least one warm segment");

    // Gather expected local bytes first (async), then do the entire S3-side check —
    // construct, download, drop — inside one spawn_blocking closure. `S3RemoteStore`
    // owns a `Runtime`; both its construction *and* its drop panic if they happen in
    // async context (drop: "Cannot drop a runtime in a context where blocking is not
    // allowed"). Confining the whole verifier's lifetime to one blocking-thread closure
    // means it's never touched from async context at all, construction through drop.
    let mut expected: Vec<([u8; 32], Vec<u8>)> = Vec::new();
    for meta in &manifest {
        let path = storage.data_dir().join(format!("segment-{:08}.dat", meta.segment_id));
        let bytes = tokio::fs::read(path).await.unwrap();
        let hash: [u8; 32] = meta.segment_hash.as_slice().try_into().unwrap();
        expected.push((hash, bytes));
    }

    tokio::task::spawn_blocking(move || {
        let verifier = S3RemoteStore::new(&bucket, Some(prefix), Some(&endpoint)).expect("build verifier store");
        for (hash, local_bytes) in expected {
            let hex: String = hash.iter().map(|b| format!("{b:02x}")).collect();
            let downloaded = verifier.download(&hash).unwrap_or_else(|e| panic!("segment {hex} not found in S3: {e}"));
            assert_eq!(downloaded, local_bytes, "backed-up bytes must match the local segment exactly");
        }
    })
    .await
    .unwrap();
}
