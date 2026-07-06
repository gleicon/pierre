use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use pierre::listener;
use pierre::query;
use pierre::record::WireRecord;
use pierre::storage::Storage;

async fn send_batch(addr: &str, batch: &[WireRecord]) -> u8 {
    let payload = serde_json::to_vec(batch).unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream.write_all(&(payload.len() as u32).to_be_bytes()).await.unwrap();
    stream.write_all(&payload).await.unwrap();
    let mut ack = [0u8; 1];
    stream.read_exact(&mut ack).await.unwrap();
    ack[0]
}

#[tokio::test]
async fn native_write_is_durable_and_immediately_readable() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(Storage::open(dir.path()).await.unwrap());
    let allowed_fields = Arc::new(vec!["level".to_string(), "trace_id".to_string()]);

    // Bind on an ephemeral port, then spawn the listener.
    let listener_socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener_socket.local_addr().unwrap().to_string();
    drop(listener_socket); // free the port; serve() will rebind it

    let storage_for_server = storage.clone();
    let fields_for_server = allowed_fields.clone();
    let addr_clone = addr.clone();
    tokio::spawn(async move {
        listener::native::serve(&addr_clone, storage_for_server, fields_for_server, None, None)
            .await
            .unwrap();
    });
    // Give the listener a moment to bind.
    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut fields = BTreeMap::new();
    fields.insert("level".to_string(), "error".to_string());
    fields.insert("trace_id".to_string(), "abc123".to_string());

    let record = WireRecord {
        timestamp_ns: 1_000_000_000,
        message: "request 500 failed after 42ms".to_string(),
        fields,
    };

    let ack = send_batch(&addr, std::slice::from_ref(&record)).await;
    assert_eq!(ack, 1, "batch should be acked as durable");

    // Immediately readable via the query path, no async delay.
    let mut filter = BTreeMap::new();
    filter.insert("level".to_string(), "error".to_string());
    let results = query::select(&storage, 0, 2_000_000_000, &filter).await.unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].message, "request 500 failed after 42ms");
    assert_eq!(results[0].fields.get("trace_id"), Some(&"abc123".to_string()));
    assert!(results[0].template_id != 0);

    // A query outside the time range must not match.
    let none = query::select(&storage, 0, 500_000_000, &filter).await.unwrap();
    assert!(none.is_empty());

    // A selector that doesn't match must not return the record.
    let mut wrong_filter = BTreeMap::new();
    wrong_filter.insert("level".to_string(), "info".to_string());
    let none2 = query::select(&storage, 0, 2_000_000_000, &wrong_filter).await.unwrap();
    assert!(none2.is_empty());
}

#[tokio::test]
async fn unconfigured_fields_are_dropped_at_ingest() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(Storage::open(dir.path()).await.unwrap());
    let allowed_fields = vec!["level".to_string()];

    let mut fields = BTreeMap::new();
    fields.insert("level".to_string(), "warn".to_string());
    fields.insert("not_configured".to_string(), "should be dropped".to_string());

    let wire = WireRecord {
        timestamp_ns: 42,
        message: "hello world".to_string(),
        fields,
    };
    pierre::ingest::commit(&storage, wire, &allowed_fields, None, None).await.unwrap();

    let results = query::select(&storage, 0, 1000, &BTreeMap::new()).await.unwrap();
    assert_eq!(results.len(), 1);
    assert!(results[0].fields.contains_key("level"));
    assert!(!results[0].fields.contains_key("not_configured"));
}
