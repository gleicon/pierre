use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use pierre::query;
use pierre::storage::Storage;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpStream, UdpSocket};

async fn spawn_server(storage: Arc<Storage>) -> String {
    let listener_socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener_socket.local_addr().unwrap().to_string();
    drop(listener_socket); // free the port; serve() rebinds both TCP and UDP on it

    let allowed_fields = Arc::new(vec!["level".to_string(), "hostname".to_string()]);
    let addr_for_server = addr.clone();
    tokio::spawn(async move {
        let _ = pierre::listener::syslog::serve(
            &addr_for_server,
            storage,
            allowed_fields,
            None,
            None,
            pierre::stats::IngestStats::default(),
        )
        .await;
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    addr
}

/// One UDP datagram = one message — the common case for appliances and legacy
/// systems that ship syslog over UDP (the PRD's "long tail that never leaves").
#[tokio::test]
async fn udp_datagram_lands_as_a_real_record() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(Storage::open(dir.path()).await.unwrap());
    let addr = spawn_server(storage.clone()).await;

    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let line =
        "<34>1 2026-07-25T18:00:00.000Z myhost.example.com su - ID47 - login failure for root";
    socket.send_to(line.as_bytes(), &addr).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut filter = BTreeMap::new();
    filter.insert("hostname".to_string(), "myhost.example.com".to_string());
    let results = query::select(&storage, 0, i64::MAX, &filter).await.unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].message, "login failure for root");
    assert_eq!(
        results[0].fields.get("level").map(String::as_str),
        Some("critical")
    );
}

/// TCP, newline-delimited — multiple messages over one persistent connection,
/// the way a real relay (rsyslog/syslog-ng forwarding) keeps a connection open.
#[tokio::test]
async fn tcp_newline_delimited_messages_all_land() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(Storage::open(dir.path()).await.unwrap());
    let addr = spawn_server(storage.clone()).await;

    let mut stream = TcpStream::connect(&addr).await.unwrap();
    let lines = "<13>1 2026-07-25T18:00:01.000Z hosta app - - - first\n<13>1 2026-07-25T18:00:02.000Z hostb app - - - second\n";
    stream.write_all(lines.as_bytes()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let results = query::select(&storage, 0, i64::MAX, &BTreeMap::new())
        .await
        .unwrap();
    assert_eq!(results.len(), 2);
    let messages: Vec<&str> = results.iter().map(|r| r.message.as_str()).collect();
    assert!(messages.contains(&"first"));
    assert!(messages.contains(&"second"));
}

/// A line that isn't RFC5424 at all — a real relay's own malformed or legacy
/// output — must still land as a raw-text record rather than being dropped or
/// crashing the connection for every message after it.
#[tokio::test]
async fn non_conformant_line_still_lands_and_does_not_kill_the_connection() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(Storage::open(dir.path()).await.unwrap());
    let addr = spawn_server(storage.clone()).await;

    let mut stream = TcpStream::connect(&addr).await.unwrap();
    let lines =
        "not RFC5424 at all\n<13>1 2026-07-25T18:00:03.000Z host app - - - a real one after it\n";
    stream.write_all(lines.as_bytes()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let results = query::select(&storage, 0, i64::MAX, &BTreeMap::new())
        .await
        .unwrap();
    assert_eq!(results.len(), 2);
    let messages: Vec<&str> = results.iter().map(|r| r.message.as_str()).collect();
    assert!(messages.contains(&"not RFC5424 at all"));
    assert!(
        messages.contains(&"a real one after it"),
        "the connection must not die after one malformed line"
    );
}
