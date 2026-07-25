use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use pierre::otlpproto::opentelemetry::proto::collector::logs::v1::logs_service_client::LogsServiceClient;
use pierre::otlpproto::opentelemetry::proto::collector::logs::v1::ExportLogsServiceRequest;
use pierre::otlpproto::opentelemetry::proto::common::v1::any_value::Value;
use pierre::otlpproto::opentelemetry::proto::common::v1::{AnyValue, KeyValue};
use pierre::otlpproto::opentelemetry::proto::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use pierre::otlpproto::opentelemetry::proto::resource::v1::Resource;
use pierre::query;
use pierre::storage::Storage;
use prost::Message;

fn kv(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(Value::StringValue(value.to_string())),
        }),
        key_strindex: 0,
    }
}

fn sample_request(message: &str) -> ExportLogsServiceRequest {
    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![kv("service.name", "checkout")],
                dropped_attributes_count: 0,
                entity_refs: vec![],
            }),
            scope_logs: vec![ScopeLogs {
                scope: None,
                log_records: vec![LogRecord {
                    time_unix_nano: 1_753_000_000_000_000_000,
                    observed_time_unix_nano: 0,
                    severity_number: 17,
                    severity_text: "ERROR".to_string(),
                    body: Some(AnyValue {
                        value: Some(Value::StringValue(message.to_string())),
                    }),
                    attributes: vec![],
                    dropped_attributes_count: 0,
                    flags: 0,
                    trace_id: vec![],
                    span_id: vec![],
                    event_name: String::new(),
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

/// Real gRPC transport, real client stub — the primary way OTel exporters
/// actually send OTLP (most default to gRPC over HTTP).
#[tokio::test]
async fn grpc_export_lands_as_a_real_record() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(Storage::open(dir.path()).await.unwrap());

    let listener_socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener_socket.local_addr().unwrap().to_string();
    drop(listener_socket);

    let addr_for_server = addr.clone();
    let storage_for_server = storage.clone();
    tokio::spawn(async move {
        let _ = pierre::listener::otlp::serve_grpc(
            &addr_for_server,
            storage_for_server,
            Arc::new(vec![]),
            None,
            None,
            pierre::stats::IngestStats::default(),
        )
        .await;
    });
    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut client = LogsServiceClient::connect(format!("http://{addr}"))
        .await
        .unwrap();
    let response = client
        .export(sample_request("payment declined via grpc"))
        .await
        .unwrap();
    assert!(
        response.into_inner().partial_success.is_none(),
        "a fully-accepted export must not report a partial success"
    );

    let results = query::select(&storage, 0, i64::MAX, &BTreeMap::new())
        .await
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].message, "payment declined via grpc");
    assert_eq!(results[0].timestamp_ns, 1_753_000_000_000_000_000);
}

/// OTLP/HTTP, protobuf content-type — the axum-based variant, distinct code path
/// from gRPC but decoding the identical message types.
#[tokio::test]
async fn http_protobuf_export_lands_as_a_real_record() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(Storage::open(dir.path()).await.unwrap());

    let listener_socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener_socket.local_addr().unwrap().to_string();
    drop(listener_socket);

    let addr_for_server = addr.clone();
    let storage_for_server = storage.clone();
    tokio::spawn(async move {
        let _ = pierre::listener::otlp::serve_http(
            &addr_for_server,
            storage_for_server,
            Arc::new(vec![]),
            None,
            None,
            pierre::stats::IngestStats::default(),
        )
        .await;
    });
    tokio::time::sleep(Duration::from_millis(100)).await;

    let request = sample_request("payment declined via http");
    let body = request.encode_to_vec();

    let client = reqwest_like_post(&addr, body).await;
    assert_eq!(client, 200);

    let results = query::select(&storage, 0, i64::MAX, &BTreeMap::new())
        .await
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].message, "payment declined via http");
}

/// A JSON body must get a clear 415, not a silent misparse — OTLP/JSON is a
/// documented scope cut (see `listener/otlp.rs`), not an oversight.
#[tokio::test]
async fn json_content_type_is_rejected_not_silently_misparsed() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(Storage::open(dir.path()).await.unwrap());

    let listener_socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener_socket.local_addr().unwrap().to_string();
    drop(listener_socket);

    let addr_for_server = addr.clone();
    tokio::spawn(async move {
        let _ = pierre::listener::otlp::serve_http(
            &addr_for_server,
            storage,
            Arc::new(vec![]),
            None,
            None,
            pierre::stats::IngestStats::default(),
        )
        .await;
    });
    tokio::time::sleep(Duration::from_millis(100)).await;

    let status = post_with_content_type(&addr, b"{}".to_vec(), "application/json").await;
    assert_eq!(status, 415);
}

/// Minimal raw HTTP client over TCP — avoids adding a dev-dependency (reqwest)
/// just for two integration tests when a plain socket write does the job.
async fn reqwest_like_post(addr: &str, body: Vec<u8>) -> u16 {
    post_with_content_type(addr, body, "application/x-protobuf").await
}

async fn post_with_content_type(addr: &str, body: Vec<u8>, content_type: &str) -> u16 {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    let request = format!(
        "POST /v1/logs HTTP/1.1\r\nHost: localhost\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(request.as_bytes()).await.unwrap();
    stream.write_all(&body).await.unwrap();

    let mut response = Vec::new();
    stream.read_to_end(&mut response).await.unwrap();
    let response_text = String::from_utf8_lossy(&response);
    let status_line = response_text.lines().next().unwrap();
    status_line
        .split_whitespace()
        .nth(1)
        .unwrap()
        .parse()
        .unwrap()
}
