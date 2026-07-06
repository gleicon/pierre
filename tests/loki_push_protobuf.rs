use std::collections::BTreeMap;
use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use pierre::listener::loki;
use pierre::query;
use pierre::storage::Storage;
use tower::ServiceExt;

/// Real collectors (Promtail, Alloy, Vector, Fluent Bit) send protobuf+raw-snappy
/// by default, not JSON — confirmed by an actual Promtail container rejecting the
/// old JSON-only endpoint with a 415. This proves Pierre decodes that wire format
/// correctly, independent of a running collector.
#[tokio::test]
async fn loki_push_decodes_real_protobuf_snappy_wire_format() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(Storage::open(dir.path()).await.unwrap());
    let allowed_fields = Arc::new(vec!["level".to_string()]);
    let app = loki::router(storage.clone(), allowed_fields, None, None, pierre::auth::AuthTokens::new(vec![]));

    let push_request = pierre::lokiproto::PushRequest {
        streams: vec![pierre::lokiproto::StreamAdapter {
            labels: r#"{level="error"}"#.to_string(),
            entries: vec![pierre::lokiproto::EntryAdapter {
                timestamp: Some(prost_types::Timestamp { seconds: 1, nanos: 0 }),
                line: "boom via real protobuf wire format".to_string(),
                structured_metadata: vec![],
                parsed: vec![],
            }],
            hash: 0,
        }],
        format: "".to_string(),
    };

    let mut encoded = Vec::new();
    prost::Message::encode(&push_request, &mut encoded).unwrap();
    let compressed = snap::raw::Encoder::new().compress_vec(&encoded).unwrap();

    let request = Request::builder()
        .method("POST")
        .uri("/loki/api/v1/push")
        .header("content-type", "application/x-protobuf")
        .body(Body::from(compressed))
        .unwrap();

    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let mut filter = BTreeMap::new();
    filter.insert("level".to_string(), "error".to_string());
    let results = query::select(&storage, 0, 2_000_000_000, &filter).await.unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].message, "boom via real protobuf wire format");
    assert_eq!(results[0].timestamp_ns, 1_000_000_000);
}

/// Missing Content-Type must also decode as protobuf — matches Loki's own server
/// behavior ("when no content-type header is set... expect snappy compression").
#[tokio::test]
async fn loki_push_defaults_to_protobuf_when_content_type_is_absent() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(Storage::open(dir.path()).await.unwrap());
    let allowed_fields = Arc::new(vec![]);
    let app = loki::router(storage.clone(), allowed_fields, None, None, pierre::auth::AuthTokens::new(vec![]));

    let push_request = pierre::lokiproto::PushRequest {
        streams: vec![pierre::lokiproto::StreamAdapter {
            labels: "{}".to_string(),
            entries: vec![pierre::lokiproto::EntryAdapter {
                timestamp: Some(prost_types::Timestamp { seconds: 2, nanos: 0 }),
                line: "no content-type header".to_string(),
                structured_metadata: vec![],
                parsed: vec![],
            }],
            hash: 0,
        }],
        format: "".to_string(),
    };
    let mut encoded = Vec::new();
    prost::Message::encode(&push_request, &mut encoded).unwrap();
    let compressed = snap::raw::Encoder::new().compress_vec(&encoded).unwrap();

    let request = Request::builder().method("POST").uri("/loki/api/v1/push").body(Body::from(compressed)).unwrap();
    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let results = query::select(&storage, 0, 3_000_000_000, &BTreeMap::new()).await.unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].message, "no content-type header");
}
