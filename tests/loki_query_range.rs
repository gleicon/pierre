use std::collections::BTreeMap;
use std::sync::Arc;

use axum::body::Body;
use axum::http::Request;
use pierre::listener::loki;
use pierre::record::WireRecord;
use pierre::storage::Storage;
use tower::ServiceExt;

async fn body_json(response: axum::response::Response) -> serde_json::Value {
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

async fn ingest(storage: &Storage, ts: i64, message: &str, fields: BTreeMap<String, String>) {
    let wire = WireRecord { timestamp_ns: ts, message: message.to_string(), fields };
    let allowed: Vec<String> = wire.fields.keys().cloned().collect();
    pierre::ingest::commit(storage, wire, &allowed, None, None).await.unwrap();
}

#[tokio::test]
async fn query_range_answers_selector_plus_line_filter() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(Storage::open(dir.path()).await.unwrap());

    let mut error_fields = BTreeMap::new();
    error_fields.insert("level".to_string(), "error".to_string());
    ingest(&storage, 1_000_000_000, "payment timeout", error_fields.clone()).await;
    ingest(&storage, 1_100_000_000, "unrelated error line", error_fields).await;

    let mut info_fields = BTreeMap::new();
    info_fields.insert("level".to_string(), "info".to_string());
    ingest(&storage, 1_200_000_000, "user logged in", info_fields).await;

    let app = loki::router(storage, Arc::new(vec!["level".to_string()]), None, None, pierre::auth::AuthTokens::new(vec![]));

    let uri = "/loki/api/v1/query_range?query=%7Blevel%3D%22error%22%7D%20%7C%3D%20%22timeout%22&start=0&end=2000000000";
    let req = Request::builder().uri(uri).body(Body::empty()).unwrap();
    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), 200);

    let json = body_json(response).await;
    assert_eq!(json["status"], "success");
    let result = json["data"]["result"].as_array().unwrap();
    assert_eq!(result.len(), 1, "only one stream (level=error), and only the line matching the |= filter");
    let values = result[0]["values"].as_array().unwrap();
    assert_eq!(values.len(), 1);
    assert_eq!(values[0][1], "payment timeout");
}

#[tokio::test]
async fn query_range_rejects_malformed_logql() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(Storage::open(dir.path()).await.unwrap());
    let app = loki::router(storage, Arc::new(vec![]), None, None, pierre::auth::AuthTokens::new(vec![]));

    let req = Request::builder()
        .uri("/loki/api/v1/query_range?query=not-a-selector&start=0&end=1000")
        .body(Body::empty())
        .unwrap();
    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), 400, "malformed LogQL must be a client error, not silently empty or a 500");
}
