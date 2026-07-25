use std::collections::BTreeMap;
use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use pierre::listener::es_bulk;
use pierre::query;
use pierre::storage::Storage;
use tower::ServiceExt;

fn router(storage: Arc<Storage>, allowed_fields: Vec<String>) -> axum::Router {
    es_bulk::router(
        storage,
        Arc::new(allowed_fields),
        None,
        None,
        pierre::auth::AuthTokens::new(vec![]),
        pierre::stats::IngestStats::default(),
    )
}

async fn body_json(response: axum::response::Response) -> serde_json::Value {
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

/// Real Elasticsearch `_bulk` NDJSON: an action-metadata line, then a document
/// line, repeated. This is exactly what Filebeat/Logstash/Fluent Bit's ES output
/// already send — verifies Pierre accepts it with zero collector-side change.
#[tokio::test]
async fn bulk_index_action_lands_in_the_same_store_as_native() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(Storage::open(dir.path()).await.unwrap());
    let app = router(storage.clone(), vec!["level".to_string()]);

    let ndjson = concat!(
        r#"{"index":{"_index":"logs-2026.07.25","_id":"1"}}"#,
        "\n",
        r#"{"message":"boom 500 after 42ms","level":"error","@timestamp":"2026-07-25T00:00:01.000Z"}"#,
        "\n",
    );

    let request = Request::builder()
        .method("POST")
        .uri("/_bulk")
        .header("content-type", "application/x-ndjson")
        .body(Body::from(ndjson))
        .unwrap();

    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    assert_eq!(json["errors"], false);
    assert_eq!(json["items"][0]["index"]["status"], 201);

    let mut filter = BTreeMap::new();
    filter.insert("level".to_string(), "error".to_string());
    let results = query::select(&storage, 0, 2_000_000_000_000_000_000, &filter)
        .await
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].message, "boom 500 after 42ms");
}

/// The index-scoped form (`POST /:index/_bulk`) real clients also send — the
/// index name itself is discarded, Pierre has no ES-style index concept.
#[tokio::test]
async fn index_scoped_bulk_path_is_also_accepted() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(Storage::open(dir.path()).await.unwrap());
    let app = router(storage.clone(), vec![]);

    let ndjson = concat!(
        r#"{"create":{}}"#,
        "\n",
        r#"{"message":"hello from create"}"#,
        "\n",
    );

    let request = Request::builder()
        .method("POST")
        .uri("/logs-2026.07.25/_bulk")
        .body(Body::from(ndjson))
        .unwrap();

    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let results = query::select(&storage, 0, i64::MAX, &BTreeMap::new())
        .await
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].message, "hello from create");
}

/// `delete` actions carry no document line — must not be treated as an error or
/// consume the next action line as a (nonexistent) document.
#[tokio::test]
async fn delete_action_has_no_document_line_and_does_not_misalign_parsing() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(Storage::open(dir.path()).await.unwrap());
    let app = router(storage.clone(), vec![]);

    let ndjson = concat!(
        r#"{"delete":{"_id":"1"}}"#,
        "\n",
        r#"{"index":{}}"#,
        "\n",
        r#"{"message":"the only real document"}"#,
        "\n",
    );

    let request = Request::builder()
        .method("POST")
        .uri("/_bulk")
        .body(Body::from(ndjson))
        .unwrap();
    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    assert_eq!(
        json["items"].as_array().unwrap().len(),
        2,
        "delete + index must both produce one item each"
    );

    let results = query::select(&storage, 0, i64::MAX, &BTreeMap::new())
        .await
        .unwrap();
    assert_eq!(
        results.len(),
        1,
        "only the index action's document should be committed"
    );
    assert_eq!(results[0].message, "the only real document");
}

/// No `message`/`msg`/`@timestamp` at all — must not error or drop the document;
/// falls back to the whole doc serialized as the message and wall-clock time.
#[tokio::test]
async fn document_without_message_field_falls_back_to_whole_document() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(Storage::open(dir.path()).await.unwrap());
    let app = router(storage.clone(), vec![]);

    let ndjson = concat!(r#"{"index":{}}"#, "\n", r#"{"foo":"bar","count":42}"#, "\n",);

    let request = Request::builder()
        .method("POST")
        .uri("/_bulk")
        .body(Body::from(ndjson))
        .unwrap();
    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let now_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as i64;
    let results = query::select(
        &storage,
        now_ns - 10_000_000_000,
        now_ns + 10_000_000_000,
        &BTreeMap::new(),
    )
    .await
    .unwrap();
    assert_eq!(results.len(), 1);
    assert!(
        results[0].message.contains("foo") && results[0].message.contains("bar"),
        "message should fall back to the serialized document, got: {}",
        results[0].message
    );
}
