use std::collections::BTreeMap;
use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use pierre::listener::loki;
use pierre::query;
use pierre::storage::Storage;
use tower::ServiceExt;

#[tokio::test]
async fn loki_push_lands_in_the_same_store_as_native() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(Storage::open(dir.path()).await.unwrap());
    let allowed_fields = Arc::new(vec!["level".to_string()]);

    let app = loki::router(
        storage.clone(),
        allowed_fields,
        None,
        None,
        pierre::auth::AuthTokens::new(vec![]),
        pierre::stats::IngestStats::default(),
    );

    let body = serde_json::json!({
        "streams": [
            {
                "stream": { "level": "error" },
                "values": [
                    ["1000000000", "boom 500 after 42ms"]
                ]
            }
        ]
    });

    let request = Request::builder()
        .method("POST")
        .uri("/loki/api/v1/push")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap();

    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let mut filter = BTreeMap::new();
    filter.insert("level".to_string(), "error".to_string());
    let results = query::select(&storage, 0, 2_000_000_000, &filter)
        .await
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].message, "boom 500 after 42ms");
}
