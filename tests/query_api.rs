use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::Request;
use pierre::record::WireRecord;
use pierre::rollup::RollupKind;
use pierre::storage::Storage;
use tower::ServiceExt;

async fn body_json(response: axum::response::Response) -> serde_json::Value {
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

#[tokio::test]
async fn logs_endpoint_answers_selector_and_time_range() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(Storage::open(dir.path()).await.unwrap());
    let allowed_fields = vec!["level".to_string()];

    let mut fields = BTreeMap::new();
    fields.insert("level".to_string(), "error".to_string());
    let wire = WireRecord {
        timestamp_ns: 1_000_000_000,
        message: "boom".to_string(),
        fields,
    };
    pierre::ingest::commit(&storage, wire, &allowed_fields, None, None)
        .await
        .unwrap();

    let app = pierre::listener::query_api::router(
        storage.clone(),
        Duration::from_secs(3600),
        pierre::auth::AuthTokens::new(vec![]),
    );

    let req = Request::builder()
        .uri("/query/logs?start=0&end=2000000000&level=error")
        .body(Body::empty())
        .unwrap();
    let response = app.clone().oneshot(req).await.unwrap();
    assert_eq!(response.status(), 200);
    let json = body_json(response).await;
    assert_eq!(json.as_array().unwrap().len(), 1);
    assert_eq!(json[0]["message"], "boom");

    // Selector that doesn't match must return empty, not an error.
    let req = Request::builder()
        .uri("/query/logs?start=0&end=2000000000&level=info")
        .body(Body::empty())
        .unwrap();
    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), 200);
    let json = body_json(response).await;
    assert!(json.as_array().unwrap().is_empty());
}

#[tokio::test]
async fn logs_endpoint_rejects_missing_range_params() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(Storage::open(dir.path()).await.unwrap());
    let app = pierre::listener::query_api::router(
        storage,
        Duration::from_secs(3600),
        pierre::auth::AuthTokens::new(vec![]),
    );

    let req = Request::builder()
        .uri("/query/logs?start=0")
        .body(Body::empty())
        .unwrap();
    let response = app.oneshot(req).await.unwrap();
    assert_eq!(
        response.status(),
        400,
        "missing `end` must be a client error, not a 500 or silent empty result"
    );
}

#[tokio::test]
async fn search_endpoint_returns_full_record_for_each_hit() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(Storage::open(dir.path()).await.unwrap());
    let bucket_duration = Duration::from_secs(3600);
    let (textindex, _worker) =
        pierre::textindex::spawn(storage.clone(), bucket_duration, Duration::from_millis(50));

    let wire = WireRecord {
        timestamp_ns: 1_000_000_000,
        message: "payment gateway timeout detected".to_string(),
        fields: BTreeMap::new(),
    };
    pierre::ingest::commit(&storage, wire, &[], None, Some(&textindex))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;

    let app = pierre::listener::query_api::router(
        storage,
        bucket_duration,
        pierre::auth::AuthTokens::new(vec![]),
    );
    let req = Request::builder()
        .uri("/query/search?start=0&end=2000000000&q=timeout&k=10")
        .body(Body::empty())
        .unwrap();
    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), 200);
    let json = body_json(response).await;
    let hits = json.as_array().unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(
        hits[0]["record"]["message"],
        "payment gateway timeout detected"
    );
}

#[tokio::test]
async fn aggregate_endpoint_serves_exact_count_from_rollup_sketch() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(Storage::open(dir.path()).await.unwrap());
    let allowed_fields = vec!["level".to_string()];
    let field_kinds = HashMap::from([("level".to_string(), RollupKind::Exact)]);
    let tiers = pierre::rollup::worker::TierConfig {
        minute_duration: Duration::from_millis(50),
        hour_duration: Duration::from_secs(3600),
        day_duration: Duration::from_secs(3600),
        month_duration: Duration::from_secs(3600),
        minute_ttl_secs: 3600,
        hour_ttl_secs: 3600,
        day_ttl_secs: 3600,
        month_ttl_secs: 3600,
    };
    let rollup = pierre::rollup::spawn(storage.clone(), field_kinds, tiers);

    for level in ["error", "error", "info"] {
        let mut fields = BTreeMap::new();
        fields.insert("level".to_string(), level.to_string());
        let wire = WireRecord {
            timestamp_ns: 1,
            message: "x".to_string(),
            fields,
        };
        pierre::ingest::commit(&storage, wire, &allowed_fields, Some(&rollup), None)
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(200)).await;

    // The rollup worker buckets by *wall-clock* time when it processes a sample
    // (`now_ns()` inside the worker), not by the record's own `timestamp_ns` — so the
    // query window here must bracket real "now", not the record's (irrelevant) 1ns
    // timestamp. Keep the span within the minute-tier routing threshold (<= 1 hour,
    // see aggregate::merged_sketch) — that's the only tier this test's short sleep
    // gives time to populate (hour/day/month ticks are on a 1-hour timer here).
    let app = pierre::listener::query_api::router(
        storage,
        Duration::from_secs(3600),
        pierre::auth::AuthTokens::new(vec![]),
    );
    let now_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as i64;
    let req = Request::builder()
        .uri(format!(
            "/query/aggregate?field=level&start={}&end={}&op=count",
            now_ns - 10_000_000_000,
            now_ns + 10_000_000_000
        ))
        .body(Body::empty())
        .unwrap();
    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), 200);
    let json = body_json(response).await;
    assert_eq!(json["error"], 2);
    assert_eq!(json["info"], 1);
}

#[tokio::test]
async fn aggregate_endpoint_404s_when_no_rollup_data_in_range() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(Storage::open(dir.path()).await.unwrap());
    let app = pierre::listener::query_api::router(
        storage,
        Duration::from_secs(3600),
        pierre::auth::AuthTokens::new(vec![]),
    );

    let req = Request::builder()
        .uri("/query/aggregate?field=level&start=0&end=1000&op=count")
        .body(Body::empty())
        .unwrap();
    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), 404);
}
