use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::Request;
use pierre::auth::AuthTokens;
use pierre::listener::query_api;
use pierre::storage::Storage;
use tower::ServiceExt;

#[tokio::test]
async fn no_configured_tokens_means_auth_is_off() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(Storage::open(dir.path()).await.unwrap());
    let app = query_api::router(storage, Duration::from_secs(3600), AuthTokens::new(vec![]));

    let req = Request::builder()
        .uri("/query/logs?start=0&end=1")
        .body(Body::empty())
        .unwrap();
    let response = app.oneshot(req).await.unwrap();
    assert_eq!(
        response.status(),
        200,
        "no tokens configured must mean no auth enforcement at all"
    );
}

#[tokio::test]
async fn configured_tokens_reject_missing_authorization_header() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(Storage::open(dir.path()).await.unwrap());
    let app = query_api::router(
        storage,
        Duration::from_secs(3600),
        AuthTokens::new(vec!["secret-token".to_string()]),
    );

    let req = Request::builder()
        .uri("/query/logs?start=0&end=1")
        .body(Body::empty())
        .unwrap();
    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), 401);
}

#[tokio::test]
async fn configured_tokens_reject_wrong_token() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(Storage::open(dir.path()).await.unwrap());
    let app = query_api::router(
        storage,
        Duration::from_secs(3600),
        AuthTokens::new(vec!["secret-token".to_string()]),
    );

    let req = Request::builder()
        .uri("/query/logs?start=0&end=1")
        .header("authorization", "Bearer wrong-token")
        .body(Body::empty())
        .unwrap();
    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), 401);
}

#[tokio::test]
async fn configured_tokens_accept_the_correct_token() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(Storage::open(dir.path()).await.unwrap());
    let app = query_api::router(
        storage,
        Duration::from_secs(3600),
        AuthTokens::new(vec!["secret-token".to_string()]),
    );

    let req = Request::builder()
        .uri("/query/logs?start=0&end=1")
        .header("authorization", "Bearer secret-token")
        .body(Body::empty())
        .unwrap();
    let response = app.oneshot(req).await.unwrap();
    assert_eq!(response.status(), 200);
}

#[tokio::test]
async fn loki_router_enforces_the_same_auth() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(Storage::open(dir.path()).await.unwrap());
    let app = pierre::listener::loki::router(
        storage,
        Arc::new(vec![]),
        None,
        None,
        AuthTokens::new(vec!["secret-token".to_string()]),
        pierre::stats::IngestStats::default(),
    );

    let req = Request::builder()
        .uri("/loki/api/v1/query_range?query={}&start=0&end=1")
        .body(Body::empty())
        .unwrap();
    let response = app.clone().oneshot(req).await.unwrap();
    assert_eq!(
        response.status(),
        401,
        "loki router must enforce the same bearer-token check"
    );

    let req = Request::builder()
        .uri("/loki/api/v1/query_range?query={}&start=0&end=1")
        .header("authorization", "Bearer secret-token")
        .body(Body::empty())
        .unwrap();
    let response = app.oneshot(req).await.unwrap();
    assert_ne!(response.status(), 401);
}
