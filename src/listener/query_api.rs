use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::middleware;
use axum::routing::get;
use axum::{Extension, Json, Router};
use serde::Serialize;

use crate::auth::AuthTokens;
use crate::storage::Storage;
use crate::{aggregate, query, textindex};

#[derive(Clone)]
struct AppState {
    storage: Arc<Storage>,
    textindex_bucket_duration: Duration,
}

pub fn router(storage: Arc<Storage>, textindex_bucket_duration: Duration, auth_tokens: AuthTokens) -> Router {
    Router::new()
        .route("/query/logs", get(logs_handler))
        .route("/query/search", get(search_handler))
        .route("/query/aggregate", get(aggregate_handler))
        .with_state(AppState { storage, textindex_bucket_duration })
        .layer(middleware::from_fn(crate::auth::require_bearer_token))
        .layer(Extension(auth_tokens))
}

pub async fn serve(addr: &str, storage: Arc<Storage>, textindex_bucket_duration: Duration, auth_tokens: AuthTokens) -> anyhow::Result<()> {
    let app = router(storage, textindex_bucket_duration, auth_tokens);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

/// `GET /query/logs?start=<ns>&end=<ns>&<field>=<value>...` — selector + time-range (FR-13).
/// Any query param besides `start`/`end` is treated as a field-equality filter.
async fn logs_handler(
    State(state): State<AppState>,
    Query(params): Query<BTreeMap<String, String>>,
) -> Result<Json<Vec<crate::record::Record>>, (StatusCode, String)> {
    let (start_ns, end_ns) = parse_range(&params)?;
    let mut filters = params;
    filters.remove("start");
    filters.remove("end");

    query::select(&state.storage, start_ns, end_ns, &filters)
        .await
        .map(Json)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
}

#[derive(Debug, Serialize)]
struct SearchHit {
    record: Option<crate::record::Record>,
    score: f32,
}

/// `GET /query/search?start=<ns>&end=<ns>&q=<text>&k=<n>` — BM25 line-filter (FR-14).
/// Resolves each hit's `doc_id` back to the full record (`record: null` if it was
/// somehow expired/removed between the search and the lookup — rare, not an error).
async fn search_handler(
    State(state): State<AppState>,
    Query(params): Query<BTreeMap<String, String>>,
) -> Result<Json<Vec<SearchHit>>, (StatusCode, String)> {
    let (start_ns, end_ns) = parse_range(&params)?;
    let query_text = params.get("q").ok_or((StatusCode::BAD_REQUEST, "missing `q` param".to_string()))?;
    let k: usize = params
        .get("k")
        .map(|s| s.parse())
        .transpose()
        .map_err(|_| (StatusCode::BAD_REQUEST, "invalid `k` param".to_string()))?
        .unwrap_or(10);

    let results = textindex::search(&state.storage, start_ns, end_ns, state.textindex_bucket_duration, query_text, k)
        .await
        .map_err(|e| {
            // "narrow the time range" is textindex::search's bucket-count-limit bail
            // message — a client-fixable input error, not a server fault.
            let status = if e.to_string().contains("narrow the time range") { StatusCode::BAD_REQUEST } else { StatusCode::INTERNAL_SERVER_ERROR };
            (status, e.to_string())
        })?;

    let mut hits = Vec::with_capacity(results.len());
    for r in results {
        let record = state
            .storage
            .get_record(&r.doc_id)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        hits.push(SearchHit { record, score: r.score });
    }
    Ok(Json(hits))
}

#[derive(Debug, Serialize)]
#[serde(untagged)]
enum AggregateResponse {
    Count(BTreeMap<String, u64>),
    Cardinality(f64),
    TopK(Vec<(String, u64)>),
    Quantile(f64),
}

/// `GET /query/aggregate?field=<name>&start=<ns>&end=<ns>&op=count|cardinality|topk|quantile&q=<0.0-1.0>&k=<n>`
/// (FR-17): served exclusively from pre-computed rollup sketches, never a raw rescan.
async fn aggregate_handler(
    State(state): State<AppState>,
    Query(params): Query<BTreeMap<String, String>>,
) -> Result<Json<AggregateResponse>, (StatusCode, String)> {
    let (start_ns, end_ns) = parse_range(&params)?;
    let field = params.get("field").ok_or((StatusCode::BAD_REQUEST, "missing `field` param".to_string()))?;
    let op = params.get("op").map(String::as_str).unwrap_or("count");

    let mut sketch = aggregate::merged_sketch(&state.storage, field, start_ns, end_ns)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .ok_or((StatusCode::NOT_FOUND, format!("no rollup data for field {field:?} in range")))?;

    match op {
        "count" => {
            let counts = sketch.exact_counts().ok_or((StatusCode::BAD_REQUEST, "field is not an exact-counter rollup".to_string()))?;
            Ok(Json(AggregateResponse::Count(counts)))
        }
        "cardinality" => {
            let estimate = sketch
                .hll_estimate()
                .ok_or((StatusCode::BAD_REQUEST, "field is not an hll rollup".to_string()))?;
            Ok(Json(AggregateResponse::Cardinality(estimate)))
        }
        "topk" => {
            let k: usize = params
                .get("k")
                .map(|s| s.parse())
                .transpose()
                .map_err(|_| (StatusCode::BAD_REQUEST, "invalid `k` param".to_string()))?
                .unwrap_or(10);
            let top = sketch.top_k(k).ok_or((StatusCode::BAD_REQUEST, "field is not a topk rollup".to_string()))?;
            Ok(Json(AggregateResponse::TopK(top)))
        }
        "quantile" => {
            let q: f64 = params
                .get("q")
                .ok_or((StatusCode::BAD_REQUEST, "missing `q` param for quantile".to_string()))?
                .parse()
                .map_err(|_| (StatusCode::BAD_REQUEST, "invalid `q` param".to_string()))?;
            let value = sketch.quantile(q).ok_or((StatusCode::BAD_REQUEST, "field is not a ddsketch rollup".to_string()))?;
            Ok(Json(AggregateResponse::Quantile(value)))
        }
        other => Err((StatusCode::BAD_REQUEST, format!("unknown op {other:?}"))),
    }
}

fn parse_range(params: &BTreeMap<String, String>) -> Result<(i64, i64), (StatusCode, String)> {
    let start_ns: i64 = params
        .get("start")
        .ok_or((StatusCode::BAD_REQUEST, "missing `start` param".to_string()))?
        .parse()
        .map_err(|_| (StatusCode::BAD_REQUEST, "invalid `start` param".to_string()))?;
    let end_ns: i64 = params
        .get("end")
        .ok_or((StatusCode::BAD_REQUEST, "missing `end` param".to_string()))?
        .parse()
        .map_err(|_| (StatusCode::BAD_REQUEST, "invalid `end` param".to_string()))?;
    Ok((start_ns, end_ns))
}
