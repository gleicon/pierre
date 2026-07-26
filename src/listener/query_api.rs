use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Json, Router};
use serde::Serialize;

use crate::auth::AuthTokens;
use crate::listener::parse_range;
use crate::rollup::RollupHandle;
use crate::stats::IngestStats;
use crate::storage::Storage;
use crate::textindex::TextIndexHandle;
use crate::{aggregate, query, textindex};

#[derive(Clone)]
struct AppState {
    storage: Arc<Storage>,
    textindex_bucket_duration: Duration,
    stats: IngestStats,
    rollup: Option<RollupHandle>,
    textindex_handle: Option<TextIndexHandle>,
}

pub fn router(
    storage: Arc<Storage>,
    textindex_bucket_duration: Duration,
    auth_tokens: AuthTokens,
    stats: IngestStats,
    rollup: Option<RollupHandle>,
    textindex_handle: Option<TextIndexHandle>,
) -> Router {
    let router = Router::new()
        .route("/query/logs", get(logs_handler))
        .route("/query/search", get(search_handler))
        .route("/query/aggregate", get(aggregate_handler))
        .route("/metrics", get(metrics_handler))
        .with_state(AppState {
            storage,
            textindex_bucket_duration,
            stats,
            rollup,
            textindex_handle,
        });
    crate::auth::layer(router, auth_tokens)
}

pub async fn serve(
    addr: &str,
    storage: Arc<Storage>,
    textindex_bucket_duration: Duration,
    auth_tokens: AuthTokens,
    stats: IngestStats,
    rollup: Option<RollupHandle>,
    textindex_handle: Option<TextIndexHandle>,
) -> anyhow::Result<()> {
    let app = router(
        storage,
        textindex_bucket_duration,
        auth_tokens,
        stats,
        rollup,
        textindex_handle,
    );
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
        .map_err(|e| {
            log::warn!("logs_handler: query::select failed: {e}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal error".to_string(),
            )
        })
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
    let query_text = params
        .get("q")
        .ok_or((StatusCode::BAD_REQUEST, "missing `q` param".to_string()))?;
    let k: usize = params
        .get("k")
        .map(|s| s.parse())
        .transpose()
        .map_err(|_| (StatusCode::BAD_REQUEST, "invalid `k` param".to_string()))?
        .unwrap_or(10);

    let results = textindex::search(
        &state.storage,
        start_ns,
        end_ns,
        state.textindex_bucket_duration,
        query_text,
        k,
    )
    .await
    .map_err(|e| {
        // "narrow the time range" is textindex::search's bucket-count-limit bail
        // message — a client-fixable input error, safe and useful to return
        // verbatim, unlike a genuine server-side fault below.
        let text = e.to_string();
        if text.contains("narrow the time range") {
            (StatusCode::BAD_REQUEST, text)
        } else {
            log::warn!("search_handler: textindex::search failed: {text}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal error".to_string(),
            )
        }
    })?;

    let mut hits = Vec::with_capacity(results.len());
    for r in results {
        let record = state.storage.get_record(&r.doc_id).await.map_err(|e| {
            log::warn!("search_handler: get_record failed: {e}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal error".to_string(),
            )
        })?;
        hits.push(SearchHit {
            record,
            score: r.score,
        });
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
    let field = params
        .get("field")
        .ok_or((StatusCode::BAD_REQUEST, "missing `field` param".to_string()))?;
    let op = params.get("op").map(String::as_str).unwrap_or("count");

    let mut sketch = aggregate::merged_sketch(&state.storage, field, start_ns, end_ns)
        .await
        .map_err(|e| {
            log::warn!("aggregate_handler: merged_sketch failed: {e}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal error".to_string(),
            )
        })?
        .ok_or((
            StatusCode::NOT_FOUND,
            format!("no rollup data for field {field:?} in range"),
        ))?;

    match op {
        "count" => {
            let counts = sketch.exact_counts().ok_or((
                StatusCode::BAD_REQUEST,
                "field is not an exact-counter rollup".to_string(),
            ))?;
            Ok(Json(AggregateResponse::Count(counts)))
        }
        "cardinality" => {
            let estimate = sketch.hll_estimate().ok_or((
                StatusCode::BAD_REQUEST,
                "field is not an hll rollup".to_string(),
            ))?;
            Ok(Json(AggregateResponse::Cardinality(estimate)))
        }
        "topk" => {
            let k: usize = params
                .get("k")
                .map(|s| s.parse())
                .transpose()
                .map_err(|_| (StatusCode::BAD_REQUEST, "invalid `k` param".to_string()))?
                .unwrap_or(10);
            let top = sketch.top_k(k).ok_or((
                StatusCode::BAD_REQUEST,
                "field is not a topk rollup".to_string(),
            ))?;
            Ok(Json(AggregateResponse::TopK(top)))
        }
        "quantile" => {
            let q: f64 = params
                .get("q")
                .ok_or((
                    StatusCode::BAD_REQUEST,
                    "missing `q` param for quantile".to_string(),
                ))?
                .parse()
                .map_err(|_| (StatusCode::BAD_REQUEST, "invalid `q` param".to_string()))?;
            let value = sketch.quantile(q).ok_or((
                StatusCode::BAD_REQUEST,
                "field is not a ddsketch rollup".to_string(),
            ))?;
            Ok(Json(AggregateResponse::Quantile(value)))
        }
        other => Err((StatusCode::BAD_REQUEST, format!("unknown op {other:?}"))),
    }
}

/// `GET /metrics` — Prometheus text exposition format (same counters as the
/// periodic stats log line, see `stats.rs`), so a scraper gets the same live
/// signal without tailing logs. Deliberately just the three counters that
/// already exist; not a general observability surface.
async fn metrics_handler(
    State(state): State<AppState>,
) -> (StatusCode, [(&'static str, &'static str); 1], String) {
    let ingest_total = state.stats.committed_count();
    let rollup_dropped = state
        .rollup
        .as_ref()
        .map(RollupHandle::dropped_count)
        .unwrap_or(0);
    let textindex_dropped = state
        .textindex_handle
        .as_ref()
        .map(TextIndexHandle::dropped_count)
        .unwrap_or(0);

    let body = format!(
        "# HELP pierre_ingest_records_total Total records committed via ingest.\n\
         # TYPE pierre_ingest_records_total counter\n\
         pierre_ingest_records_total {ingest_total}\n\
         # HELP pierre_rollup_dropped_total Total rollup contributions dropped because the channel was full.\n\
         # TYPE pierre_rollup_dropped_total counter\n\
         pierre_rollup_dropped_total {rollup_dropped}\n\
         # HELP pierre_textindex_dropped_total Total textindex contributions dropped because the channel was full.\n\
         # TYPE pierre_textindex_dropped_total counter\n\
         pierre_textindex_dropped_total {textindex_dropped}\n"
    );
    (
        StatusCode::OK,
        [("content-type", "text/plain; version=0.0.4")],
        body,
    )
}
