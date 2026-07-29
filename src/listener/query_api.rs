use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Json, Router};
use serde::Serialize;

use crate::auth::AuthTokens;
use crate::config::EmbeddingConfig;
use crate::listener::parse_range;
use crate::rollup::RollupHandle;
use crate::stats::IngestStats;
use crate::storage::Storage;
use crate::textindex::TextIndexHandle;
use crate::{aggregate, embedding, query, textindex};

#[derive(Clone)]
struct AppState {
    storage: Arc<Storage>,
    textindex_bucket_duration: Duration,
    stats: IngestStats,
    rollup: Option<RollupHandle>,
    textindex_handle: Option<TextIndexHandle>,
    embedding_config: Option<EmbeddingConfig>,
}

pub fn router(
    storage: Arc<Storage>,
    textindex_bucket_duration: Duration,
    auth_tokens: AuthTokens,
    stats: IngestStats,
    rollup: Option<RollupHandle>,
    textindex_handle: Option<TextIndexHandle>,
    embedding_config: Option<EmbeddingConfig>,
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
            embedding_config,
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
    embedding_config: Option<EmbeddingConfig>,
) -> anyhow::Result<()> {
    let app = router(
        storage,
        textindex_bucket_duration,
        auth_tokens,
        stats,
        rollup,
        textindex_handle,
        embedding_config,
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

/// `GET /query/search?start=<ns>&end=<ns>&q=<text>&k=<n>[&mode=hybrid]`
///
/// Default `mode=text` (BM25 only, FR-14). `mode=hybrid` runs BM25 + vector
/// search in parallel, fuses via RRF (k=60), and returns the merged ranking.
/// Hybrid falls back to text-only if embedding is not configured.
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
        .unwrap_or(10)
        .min(1000);
    let hybrid = params.get("mode").map(String::as_str) == Some("hybrid");

    // Text search (always) and query embedding (only for hybrid) run in parallel.
    let text_fut = textindex::search(
        &state.storage,
        start_ns,
        end_ns,
        state.textindex_bucket_duration,
        query_text,
        k * 2, // fetch more candidates for RRF
    );

    let embed_fut = async {
        if hybrid {
            if let Some(cfg) = &state.embedding_config {
                return embedding::embed_query(cfg, query_text).await;
            }
        }
        None
    };

    let (text_results, query_embedding) = tokio::join!(text_fut, embed_fut);

    let text_results = text_results.map_err(|e| {
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

    // If hybrid and we have an embedding, run vector search and fuse.
    let ranked_keys: Vec<(Vec<u8>, f32)> = if hybrid {
        if let (Some(cfg), Some(qdata)) = (&state.embedding_config, query_embedding) {
            let vec_results = state
                .storage
                .vector_search(cfg.dims, qdata, k * 2)
                .await
                .map_err(|e| {
                    log::warn!("search_handler: vector_search failed: {e}");
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "internal error".to_string(),
                    )
                })?;
            rrf_fuse(&text_results, &vec_results, k)
        } else {
            // Embedding disabled or query embedding failed — fall back to text.
            text_results
                .iter()
                .map(|r| (r.doc_id.clone(), r.score))
                .take(k)
                .collect()
        }
    } else {
        text_results
            .iter()
            .map(|r| (r.doc_id.clone(), r.score))
            .take(k)
            .collect()
    };

    let mut hits = Vec::with_capacity(ranked_keys.len());
    for (key, score) in ranked_keys {
        let record = state.storage.get_record(&key).await.map_err(|e| {
            log::warn!("search_handler: get_record failed: {e}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal error".to_string(),
            )
        })?;
        hits.push(SearchHit { record, score });
    }
    Ok(Json(hits))
}

/// Reciprocal Rank Fusion of BM25 and vector results, returning top-k.
/// `score(doc) = Σ 1/(60 + rank)` summed over both lists.
fn rrf_fuse(
    text: &[edgestore::TextSearchResult],
    vector: &[(Vec<u8>, f32)],
    k: usize,
) -> Vec<(Vec<u8>, f32)> {
    use std::collections::HashMap;
    const RRF_K: f32 = 60.0;

    let mut scores: HashMap<&[u8], f32> = HashMap::new();

    for (rank, r) in text.iter().enumerate() {
        *scores.entry(r.doc_id.as_slice()).or_default() += 1.0 / (RRF_K + rank as f32 + 1.0);
    }
    for (rank, (key, _dist)) in vector.iter().enumerate() {
        *scores.entry(key.as_slice()).or_default() += 1.0 / (RRF_K + rank as f32 + 1.0);
    }

    let mut ranked: Vec<(Vec<u8>, f32)> =
        scores.into_iter().map(|(k, s)| (k.to_vec(), s)).collect();
    ranked.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    ranked.truncate(k);
    ranked
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
