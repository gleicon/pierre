use std::collections::BTreeMap;
use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};

use crate::auth::AuthTokens;
use crate::listener::parse_range;
use crate::record::WireRecord;
use crate::rollup::RollupHandle;
use crate::storage::Storage;
use crate::textindex::TextIndexHandle;

#[derive(Clone)]
struct AppState {
    storage: Arc<Storage>,
    allowed_fields: Arc<Vec<String>>,
    rollup: Option<RollupHandle>,
    textindex: Option<TextIndexHandle>,
}

/// Loki's push API request shape: one entry per label set, each with its own
/// `[timestamp_ns_string, line]` pairs. Labels double as Pierre's typed fields,
/// filtered through the same allowlist the native listener uses.
#[derive(Debug, Deserialize)]
struct LokiPushRequest {
    streams: Vec<LokiStream>,
}

#[derive(Debug, Deserialize)]
struct LokiStream {
    stream: BTreeMap<String, String>,
    values: Vec<(String, String)>,
}

pub fn router(
    storage: Arc<Storage>,
    allowed_fields: Arc<Vec<String>>,
    rollup: Option<RollupHandle>,
    textindex: Option<TextIndexHandle>,
    auth_tokens: AuthTokens,
) -> Router {
    let router = Router::new()
        .route("/loki/api/v1/push", post(push_handler))
        .route("/loki/api/v1/query_range", get(query_range_handler))
        .with_state(AppState { storage, allowed_fields, rollup, textindex });
    crate::auth::layer(router, auth_tokens)
}

pub async fn serve(
    addr: &str,
    storage: Arc<Storage>,
    allowed_fields: Arc<Vec<String>>,
    rollup: Option<RollupHandle>,
    textindex: Option<TextIndexHandle>,
    auth_tokens: AuthTokens,
) -> anyhow::Result<()> {
    let app = router(storage, allowed_fields, rollup, textindex, auth_tokens);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

/// Real collectors (Promtail, Alloy, Vector, Fluent Bit's loki output) default to
/// Loki's protobuf+raw-snappy wire format, not JSON — confirmed by an actual Promtail
/// container rejecting a JSON-only endpoint with a 415 in this project's own e2e
/// test. Dispatch on Content-Type the same way Loki's own server does: JSON only
/// when explicitly declared, protobuf+snappy otherwise (including when the header
/// is absent entirely).
async fn push_handler(State(state): State<AppState>, headers: HeaderMap, body: Bytes) -> Result<StatusCode, StatusCode> {
    let is_json = headers
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|ct| ct.starts_with("application/json"));

    let streams: Vec<(BTreeMap<String, String>, Vec<(i64, String)>)> = if is_json {
        let req: LokiPushRequest = serde_json::from_slice(&body).map_err(|_| StatusCode::BAD_REQUEST)?;
        req.streams
            .into_iter()
            .map(|s| {
                let entries = s
                    .values
                    .into_iter()
                    .map(|(ts, line)| ts.parse::<i64>().map(|ts| (ts, line)))
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|_| StatusCode::BAD_REQUEST)?;
                Ok((s.stream, entries))
            })
            .collect::<Result<Vec<_>, StatusCode>>()?
    } else {
        crate::lokiproto::decode_push_request(&body)
            .map_err(|_| StatusCode::BAD_REQUEST)?
            .into_iter()
            .map(|s| (s.labels, s.entries))
            .collect()
    };

    for (labels, entries) in streams {
        for (timestamp_ns, line) in entries {
            let wire = WireRecord { timestamp_ns, message: line, fields: labels.clone() };
            crate::ingest::commit(&state.storage, wire, &state.allowed_fields, state.rollup.as_ref(), state.textindex.as_ref())
                .await
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        }
    }
    // Real Loki returns 204 with no body on a successful push.
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Debug, Serialize)]
struct LokiQueryResponse {
    status: &'static str,
    data: LokiQueryData,
}

#[derive(Debug, Serialize)]
struct LokiQueryData {
    #[serde(rename = "resultType")]
    result_type: &'static str,
    result: Vec<LokiResultStream>,
}

#[derive(Debug, Serialize)]
struct LokiResultStream {
    stream: BTreeMap<String, String>,
    values: Vec<(String, String)>,
}

/// `GET /loki/api/v1/query_range?query={label="value"} |= "text"&start=<ns>&end=<ns>` —
/// the deliberate LogQL subset (FR-13/FR-14; full LogQL is a non-goal). Matches real
/// Loki's response shape (`status`/`data.resultType`/`data.result[].stream|values`) so
/// Grafana's Loki datasource can parse it, without implementing LogQL's metric-query
/// planner.
async fn query_range_handler(
    State(state): State<AppState>,
    Query(params): Query<BTreeMap<String, String>>,
) -> Result<Json<LokiQueryResponse>, (StatusCode, String)> {
    let query_str = params.get("query").ok_or((StatusCode::BAD_REQUEST, "missing `query` param".to_string()))?;
    let parsed = crate::logql::parse(query_str).map_err(|e| (StatusCode::BAD_REQUEST, e))?;
    let (start_ns, end_ns) = parse_range(&params)?;

    let records = crate::query::select(&state.storage, start_ns, end_ns, &parsed.selector)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let matching = records
        .into_iter()
        .filter(|r| parsed.line_filter.as_ref().is_none_or(|f| r.message.contains(f.as_str())));

    // Group by the record's full field set, matching Loki's "one entry per label
    // combination" stream model.
    let mut streams: BTreeMap<Vec<(String, String)>, Vec<(String, String)>> = BTreeMap::new();
    for r in matching {
        let key: Vec<(String, String)> = r.fields.into_iter().collect();
        streams.entry(key).or_default().push((r.timestamp_ns.to_string(), r.message));
    }

    let result = streams
        .into_iter()
        .map(|(labels, values)| LokiResultStream { stream: labels.into_iter().collect(), values })
        .collect();

    Ok(Json(LokiQueryResponse {
        status: "success",
        data: LokiQueryData { result_type: "streams", result },
    }))
}
