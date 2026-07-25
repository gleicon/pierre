use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::post;
use axum::{Json, Router};
use serde::Serialize;
use serde_json::{Map, Value};

use crate::auth::AuthTokens;
use crate::record::WireRecord;
use crate::rollup::RollupHandle;
use crate::stats::IngestStats;
use crate::storage::Storage;
use crate::textindex::TextIndexHandle;

#[derive(Clone)]
struct AppState {
    storage: Arc<Storage>,
    allowed_fields: Arc<Vec<String>>,
    rollup: Option<RollupHandle>,
    textindex: Option<TextIndexHandle>,
    stats: IngestStats,
}

/// Elasticsearch's actual wire format: real shippers (Filebeat, Logstash, Fluent
/// Bit's ES output, Vector's ES sink — "the single largest installed base of
/// shippers on earth" per the PRD) already speak this without any pipeline change.
/// Accepts both `POST /_bulk` and `POST /:index/_bulk` (the index-scoped form real
/// clients also send; the index name itself isn't used for anything — Pierre has
/// no concept of an ES-style index).
pub fn router(
    storage: Arc<Storage>,
    allowed_fields: Arc<Vec<String>>,
    rollup: Option<RollupHandle>,
    textindex: Option<TextIndexHandle>,
    auth_tokens: AuthTokens,
    stats: IngestStats,
) -> Router {
    let router = Router::new()
        .route("/_bulk", post(bulk_handler))
        .route("/{index}/_bulk", post(bulk_handler))
        .with_state(AppState {
            storage,
            allowed_fields,
            rollup,
            textindex,
            stats,
        });
    crate::auth::layer(router, auth_tokens)
}

pub async fn serve(
    addr: &str,
    storage: Arc<Storage>,
    allowed_fields: Arc<Vec<String>>,
    rollup: Option<RollupHandle>,
    textindex: Option<TextIndexHandle>,
    auth_tokens: AuthTokens,
    stats: IngestStats,
) -> anyhow::Result<()> {
    let app = router(
        storage,
        allowed_fields,
        rollup,
        textindex,
        auth_tokens,
        stats,
    );
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

#[derive(Debug, Serialize)]
struct BulkResponse {
    took: u64,
    errors: bool,
    items: Vec<BulkItemResult>,
}

#[derive(Debug, Serialize)]
struct BulkItemResult {
    index: BulkItemStatus,
}

#[derive(Debug, Serialize)]
struct BulkItemStatus {
    status: u16,
}

/// NDJSON body: alternating action-metadata and document lines (`index`/`create`
/// action lines are followed by a document line; `delete` has none — see the ES
/// bulk API spec). Pierre has no index/mapping concept, so the only thing read out
/// of the action line is which variant it is, to know whether a document line
/// follows.
async fn bulk_handler(
    State(state): State<AppState>,
    body: Bytes,
) -> Result<Json<BulkResponse>, (StatusCode, String)> {
    let text = std::str::from_utf8(&body).map_err(|_| {
        (
            StatusCode::BAD_REQUEST,
            "body is not valid UTF-8".to_string(),
        )
    })?;
    let mut lines = text.lines().filter(|l| !l.trim().is_empty());
    let mut items = Vec::new();

    while let Some(action_line) = lines.next() {
        let action: Value = serde_json::from_str(action_line)
            .map_err(|e| (StatusCode::BAD_REQUEST, format!("invalid action line: {e}")))?;
        let action_obj = action.as_object().ok_or((
            StatusCode::BAD_REQUEST,
            "action line must be a JSON object".to_string(),
        ))?;

        // `delete` carries no document line; every other action (`index`, `create`,
        // `update`) is followed by exactly one.
        if action_obj.contains_key("delete") {
            items.push(BulkItemResult {
                index: BulkItemStatus { status: 200 },
            });
            continue;
        }

        let doc_line = lines.next().ok_or((
            StatusCode::BAD_REQUEST,
            "action line with no following document line".to_string(),
        ))?;
        let doc: Value = serde_json::from_str(doc_line).map_err(|e| {
            (
                StatusCode::BAD_REQUEST,
                format!("invalid document line: {e}"),
            )
        })?;
        let doc = doc.as_object().ok_or((
            StatusCode::BAD_REQUEST,
            "document line must be a JSON object".to_string(),
        ))?;

        let wire = wire_record_from_doc(doc);
        crate::ingest::commit(
            &state.storage,
            wire,
            &state.allowed_fields,
            state.rollup.as_ref(),
            state.textindex.as_ref(),
        )
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        state.stats.record_commit();

        items.push(BulkItemResult {
            index: BulkItemStatus { status: 201 },
        });
    }

    Ok(Json(BulkResponse {
        took: 0,
        errors: false,
        items,
    }))
}

/// `message`/`msg` becomes the log line (checked in that order, matching common
/// shipper conventions — Filebeat/Logstash use `message`); `@timestamp` (RFC3339,
/// the ES/Beats convention) becomes the record's time, defaulting to now if absent
/// or unparseable. Every other scalar top-level field becomes a candidate field
/// (subject to the same allowlist every other listener already filters through);
/// nested objects/arrays are skipped rather than flattened, matching this
/// listener's job of wire-format translation, not a schema mapper.
fn wire_record_from_doc(doc: &Map<String, Value>) -> WireRecord {
    let message = doc
        .get("message")
        .or_else(|| doc.get("msg"))
        .and_then(Value::as_str)
        .map(str::to_string)
        .unwrap_or_else(|| Value::Object(doc.clone()).to_string());

    let timestamp_ns = doc
        .get("@timestamp")
        .and_then(Value::as_str)
        .and_then(|s| s.parse::<jiff::Timestamp>().ok())
        .map(|t| t.as_nanosecond() as i64)
        .unwrap_or_else(|| jiff::Timestamp::now().as_nanosecond() as i64);

    let mut fields = std::collections::BTreeMap::new();
    for (key, value) in doc {
        if key == "message" || key == "msg" || key == "@timestamp" {
            continue;
        }
        let value_str = match value {
            Value::String(s) => s.clone(),
            Value::Number(n) => n.to_string(),
            Value::Bool(b) => b.to_string(),
            Value::Null | Value::Array(_) | Value::Object(_) => continue,
        };
        fields.insert(key.clone(), value_str);
    }

    WireRecord {
        timestamp_ns,
        message,
        fields,
    }
}
