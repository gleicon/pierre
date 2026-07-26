//! MCP (Model Context Protocol) server — PRD v0.2 Block A-1. Read-only agent-facing
//! tools built on the official `rmcp` SDK (github.com/modelcontextprotocol/rust-sdk),
//! mounted as a Streamable HTTP service (`src/listener/mcp.rs`), the same way every
//! other Pierre HTTP surface mounts on axum.
//!
//! Scope note: several PRD design constraints (real query-cost accounting via
//! edgestore's `QueryStats`, BM25 position-based snippets, per-query scan budgets)
//! depend on edgestore capabilities that exist on the *sync* `Engine`/`TieredEngine`
//! as of 1.5.0 but aren't yet wrapped through `edgestore-tokio`'s `AsyncTieredEngine`
//! — the same async-wrapping gap every previous edgestore feature has needed closed
//! before Pierre could use it (`put_with_ttl`, `index_text`, etc. all needed their
//! own addition first). Not touched here — edgestore stays hands-off per
//! DECISIONS.md. This module uses only what's already async-wrapped
//! (`search_text`/`range`/`prefix`/`get`) and each tool is honest in its response
//! about what it can't yet report, rather than faking the missing numbers.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use rmcp::handler::server::router::tool::ToolRouter;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::*;
use rmcp::{tool, tool_handler, tool_router, ErrorData as McpError, ServerHandler};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::record::Record;
use crate::storage::Storage;

const DEFAULT_K: usize = 20;
const DEFAULT_CONTEXT_LINES: usize = 5;
const DEFAULT_MAX_MESSAGE_CHARS: usize = 240;

fn default_k() -> usize {
    DEFAULT_K
}
fn default_context_lines() -> usize {
    DEFAULT_CONTEXT_LINES
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Decodes on raw bytes (`s.as_bytes()`), never `str` byte-range slicing: `doc_id`
/// is caller-controlled MCP tool-call input, and slicing a `&str` at a fixed byte
/// stride panics ("byte index N is not a char boundary") the moment the string
/// contains any multi-byte UTF-8 character at an odd position — e.g. `"€a"` is 4
/// bytes (passes the even-length check) but panics on `s[0..2]`, confirmed via a
/// standalone repro before this fix. Casting a `u8` to `char` is always valid (the
/// first 256 Unicode scalar values), so working on raw bytes throughout sidesteps
/// UTF-8 boundaries entirely — a byte that's part of a multi-byte sequence just
/// fails `to_digit(16)` like any other non-hex-digit input, no panic possible.
fn hex_decode(s: &str) -> Result<Vec<u8>, McpError> {
    let bytes = s.as_bytes();
    if !bytes.len().is_multiple_of(2) {
        return Err(McpError::invalid_params(
            "doc_id must be an even-length hex string",
            None,
        ));
    }
    bytes
        .chunks(2)
        .map(|pair| {
            let hi = (pair[0] as char).to_digit(16);
            let lo = (pair[1] as char).to_digit(16);
            match (hi, lo) {
                (Some(hi), Some(lo)) => Ok(((hi as u8) << 4) | lo as u8),
                _ => Err(McpError::invalid_params("doc_id is not valid hex", None)),
            }
        })
        .collect()
}

fn internal_err(e: impl std::fmt::Display) -> McpError {
    McpError::internal_error(e.to_string(), None)
}

fn json_result<T: Serialize>(value: &T) -> Result<CallToolResult, McpError> {
    let text = serde_json::to_string(value).map_err(internal_err)?;
    Ok(CallToolResult::success(vec![ContentBlock::text(text)]))
}

/// Token-bounding for a raw message: MCP responses go to an agent's context
/// window, not a human's screen — "an agent should never receive a 4KB JSON blob
/// to find a 40-character error" (PRD A-1). This is a plain truncation, not real
/// position-based snippet extraction — see this module's doc comment.
fn bound_message(message: &str) -> (String, bool) {
    if message.chars().count() <= DEFAULT_MAX_MESSAGE_CHARS {
        (message.to_string(), false)
    } else {
        let truncated: String = message.chars().take(DEFAULT_MAX_MESSAGE_CHARS).collect();
        (format!("{truncated}…"), true)
    }
}

#[derive(Debug, Clone, Serialize)]
struct LogHit {
    doc_id: String,
    timestamp_ns: i64,
    message: String,
    truncated: bool,
    score: Option<f32>,
    fields: BTreeMap<String, String>,
}

/// One `search_logs` candidate before pagination/formatting: its storage key (when
/// known — the label-only path always has one; kept `Option` to share this shape
/// with call sites that build it uniformly), the record itself, and a BM25 score
/// when the hit came from the text-search path.
type SearchCandidate = (Option<Vec<u8>>, Record, Option<f32>);

#[derive(Debug, Deserialize, JsonSchema)]
pub struct SearchLogsParams {
    /// Time range start, nanoseconds since epoch.
    pub start_ns: i64,
    /// Time range end, nanoseconds since epoch.
    pub end_ns: i64,
    /// Full-text query (BM25). Omit to filter by label selector only.
    #[serde(default)]
    pub q: Option<String>,
    /// Field-equality filters.
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    /// Max results to return.
    #[serde(default = "default_k")]
    pub k: usize,
    /// Opaque cursor from a previous `search_logs` response, to continue where it left off.
    #[serde(default)]
    pub cursor: Option<String>,
}

#[derive(Debug, Serialize)]
struct SearchLogsResult {
    hits: Vec<LogHit>,
    next_cursor: Option<String>,
    /// Real cost accounting (bytes scanned, tier touched) isn't available yet — see
    /// this module's doc comment. This is only the number of underlying records
    /// considered before filtering: an honest floor, not a real byte count.
    considered: usize,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct GetContextParams {
    /// A `doc_id` from a previous `search_logs` result.
    pub doc_id: String,
    /// Number of lines before and after to include.
    #[serde(default = "default_context_lines")]
    pub n: usize,
}

#[derive(Debug, Serialize)]
struct ContextLine {
    doc_id: String,
    timestamp_ns: i64,
    message: String,
    is_anchor: bool,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct ListStreamsParams {
    pub start_ns: i64,
    pub end_ns: i64,
}

#[derive(Debug, Serialize)]
struct FieldSummary {
    field: String,
    distinct_values: usize,
    sample_values: Vec<String>,
    /// "hll_rollup_estimate" when this field has an `hll` rollup configured
    /// (`pierre.toml`'s `[[rollup]]` blocks, real HyperLogLog cardinality), else
    /// "exact_in_window" — an exact count over only the sampled window, not the
    /// field's true all-time cardinality.
    cardinality_source: &'static str,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct AggregateParams {
    pub field: String,
    pub start_ns: i64,
    pub end_ns: i64,
    /// One of "count", "cardinality", "topk", "quantile".
    pub op: String,
    /// Required for op="quantile", 0.0-1.0.
    #[serde(default)]
    pub q: Option<f64>,
    /// Result count for op="topk".
    #[serde(default = "default_k")]
    pub k: usize,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct FindAnomaliesParams {
    /// The window to check for anomalies.
    pub start_ns: i64,
    pub end_ns: i64,
    /// Baseline window to compare against. Defaults to the equal-duration window
    /// immediately preceding `start_ns`.
    #[serde(default)]
    pub baseline_start_ns: Option<i64>,
    #[serde(default)]
    pub baseline_end_ns: Option<i64>,
}

#[derive(Debug, Serialize)]
struct AnomalyTemplate {
    template_id: String,
    example_message: String,
    current_count: usize,
    baseline_count: usize,
    /// "new" (absent from baseline) or "volume_spike" (present, but the count
    /// ratio crossed `VOLUME_SPIKE_RATIO`).
    kind: &'static str,
}

#[derive(Clone)]
pub struct PierreMcpServer {
    storage: Arc<Storage>,
    allowed_fields: Arc<Vec<String>>,
    textindex_bucket_duration: Duration,
    // Read by the #[tool_handler]-generated ServerHandler::call_tool impl, which
    // rustc's dead-code analysis doesn't see through — same false-positive rmcp's
    // own examples hit.
    #[allow(dead_code)]
    tool_router: ToolRouter<PierreMcpServer>,
}

#[tool_router]
impl PierreMcpServer {
    pub fn new(
        storage: Arc<Storage>,
        allowed_fields: Arc<Vec<String>>,
        textindex_bucket_duration: Duration,
    ) -> Self {
        Self {
            storage,
            allowed_fields,
            textindex_bucket_duration,
            tool_router: Self::tool_router(),
        }
    }

    #[tool(
        description = "Query logs by full-text search, label selector, and time range. Returns ranked results."
    )]
    async fn search_logs(
        &self,
        Parameters(params): Parameters<SearchLogsParams>,
    ) -> Result<CallToolResult, McpError> {
        let offset: usize = params
            .cursor
            .as_deref()
            .and_then(|c| c.parse().ok())
            .unwrap_or(0);

        let (hits_all, considered): (Vec<SearchCandidate>, usize) = if let Some(q) = &params.q {
            let text_hits = crate::textindex::search(
                &self.storage,
                params.start_ns,
                params.end_ns,
                self.textindex_bucket_duration,
                q,
                offset + params.k + 1,
            )
            .await
            .map_err(internal_err)?;
            let considered = text_hits.len();
            let mut out = Vec::with_capacity(text_hits.len());
            for hit in text_hits {
                if let Some(record) = self
                    .storage
                    .get_record(&hit.doc_id)
                    .await
                    .map_err(internal_err)?
                {
                    if params
                        .labels
                        .iter()
                        .all(|(k, v)| record.fields.get(k).is_some_and(|actual| actual == v))
                    {
                        out.push((Some(hit.doc_id), record, Some(hit.score)));
                    }
                }
            }
            (out, considered)
        } else {
            let all = self
                .storage
                .range_with_keys(params.start_ns, params.end_ns)
                .await
                .map_err(internal_err)?;
            let considered = all.len();
            let out = all
                .into_iter()
                .filter(|(_, r)| {
                    params
                        .labels
                        .iter()
                        .all(|(k, v)| r.fields.get(k).is_some_and(|actual| actual == v))
                })
                .map(|(key, r)| (Some(key), r, None))
                .collect();
            (out, considered)
        };

        let total = hits_all.len();
        let page: Vec<LogHit> = hits_all
            .into_iter()
            .skip(offset)
            .take(params.k)
            .map(|(key, record, score)| {
                let (message, truncated) = bound_message(&record.message);
                LogHit {
                    doc_id: key.map(|k| hex_encode(&k)).unwrap_or_default(),
                    timestamp_ns: record.timestamp_ns,
                    message,
                    truncated,
                    score,
                    fields: record.fields,
                }
            })
            .collect();

        let next_cursor = if offset + page.len() < total {
            Some((offset + page.len()).to_string())
        } else {
            None
        };

        json_result(&SearchLogsResult {
            hits: page,
            next_cursor,
            considered,
        })
    }

    #[tool(
        description = "Given a doc_id from search_logs, return the surrounding lines from the same stream (same field values) — the single most-used move in an investigation."
    )]
    async fn get_context(
        &self,
        Parameters(params): Parameters<GetContextParams>,
    ) -> Result<CallToolResult, McpError> {
        let key = hex_decode(&params.doc_id)?;
        let anchor = self
            .storage
            .get_record(&key)
            .await
            .map_err(internal_err)?
            .ok_or_else(|| McpError::invalid_params("doc_id not found", None))?;

        // A generous fixed window rather than an adaptive one — keeps this tool's
        // cost bounded and predictable; a stream with fewer than n lines within ~10
        // minutes either side just returns what's there. Clamped at 0: a real record
        // is never timestamped before the epoch, so there's nothing to gain from
        // scanning negative time — not a correctness requirement (`encode_key`
        // handles negative timestamps correctly), just a pointless-scan guard.
        // Saturating: `anchor.timestamp_ns` is whatever the original ingest surface
        // was handed (every ingest surface accepts a client-supplied timestamp_ns
        // with no bounds check), so a record deliberately or accidentally stored
        // near i64::MIN/MAX must not overflow this arithmetic.
        const WINDOW_NS: i64 = 10 * 60 * 1_000_000_000;
        let window = self
            .storage
            .range_with_keys(
                anchor.timestamp_ns.saturating_sub(WINDOW_NS).max(0),
                anchor.timestamp_ns.saturating_add(WINDOW_NS),
            )
            .await
            .map_err(internal_err)?;

        let mut same_stream: Vec<(Vec<u8>, Record)> = window
            .into_iter()
            .filter(|(_, r)| r.fields == anchor.fields)
            .collect();
        same_stream.sort_by_key(|(_, r)| r.timestamp_ns);
        let anchor_pos = same_stream.iter().position(|(k, _)| *k == key).unwrap_or(0);

        let start = anchor_pos.saturating_sub(params.n);
        let end = (anchor_pos + params.n + 1).min(same_stream.len());
        let lines: Vec<ContextLine> = same_stream[start..end]
            .iter()
            .map(|(k, r)| ContextLine {
                doc_id: hex_encode(k),
                timestamp_ns: r.timestamp_ns,
                message: r.message.clone(),
                is_anchor: *k == key,
            })
            .collect();

        json_result(&lines)
    }

    #[tool(
        description = "Enumerate configured field dimensions and their cardinality/sample values in a time window, so an agent can orient before querying."
    )]
    async fn list_streams(
        &self,
        Parameters(params): Parameters<ListStreamsParams>,
    ) -> Result<CallToolResult, McpError> {
        let records = self
            .storage
            .range(params.start_ns, params.end_ns)
            .await
            .map_err(internal_err)?;

        let mut summaries = Vec::new();
        for field in self.allowed_fields.iter() {
            if let Ok(Some(mut sketch)) = crate::aggregate::merged_sketch(
                &self.storage,
                field,
                params.start_ns,
                params.end_ns,
            )
            .await
            {
                if let Some(estimate) = sketch.hll_estimate() {
                    summaries.push(FieldSummary {
                        field: field.clone(),
                        distinct_values: estimate.round() as usize,
                        sample_values: vec![],
                        cardinality_source: "hll_rollup_estimate",
                    });
                    continue;
                }
            }
            let mut distinct: std::collections::BTreeSet<String> =
                std::collections::BTreeSet::new();
            for r in &records {
                if let Some(v) = r.fields.get(field) {
                    distinct.insert(v.clone());
                }
            }
            summaries.push(FieldSummary {
                field: field.clone(),
                distinct_values: distinct.len(),
                sample_values: distinct.into_iter().take(10).collect(),
                cardinality_source: "exact_in_window",
            });
        }

        json_result(&summaries)
    }

    #[tool(
        description = "Run a pre-computed aggregation (count/cardinality/topk/quantile) over a field and time window — never a raw rescan."
    )]
    async fn aggregate(
        &self,
        Parameters(params): Parameters<AggregateParams>,
    ) -> Result<CallToolResult, McpError> {
        let mut sketch = crate::aggregate::merged_sketch(
            &self.storage,
            &params.field,
            params.start_ns,
            params.end_ns,
        )
        .await
        .map_err(internal_err)?
        .ok_or_else(|| {
            McpError::invalid_params(
                format!("no rollup data for field {:?} in range", params.field),
                None,
            )
        })?;

        let value = match params.op.as_str() {
            "count" => serde_json::to_value(sketch.exact_counts()),
            "cardinality" => serde_json::to_value(sketch.hll_estimate()),
            "topk" => serde_json::to_value(sketch.top_k(params.k)),
            "quantile" => {
                let q = params
                    .q
                    .ok_or_else(|| McpError::invalid_params("missing q for quantile", None))?;
                serde_json::to_value(sketch.quantile(q))
            }
            other => {
                return Err(McpError::invalid_params(
                    format!("unknown op {other:?}"),
                    None,
                ))
            }
        }
        .map_err(internal_err)?;

        Ok(CallToolResult::success(vec![ContentBlock::text(
            value.to_string(),
        )]))
    }

    #[tool(
        description = "Find statistically unusual message shapes (new template IDs) or volume spikes in a window relative to a baseline."
    )]
    async fn find_anomalies(
        &self,
        Parameters(params): Parameters<FindAnomaliesParams>,
    ) -> Result<CallToolResult, McpError> {
        // Saturating: start_ns/end_ns are caller-supplied tool arguments with no
        // bounds validation — a plain `-` can overflow (panics in a debug build,
        // silently wraps in release) for an extreme pair.
        let span = params.end_ns.saturating_sub(params.start_ns);
        let baseline_start = params
            .baseline_start_ns
            .unwrap_or(params.start_ns.saturating_sub(span));
        let baseline_end = params.baseline_end_ns.unwrap_or(params.start_ns);

        let current = self
            .storage
            .range(params.start_ns, params.end_ns)
            .await
            .map_err(internal_err)?;
        let baseline = self
            .storage
            .range(baseline_start, baseline_end)
            .await
            .map_err(internal_err)?;

        let mut current_counts: BTreeMap<u64, (usize, String)> = BTreeMap::new();
        for r in &current {
            let entry = current_counts
                .entry(r.template_id)
                .or_insert((0, r.message.clone()));
            entry.0 += 1;
        }
        let mut baseline_counts: BTreeMap<u64, usize> = BTreeMap::new();
        for r in &baseline {
            *baseline_counts.entry(r.template_id).or_insert(0) += 1;
        }

        const VOLUME_SPIKE_RATIO: f64 = 3.0;
        let mut anomalies = Vec::new();
        for (template_id, (current_count, example)) in &current_counts {
            let baseline_count = baseline_counts.get(template_id).copied().unwrap_or(0);
            if baseline_count == 0 {
                anomalies.push(AnomalyTemplate {
                    template_id: template_id.to_string(),
                    example_message: example.clone(),
                    current_count: *current_count,
                    baseline_count,
                    kind: "new",
                });
            } else if (*current_count as f64) / (baseline_count as f64) >= VOLUME_SPIKE_RATIO {
                anomalies.push(AnomalyTemplate {
                    template_id: template_id.to_string(),
                    example_message: example.clone(),
                    current_count: *current_count,
                    baseline_count,
                    kind: "volume_spike",
                });
            }
        }
        anomalies.sort_by_key(|a| std::cmp::Reverse(a.current_count));

        json_result(&anomalies)
    }
}

#[tool_handler]
impl ServerHandler for PierreMcpServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::from_build_env())
            .with_instructions(
                "Pierre log store, read-only agent interface. Tools: search_logs (text/label/time query), \
                 get_context (surrounding lines for a doc_id), list_streams (orient before querying), \
                 aggregate (pre-computed count/cardinality/topk/quantile), find_anomalies (new or spiking \
                 message shapes vs. a baseline window). This surface cannot delete, mutate retention, or \
                 reconfigure Pierre."
                    .to_string(),
            )
    }
}
