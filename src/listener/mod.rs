pub mod loki;
pub mod native;
pub mod query_api;

use std::collections::BTreeMap;

use axum::http::StatusCode;

/// Parses `start`/`end` query params into a nanosecond time range — shared by every
/// listener that answers a time-range query (`query_api`'s selector/BM25/aggregate
/// endpoints, Loki's `query_range`) so the parsing rules only need to be right once.
pub(crate) fn parse_range(params: &BTreeMap<String, String>) -> Result<(i64, i64), (StatusCode, String)> {
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
