use std::collections::BTreeMap;
use std::time::Duration;

use crate::record::WireRecord;
use crate::storage::Storage;

#[derive(Debug, Clone, PartialEq)]
pub enum UnmappedStrategy {
    /// Drop fields not in Pierre's schema.
    Lossy,
    /// Serialize unmapped fields as JSON into a `_meta` structured field.
    Preserve,
}

impl std::str::FromStr for UnmappedStrategy {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "lossy" => Ok(UnmappedStrategy::Lossy),
            "preserve" => Ok(UnmappedStrategy::Preserve),
            other => Err(format!(
                "unknown --unmapped value: {other:?}; use 'lossy' or 'preserve'"
            )),
        }
    }
}

/// Migrate logs from an Elasticsearch index into Pierre via ES scroll API.
///
/// Usage: `pierre migrate --from elasticsearch --url <url> --index <name> [--unmapped lossy|preserve]`
pub async fn run_elasticsearch(
    storage: &Storage,
    allowed_fields: &[String],
    es_url: &str,
    index: &str,
    unmapped: UnmappedStrategy,
    rollup: Option<&crate::rollup::RollupHandle>,
    textindex: Option<&crate::textindex::TextIndexHandle>,
) -> anyhow::Result<()> {
    // Validate index name: ES index names must not contain path separators or query
    // characters — rejecting early prevents {index} from escaping its path segment
    // in the URL built below (e.g. "foo/../_cluster/state" hitting unintended APIs).
    if index.contains(['/', '?', '#', '&', ' ', '\\']) {
        anyhow::bail!(
            "invalid --index {index:?}: must not contain '/', '?', '#', '&', '\\\\', or spaces"
        );
    }

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(120))
        .build()
        .map_err(|e| anyhow::anyhow!("failed to build HTTP client: {e}"))?;
    let scroll_ttl = "1m";

    // Open scroll — first page
    let mut current: serde_json::Value = client
        .post(format!("{es_url}/{index}/_search?scroll={scroll_ttl}"))
        .json(&serde_json::json!({
            "size": 1000,
            "query": { "match_all": {} },
            "sort": ["_doc"]
        }))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;

    let mut total = 0usize;

    loop {
        let scroll_id = current["_scroll_id"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("ES response missing _scroll_id"))?
            .to_string();

        let hits = current["hits"]["hits"]
            .as_array()
            .ok_or_else(|| anyhow::anyhow!("ES response missing hits.hits"))?;

        if hits.is_empty() {
            // Clean up scroll context
            let _ = client
                .delete(format!("{es_url}/_search/scroll"))
                .json(&serde_json::json!({ "scroll_id": scroll_id }))
                .send()
                .await;
            break;
        }

        for hit in hits {
            if let Some(source) = hit["_source"].as_object() {
                let wire = doc_to_wire(source, allowed_fields, &unmapped);
                crate::ingest::commit(storage, wire, allowed_fields, rollup, textindex, None)
                    .await?;
                total += 1;
            }
        }

        if total % 10_000 == 0 {
            log::info!("migrate: {total} records ingested so far...");
        }

        // Fetch next page
        current = client
            .post(format!("{es_url}/_search/scroll"))
            .json(&serde_json::json!({
                "scroll": scroll_ttl,
                "scroll_id": scroll_id,
            }))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
    }

    log::info!("migrate: done — {total} records ingested from index {index:?}");
    Ok(())
}

fn doc_to_wire(
    source: &serde_json::Map<String, serde_json::Value>,
    allowed_fields: &[String],
    unmapped: &UnmappedStrategy,
) -> WireRecord {
    let timestamp_ns = source
        .get("@timestamp")
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<jiff::Timestamp>().ok())
        .map(|t| t.as_nanosecond() as i64)
        .or_else(|| {
            source
                .get("@timestamp")
                .and_then(|v| v.as_i64())
                .map(|ms| ms * 1_000_000)
        })
        .unwrap_or_else(crate::clock::now_ns);

    let message = source
        .get("message")
        .and_then(|v| v.as_str())
        .or_else(|| {
            source
                .get("log")
                .and_then(|v| v.get("message"))
                .and_then(|v| v.as_str())
        })
        .unwrap_or("")
        .to_string();

    let mut fields = BTreeMap::new();
    for field in allowed_fields {
        if let Some(v) = source.get(field) {
            let sv = match v {
                serde_json::Value::String(s) => s.clone(),
                other => other.to_string(),
            };
            fields.insert(field.clone(), sv);
        }
    }

    if *unmapped == UnmappedStrategy::Preserve {
        let skip: std::collections::HashSet<&str> = allowed_fields
            .iter()
            .map(String::as_str)
            .chain(["@timestamp", "message"].iter().copied())
            .collect();
        let mut unmapped_map = serde_json::Map::new();
        for (k, v) in source {
            if !skip.contains(k.as_str()) {
                unmapped_map.insert(k.clone(), v.clone());
            }
        }
        if !unmapped_map.is_empty() {
            fields.insert(
                "_meta".to_string(),
                serde_json::Value::Object(unmapped_map).to_string(),
            );
        }
    }

    WireRecord {
        timestamp_ns,
        message,
        fields,
    }
}
