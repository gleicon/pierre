use std::collections::BTreeMap;

use crate::record::Record;
use crate::storage::Storage;

/// Answers a selector + time-range read (FR-13): time range picks the scan window,
/// field predicates filter in-memory. Field-predicate pushdown into the storage
/// layer is a later optimization, not required for correctness here.
pub async fn select(
    storage: &Storage,
    start_ns: i64,
    end_ns: i64,
    field_filters: &BTreeMap<String, String>,
) -> anyhow::Result<Vec<Record>> {
    let records = storage.range(start_ns, end_ns).await?;
    Ok(records
        .into_iter()
        .filter(|r| {
            field_filters
                .iter()
                .all(|(k, v)| r.fields.get(k).is_some_and(|actual| actual == v))
        })
        .collect())
}
