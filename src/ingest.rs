use crate::record::{Record, WireRecord};
use crate::rollup::RollupHandle;
use crate::storage::Storage;
use crate::textindex::TextIndexHandle;

/// Normalizes a wire record (field extraction + template id, both synchronous —
/// FR-5/FR-6) and commits it durably before returning, so the caller's ack means
/// the write is WAL-durable. Rollup and BM25-indexing contributions are fed
/// non-blockingly afterward (FR-18/19, FR-10) — a full or absent pipeline never
/// affects ingest latency.
pub async fn commit(
    storage: &Storage,
    wire: WireRecord,
    allowed_fields: &[String],
    rollup: Option<&RollupHandle>,
    textindex: Option<&TextIndexHandle>,
) -> anyhow::Result<()> {
    let record = Record::from_wire(wire, allowed_fields);
    let key = storage.commit(&record).await?;

    if let Some(rollup) = rollup {
        for (field, value) in &record.fields {
            rollup.record(field.clone(), value.clone());
        }
    }

    if let Some(textindex) = textindex {
        textindex.record(key, record.message.clone(), record.timestamp_ns);
    }

    Ok(())
}
