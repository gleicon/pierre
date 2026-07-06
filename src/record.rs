use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Wire format for the native protocol — what a shipper sends.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WireRecord {
    pub timestamp_ns: i64,
    pub message: String,
    #[serde(default)]
    pub fields: BTreeMap<String, String>,
}

/// Internal normalized record — what every listener converts its wire format into.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Record {
    pub timestamp_ns: i64,
    pub message: String,
    pub fields: BTreeMap<String, String>,
    pub template_id: u64,
}

impl Record {
    /// Normalizes a `WireRecord` plus the configured field allowlist into a `Record`:
    /// filters fields to the allowlist and computes the Drain-style template id.
    pub fn from_wire(wire: WireRecord, allowed_fields: &[String]) -> Self {
        let fields = wire
            .fields
            .into_iter()
            .filter(|(k, _)| allowed_fields.iter().any(|f| f == k))
            .collect();
        let template_id = crate::template::template_id(&wire.message);
        Record {
            timestamp_ns: wire.timestamp_ns,
            message: wire.message,
            fields,
            template_id,
        }
    }
}
