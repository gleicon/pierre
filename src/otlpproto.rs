//! Real OTLP logs wire format, vendored from `open-telemetry/opentelemetry-proto`
//! (v1.11.0) — same reasoning as `lokiproto.rs`: field numbers/types determine wire
//! compatibility, not any codegen sugar, so the schema comes straight from
//! upstream rather than being hand-written. Module nesting mirrors the `.proto`
//! package structure exactly (`opentelemetry.proto.common.v1` etc.), matching how
//! `tonic_prost_build`/`prost_build` generate one file per package and
//! cross-reference them via that same nesting.
pub mod opentelemetry {
    pub mod proto {
        pub mod common {
            pub mod v1 {
                include!(concat!(
                    env!("OUT_DIR"),
                    "/opentelemetry.proto.common.v1.rs"
                ));
            }
        }
        pub mod resource {
            pub mod v1 {
                include!(concat!(
                    env!("OUT_DIR"),
                    "/opentelemetry.proto.resource.v1.rs"
                ));
            }
        }
        pub mod logs {
            pub mod v1 {
                include!(concat!(env!("OUT_DIR"), "/opentelemetry.proto.logs.v1.rs"));
            }
        }
        pub mod collector {
            pub mod logs {
                pub mod v1 {
                    include!(concat!(
                        env!("OUT_DIR"),
                        "/opentelemetry.proto.collector.logs.v1.rs"
                    ));
                }
            }
        }
    }
}

use std::collections::BTreeMap;

use opentelemetry::proto::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry::proto::common::v1::{any_value::Value, AnyValue, KeyValue};

use crate::record::WireRecord;

/// Flattens one `ExportLogsServiceRequest` (which may batch multiple resources,
/// each with multiple scopes, each with multiple log records — the real shape a
/// collector sends, not just a single-record convenience case) into Pierre's own
/// `WireRecord`s. Resource attributes (e.g. `service.name`) are merged into every
/// record's fields alongside the record's own attributes — the standard OTel
/// convention that a log's resource context travels with it, not a Pierre
/// invention.
pub fn decode_export_request(request: &ExportLogsServiceRequest) -> Vec<WireRecord> {
    let mut records = Vec::new();
    for resource_logs in &request.resource_logs {
        let resource_fields = resource_logs
            .resource
            .as_ref()
            .map(|r| attributes_to_fields(&r.attributes))
            .unwrap_or_default();

        for scope_logs in &resource_logs.scope_logs {
            for log_record in &scope_logs.log_records {
                let mut fields = resource_fields.clone();
                fields.extend(attributes_to_fields(&log_record.attributes));

                if log_record.severity_number != 0 {
                    fields.insert(
                        "severity_number".to_string(),
                        log_record.severity_number.to_string(),
                    );
                }
                if !log_record.severity_text.is_empty() {
                    fields.insert("level".to_string(), log_record.severity_text.to_lowercase());
                }
                if !log_record.trace_id.is_empty() {
                    fields.insert("trace_id".to_string(), hex::encode(&log_record.trace_id));
                }
                if !log_record.span_id.is_empty() {
                    fields.insert("span_id".to_string(), hex::encode(&log_record.span_id));
                }

                let message = log_record
                    .body
                    .as_ref()
                    .map(any_value_to_string)
                    .unwrap_or_default();

                // `time_unix_nano` is the log's own event time; `observed_time_unix_nano`
                // is when the collector/receiver observed it (added when the source
                // can't supply its own clock reading) — prefer the real event time,
                // falling back to observed time, and only to wall-clock now if OTLP
                // sent neither, which is technically permitted by the spec.
                //
                // Both are wire `u64`; `try_from` rather than `as i64` so a value past
                // `i64::MAX` (never sent by a real OTel exporter — realistic epoch-ns
                // magnitudes are nowhere close — but the field is otherwise
                // unvalidated wire input) maps to `None` and falls through to the next
                // fallback in the chain, the same way a genuinely-absent (zero) field
                // already does, instead of silently reinterpreting as a negative
                // timestamp.
                let time_unix_nano = i64::try_from(log_record.time_unix_nano).ok();
                let observed_time_unix_nano =
                    i64::try_from(log_record.observed_time_unix_nano).ok();
                let timestamp_ns = time_unix_nano
                    .filter(|&t| t != 0)
                    .or_else(|| observed_time_unix_nano.filter(|&t| t != 0))
                    .unwrap_or_else(crate::clock::now_ns);

                records.push(WireRecord {
                    timestamp_ns,
                    message,
                    fields,
                });
            }
        }
    }
    records
}

fn attributes_to_fields(attributes: &[KeyValue]) -> BTreeMap<String, String> {
    let mut fields = BTreeMap::new();
    for kv in attributes {
        if let Some(value) = &kv.value {
            fields.insert(kv.key.clone(), any_value_to_string(value));
        }
    }
    fields
}

/// `AnyValue` is OTLP's tagged union (string/bool/int/double/bytes/array/kvlist).
/// Scalars stringify directly; the two container variants (array/kvlist) go
/// through their existing JSON `Debug`-free representation via `format!` rather
/// than a bespoke recursive serializer — this is wire-format translation, not a
/// schema mapper, matching `es_bulk.rs`'s equivalent choice for nested JSON.
fn any_value_to_string(value: &AnyValue) -> String {
    match &value.value {
        Some(Value::StringValue(s)) => s.clone(),
        Some(Value::BoolValue(b)) => b.to_string(),
        Some(Value::IntValue(i)) => i.to_string(),
        Some(Value::DoubleValue(d)) => d.to_string(),
        Some(Value::BytesValue(b)) => hex::encode(b),
        Some(Value::ArrayValue(arr)) => format!("{arr:?}"),
        Some(Value::KvlistValue(kv)) => format!("{kv:?}"),
        // `string_value_strindex` is Alpha/Profiling-only (references a separate
        // string table this request doesn't carry) — the proto's own doc comment
        // says non-Profiling receivers should treat its presence as equivalent to
        // an absent value, which is exactly what the `None` arm already does.
        Some(Value::StringValueStrindex(_)) | None => String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry::proto::common::v1::InstrumentationScope;
    use opentelemetry::proto::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
    use opentelemetry::proto::resource::v1::Resource;

    fn kv(key: &str, value: Value) -> KeyValue {
        KeyValue {
            key: key.to_string(),
            value: Some(AnyValue { value: Some(value) }),
            key_strindex: 0,
        }
    }

    #[test]
    fn resource_and_log_attributes_merge_into_fields() {
        let request = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                resource: Some(Resource {
                    attributes: vec![kv(
                        "service.name",
                        Value::StringValue("checkout".to_string()),
                    )],
                    dropped_attributes_count: 0,
                    entity_refs: vec![],
                }),
                scope_logs: vec![ScopeLogs {
                    scope: Some(InstrumentationScope::default()),
                    log_records: vec![LogRecord {
                        time_unix_nano: 1_700_000_000_000_000_000,
                        observed_time_unix_nano: 0,
                        severity_number: 17, // SEVERITY_NUMBER_ERROR
                        severity_text: "ERROR".to_string(),
                        body: Some(AnyValue {
                            value: Some(Value::StringValue("payment declined".to_string())),
                        }),
                        attributes: vec![kv("order.id", Value::StringValue("abc123".to_string()))],
                        dropped_attributes_count: 0,
                        flags: 0,
                        trace_id: vec![0xde, 0xad, 0xbe, 0xef],
                        span_id: vec![],
                        event_name: String::new(),
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };

        let records = decode_export_request(&request);
        assert_eq!(records.len(), 1);
        let record = &records[0];

        assert_eq!(record.timestamp_ns, 1_700_000_000_000_000_000);
        assert_eq!(record.message, "payment declined");
        assert_eq!(
            record.fields.get("service.name").map(String::as_str),
            Some("checkout"),
            "resource attributes must merge into the record's fields"
        );
        assert_eq!(
            record.fields.get("order.id").map(String::as_str),
            Some("abc123")
        );
        assert_eq!(
            record.fields.get("level").map(String::as_str),
            Some("error")
        );
        assert_eq!(
            record.fields.get("trace_id").map(String::as_str),
            Some("deadbeef")
        );
        assert!(
            !record.fields.contains_key("span_id"),
            "empty span_id must not become a field"
        );
    }

    #[test]
    fn falls_back_to_observed_time_when_event_time_is_absent() {
        let request = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                resource: None,
                scope_logs: vec![ScopeLogs {
                    scope: None,
                    log_records: vec![LogRecord {
                        time_unix_nano: 0,
                        observed_time_unix_nano: 1_650_000_000_000_000_000,
                        severity_number: 0,
                        severity_text: String::new(),
                        body: Some(AnyValue {
                            value: Some(Value::StringValue("no event time".to_string())),
                        }),
                        attributes: vec![],
                        dropped_attributes_count: 0,
                        flags: 0,
                        trace_id: vec![],
                        span_id: vec![],
                        event_name: String::new(),
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };

        let records = decode_export_request(&request);
        assert_eq!(records[0].timestamp_ns, 1_650_000_000_000_000_000);
        assert!(
            !records[0].fields.contains_key("severity_number"),
            "severity_number of 0 (unspecified) must not become a field"
        );
    }

    /// `time_unix_nano`/`observed_time_unix_nano` are wire `u64`, unvalidated —
    /// a value past `i64::MAX` must fall through to the next fallback in the
    /// chain (same as a genuinely-absent zero field), not silently reinterpret
    /// as a negative timestamp via a plain `as i64` cast.
    #[test]
    fn out_of_i64_range_time_unix_nano_falls_through_to_observed_time() {
        let request = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                resource: None,
                scope_logs: vec![ScopeLogs {
                    scope: None,
                    log_records: vec![LogRecord {
                        time_unix_nano: u64::MAX,
                        observed_time_unix_nano: 1_650_000_000_000_000_000,
                        severity_number: 0,
                        severity_text: String::new(),
                        body: Some(AnyValue {
                            value: Some(Value::StringValue("out of range".to_string())),
                        }),
                        attributes: vec![],
                        dropped_attributes_count: 0,
                        flags: 0,
                        trace_id: vec![],
                        span_id: vec![],
                        event_name: String::new(),
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };

        let records = decode_export_request(&request);
        assert_eq!(records[0].timestamp_ns, 1_650_000_000_000_000_000);
    }

    /// If *both* fields are out of range, it must fall all the way through to
    /// wall-clock now rather than landing a negative timestamp.
    #[test]
    fn out_of_i64_range_on_both_timestamp_fields_falls_through_to_now() {
        let request = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                resource: None,
                scope_logs: vec![ScopeLogs {
                    scope: None,
                    log_records: vec![LogRecord {
                        time_unix_nano: u64::MAX,
                        observed_time_unix_nano: u64::MAX,
                        severity_number: 0,
                        severity_text: String::new(),
                        body: Some(AnyValue {
                            value: Some(Value::StringValue("both out of range".to_string())),
                        }),
                        attributes: vec![],
                        dropped_attributes_count: 0,
                        flags: 0,
                        trace_id: vec![],
                        span_id: vec![],
                        event_name: String::new(),
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };

        let before = crate::clock::now_ns();
        let records = decode_export_request(&request);
        let after = crate::clock::now_ns();
        assert!(
            records[0].timestamp_ns >= before && records[0].timestamp_ns <= after,
            "must fall back to wall-clock now, got {}",
            records[0].timestamp_ns
        );
    }
}
