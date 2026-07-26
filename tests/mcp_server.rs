use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use pierre::record::WireRecord;
use pierre::rollup::worker::TierConfig;
use pierre::rollup::RollupKind;
use pierre::storage::Storage;
use rmcp::model::CallToolRequestParams;
use rmcp::service::{RunningService, ServiceExt};
use rmcp::transport::StreamableHttpClientTransport;
use rmcp::RoleClient;
use serde_json::{json, Value};

/// Spins up a real `listener::mcp::serve` on an ephemeral port and connects a
/// real `rmcp` Streamable HTTP client to it — the same "real client against a
/// real running listener" discipline used for the OTLP/native listeners
/// (`tests/otlp_ingest.rs`, `tests/native_ingest_roundtrip.rs`), not a
/// hand-rolled JSON-RPC harness.
async fn connect(
    storage: Arc<Storage>,
    allowed_fields: Vec<String>,
) -> RunningService<RoleClient, ()> {
    let listener_socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener_socket.local_addr().unwrap().to_string();
    drop(listener_socket);

    let addr_for_server = addr.clone();
    tokio::spawn(async move {
        let _ = pierre::listener::mcp::serve(
            &addr_for_server,
            storage,
            Arc::new(allowed_fields),
            Duration::from_secs(300),
            pierre::auth::AuthTokens::new(vec![]),
        )
        .await;
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    let transport = StreamableHttpClientTransport::from_uri(format!("http://{addr}/mcp"));
    ().serve(transport).await.unwrap()
}

fn tool_json(result: &rmcp::model::CallToolResult) -> Value {
    assert_ne!(
        result.is_error,
        Some(true),
        "tool call reported an error: {result:?}"
    );
    let text = &result.content[0].as_text().unwrap().text;
    serde_json::from_str(text).unwrap()
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Writes one record directly via `Storage::commit` and returns its storage
/// key — `search_logs`'s label path and `get_context` both read straight off
/// `Storage`, so the fixtures don't need `pierre::ingest::commit`'s rollup/
/// textindex side effects unless a test asks for them explicitly.
async fn ingest(
    storage: &Storage,
    fields: &[String],
    timestamp_ns: i64,
    message: &str,
    labels: &[(&str, &str)],
) -> Vec<u8> {
    let mut record_fields = BTreeMap::new();
    for (k, v) in labels {
        record_fields.insert((*k).to_string(), (*v).to_string());
    }
    let wire = WireRecord {
        timestamp_ns,
        message: message.to_string(),
        fields: record_fields,
    };
    let record = pierre::record::Record::from_wire(wire, fields);
    storage.commit(&record).await.unwrap()
}

#[tokio::test]
async fn search_logs_filters_by_label_and_time_range() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(Storage::open(dir.path()).await.unwrap());
    let fields = vec!["level".to_string()];

    ingest(
        &storage,
        &fields,
        1_000_000_000,
        "everything is fine",
        &[("level", "info")],
    )
    .await;
    ingest(
        &storage,
        &fields,
        2_000_000_000,
        "disk is on fire",
        &[("level", "error")],
    )
    .await;

    let client = connect(storage, fields).await;

    let mut args = serde_json::Map::new();
    args.insert("start_ns".to_string(), json!(0));
    args.insert("end_ns".to_string(), json!(3_000_000_000i64));
    args.insert("labels".to_string(), json!({"level": "error"}));

    let result = client
        .peer()
        .call_tool(CallToolRequestParams::new("search_logs").with_arguments(args))
        .await
        .unwrap();
    let body = tool_json(&result);

    let hits = body["hits"].as_array().unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0]["message"], "disk is on fire");
    assert_eq!(hits[0]["fields"]["level"], "error");
    assert!(!hits[0]["doc_id"].as_str().unwrap().is_empty());

    client.cancel().await.unwrap();
}

#[tokio::test]
async fn search_logs_full_text_query_finds_indexed_message() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(Storage::open(dir.path()).await.unwrap());
    let fields: Vec<String> = vec![];

    let bucket_duration = Duration::from_secs(300);
    let (textindex, _worker) =
        pierre::textindex::spawn(storage.clone(), bucket_duration, Duration::from_millis(50));

    let wire = WireRecord {
        timestamp_ns: 1_000_000_000,
        message: "checkout request timed out after 30s".to_string(),
        fields: BTreeMap::new(),
    };
    pierre::ingest::commit(&storage, wire, &fields, None, Some(&textindex))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;

    let client = connect(storage, fields).await;

    let mut args = serde_json::Map::new();
    args.insert("start_ns".to_string(), json!(0));
    args.insert("end_ns".to_string(), json!(2_000_000_000i64));
    args.insert("q".to_string(), json!("timed out"));

    let result = client
        .peer()
        .call_tool(CallToolRequestParams::new("search_logs").with_arguments(args))
        .await
        .unwrap();
    let body = tool_json(&result);

    let hits = body["hits"].as_array().unwrap();
    assert_eq!(hits.len(), 1);
    assert!(hits[0]["score"].is_number());
    assert!(hits[0]["message"].as_str().unwrap().contains("timed out"));

    client.cancel().await.unwrap();
}

#[tokio::test]
async fn get_context_returns_surrounding_lines_from_same_stream() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(Storage::open(dir.path()).await.unwrap());
    let fields = vec!["trace_id".to_string()];

    ingest(
        &storage,
        &fields,
        1_000_000_000,
        "step one",
        &[("trace_id", "abc")],
    )
    .await;
    let anchor_key = ingest(
        &storage,
        &fields,
        2_000_000_000,
        "step two",
        &[("trace_id", "abc")],
    )
    .await;
    ingest(
        &storage,
        &fields,
        3_000_000_000,
        "step three",
        &[("trace_id", "abc")],
    )
    .await;
    // Different stream — must not appear in the context window.
    ingest(
        &storage,
        &fields,
        2_500_000_000,
        "unrelated stream",
        &[("trace_id", "xyz")],
    )
    .await;

    let client = connect(storage, fields).await;

    let mut args = serde_json::Map::new();
    args.insert("doc_id".to_string(), json!(hex_encode(&anchor_key)));
    args.insert("n".to_string(), json!(1));

    let result = client
        .peer()
        .call_tool(CallToolRequestParams::new("get_context").with_arguments(args))
        .await
        .unwrap();
    let body = tool_json(&result);

    let lines = body.as_array().unwrap();
    assert_eq!(lines.len(), 3);
    assert_eq!(lines[0]["message"], "step one");
    assert_eq!(lines[1]["message"], "step two");
    assert_eq!(lines[1]["is_anchor"], true);
    assert_eq!(lines[2]["message"], "step three");

    client.cancel().await.unwrap();
}

#[tokio::test]
async fn list_streams_reports_exact_cardinality_when_no_rollup_configured() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(Storage::open(dir.path()).await.unwrap());
    let fields = vec!["level".to_string()];

    ingest(&storage, &fields, 1_000_000_000, "a", &[("level", "info")]).await;
    ingest(&storage, &fields, 1_500_000_000, "b", &[("level", "error")]).await;
    ingest(&storage, &fields, 2_000_000_000, "c", &[("level", "info")]).await;

    let client = connect(storage, fields).await;

    let mut args = serde_json::Map::new();
    args.insert("start_ns".to_string(), json!(0));
    args.insert("end_ns".to_string(), json!(3_000_000_000i64));

    let result = client
        .peer()
        .call_tool(CallToolRequestParams::new("list_streams").with_arguments(args))
        .await
        .unwrap();
    let body = tool_json(&result);

    let summaries = body.as_array().unwrap();
    assert_eq!(summaries.len(), 1);
    assert_eq!(summaries[0]["field"], "level");
    assert_eq!(summaries[0]["distinct_values"], 2);
    assert_eq!(summaries[0]["cardinality_source"], "exact_in_window");
    let mut samples: Vec<String> = summaries[0]["sample_values"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap().to_string())
        .collect();
    samples.sort();
    assert_eq!(samples, vec!["error", "info"]);

    client.cancel().await.unwrap();
}

#[tokio::test]
async fn aggregate_count_uses_rollup_data() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(Storage::open(dir.path()).await.unwrap());
    let fields = vec!["level".to_string()];
    let field_kinds = HashMap::from([("level".to_string(), RollupKind::Exact)]);

    let mut tiers = TierConfig::production_defaults();
    tiers.minute_duration = Duration::from_millis(100);
    let rollup = pierre::rollup::spawn(storage.clone(), field_kinds, tiers);

    for level in ["error", "error", "info"] {
        let wire = WireRecord {
            timestamp_ns: 1,
            message: "x".to_string(),
            fields: BTreeMap::from([("level".to_string(), level.to_string())]),
        };
        pierre::ingest::commit(&storage, wire, &fields, Some(&rollup), None)
            .await
            .unwrap();
    }
    // The rollup worker persists asynchronously via a bounded channel — give it
    // a moment to flush the minute bucket before querying it back.
    tokio::time::sleep(Duration::from_millis(150)).await;

    let client = connect(storage, fields).await;

    // Rollup buckets are keyed by wall-clock time when the worker persisted
    // them, not by the ingested record's own `timestamp_ns` (see
    // `rollup::worker` / `aggregate::merged_sketch`) — query a real window
    // around "now", narrow enough to stay in the minute tier (span <= 1h).
    let now_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as i64;
    let mut args = serde_json::Map::new();
    args.insert("field".to_string(), json!("level"));
    args.insert("start_ns".to_string(), json!(now_ns - 5 * 60_000_000_000));
    args.insert("end_ns".to_string(), json!(now_ns + 5 * 60_000_000_000));
    args.insert("op".to_string(), json!("count"));

    let result = client
        .peer()
        .call_tool(CallToolRequestParams::new("aggregate").with_arguments(args))
        .await
        .unwrap();
    let body = tool_json(&result);

    assert_eq!(body["error"], 2);
    assert_eq!(body["info"], 1);

    client.cancel().await.unwrap();
}

#[tokio::test]
async fn find_anomalies_flags_a_template_absent_from_baseline() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(Storage::open(dir.path()).await.unwrap());
    let fields: Vec<String> = vec![];

    // Baseline window: only the "user login succeeded" template.
    for i in 0..3 {
        ingest(&storage, &fields, i, "user login succeeded", &[]).await;
    }
    // Current window: the same template, plus a brand-new one never seen before.
    for i in 1_000..1_003 {
        ingest(&storage, &fields, i, "user login succeeded", &[]).await;
    }
    ingest(
        &storage,
        &fields,
        1_003,
        "database connection pool exhausted",
        &[],
    )
    .await;

    let client = connect(storage, fields).await;

    let mut args = serde_json::Map::new();
    args.insert("start_ns".to_string(), json!(1_000));
    args.insert("end_ns".to_string(), json!(1_004));
    args.insert("baseline_start_ns".to_string(), json!(0));
    args.insert("baseline_end_ns".to_string(), json!(3));

    let result = client
        .peer()
        .call_tool(CallToolRequestParams::new("find_anomalies").with_arguments(args))
        .await
        .unwrap();
    let body = tool_json(&result);

    let anomalies = body.as_array().unwrap();
    assert_eq!(anomalies.len(), 1);
    assert_eq!(anomalies[0]["kind"], "new");
    assert_eq!(
        anomalies[0]["example_message"],
        "database connection pool exhausted"
    );
    assert_eq!(anomalies[0]["current_count"], 1);
    assert_eq!(anomalies[0]["baseline_count"], 0);

    client.cancel().await.unwrap();
}
