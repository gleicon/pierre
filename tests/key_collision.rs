use std::collections::BTreeMap;

use pierre::record::Record;
use pierre::storage::Storage;

/// Proves the fix for the restart-collision risk: two records committed at the exact
/// same `timestamp_ns` get distinct keys (fresh randomness per record, not a counter
/// that resets to 0 on every restart), and both stay independently retrievable — no
/// silent overwrite under normal KV last-write-wins semantics.
#[tokio::test]
async fn same_timestamp_records_get_distinct_keys_and_both_survive() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).await.unwrap();

    let same_ts = 1_700_000_000_000_000_000i64;
    let mut keys = Vec::new();
    for i in 0..1000 {
        let mut fields = BTreeMap::new();
        fields.insert("seq".to_string(), i.to_string());
        let record = Record {
            timestamp_ns: same_ts,
            message: format!("record {i}"),
            fields,
            template_id: 0,
        };
        let key = storage.commit(&record).await.unwrap();
        keys.push(key);
    }

    let unique_keys: std::collections::HashSet<_> = keys.iter().collect();
    assert_eq!(
        unique_keys.len(),
        1000,
        "1000 records at the identical timestamp must all get distinct keys"
    );

    // All 1000 must be independently retrievable (no overwrite/collision).
    let results = storage.range(same_ts, same_ts + 1).await.unwrap();
    assert_eq!(
        results.len(),
        1000,
        "all 1000 same-timestamp records must survive, none silently overwritten"
    );
}
