use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use edgestore::TextSearchResult;
use tokio::sync::mpsc;

use crate::storage::Storage;

#[derive(Debug, Clone)]
struct TextSample {
    key: Vec<u8>,
    text: String,
    timestamp_ns: i64,
}

/// Handle ingest holds to feed the BM25 indexer without ever blocking on it (FR-10).
#[derive(Clone)]
pub struct TextIndexHandle {
    sender: mpsc::Sender<TextSample>,
    dropped: Arc<AtomicU64>,
}

impl TextIndexHandle {
    /// Non-blocking; drops and counts the sample if the worker is behind, same
    /// backpressure philosophy as rollups (FR-19) even though BM25 has no matching FR.
    pub fn record(&self, key: Vec<u8>, text: String, timestamp_ns: i64) {
        if self.sender.try_send(TextSample { key, text, timestamp_ns }).is_err() {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn dropped_count(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}

/// Spawns the BM25 async indexer. `bucket_duration` bounds each namespace's BM25
/// corpus size (FR-11) — a new namespace per completed bucket window, so query
/// latency and in-memory index footprint stay bounded regardless of total retention.
/// `flush_interval` bounds the crash-recovery rebuild cost (FR-23) by keeping the
/// persisted index sidecar close to fresh. Returns the worker's `JoinHandle` alongside
/// the handle — the worker loop never returns under normal operation (it holds its own
/// `Arc<Storage>` clone for as long as it runs), so callers that need to actually tear
/// down storage (tests simulating a crash, graceful shutdown) must abort it explicitly.
pub fn spawn(
    storage: Arc<Storage>,
    bucket_duration: Duration,
    flush_interval: Duration,
) -> (TextIndexHandle, tokio::task::JoinHandle<()>) {
    let (sender, mut receiver) = mpsc::channel::<TextSample>(1024);
    let dropped = Arc::new(AtomicU64::new(0));

    let join_handle = tokio::spawn(async move {
        let mut flush_tick = tokio::time::interval(flush_interval);
        flush_tick.tick().await; // first tick fires immediately; consume it (see backup::spawn)

        loop {
            tokio::select! {
                sample = receiver.recv() => {
                    match sample {
                        Some(sample) => {
                            let ns = bucket_namespace(sample.timestamp_ns, bucket_duration);
                            if let Err(e) = storage.index_text(&ns, &sample.key, &sample.text).await {
                                log::warn!("BM25 indexing failed: {e}");
                            }
                        }
                        None => return, // all senders dropped
                    }
                }
                _ = flush_tick.tick() => {
                    if let Err(e) = storage.flush().await {
                        log::warn!("BM25 index flush failed: {e}");
                    }
                }
            }
        }
    });

    (TextIndexHandle { sender, dropped }, join_handle)
}

/// Answers a line-filter query (FR-14) by searching every bucket namespace overlapping
/// `[start_ns, end_ns)` and merging results by score, descending.
/// `bucket_namespaces_for_range` enumerates every bucket *index* in the requested
/// range, not just ones that actually hold data — each one costs a real async
/// `search_text` round-trip. An unbounded range (e.g. a client naively asking to
/// search "all time", `start=0, end=i64::MAX`) would otherwise enumerate millions
/// of buckets and hang the server. Found via this project's own end-to-end Promtail
/// test using a genuinely maximal range, not caught by any prior test (which always
/// used reasonably narrow ranges).
const MAX_BUCKETS_PER_SEARCH: usize = 200;

pub async fn search(
    storage: &Storage,
    start_ns: i64,
    end_ns: i64,
    bucket_duration: Duration,
    query: &str,
    k: usize,
) -> anyhow::Result<Vec<TextSearchResult>> {
    let bucket_ns = bucket_duration.as_nanos().max(1) as i64;
    let bucket_count = (end_ns.saturating_sub(start_ns) / bucket_ns).saturating_add(1);
    if bucket_count > MAX_BUCKETS_PER_SEARCH as i64 {
        anyhow::bail!(
            "search range spans ~{bucket_count} buckets (limit {MAX_BUCKETS_PER_SEARCH}) at {}s bucket duration — narrow the time range",
            bucket_duration.as_secs()
        );
    }

    let namespaces = bucket_namespaces_for_range(start_ns, end_ns, bucket_duration);
    let mut merged = Vec::new();
    for ns in namespaces {
        merged.extend(storage.search_text(&ns, query, k).await?);
    }
    merged.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));
    merged.truncate(k);
    Ok(merged)
}

/// The bucket namespace a given timestamp falls into — one BM25 index per window.
fn bucket_namespace(timestamp_ns: i64, bucket_duration: Duration) -> Vec<u8> {
    let bucket_ns = bucket_duration.as_nanos().max(1) as i64;
    let bucket_start = (timestamp_ns.div_euclid(bucket_ns)) * bucket_ns;
    format!("text_bucket_{bucket_start}").into_bytes()
}

/// All distinct bucket namespaces overlapping `[start_ns, end_ns)`.
fn bucket_namespaces_for_range(start_ns: i64, end_ns: i64, bucket_duration: Duration) -> Vec<Vec<u8>> {
    let bucket_ns = bucket_duration.as_nanos().max(1) as i64;
    let first_bucket = start_ns.div_euclid(bucket_ns) * bucket_ns;
    let mut out = Vec::new();
    let mut bucket_start = first_bucket;
    while bucket_start < end_ns {
        out.push(format!("text_bucket_{bucket_start}").into_bytes());
        bucket_start += bucket_ns;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_bucket_for_timestamps_in_the_same_window() {
        let bucket_duration = Duration::from_secs(60);
        let ns1 = bucket_namespace(0, bucket_duration);
        let ns2 = bucket_namespace(59_999_999_999, bucket_duration);
        assert_eq!(ns1, ns2);
    }

    #[test]
    fn different_bucket_across_a_window_boundary() {
        let bucket_duration = Duration::from_secs(60);
        let ns1 = bucket_namespace(59_999_999_999, bucket_duration);
        let ns2 = bucket_namespace(60_000_000_000, bucket_duration);
        assert_ne!(ns1, ns2);
    }

    #[test]
    fn range_spanning_three_buckets_returns_three_namespaces() {
        let bucket_duration = Duration::from_secs(60);
        let namespaces = bucket_namespaces_for_range(0, 150_000_000_000, bucket_duration);
        assert_eq!(namespaces.len(), 3);
    }
}
