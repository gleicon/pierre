//! SPEC.md #L3: end-to-end ingest throughput (HTTP/TCP → WAL → ack), not the
//! in-process engine-only numbers the 10,000 req/s NFR-2 target was originally
//! derived from. Drives Pierre's real native listener over real TCP with many
//! concurrent connections for a fixed duration and reports achieved req/s.
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const CONCURRENCY: usize = 50;
const DURATION: Duration = Duration::from_secs(5);

#[tokio::main]
async fn main() {
    let dir = tempfile::tempdir().unwrap();
    let storage = Arc::new(pierre::storage::Storage::open(dir.path()).await.unwrap());

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    drop(listener);
    let addr_clone = addr.clone();
    let storage_clone = storage.clone();
    tokio::spawn(async move {
        pierre::listener::native::serve(
            &addr_clone,
            storage_clone,
            Arc::new(vec!["level".to_string()]),
            None,
            None,
            pierre::stats::IngestStats::default(),
        )
        .await
        .unwrap();
    });
    tokio::time::sleep(Duration::from_millis(100)).await;

    let completed = Arc::new(AtomicU64::new(0));
    let stop_at = Instant::now() + DURATION;

    let mut handles = Vec::with_capacity(CONCURRENCY);
    for _ in 0..CONCURRENCY {
        let addr = addr.clone();
        let completed = completed.clone();
        handles.push(tokio::spawn(async move {
            let mut stream = TcpStream::connect(&addr).await.unwrap();
            let payload = serde_json::to_vec(&vec![pierre::record::WireRecord {
                timestamp_ns: 1,
                message: "benchmark ingest line for throughput measurement".to_string(),
                fields: [("level".to_string(), "info".to_string())]
                    .into_iter()
                    .collect(),
            }])
            .unwrap();
            let len_prefix = (payload.len() as u32).to_be_bytes();

            while Instant::now() < stop_at {
                stream.write_all(&len_prefix).await.unwrap();
                stream.write_all(&payload).await.unwrap();
                let mut ack = [0u8; 1];
                stream.read_exact(&mut ack).await.unwrap();
                completed.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    let total = completed.load(Ordering::Relaxed);
    let rate = total as f64 / DURATION.as_secs_f64();
    println!("concurrency={CONCURRENCY}  duration={DURATION:?}  total_acked={total}  achieved_req_s={rate:.0}");
    println!(
        "NFR-2 target: 10,000 req/s — {}",
        if rate >= 10_000.0 {
            "MET"
        } else {
            "NOT MET at this concurrency"
        }
    );
}
