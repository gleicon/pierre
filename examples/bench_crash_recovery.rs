//! SPEC.md #L2: measures `Storage::open()` (crash-recovery rebuild) latency as a
//! function of namespace size. A crash before any `flush()` leaves the BM25 index
//! sidecar missing entirely, forcing `rebuild_text_indices()`'s full-rebuild path on
//! reopen — this benchmark measures exactly that cost, not the happy path.
use std::time::Instant;

use pierre::storage::Storage;

#[tokio::main]
async fn main() {
    for &n in &[10_000usize, 100_000, 1_000_000] {
        let dir = tempfile::tempdir().unwrap();
        let remote_dir = tempfile::tempdir().unwrap();

        {
            let remote = edgestore_repl::FilesystemRemoteStore::new(remote_dir.path().to_path_buf()).unwrap();
            let storage = Storage::open_with_remote(dir.path(), Box::new(remote)).await.unwrap();

            let index_start = Instant::now();
            for i in 0..n {
                let text = format!("request {i} completed in {}ms with status ok", i % 500);
                storage.index_text(b"bench_ns", format!("doc-{i:08}").as_bytes(), &text).await.unwrap();
            }
            let index_elapsed = index_start.elapsed();
            println!(
                "n={n:>8}  indexed in {:>8.2?}  ({:>9.0} docs/sec)  — sidecar never flushed (simulated crash before any flush)",
                index_elapsed,
                n as f64 / index_elapsed.as_secs_f64()
            );
            // No flush() call: the crash happens with the sidecar entirely unpersisted.
        } // `storage` dropped here — the "crash".

        let reopen_start = Instant::now();
        let remote = edgestore_repl::FilesystemRemoteStore::new(remote_dir.path().to_path_buf()).unwrap();
        let recovered = Storage::open_with_remote(dir.path(), Box::new(remote)).await.unwrap();
        let reopen_elapsed = reopen_start.elapsed();

        // Confirm the rebuild actually happened and is correct, not just fast.
        let results = recovered.search_text(b"bench_ns", "request", 1).await.unwrap();
        assert_eq!(results.len(), 1, "rebuild must have actually restored the index");

        println!("n={n:>8}  Storage::open() after crash: {:>8.2?}\n", reopen_elapsed);
    }
}
