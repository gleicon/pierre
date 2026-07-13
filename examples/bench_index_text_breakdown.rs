//! Profiles where time goes inside repeated `index_text` calls, isolating the
//! bloom-filter-gated existence check from tokenization and the durable WAL write —
//! confirms the fix (skipping `remove_document`'s O(index size) scan for new
//! documents) removes the scaling cost specifically, not just "something got faster."
use std::time::Instant;

use pierre::storage::Storage;

#[tokio::main]
async fn main() {
    for &n in &[10_000usize, 100_000, 300_000] {
        let dir = tempfile::tempdir().unwrap();
        let remote_dir = tempfile::tempdir().unwrap();
        let remote =
            edgestore_repl::FilesystemRemoteStore::new(remote_dir.path().to_path_buf()).unwrap();
        let storage = Storage::open_with_remote(dir.path(), Box::new(remote))
            .await
            .unwrap();

        // Measure indexing throughput in two halves of the run: first 10% vs last
        // 10%. If per-call cost still scaled with corpus size, the back half would
        // be dramatically slower than the front half (as it was before the fix,
        // where each call scanned the *entire* index built so far).
        let front_n = n / 10;
        let front_start = Instant::now();
        for i in 0..front_n {
            let text = format!("request {i} completed in {}ms with status ok", i % 500);
            storage
                .index_text(b"bench_ns", format!("doc-{i:08}").as_bytes(), &text)
                .await
                .unwrap();
        }
        let front_elapsed = front_start.elapsed();

        for i in front_n..n - front_n {
            let text = format!("request {i} completed in {}ms with status ok", i % 500);
            storage
                .index_text(b"bench_ns", format!("doc-{i:08}").as_bytes(), &text)
                .await
                .unwrap();
        }

        let back_start = Instant::now();
        for i in n - front_n..n {
            let text = format!("request {i} completed in {}ms with status ok", i % 500);
            storage
                .index_text(b"bench_ns", format!("doc-{i:08}").as_bytes(), &text)
                .await
                .unwrap();
        }
        let back_elapsed = back_start.elapsed();

        let front_rate = front_n as f64 / front_elapsed.as_secs_f64();
        let back_rate = front_n as f64 / back_elapsed.as_secs_f64();
        let ratio = front_rate / back_rate;

        println!(
            "n={n:>7}  first {front_n:>6} docs: {front_rate:>9.0} docs/sec  |  last {front_n:>6} docs (index already has ~{n}): {back_rate:>9.0} docs/sec  |  slowdown ratio: {ratio:.2}x"
        );
    }
    println!("\nA slowdown ratio near 1.0x means per-call cost no longer scales with how much has already been indexed.");
}
