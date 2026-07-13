//! Finds exactly where indexing throughput stalls on the way to 1M documents —
//! bench_index_text_breakdown.rs only sampled up to 300K and looked flat there, but
//! the full 1M crash-recovery benchmark hung well past where that trend predicted.
//! Prints a checkpoint every 50K docs (cumulative + instantaneous rate) so any
//! remaining scaling issue shows up as a clear inflection point.
use std::time::Instant;

use pierre::storage::Storage;

#[tokio::main]
async fn main() {
    let dir = tempfile::tempdir().unwrap();
    let remote_dir = tempfile::tempdir().unwrap();
    let remote =
        edgestore_repl::FilesystemRemoteStore::new(remote_dir.path().to_path_buf()).unwrap();
    let storage = Storage::open_with_remote(dir.path(), Box::new(remote))
        .await
        .unwrap();

    let checkpoint = 50_000usize;
    let total = 1_000_000usize;
    let run_start = Instant::now();
    let mut checkpoint_start = Instant::now();

    for i in 0..total {
        let text = format!("request {i} completed in {}ms with status ok", i % 500);
        storage
            .index_text(b"bench_ns", format!("doc-{i:08}").as_bytes(), &text)
            .await
            .unwrap();

        if (i + 1) % checkpoint == 0 {
            let checkpoint_elapsed = checkpoint_start.elapsed();
            let rate = checkpoint as f64 / checkpoint_elapsed.as_secs_f64();
            println!(
                "at {:>8} docs: checkpoint took {:>8.2?} ({:>9.0} docs/sec)  total elapsed: {:>8.2?}",
                i + 1,
                checkpoint_elapsed,
                rate,
                run_start.elapsed()
            );
            checkpoint_start = Instant::now();
        }
    }

    println!("\ntotal: {:?} for {total} docs", run_start.elapsed());
}
