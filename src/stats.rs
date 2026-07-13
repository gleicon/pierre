use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::rollup::RollupHandle;
use crate::textindex::TextIndexHandle;

/// Shared ingest counter, incremented once per committed record. Not a
/// metrics/observability surface (Pierre doesn't have one yet, see STATUS.md) —
/// just enough live signal, read periodically by `spawn` below, to watch ingest
/// rate while pushing real load.
#[derive(Clone, Default)]
pub struct IngestStats {
    committed: Arc<AtomicU64>,
}

impl IngestStats {
    pub fn record_commit(&self) {
        self.committed.fetch_add(1, Ordering::Relaxed);
    }

    fn committed_count(&self) -> u64 {
        self.committed.load(Ordering::Relaxed)
    }
}

/// Logs ingest rate plus the rollup/textindex drop counters on a fixed interval —
/// same worker-loop shape as `backup::spawn`/`rollup::spawn`. `dropped_count()` is
/// cumulative, so each tick logs the running total, not a delta; ingest rate is a
/// delta computed from the previous tick's count.
pub fn spawn(stats: IngestStats, rollup: Option<RollupHandle>, textindex: Option<TextIndexHandle>, interval: Duration) {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(interval);
        let mut last_committed = 0u64;
        loop {
            tick.tick().await;
            let committed = stats.committed_count();
            let rate = (committed - last_committed) as f64 / interval.as_secs_f64();
            last_committed = committed;

            let rollup_dropped = rollup.as_ref().map(RollupHandle::dropped_count).unwrap_or(0);
            let textindex_dropped = textindex.as_ref().map(TextIndexHandle::dropped_count).unwrap_or(0);
            log::info!(
                "stats: ingest={committed} total, {rate:.1} rec/s | rollup dropped={rollup_dropped} | textindex dropped={textindex_dropped}"
            );
        }
    });
}
