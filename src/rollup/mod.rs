pub mod sketch;
pub mod space_saving;
pub mod worker;

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tokio::sync::mpsc;

use crate::storage::Storage;
pub use sketch::RollupKind;
use worker::TierConfig;

#[derive(Debug, Clone)]
pub struct RollupSample {
    pub field: String,
    pub value: String,
}

/// Handle ingest holds to feed the rollup worker without ever blocking on it.
#[derive(Clone)]
pub struct RollupHandle {
    sender: mpsc::Sender<RollupSample>,
    dropped: Arc<AtomicU64>,
}

impl RollupHandle {
    /// Non-blocking; drops and counts the sample if the worker is behind (FR-19).
    pub fn record(&self, field: String, value: String) {
        if self.sender.try_send(RollupSample { field, value }).is_err() {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn dropped_count(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}

/// Spawns the rollup worker for the given field→kind mapping, maintaining all four
/// tiers (minute/hour/day/month) per `tiers`.
pub fn spawn(storage: Arc<Storage>, field_kinds: HashMap<String, RollupKind>, tiers: TierConfig) -> RollupHandle {
    let (sender, receiver) = mpsc::channel(1024);
    let dropped = Arc::new(AtomicU64::new(0));
    tokio::spawn(worker::run(receiver, storage, field_kinds, tiers));
    RollupHandle { sender, dropped }
}
