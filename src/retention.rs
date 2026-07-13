//! A small, shared "wait for an event, then act" primitive (DECISIONS.md "Shared
//! lifecycle/retention pattern"). Local segment pruning is the first user; a future
//! concern with the same shape (e.g. stripping a cold segment's BM25 index once
//! edgestore ships a lifecycle hook for it) should reuse this rather than growing
//! its own bespoke tick loop. Deliberately just a predicate function, not a
//! scheduler — callers drive it from whatever worker loop they already have.

use std::time::{Duration, SystemTime};

/// Whether an event that completed at `event_time` is due for its follow-up action,
/// given `grace_period` must have elapsed. An `event_time` in the future (clock skew
/// or a bug upstream) is treated as not-yet-due rather than an error.
pub fn is_due(event_time: SystemTime, grace_period: Duration) -> bool {
    event_time
        .elapsed()
        .map(|elapsed| elapsed >= grace_period)
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn not_due_before_grace_period_elapses() {
        let event_time = SystemTime::now();
        assert!(!is_due(event_time, Duration::from_secs(3600)));
    }

    #[test]
    fn due_once_grace_period_has_elapsed() {
        let event_time = SystemTime::now() - Duration::from_secs(10);
        assert!(is_due(event_time, Duration::from_secs(5)));
    }

    #[test]
    fn future_event_time_is_not_due() {
        let event_time = SystemTime::now() + Duration::from_secs(3600);
        assert!(!is_due(event_time, Duration::from_secs(1)));
    }
}
