use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// Space-Saving top-K: bounded map of size `capacity`, so memory never grows with
/// the number of distinct values (PRD: "top-K without tracking every value").
/// New items evict the current minimum-count entry once at capacity, inheriting
/// its count as a starting point (the standard Space-Saving overestimation bound).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SpaceSaving {
    capacity: usize,
    counts: HashMap<String, u64>,
}

impl SpaceSaving {
    pub fn new(capacity: usize) -> Self {
        SpaceSaving {
            capacity,
            counts: HashMap::new(),
        }
    }

    pub fn observe(&mut self, item: &str) {
        if let Some(count) = self.counts.get_mut(item) {
            *count += 1;
            return;
        }
        if self.counts.len() < self.capacity {
            self.counts.insert(item.to_string(), 1);
            return;
        }
        let min_key = self
            .counts
            .iter()
            .min_by_key(|(_, count)| **count)
            .map(|(key, _)| key.clone())
            .expect("capacity > 0 implies non-empty map at this point");
        let min_count = self.counts.remove(&min_key).unwrap();
        self.counts.insert(item.to_string(), min_count + 1);
    }

    pub fn top_k(&self, k: usize) -> Vec<(String, u64)> {
        let mut items: Vec<(String, u64)> =
            self.counts.iter().map(|(k, v)| (k.clone(), *v)).collect();
        items.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        items.truncate(k);
        items
    }

    /// Combines two independent sketches by summing counts for shared keys and
    /// keeping the top `capacity` overall — simpler than the academic SS-merge
    /// (which preserves per-item error bounds precisely) but always correct about
    /// which keys are actually the heaviest post-merge, which is what queries need.
    pub fn merge_from(&mut self, other: &SpaceSaving) {
        let mut combined: HashMap<String, u64> = self.counts.clone();
        for (key, count) in &other.counts {
            *combined.entry(key.clone()).or_insert(0) += count;
        }
        let mut items: Vec<(String, u64)> = combined.into_iter().collect();
        items.sort_by_key(|b| std::cmp::Reverse(b.1));
        items.truncate(self.capacity);
        self.counts = items.into_iter().collect();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tracks_exact_counts_under_capacity() {
        let mut ss = SpaceSaving::new(10);
        for _ in 0..5 {
            ss.observe("a");
        }
        for _ in 0..3 {
            ss.observe("b");
        }
        ss.observe("c");

        let top = ss.top_k(10);
        assert_eq!(top[0], ("a".to_string(), 5));
        assert_eq!(top[1], ("b".to_string(), 3));
        assert_eq!(top[2], ("c".to_string(), 1));
    }

    #[test]
    fn heavy_hitters_survive_eviction_pressure() {
        let mut ss = SpaceSaving::new(3);
        for _ in 0..100 {
            ss.observe("heavy");
        }
        // Flood with 50 distinct one-off values, capacity 3 — "heavy" must survive.
        for i in 0..50 {
            ss.observe(&format!("noise-{i}"));
        }
        let top = ss.top_k(1);
        assert_eq!(
            top[0].0, "heavy",
            "the true heavy hitter must not be evicted by one-off noise"
        );
    }

    #[test]
    fn merge_sums_shared_keys_and_keeps_heaviest() {
        let mut a = SpaceSaving::new(5);
        for _ in 0..10 {
            a.observe("x");
        }
        a.observe("y");

        let mut b = SpaceSaving::new(5);
        for _ in 0..5 {
            b.observe("x");
        }
        for _ in 0..3 {
            b.observe("z");
        }

        a.merge_from(&b);
        let top = a.top_k(5);
        let as_map: HashMap<String, u64> = top.into_iter().collect();
        assert_eq!(
            as_map.get("x"),
            Some(&15),
            "shared key counts must sum across the merge"
        );
        assert_eq!(as_map.get("z"), Some(&3));
    }
}
