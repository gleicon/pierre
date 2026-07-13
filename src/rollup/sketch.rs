use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::BuildHasher;

use hyperloglogplus::{HyperLogLog, HyperLogLogPlus};
use serde::{Deserialize, Serialize};
use sketches_ddsketch::{Config as DDConfig, DDSketch};

use super::space_saving::SpaceSaving;

/// `DefaultHasher::new()` is SipHash with a fixed zero key — unlike `RandomState`,
/// which reseeds per-process. HLL merge and cross-restart persistence both require
/// every sketch to hash values identically, so a fixed-seed hasher is required, not
/// just convenient.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FixedHasherBuilder;

impl BuildHasher for FixedHasherBuilder {
    type Hasher = DefaultHasher;
    fn build_hasher(&self) -> Self::Hasher {
        DefaultHasher::new()
    }
}

/// 2^14 registers — roughly 0.8% standard error at ~12KB per sketch, a reasonable
/// default until footprint/accuracy needs are pinned down (SPEC.md open question).
const HLL_PRECISION: u8 = 14;

pub type Hll = HyperLogLogPlus<String, FixedHasherBuilder>;

fn new_hll() -> Hll {
    Hll::new(HLL_PRECISION, FixedHasherBuilder).expect("HLL_PRECISION is a valid, fixed constant")
}

/// Default Space-Saving capacity — matches the PRD's `k = 20` top-K example.
const TOPK_CAPACITY: usize = 20;

fn new_ddsketch() -> DDSketch {
    // relative_accuracy = 0.01 matches the PRD's example config; DDConfig::defaults()
    // already uses alpha=0.01, so this is explicit for clarity rather than implicit.
    DDSketch::new(DDConfig::new(0.01, 2048, 1.0e-9))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RollupKind {
    Exact,
    Hll,
    TopK,
    DDSketch,
}

impl RollupKind {
    fn new_sketch(self) -> FieldSketch {
        match self {
            RollupKind::Exact => FieldSketch::Exact(HashMap::new()),
            RollupKind::Hll => FieldSketch::Hll(Box::new(new_hll())),
            RollupKind::TopK => FieldSketch::TopK(SpaceSaving::new(TOPK_CAPACITY)),
            RollupKind::DDSketch => FieldSketch::DDSketch(Box::new(new_ddsketch())),
        }
    }
}

/// One field's live sketch state for the current bucket window. Self-describing on
/// disk (tag byte + payload) so a merge pass can reconstruct the right variant
/// without external config.
#[derive(Clone)]
pub enum FieldSketch {
    Exact(HashMap<String, u64>),
    Hll(Box<Hll>),
    TopK(SpaceSaving),
    DDSketch(Box<DDSketch>),
}

const TAG_EXACT: u8 = 0;
const TAG_HLL: u8 = 1;
const TAG_TOPK: u8 = 2;
const TAG_DDSKETCH: u8 = 3;

impl FieldSketch {
    pub fn new_for_kind(kind: RollupKind) -> Self {
        kind.new_sketch()
    }

    /// For `DDSketch`, `value` must parse as `f64` (numeric fields only); a
    /// non-numeric value is logged and dropped rather than panicking the worker.
    pub fn observe(&mut self, value: &str) {
        match self {
            FieldSketch::Exact(counts) => {
                *counts.entry(value.to_string()).or_insert(0) += 1;
            }
            FieldSketch::Hll(hll) => hll.insert(value),
            FieldSketch::TopK(ss) => ss.observe(value),
            FieldSketch::DDSketch(dd) => match value.parse::<f64>() {
                Ok(v) => dd.add(v),
                Err(_) => {
                    log::warn!("rollup: non-numeric value {value:?} for ddsketch field, dropped")
                }
            },
        }
    }

    pub fn to_bytes(&self) -> anyhow::Result<Vec<u8>> {
        let mut buf = Vec::new();
        match self {
            FieldSketch::Exact(counts) => {
                buf.push(TAG_EXACT);
                buf.extend(serde_json::to_vec(counts)?);
            }
            FieldSketch::Hll(hll) => {
                buf.push(TAG_HLL);
                buf.extend(serde_json::to_vec(hll)?);
            }
            FieldSketch::TopK(ss) => {
                buf.push(TAG_TOPK);
                buf.extend(serde_json::to_vec(ss)?);
            }
            FieldSketch::DDSketch(dd) => {
                buf.push(TAG_DDSKETCH);
                buf.extend(serde_json::to_vec(dd)?);
            }
        }
        Ok(buf)
    }

    pub fn from_bytes(bytes: &[u8]) -> anyhow::Result<Self> {
        let (tag, rest) = bytes
            .split_first()
            .ok_or_else(|| anyhow::anyhow!("empty sketch payload"))?;
        match *tag {
            TAG_EXACT => Ok(FieldSketch::Exact(serde_json::from_slice(rest)?)),
            TAG_HLL => Ok(FieldSketch::Hll(Box::new(serde_json::from_slice(rest)?))),
            TAG_TOPK => Ok(FieldSketch::TopK(serde_json::from_slice(rest)?)),
            TAG_DDSKETCH => Ok(FieldSketch::DDSketch(Box::new(serde_json::from_slice(
                rest,
            )?))),
            other => Err(anyhow::anyhow!("unknown rollup sketch tag {other}")),
        }
    }

    /// Algebraically merges `other` into `self` — same operation the hour/day/month
    /// tiers use to fold a finer tier up without rescanning raw data (FR-16).
    pub fn merge_from(&mut self, other: &FieldSketch) -> anyhow::Result<()> {
        match (self, other) {
            (FieldSketch::Exact(a), FieldSketch::Exact(b)) => {
                for (value, count) in b {
                    *a.entry(value.clone()).or_insert(0) += count;
                }
                Ok(())
            }
            (FieldSketch::Hll(a), FieldSketch::Hll(b)) => a
                .merge(b)
                .map_err(|e| anyhow::anyhow!("HLL merge failed: {e:?}")),
            (FieldSketch::TopK(a), FieldSketch::TopK(b)) => {
                a.merge_from(b);
                Ok(())
            }
            (FieldSketch::DDSketch(a), FieldSketch::DDSketch(b)) => a
                .merge(b)
                .map_err(|e| anyhow::anyhow!("DDSketch merge failed: {e}")),
            _ => Err(anyhow::anyhow!(
                "cannot merge mismatched rollup sketch kinds"
            )),
        }
    }

    /// Raw value→count map — only meaningful for `Exact`; other kinds return `None`.
    pub fn exact_counts(&self) -> Option<std::collections::BTreeMap<String, u64>> {
        match self {
            FieldSketch::Exact(counts) => {
                Some(counts.iter().map(|(k, v)| (k.clone(), *v)).collect())
            }
            _ => None,
        }
    }

    /// Cardinality estimate — only meaningful for `Hll`; other kinds return `None`.
    pub fn hll_estimate(&mut self) -> Option<f64> {
        match self {
            FieldSketch::Hll(hll) => Some(hll.count()),
            _ => None,
        }
    }

    /// Top-`k` heaviest values — only meaningful for `TopK`; other kinds return `None`.
    pub fn top_k(&self, k: usize) -> Option<Vec<(String, u64)>> {
        match self {
            FieldSketch::TopK(ss) => Some(ss.top_k(k)),
            _ => None,
        }
    }

    /// Relative-error quantile (e.g. `q=0.99` for p99) — only meaningful for
    /// `DDSketch`; other kinds return `None`.
    pub fn quantile(&self, q: f64) -> Option<f64> {
        match self {
            FieldSketch::DDSketch(dd) => dd.quantile(q).ok().flatten(),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hll_roundtrips_through_bytes() {
        let mut sketch = FieldSketch::new_for_kind(RollupKind::Hll);
        for i in 0..500 {
            sketch.observe(&format!("user-{i}"));
        }
        let bytes = sketch.to_bytes().unwrap();
        let mut decoded = FieldSketch::from_bytes(&bytes).unwrap();
        let estimate = decoded.hll_estimate().unwrap();
        assert!(
            (450.0..550.0).contains(&estimate),
            "estimate {estimate} should be close to 500"
        );
    }

    #[test]
    fn hll_merge_unions_distinct_counts() {
        let mut a = FieldSketch::new_for_kind(RollupKind::Hll);
        for i in 0..300 {
            a.observe(&format!("user-{i}"));
        }
        let mut b = FieldSketch::new_for_kind(RollupKind::Hll);
        for i in 200..500 {
            // overlaps a in [200,300)
            b.observe(&format!("user-{i}"));
        }
        a.merge_from(&b).unwrap();
        let estimate = a.hll_estimate().unwrap();
        // union of [0,300) and [200,500) is [0,500) => ~500 distinct, not 300+300=600
        assert!(
            (450.0..550.0).contains(&estimate),
            "merged estimate {estimate} should reflect the union, not the sum"
        );
    }

    #[test]
    fn topk_roundtrips_and_merges_through_bytes() {
        let mut a = FieldSketch::new_for_kind(RollupKind::TopK);
        for _ in 0..10 {
            a.observe("/api/orders");
        }
        a.observe("/api/rare");

        let bytes = a.to_bytes().unwrap();
        let mut decoded = FieldSketch::from_bytes(&bytes).unwrap();

        let mut b = FieldSketch::new_for_kind(RollupKind::TopK);
        for _ in 0..5 {
            b.observe("/api/orders");
        }
        decoded.merge_from(&b).unwrap();

        let top = decoded.top_k(1).unwrap();
        assert_eq!(top[0], ("/api/orders".to_string(), 15));
    }

    #[test]
    fn ddsketch_estimates_quantile_within_relative_error() {
        let mut sketch = FieldSketch::new_for_kind(RollupKind::DDSketch);
        for i in 1..=1000 {
            sketch.observe(&i.to_string());
        }
        let p99 = sketch.quantile(0.99).unwrap();
        // True p99 of 1..=1000 is 990; DDSketch guarantees relative error <= alpha (0.01).
        assert!(
            (980.0..1000.0).contains(&p99),
            "p99 estimate {p99} should be within ~1% of 990"
        );
    }

    #[test]
    fn ddsketch_roundtrips_and_merges_through_bytes() {
        let mut a = FieldSketch::new_for_kind(RollupKind::DDSketch);
        for i in 1..=500 {
            a.observe(&i.to_string());
        }
        let bytes = a.to_bytes().unwrap();
        let mut decoded = FieldSketch::from_bytes(&bytes).unwrap();

        let mut b = FieldSketch::new_for_kind(RollupKind::DDSketch);
        for i in 501..=1000 {
            b.observe(&i.to_string());
        }
        decoded.merge_from(&b).unwrap();

        let p99 = decoded.quantile(0.99).unwrap();
        assert!(
            (980.0..1000.0).contains(&p99),
            "merged p99 estimate {p99} should reflect the full 1..=1000 range"
        );
    }

    #[test]
    fn ddsketch_drops_non_numeric_values_without_panicking() {
        let mut sketch = FieldSketch::new_for_kind(RollupKind::DDSketch);
        sketch.observe("not-a-number"); // must be silently dropped, not panic or get added
        sketch.observe("42");
        let median = sketch.quantile(0.5).unwrap();
        assert!(
            (41.0..43.0).contains(&median),
            "only the numeric value should have been added; got median {median}"
        );
    }

    #[test]
    fn exact_merge_sums_counts() {
        let mut a = FieldSketch::new_for_kind(RollupKind::Exact);
        a.observe("x");
        a.observe("x");
        let mut b = FieldSketch::new_for_kind(RollupKind::Exact);
        b.observe("x");
        b.observe("y");
        a.merge_from(&b).unwrap();
        match a {
            FieldSketch::Exact(counts) => {
                assert_eq!(counts.get("x"), Some(&3));
                assert_eq!(counts.get("y"), Some(&1));
            }
            _ => panic!("expected Exact"),
        }
    }
}
