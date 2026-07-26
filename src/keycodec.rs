//! Order-preserving encoding for a signed nanosecond timestamp into the
//! unsigned big-endian byte prefix every sortable storage key in Pierre
//! starts with (`Storage::commit`'s log keys, `rollup::worker`'s bucket keys).
//!
//! A plain `timestamp_ns as u64` cast only preserves ordering within one sign:
//! a negative `i64` wraps into the *upper* half of `u64`, sorting after every
//! non-negative timestamp instead of before it. That inverted a real caller's
//! `start_key < end_key` assumption the first time a negative `start_ns` ever
//! reached it (`Storage::range`, reachable straight from an unvalidated
//! `GET /query/logs?start=<negative>` query param) — this file exists so that
//! bug has exactly one fix, not one fix in `storage.rs` plus a second latent
//! copy of the same mistake in `rollup::worker::rollup_key`.

/// Flips the sign bit so unsigned big-endian byte comparison of the result
/// matches `timestamp_ns`'s real signed ordering — negative, zero, and
/// positive values all land in the correct relative order.
pub fn order_preserving_ns(timestamp_ns: i64) -> u64 {
    (timestamp_ns as u64) ^ (1u64 << 63)
}

/// Inverse of `order_preserving_ns`.
pub fn decode_order_preserving_ns(biased: u64) -> i64 {
    (biased ^ (1u64 << 63)) as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn order_holds_across_negative_and_positive_timestamps() {
        let min = order_preserving_ns(i64::MIN);
        let negative = order_preserving_ns(-1_000_000_000);
        let zero = order_preserving_ns(0);
        let positive = order_preserving_ns(1_000_000_000);
        let max = order_preserving_ns(i64::MAX);
        assert!(min < negative);
        assert!(negative < zero);
        assert!(zero < positive);
        assert!(positive < max);
    }

    #[test]
    fn decode_undoes_encode() {
        for ts in [i64::MIN, -1_000_000_000, -1, 0, 1, 1_000_000_000, i64::MAX] {
            assert_eq!(decode_order_preserving_ns(order_preserving_ns(ts)), ts);
        }
    }
}
