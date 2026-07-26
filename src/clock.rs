//! Current time as epoch nanoseconds — every ingest surface needs "now" as a
//! fallback when its wire format's own timestamp is absent or unparseable
//! (syslog's nil `TIMESTAMP`, ES `_bulk`'s missing `@timestamp`, OTLP's absent
//! `time_unix_nano`/`observed_time_unix_nano`, the rollup worker's bucket
//! boundaries). One shared reading instead of `jiff::Timestamp::now()
//! .as_nanosecond() as i64` copy-pasted at each call site — this used to be
//! duplicated verbatim in three files, plus a fourth, separately-implemented
//! version built on `std::time::SystemTime` in `rollup::worker`, that could
//! silently drift from the other three if either implementation ever changed.

/// Current wall-clock time, nanoseconds since the Unix epoch.
pub fn now_ns() -> i64 {
    jiff::Timestamp::now().as_nanosecond() as i64
}
