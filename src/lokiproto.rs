//! Decodes Loki's real push wire format: a `PushRequest` protobuf message,
//! raw-snappy-compressed (not framed), which is what Promtail/Alloy/Vector/Fluent Bit
//! all send by default — confirmed against Loki's own server-side handling
//! (`pkg/loghttp/push/push.go`: "when no content-type header is set, or set to
//! `application/x-protobuf`: expect snappy compression", decompressed via
//! `util.RawSnappy` then `proto.Unmarshal`). JSON is the alternate, not the default,
//! format real collectors use — this module is what makes Pierre's Loki endpoint
//! actually usable by a stock collector config, not just a synthetic JSON push.

include!(concat!(env!("OUT_DIR"), "/logproto.rs"));

use std::collections::BTreeMap;

use crate::logql;

/// One decoded stream: its label set, plus `(timestamp_ns, line)` entries.
pub struct DecodedStream {
    pub labels: BTreeMap<String, String>,
    pub entries: Vec<(i64, String)>,
}

/// Caps the *claimed* decompressed size before allocating for it. Raw snappy's
/// header embeds the uncompressed length and `snap`'s own decoder only rejects
/// claims above ~4GB (`u32::MAX`) — well within reach of a single small POST body,
/// since the claim itself is just a few header bytes, not the actual payload. Sized
/// generously above any realistic Loki push batch (found via `/ds-security-review`).
const MAX_DECOMPRESSED_BYTES: usize = 64 * 1024 * 1024;

pub fn decode_push_request(body: &[u8]) -> anyhow::Result<Vec<DecodedStream>> {
    let claimed_len = snap::raw::decompress_len(body).map_err(|e| anyhow::anyhow!("invalid snappy header: {e}"))?;
    if claimed_len > MAX_DECOMPRESSED_BYTES {
        anyhow::bail!("snappy payload claims {claimed_len} decompressed bytes, exceeding the {MAX_DECOMPRESSED_BYTES} limit");
    }

    let decompressed = snap::raw::Decoder::new()
        .decompress_vec(body)
        .map_err(|e| anyhow::anyhow!("snappy decompress failed: {e}"))?;

    let req = <PushRequest as prost::Message>::decode(decompressed.as_slice())
        .map_err(|e| anyhow::anyhow!("protobuf decode failed: {e}"))?;

    let mut streams = Vec::with_capacity(req.streams.len());
    for stream in req.streams {
        let labels = logql::parse_label_set(&stream.labels)
            .map_err(|e| anyhow::anyhow!("malformed stream labels {:?}: {e}", stream.labels))?;
        let entries = stream
            .entries
            .into_iter()
            .map(|entry| {
                let ts = entry.timestamp.unwrap_or_default();
                let timestamp_ns = ts.seconds * 1_000_000_000 + ts.nanos as i64;
                (timestamp_ns, entry.line)
            })
            .collect();
        streams.push(DecodedStream { labels, entries });
    }
    Ok(streams)
}
