/// Proves the decompression-bomb guard (`/ds-security-review` finding): a tiny
/// payload whose raw-snappy header *claims* a huge decompressed size must be
/// rejected before any large allocation is attempted — not just eventually fail
/// after allocating. `snap`'s own decoder only caps the claim at ~4GB
/// (`u32::MAX`), which is itself large enough to be a real memory-exhaustion risk
/// from a request body of only a few bytes.
#[test]
fn oversized_snappy_claim_is_rejected_without_decompressing() {
    // Raw snappy format: a varint-encoded claimed length, followed by the
    // compressed block. We only need a valid header — decode_push_request must
    // reject based on the claim alone, so no real compressed data follows.
    let claimed_len: u64 = 200_000_000; // 200MB, well over the 64MB limit
    let mut body = encode_varint(claimed_len);
    body.push(0); // a token trailing byte; irrelevant, rejection happens first

    match pierre::lokiproto::decode_push_request(&body) {
        Ok(_) => panic!("a snappy payload claiming 200MB decompressed must be rejected"),
        Err(e) => assert!(
            e.to_string().contains("claims"),
            "error should explain the oversized claim, got: {e}"
        ),
    }
}

#[test]
fn realistic_snappy_payload_still_decodes_correctly() {
    // Sanity check the guard doesn't reject legitimate, modestly-sized payloads.
    let push_request = pierre::lokiproto::PushRequest {
        streams: vec![pierre::lokiproto::StreamAdapter {
            labels: r#"{level="info"}"#.to_string(),
            entries: vec![pierre::lokiproto::EntryAdapter {
                timestamp: Some(prost_types::Timestamp {
                    seconds: 1,
                    nanos: 0,
                }),
                line: "well within limits".to_string(),
                structured_metadata: vec![],
                parsed: vec![],
            }],
            hash: 0,
        }],
        format: "".to_string(),
    };
    let mut encoded = Vec::new();
    prost::Message::encode(&push_request, &mut encoded).unwrap();
    let compressed = snap::raw::Encoder::new().compress_vec(&encoded).unwrap();

    let decoded = pierre::lokiproto::decode_push_request(&compressed).unwrap();
    assert_eq!(decoded.len(), 1);
    assert_eq!(decoded[0].entries[0].1, "well within limits");
}

/// Boundary check for the guard above: a claim of exactly `MAX_DECOMPRESSED_BYTES`
/// (64MB in lokiproto.rs, not exported — mirrored here) must not be rejected by the
/// size guard. No real compressed data follows, so decoding still fails past the
/// guard — this only proves the guard itself doesn't reject at exactly the limit.
#[test]
fn claim_exactly_at_the_limit_is_not_rejected_by_the_size_guard() {
    let claimed_len: u64 = 64 * 1024 * 1024;
    let mut body = encode_varint(claimed_len);
    body.push(0);

    match pierre::lokiproto::decode_push_request(&body) {
        Ok(_) => panic!("no real compressed data was provided, decoding should still fail for a different reason"),
        Err(e) => assert!(
            !e.to_string().contains("claims"),
            "a claim of exactly the limit must not be rejected by the size guard, got: {e}"
        ),
    }
}

#[test]
fn claim_one_byte_over_the_limit_is_rejected() {
    let claimed_len: u64 = 64 * 1024 * 1024 + 1;
    let mut body = encode_varint(claimed_len);
    body.push(0);

    match pierre::lokiproto::decode_push_request(&body) {
        Ok(_) => panic!("a snappy payload claiming one byte over the limit must be rejected"),
        Err(e) => assert!(
            e.to_string().contains("claims"),
            "error should explain the oversized claim, got: {e}"
        ),
    }
}

/// Raw snappy's varint header: 7 bits per byte, LSB first, continuation bit (0x80)
/// set on every byte but the last.
fn encode_varint(mut n: u64) -> Vec<u8> {
    let mut bytes = Vec::new();
    loop {
        let mut byte = (n & 0x7F) as u8;
        n >>= 7;
        if n != 0 {
            byte |= 0x80;
        }
        bytes.push(byte);
        if n == 0 {
            break;
        }
    }
    bytes
}
