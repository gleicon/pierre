use std::collections::BTreeMap;
use std::sync::Arc;

use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::sync::Semaphore;

use crate::record::WireRecord;
use crate::rollup::RollupHandle;
use crate::stats::IngestStats;
use crate::storage::Storage;
use crate::textindex::TextIndexHandle;

/// RFC5424 messages are typically well under 1KB; this bounds a single UDP
/// datagram or TCP line before parsing, matching `native.rs`'s per-frame cap —
/// this protocol is deliberately unauthenticated (the appliances/relays that
/// speak it have no bearer-token convention), so an unbounded read is an
/// unauthenticated memory-exhaustion DoS the same way an unbounded frame would be.
const MAX_MESSAGE_BYTES: usize = 64 * 1024;

/// Same reasoning and same value as `native.rs`'s connection cap — caps concurrent
/// TCP connections so the same DoS can't be sidestepped by fanning out connections
/// instead of one big message. UDP has no connection concept, so this only applies
/// to the TCP receiver.
const MAX_CONCURRENT_CONNECTIONS: usize = 1024;

/// Runs both the UDP and TCP RFC5424 receivers on the same address — standard
/// syslog server behavior (distinct transports, no port conflict). Real appliances
/// and legacy systems ("the long tail that never leaves" per the PRD) send over
/// either depending on age/configuration; supporting only one would still leave
/// collectors out.
pub async fn serve(
    addr: &str,
    storage: Arc<Storage>,
    allowed_fields: Arc<Vec<String>>,
    rollup: Option<RollupHandle>,
    textindex: Option<TextIndexHandle>,
    stats: IngestStats,
) -> anyhow::Result<()> {
    let udp = serve_udp(
        addr,
        storage.clone(),
        allowed_fields.clone(),
        rollup.clone(),
        textindex.clone(),
        stats.clone(),
    );
    let tcp = serve_tcp(addr, storage, allowed_fields, rollup, textindex, stats);
    tokio::try_join!(udp, tcp)?;
    Ok(())
}

/// One UDP datagram is one syslog message — the transport itself preserves
/// message boundaries, no framing needed (unlike TCP).
async fn serve_udp(
    addr: &str,
    storage: Arc<Storage>,
    allowed_fields: Arc<Vec<String>>,
    rollup: Option<RollupHandle>,
    textindex: Option<TextIndexHandle>,
    stats: IngestStats,
) -> anyhow::Result<()> {
    let socket = UdpSocket::bind(addr).await?;
    let mut buf = vec![0u8; MAX_MESSAGE_BYTES];
    loop {
        let (len, _peer) = socket.recv_from(&mut buf).await?;
        if let Ok(text) = std::str::from_utf8(&buf[..len]) {
            handle_line(
                text,
                &storage,
                &allowed_fields,
                rollup.as_ref(),
                textindex.as_ref(),
                &stats,
            )
            .await;
        }
    }
}

/// TCP has no message-boundary framing of its own (RFC6587) — this uses
/// newline-delimited framing, the form most real relays and shippers (rsyslog,
/// syslog-ng) actually send over TCP in practice, rather than the rarer
/// octet-counting variant.
async fn serve_tcp(
    addr: &str,
    storage: Arc<Storage>,
    allowed_fields: Arc<Vec<String>>,
    rollup: Option<RollupHandle>,
    textindex: Option<TextIndexHandle>,
    stats: IngestStats,
) -> anyhow::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    let connection_permits = Arc::new(Semaphore::new(MAX_CONCURRENT_CONNECTIONS));
    loop {
        let (stream, _) = listener.accept().await?;
        let permit = match connection_permits.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                log::warn!("syslog TCP listener at capacity ({MAX_CONCURRENT_CONNECTIONS} connections), rejecting new connection");
                continue;
            }
        };
        let storage = storage.clone();
        let allowed_fields = allowed_fields.clone();
        let rollup = rollup.clone();
        let textindex = textindex.clone();
        let stats = stats.clone();
        tokio::spawn(async move {
            let _permit = permit;
            if let Err(e) =
                handle_tcp_connection(stream, storage, allowed_fields, rollup, textindex, stats)
                    .await
            {
                log::warn!("syslog TCP connection ended: {e}");
            }
        });
    }
}

async fn handle_tcp_connection(
    stream: TcpStream,
    storage: Arc<Storage>,
    allowed_fields: Arc<Vec<String>>,
    rollup: Option<RollupHandle>,
    textindex: Option<TextIndexHandle>,
    stats: IngestStats,
) -> anyhow::Result<()> {
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    loop {
        line.clear();
        // `read_line` has no built-in cap; take() bounds how many bytes it will
        // read before giving up, same protection MAX_MESSAGE_BYTES gives the UDP
        // path. A line longer than the cap without a newline yields Ok(0) from the
        // inner limited reader once exhausted, which read_line surfaces as EOF —
        // safe (connection just ends), not a silent truncation treated as valid.
        let mut limited = (&mut reader).take(MAX_MESSAGE_BYTES as u64);
        let n = limited.read_line(&mut line).await?;
        if n == 0 {
            return Ok(()); // connection closed (or a line exceeded the cap)
        }
        handle_line(
            line.trim_end_matches(['\r', '\n']),
            &storage,
            &allowed_fields,
            rollup.as_ref(),
            textindex.as_ref(),
            &stats,
        )
        .await;
    }
}

async fn handle_line(
    line: &str,
    storage: &Storage,
    allowed_fields: &[String],
    rollup: Option<&RollupHandle>,
    textindex: Option<&TextIndexHandle>,
    stats: &IngestStats,
) {
    if line.trim().is_empty() {
        return;
    }
    let wire = parse_rfc5424(line);
    if let Err(e) =
        crate::ingest::commit(storage, wire, allowed_fields, rollup, textindex, None).await
    {
        log::warn!("syslog message rejected: {e}");
        return;
    }
    stats.record_commit();
}

/// Parses one RFC5424 message: `<PRI>VERSION TIMESTAMP HOSTNAME APP-NAME PROCID
/// MSGID STRUCTURED-DATA MSG`. Real appliances and legacy systems ("the long tail
/// that never leaves" per the PRD) are famously inconsistent about strict RFC5424
/// conformance, so a message that doesn't parse cleanly still lands — as its raw
/// text, not dropped — rather than rejecting input a real syslog receiver would
/// have accepted.
fn parse_rfc5424(line: &str) -> WireRecord {
    let now_ns = crate::clock::now_ns();

    let Some(header) = try_parse_header(line) else {
        return WireRecord {
            timestamp_ns: now_ns,
            message: line.to_string(),
            fields: BTreeMap::new(),
        };
    };

    let mut fields = BTreeMap::new();
    fields.insert("facility".to_string(), header.facility.to_string());
    fields.insert("severity".to_string(), header.severity.to_string());
    fields.insert(
        "level".to_string(),
        severity_name(header.severity).to_string(),
    );
    if header.hostname != "-" {
        fields.insert("hostname".to_string(), header.hostname.to_string());
    }
    if header.app_name != "-" {
        fields.insert("app_name".to_string(), header.app_name.to_string());
    }
    if header.procid != "-" {
        fields.insert("procid".to_string(), header.procid.to_string());
    }
    if header.msgid != "-" {
        fields.insert("msgid".to_string(), header.msgid.to_string());
    }

    let timestamp_ns = if header.timestamp != "-" {
        header
            .timestamp
            .parse::<jiff::Timestamp>()
            .map(|t| t.as_nanosecond() as i64)
            .unwrap_or(now_ns)
    } else {
        now_ns
    };

    // MSG may carry a leading UTF-8 BOM (RFC5424 §6.4) — not part of the text.
    let message = header
        .msg
        .strip_prefix('\u{feff}')
        .unwrap_or(header.msg)
        .to_string();

    WireRecord {
        timestamp_ns,
        message,
        fields,
    }
}

struct Header<'a> {
    facility: u8,
    severity: u8,
    timestamp: &'a str,
    hostname: &'a str,
    app_name: &'a str,
    procid: &'a str,
    msgid: &'a str,
    msg: &'a str,
}

fn try_parse_header(line: &str) -> Option<Header<'_>> {
    let rest = line.strip_prefix('<')?;
    let (pri_str, rest) = rest.split_once('>')?;
    let pri: u16 = pri_str.parse().ok()?;
    if pri > 191 {
        return None;
    }
    let facility = (pri / 8) as u8;
    let severity = (pri % 8) as u8;

    // VERSION, then the 5 space-delimited header fields (none contain spaces per
    // the RFC5424 grammar — PRINTUSASCII only), then whatever remains
    // (structured-data + msg) as one piece.
    let mut parts = rest.splitn(7, ' ');
    let _version = parts.next()?;
    let timestamp = parts.next()?;
    let hostname = parts.next()?;
    let app_name = parts.next()?;
    let procid = parts.next()?;
    let msgid = parts.next()?;
    let sd_and_msg = parts.next().unwrap_or("");

    let msg = skip_structured_data(sd_and_msg);

    Some(Header {
        facility,
        severity,
        timestamp,
        hostname,
        app_name,
        procid,
        msgid,
        msg,
    })
}

/// `STRUCTURED-DATA` is either `-` (nil) or one or more bracket-delimited
/// `[SD-ID param="value" ...]` elements with no space between consecutive
/// elements. Structured-data *content* isn't extracted into fields (would need
/// full escape-aware parsing for real fidelity) — this only finds where it ends
/// so `MSG` can be located; a message with no space after `MSGID` before `[`/`-`
/// (a malformed line for a reason other than what `try_parse_header` already
/// checked) falls through to treating the whole remainder as `MSG`, never panics.
fn skip_structured_data(sd_and_msg: &str) -> &str {
    let trimmed = sd_and_msg.trim_start();
    if let Some(after_nil) = trimmed.strip_prefix('-') {
        return after_nil.strip_prefix(' ').unwrap_or(after_nil);
    }
    if !trimmed.starts_with('[') {
        return trimmed;
    }

    let bytes = trimmed.as_bytes();
    let mut depth = 0i32;
    let mut in_quotes = false;
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'\\' if in_quotes => i += 1, // skip the escaped character
            b'"' => in_quotes = !in_quotes,
            b'[' if !in_quotes => depth += 1,
            b']' if !in_quotes => {
                depth -= 1;
                if depth == 0 {
                    let after = &trimmed[i + 1..];
                    return after.strip_prefix(' ').unwrap_or(after);
                }
            }
            _ => {}
        }
        i += 1;
    }
    trimmed // unbalanced brackets: give up cleanly, treat it all as MSG
}

fn severity_name(severity: u8) -> &'static str {
    match severity {
        0 => "emergency",
        1 => "alert",
        2 => "critical",
        3 => "error",
        4 => "warning",
        5 => "notice",
        6 => "info",
        7 => "debug",
        _ => "unknown",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn well_formed_message_with_structured_data() {
        let line = r#"<34>1 2003-10-11T22:14:15.003Z mymachine.example.com su - ID47 [exampleSDID@32473 iut="3" eventSource="Application" eventID="1011"] 'su root' failed for lonvick"#;
        let wire = parse_rfc5424(line);

        // PRI 34 = facility 4 (auth), severity 2 (critical).
        assert_eq!(wire.fields.get("facility").map(String::as_str), Some("4"));
        assert_eq!(wire.fields.get("severity").map(String::as_str), Some("2"));
        assert_eq!(
            wire.fields.get("level").map(String::as_str),
            Some("critical")
        );
        assert_eq!(
            wire.fields.get("hostname").map(String::as_str),
            Some("mymachine.example.com")
        );
        assert_eq!(wire.fields.get("app_name").map(String::as_str), Some("su"));
        assert_eq!(wire.fields.get("msgid").map(String::as_str), Some("ID47"));
        assert!(
            !wire.fields.contains_key("procid"),
            "nil procid (-) must not become a field"
        );
        assert_eq!(
            wire.message, "'su root' failed for lonvick",
            "structured data must be skipped, not leak into the message"
        );

        let expected_ns = "2003-10-11T22:14:15.003Z"
            .parse::<jiff::Timestamp>()
            .unwrap()
            .as_nanosecond() as i64;
        assert_eq!(wire.timestamp_ns, expected_ns);
    }

    #[test]
    fn nil_structured_data_and_nil_procid() {
        let line = "<13>1 2003-10-11T22:14:15.003Z myhost.local myapp 1234 - - just a message";
        let wire = parse_rfc5424(line);

        assert_eq!(wire.fields.get("procid").map(String::as_str), Some("1234"));
        assert_eq!(wire.message, "just a message");
    }

    #[test]
    fn nil_timestamp_falls_back_to_now() {
        let before = crate::clock::now_ns();
        let wire = parse_rfc5424("<13>1 - myhost.local myapp - - - hello");
        let after = crate::clock::now_ns();
        assert!(
            wire.timestamp_ns >= before && wire.timestamp_ns <= after,
            "nil timestamp (-) must fall back to current time"
        );
    }

    #[test]
    fn malformed_line_lands_as_the_raw_message_instead_of_being_dropped() {
        // No leading `<PRI>` at all — a real receiver would still land this rather
        // than silently discard a legacy/non-conformant sender's output.
        let line = "this is not RFC5424 at all";
        let wire = parse_rfc5424(line);
        assert_eq!(wire.message, line);
        assert!(wire.fields.is_empty());
    }

    #[test]
    fn pri_out_of_valid_range_falls_back_to_raw_message() {
        // Valid PRI is 0-191 (facility 0-23 * 8 + severity 0-7); 999 is not a real
        // encoding and must not be parsed as if it were.
        let line = "<999>1 2003-10-11T22:14:15.003Z host app - - - message";
        let wire = parse_rfc5424(line);
        assert_eq!(wire.message, line);
    }

    #[test]
    fn bom_prefix_on_msg_is_stripped() {
        let line = "<13>1 2003-10-11T22:14:15.003Z host app - - - \u{feff}message with bom";
        let wire = parse_rfc5424(line);
        assert_eq!(wire.message, "message with bom");
    }
}
