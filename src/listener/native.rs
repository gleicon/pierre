use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;

use crate::record::WireRecord;
use crate::rollup::RollupHandle;
use crate::stats::IngestStats;
use crate::storage::Storage;
use crate::textindex::TextIndexHandle;

/// Rejects a frame outright before allocating anything if the client-supplied length
/// prefix exceeds this. Without a bound, a client (this protocol is deliberately
/// unauthenticated — see DECISIONS.md) could send a 4-byte prefix claiming up to
/// ~4GB and force an allocation of that size before a single payload byte arrives;
/// repeated across concurrent connections, that's an unauthenticated memory-
/// exhaustion DoS. Sized generously above any realistic log batch.
const MAX_FRAME_BYTES: usize = 16 * 1024 * 1024;

/// Caps concurrent connections so the same unauthenticated-client DoS above can't
/// be sidestepped by fanning out many connections instead of one big frame — each
/// held connection can otherwise pin a socket/fd plus, transiently, up to
/// `MAX_FRAME_BYTES` (found via `/ds-security-review`). Sized generously above any
/// realistic shipper fleet talking to one Pierre instance.
const MAX_CONCURRENT_CONNECTIONS: usize = 1024;

/// Native ingest protocol: each frame is a 4-byte big-endian length prefix followed
/// by a JSON-encoded batch (`Vec<WireRecord>`). One ack byte (`0x01` ok / `0x00` err)
/// is written back per frame so the shipper knows the batch is durable.
pub async fn serve(
    addr: &str,
    storage: Arc<Storage>,
    allowed_fields: Arc<Vec<String>>,
    rollup: Option<RollupHandle>,
    textindex: Option<TextIndexHandle>,
    stats: IngestStats,
) -> anyhow::Result<()> {
    serve_with_capacity(
        addr,
        storage,
        allowed_fields,
        rollup,
        textindex,
        stats,
        MAX_CONCURRENT_CONNECTIONS,
    )
    .await
}

/// `capacity` is split out from `serve` so tests can exercise cap enforcement with
/// a handful of connections instead of `MAX_CONCURRENT_CONNECTIONS` real sockets —
/// opening 1024 real connections in a test risks tripping a low default `ulimit -n`
/// (macOS/CI often default well under that) for a fact this size doesn't need to
/// prove.
async fn serve_with_capacity(
    addr: &str,
    storage: Arc<Storage>,
    allowed_fields: Arc<Vec<String>>,
    rollup: Option<RollupHandle>,
    textindex: Option<TextIndexHandle>,
    stats: IngestStats,
    capacity: usize,
) -> anyhow::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    let connection_permits = Arc::new(Semaphore::new(capacity));
    loop {
        let (stream, _) = listener.accept().await?;
        let permit = match connection_permits.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                log::warn!("native listener at capacity ({capacity} connections), rejecting new connection");
                continue; // drop stream immediately, closing it
            }
        };
        let storage = storage.clone();
        let allowed_fields = allowed_fields.clone();
        let rollup = rollup.clone();
        let textindex = textindex.clone();
        let stats = stats.clone();
        tokio::spawn(async move {
            let _permit = permit; // held for the connection's lifetime, released on drop
            if let Err(e) =
                handle_connection(stream, storage, allowed_fields, rollup, textindex, stats).await
            {
                log::warn!("native connection ended: {e}");
            }
        });
    }
}

async fn handle_connection(
    mut stream: TcpStream,
    storage: Arc<Storage>,
    allowed_fields: Arc<Vec<String>>,
    rollup: Option<RollupHandle>,
    textindex: Option<TextIndexHandle>,
    stats: IngestStats,
) -> anyhow::Result<()> {
    loop {
        let mut len_buf = [0u8; 4];
        if stream.read_exact(&mut len_buf).await.is_err() {
            return Ok(()); // connection closed
        }
        let len = u32::from_be_bytes(len_buf) as usize;
        if len > MAX_FRAME_BYTES {
            log::warn!(
                "native frame length {len} exceeds max {MAX_FRAME_BYTES}, closing connection"
            );
            stream.write_all(&[0u8]).await?;
            return Ok(());
        }
        let mut payload = vec![0u8; len];
        stream.read_exact(&mut payload).await?;

        let ack = match process_batch(
            &payload,
            &storage,
            &allowed_fields,
            rollup.as_ref(),
            textindex.as_ref(),
            &stats,
        )
        .await
        {
            Ok(()) => 1u8,
            Err(e) => {
                log::warn!("native batch rejected: {e}");
                0u8
            }
        };
        stream.write_all(&[ack]).await?;
    }
}

async fn process_batch(
    payload: &[u8],
    storage: &Storage,
    allowed_fields: &[String],
    rollup: Option<&RollupHandle>,
    textindex: Option<&TextIndexHandle>,
    stats: &IngestStats,
) -> anyhow::Result<()> {
    let batch: Vec<WireRecord> = serde_json::from_slice(payload)?;
    for wire in batch {
        crate::ingest::commit(storage, wire, allowed_fields, rollup, textindex, None).await?;
        stats.record_commit();
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Proves the same cap-enforcement wiring `MAX_CONCURRENT_CONNECTIONS` uses in
    /// production, at a capacity of 4 instead of 1024 — real sockets, real accept
    /// loop, no risk of tripping a low `ulimit -n`.
    #[tokio::test]
    async fn connections_beyond_the_cap_are_closed_immediately() {
        const CAPACITY: usize = 4;

        let dir = tempfile::tempdir().unwrap();
        let storage = Arc::new(Storage::open(dir.path()).await.unwrap());
        let allowed_fields = Arc::new(vec![]);

        let listener_socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener_socket.local_addr().unwrap().to_string();
        drop(listener_socket);

        let addr_for_server = addr.clone();
        tokio::spawn(async move {
            let _ = serve_with_capacity(
                &addr_for_server,
                storage,
                allowed_fields,
                None,
                None,
                IngestStats::default(),
                CAPACITY,
            )
            .await;
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Saturate the cap. None of these send a length prefix, so each sits held
        // open by the server, pinning one permit apiece.
        let mut held = Vec::with_capacity(CAPACITY);
        for _ in 0..CAPACITY {
            held.push(TcpStream::connect(&addr).await.unwrap());
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // One more, over the cap: must be closed by the server promptly.
        let mut over_cap = TcpStream::connect(&addr).await.unwrap();
        let result = tokio::time::timeout(std::time::Duration::from_millis(300), async {
            let mut buf = [0u8; 1];
            over_cap.read(&mut buf).await
        })
        .await;
        assert!(
            matches!(result, Ok(Ok(0))),
            "a connection over the cap must be closed by the server immediately, got: {result:?}"
        );

        // An already-held connection under the cap must be unaffected — still open,
        // still waiting to read a length prefix nobody sent.
        let under_cap = &mut held[0];
        let result = tokio::time::timeout(std::time::Duration::from_millis(300), async {
            let mut buf = [0u8; 1];
            under_cap.read(&mut buf).await
        })
        .await;
        assert!(
            result.is_err(),
            "a connection under the cap must not be closed"
        );
    }
}
