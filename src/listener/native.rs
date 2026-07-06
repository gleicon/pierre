use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::record::WireRecord;
use crate::rollup::RollupHandle;
use crate::storage::Storage;
use crate::textindex::TextIndexHandle;

/// Native ingest protocol: each frame is a 4-byte big-endian length prefix followed
/// by a JSON-encoded batch (`Vec<WireRecord>`). One ack byte (`0x01` ok / `0x00` err)
/// is written back per frame so the shipper knows the batch is durable.
pub async fn serve(
    addr: &str,
    storage: Arc<Storage>,
    allowed_fields: Arc<Vec<String>>,
    rollup: Option<RollupHandle>,
    textindex: Option<TextIndexHandle>,
) -> anyhow::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    loop {
        let (stream, _) = listener.accept().await?;
        let storage = storage.clone();
        let allowed_fields = allowed_fields.clone();
        let rollup = rollup.clone();
        let textindex = textindex.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_connection(stream, storage, allowed_fields, rollup, textindex).await {
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
) -> anyhow::Result<()> {
    loop {
        let mut len_buf = [0u8; 4];
        if stream.read_exact(&mut len_buf).await.is_err() {
            return Ok(()); // connection closed
        }
        let len = u32::from_be_bytes(len_buf) as usize;
        let mut payload = vec![0u8; len];
        stream.read_exact(&mut payload).await?;

        let ack = match process_batch(&payload, &storage, &allowed_fields, rollup.as_ref(), textindex.as_ref()).await {
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
) -> anyhow::Result<()> {
    let batch: Vec<WireRecord> = serde_json::from_slice(payload)?;
    for wire in batch {
        crate::ingest::commit(storage, wire, allowed_fields, rollup, textindex).await?;
    }
    Ok(())
}
