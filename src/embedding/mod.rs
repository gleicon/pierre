use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;

use crate::config::{EmbeddingBackend, EmbeddingConfig};
use crate::storage::Storage;

mod remote;

struct EmbedRequest {
    key: Vec<u8>,
    text: String,
}

/// Send-side handle to the background embedding worker. Clone-safe, cheap.
#[derive(Clone)]
pub struct EmbeddingHandle {
    tx: mpsc::Sender<EmbedRequest>,
    dropped: Arc<AtomicU64>,
    dims: u16,
}

impl EmbeddingHandle {
    /// Enqueue `(key, text)` for embedding. Non-blocking — drops silently if
    /// channel is full, incrementing the drop counter visible via `dropped_count()`.
    /// The ingest path never waits on embedding.
    pub fn try_record(&self, key: Vec<u8>, text: String) {
        if self.tx.try_send(EmbedRequest { key, text }).is_err() {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn dropped_count(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    /// Expected embedding dims — needed by the query path to build the query vector.
    pub fn dims(&self) -> u16 {
        self.dims
    }
}

/// Returns `None` when `config.backend == Disabled`. Otherwise spawns the
/// background worker and returns a handle. Worker exits when all senders drop.
pub fn spawn(storage: Arc<Storage>, config: EmbeddingConfig) -> Option<EmbeddingHandle> {
    if config.backend == EmbeddingBackend::Disabled {
        return None;
    }

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .expect("reqwest client init");

    let (tx, mut rx) = mpsc::channel::<EmbedRequest>(config.queue_depth);
    let dropped = Arc::new(AtomicU64::new(0));
    let handle = EmbeddingHandle {
        tx,
        dropped: dropped.clone(),
        dims: config.dims,
    };

    tokio::spawn(async move {
        let mut batch: Vec<EmbedRequest> = Vec::with_capacity(config.batch_size);
        loop {
            // Collect up to batch_size requests with a short drain timeout.
            let first = rx.recv().await;
            match first {
                None => break, // all senders dropped, worker done
                Some(req) => batch.push(req),
            }
            // Drain more without blocking — get a full batch if available.
            while batch.len() < config.batch_size {
                match rx.try_recv() {
                    Ok(req) => batch.push(req),
                    Err(_) => break,
                }
            }
            process_batch(&storage, &config, &client, &mut batch, &dropped).await;
            batch.clear();
        }
    });

    Some(handle)
}

async fn process_batch(
    storage: &Storage,
    config: &EmbeddingConfig,
    client: &reqwest::Client,
    batch: &mut Vec<EmbedRequest>,
    dropped: &AtomicU64,
) {
    for req in batch.iter() {
        let embedding = match &config.backend {
            EmbeddingBackend::Disabled => unreachable!(),
            EmbeddingBackend::Remote => {
                remote::embed(client, &config.remote_url, &config.remote_model, &req.text).await
            }
        };

        match embedding {
            Ok(floats) => {
                let data = floats_to_bytes(&floats);
                if let Err(e) = storage.vector_put(&req.key, config.dims, data).await {
                    log::warn!("embedding worker: vector_put failed: {e}");
                }
            }
            Err(e) => {
                log::warn!("embedding worker: embed failed: {e}");
                dropped.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

/// Embeds a query string at query time for hybrid search. Returns `None` if
/// embedding is disabled. Inline async — called from the search handler.
pub async fn embed_query(config: &EmbeddingConfig, text: &str) -> Option<Vec<u8>> {
    if config.backend == EmbeddingBackend::Disabled {
        return None;
    }
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .expect("reqwest client init");
    match remote::embed(&client, &config.remote_url, &config.remote_model, text).await {
        Ok(floats) => Some(floats_to_bytes(&floats)),
        Err(e) => {
            log::warn!("hybrid search: query embedding failed: {e}");
            None
        }
    }
}

fn floats_to_bytes(floats: &[f32]) -> Vec<u8> {
    let mut out = Vec::with_capacity(floats.len() * 4);
    for f in floats {
        out.extend_from_slice(&f.to_le_bytes());
    }
    out
}
