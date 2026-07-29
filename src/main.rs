use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use pierre::backup::BackupConfig;
use pierre::config::PierreConfig;
use pierre::listener;
use pierre::rollup::worker::TierConfig;
use pierre::storage::Storage;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();

    let mut args = std::env::args().skip(1).peekable();
    let first = args.next().unwrap_or_else(|| "pierre.toml".to_string());

    // Subcommand: `pierre migrate <config> --from elasticsearch --url <url> --index <name> [--unmapped lossy|preserve]`
    if first == "migrate" {
        let config_path = args.next().unwrap_or_else(|| "pierre.toml".to_string());
        return run_migrate(config_path, args.collect()).await;
    }

    let config_path = first;
    let config = PierreConfig::load(&PathBuf::from(config_path))?;
    log::info!(
        "starting pierre, data_dir={} native_listen_addr={}",
        config.data_dir,
        config.native_listen_addr
    );

    let data_dir = PathBuf::from(&config.data_dir);
    let remote = match config.backup {
        BackupConfig::None => {
            let archive_dir = data_dir.join("_archive");
            std::fs::create_dir_all(&archive_dir)?;
            Box::new(
                edgestore_repl::FilesystemRemoteStore::new(archive_dir).map_err(|e| {
                    anyhow::anyhow!("failed to build default local archive store: {e}")
                })?,
            ) as Box<dyn edgestore::RemoteStore>
        }
        explicit => pierre::backup::build_remote_store(explicit).await?,
    };
    let storage = Arc::new(
        Storage::open_with_options(
            &data_dir,
            remote,
            config.cohort_window_secs,
            config.strip_text_index_after_archive,
        )
        .await?,
    );
    let allowed_fields = Arc::new(config.fields.clone());

    let rollup = if config.rollup.is_empty() {
        None
    } else {
        let field_kinds = config
            .rollup
            .iter()
            .map(|def| (def.field.clone(), def.kind))
            .collect();
        let mut tiers = TierConfig::production_defaults();
        tiers.minute_ttl_secs = config.rollup_minute_ttl_secs;
        Some(pierre::rollup::spawn(storage.clone(), field_kinds, tiers))
    };

    let _backup_worker = pierre::backup::spawn(
        storage.clone(),
        Duration::from_secs(config.hot_to_warm_flush_interval_secs),
        Duration::from_secs(config.archive_interval_secs),
        config.local_retention_secs.map(Duration::from_secs),
        Duration::from_secs(config.local_prune_interval_secs),
    )
    .await;

    let (textindex, _textindex_worker) = pierre::textindex::spawn(
        storage.clone(),
        Duration::from_secs(config.textindex_bucket_duration_secs),
        Duration::from_secs(config.textindex_flush_interval_secs),
    );

    let textindex_bucket_duration = Duration::from_secs(config.textindex_bucket_duration_secs);
    let auth_tokens = pierre::auth::AuthTokens::new(config.auth_tokens.clone());

    let stats = pierre::stats::IngestStats::default();
    pierre::stats::spawn(
        stats.clone(),
        rollup.clone(),
        Some(textindex.clone()),
        Duration::from_secs(5),
    );

    let native = listener::native::serve(
        &config.native_listen_addr,
        storage.clone(),
        allowed_fields.clone(),
        rollup.clone(),
        Some(textindex.clone()),
        stats.clone(),
    );
    let loki = listener::loki::serve(
        &config.loki_listen_addr,
        storage.clone(),
        allowed_fields.clone(),
        rollup.clone(),
        Some(textindex.clone()),
        auth_tokens.clone(),
        stats.clone(),
    );
    let es_bulk = listener::es_bulk::serve(
        &config.es_bulk_listen_addr,
        storage.clone(),
        allowed_fields.clone(),
        rollup.clone(),
        Some(textindex.clone()),
        auth_tokens.clone(),
        stats.clone(),
    );
    let syslog = listener::syslog::serve(
        &config.syslog_listen_addr,
        storage.clone(),
        allowed_fields.clone(),
        rollup.clone(),
        Some(textindex.clone()),
        stats.clone(),
    );
    let otlp_grpc = listener::otlp::serve_grpc(
        &config.otlp_grpc_listen_addr,
        storage.clone(),
        allowed_fields.clone(),
        rollup.clone(),
        Some(textindex.clone()),
        auth_tokens.clone(),
        stats.clone(),
    );
    let otlp_http = listener::otlp::serve_http(
        &config.otlp_http_listen_addr,
        storage.clone(),
        allowed_fields.clone(),
        rollup.clone(),
        Some(textindex.clone()),
        auth_tokens.clone(),
        stats.clone(),
    );
    let mcp = listener::mcp::serve(
        &config.mcp_listen_addr,
        storage.clone(),
        allowed_fields,
        textindex_bucket_duration,
        auth_tokens.clone(),
    );
    let embedding = pierre::embedding::spawn(storage.clone(), config.embedding.clone());
    let embedding_config = if embedding.is_some() {
        Some(config.embedding.clone())
    } else {
        None
    };

    let query_api = listener::query_api::serve(
        &config.query_listen_addr,
        storage,
        textindex_bucket_duration,
        auth_tokens,
        stats,
        rollup,
        Some(textindex),
        embedding_config,
    );
    tokio::try_join!(native, loki, es_bulk, syslog, otlp_grpc, otlp_http, mcp, query_api)?;
    Ok(())
}

async fn run_migrate(config_path: String, args: Vec<String>) -> anyhow::Result<()> {
    let config = PierreConfig::load(&PathBuf::from(&config_path))?;

    // Parse: --from elasticsearch --url <url> --index <name> [--unmapped lossy|preserve]
    let mut from = None::<String>;
    let mut url = None::<String>;
    let mut index = None::<String>;
    let mut unmapped = pierre::migrate::UnmappedStrategy::Lossy;

    let mut it = args.iter();
    while let Some(flag) = it.next() {
        match flag.as_str() {
            "--from" => from = it.next().cloned(),
            "--url" => url = it.next().cloned(),
            "--index" => index = it.next().cloned(),
            "--unmapped" => {
                if let Some(v) = it.next() {
                    unmapped = v.parse().map_err(|e: String| anyhow::anyhow!("{e}"))?;
                }
            }
            other => anyhow::bail!("unknown migrate flag: {other}"),
        }
    }

    let source = from.ok_or_else(|| anyhow::anyhow!("missing --from"))?;
    if source != "elasticsearch" {
        anyhow::bail!("--from {source:?} not supported; only 'elasticsearch' is available");
    }
    let es_url = url.ok_or_else(|| anyhow::anyhow!("missing --url"))?;
    let es_index = index.ok_or_else(|| anyhow::anyhow!("missing --index"))?;

    let data_dir = PathBuf::from(&config.data_dir);
    let remote = match config.backup {
        BackupConfig::None => {
            let archive_dir = data_dir.join("_archive");
            std::fs::create_dir_all(&archive_dir)?;
            Box::new(
                edgestore_repl::FilesystemRemoteStore::new(archive_dir)
                    .map_err(|e| anyhow::anyhow!("failed to build local archive: {e}"))?,
            ) as Box<dyn edgestore::RemoteStore>
        }
        explicit => pierre::backup::build_remote_store(explicit).await?,
    };
    let storage = Arc::new(
        Storage::open_with_options(
            &data_dir,
            remote,
            config.cohort_window_secs,
            config.strip_text_index_after_archive,
        )
        .await?,
    );

    let rollup = if config.rollup.is_empty() {
        None
    } else {
        let field_kinds = config
            .rollup
            .iter()
            .map(|def| (def.field.clone(), def.kind))
            .collect();
        let mut tiers = TierConfig::production_defaults();
        tiers.minute_ttl_secs = config.rollup_minute_ttl_secs;
        Some(pierre::rollup::spawn(storage.clone(), field_kinds, tiers))
    };
    let (textindex, _worker) = pierre::textindex::spawn(
        storage.clone(),
        Duration::from_secs(config.textindex_bucket_duration_secs),
        Duration::from_secs(config.textindex_flush_interval_secs),
    );

    pierre::migrate::run_elasticsearch(
        &storage,
        &config.fields,
        &es_url,
        &es_index,
        unmapped,
        rollup.as_ref(),
        Some(&textindex),
    )
    .await
}
