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

    let config_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "pierre.toml".to_string());
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
        allowed_fields,
        rollup.clone(),
        Some(textindex.clone()),
        auth_tokens.clone(),
        stats.clone(),
    );
    let query_api = listener::query_api::serve(
        &config.query_listen_addr,
        storage,
        textindex_bucket_duration,
        auth_tokens,
        stats,
        rollup,
        Some(textindex),
    );
    tokio::try_join!(native, loki, query_api)?;
    Ok(())
}
