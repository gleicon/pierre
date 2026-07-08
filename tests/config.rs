use pierre::config::PierreConfig;
use pierre::rollup::RollupKind;

/// Parses the *actual* pierre.toml shipped with the repo — the file real users copy
/// — not a synthetic snippet. Catches config-file/struct drift directly: a serde
/// rename typo, a tagged-enum discriminator mismatch, or a field that silently stops
/// round-tripping would show up here instead of only at first deploy.
#[test]
fn real_pierre_toml_parses_correctly() {
    let config = PierreConfig::load(std::path::Path::new("pierre.toml")).unwrap();

    assert_eq!(config.native_listen_addr, "127.0.0.1:4317");
    assert_eq!(config.loki_listen_addr, "127.0.0.1:3100");
    assert_eq!(config.query_listen_addr, "127.0.0.1:3101");
    assert_eq!(config.fields, vec!["level", "status", "trace_id", "latency_ms", "path"]);

    assert_eq!(config.rollup.len(), 5);
    assert_eq!(config.rollup[0].field, "level");
    assert_eq!(config.rollup[0].kind, RollupKind::Exact);
    assert_eq!(config.rollup[2].field, "trace_id");
    assert_eq!(config.rollup[2].kind, RollupKind::Hll);

    // Commented out in the shipped file — must parse as the documented default.
    assert!(matches!(config.backup, pierre::backup::BackupConfig::None));
}

#[test]
fn load_falls_back_to_defaults_when_file_is_missing() {
    let config = PierreConfig::load(std::path::Path::new("/nonexistent/path/pierre.toml")).unwrap();
    let default = PierreConfig::default();
    assert_eq!(config.native_listen_addr, default.native_listen_addr);
    assert_eq!(config.data_dir, default.data_dir);
    assert!(config.auth_tokens.is_empty());
}

#[test]
fn backup_config_s3_variant_round_trips_through_toml() {
    // The `[backup]` S3 block is only ever shown commented-out in pierre.toml —
    // nothing else exercises this tagged-enum variant actually parsing.
    let toml_str = r#"
data_dir = "./data"

[backup]
backend = "s3"
bucket = "my-pierre-logs"
prefix = "prod/"
"#;
    let config: PierreConfig = toml::from_str(toml_str).unwrap();
    match config.backup {
        pierre::backup::BackupConfig::S3 { bucket, prefix, endpoint } => {
            assert_eq!(bucket, "my-pierre-logs");
            assert_eq!(prefix, Some("prod/".to_string()));
            assert_eq!(endpoint, None);
        }
        other => panic!("expected BackupConfig::S3, got {other:?}"),
    }
}
