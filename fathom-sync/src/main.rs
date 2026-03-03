use std::path::PathBuf;
use std::time::Duration;

use fathom_sync::{cleanup, config, scanner, uploader};
use tracing::{error, info};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "fathom_sync=info".parse().unwrap()),
        )
        .json()
        .init();

    let config_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "config.toml".to_string());

    let cfg = match config::Config::load(&PathBuf::from(&config_path)) {
        Ok(c) => c,
        Err(e) => {
            error!(%e, "failed to load config");
            std::process::exit(1);
        }
    };

    info!(
        data_dir = %cfg.data_dir.display(),
        scan_interval_s = cfg.scan_interval_s,
        bucket = cfg.s3.bucket,
        cleanup_enabled = cfg.cleanup.enabled,
        retention_hours = cfg.cleanup.retention_hours,
        "starting fathom-sync"
    );

    let uploader = match uploader::Uploader::new(&cfg.s3) {
        Ok(u) => u,
        Err(e) => {
            error!(%e, "failed to create uploader");
            std::process::exit(1);
        }
    };

    let interval = Duration::from_secs(cfg.scan_interval_s);
    let retention = Duration::from_secs(cfg.cleanup.retention_hours * 3600);

    loop {
        // Scan for completed files
        let candidates = scanner::scan(&cfg.data_dir);
        if !candidates.is_empty() {
            info!(count = candidates.len(), "found files to sync");
        }

        // Upload each file
        for c in &candidates {
            if let Err(e) = uploader.upload(c).await {
                error!(
                    path = %c.abs_path.display(),
                    %e,
                    "upload failed, will retry next cycle"
                );
            }
        }

        // Cleanup old synced files
        if cfg.cleanup.enabled
            && let Err(e) = cleanup::run(&cfg.data_dir, retention)
        {
            error!(%e, "cleanup failed");
        }

        // Wait or shutdown
        let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to register SIGTERM handler");
        tokio::select! {
            _ = tokio::time::sleep(interval) => {}
            _ = tokio::signal::ctrl_c() => {
                info!("received SIGINT, shutting down");
                break;
            }
            _ = sigterm.recv() => {
                info!("received SIGTERM, shutting down");
                break;
            }
        }
    }

    info!("fathom-sync stopped");
}
