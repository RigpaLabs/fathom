use std::path::Path;
use std::time::{Duration, SystemTime};

use tracing::info;
use walkdir::WalkDir;

use crate::error::{Error, Result};

/// Delete synced files older than `retention` and remove empty parent dirs.
pub fn run(data_dir: &Path, retention: Duration) -> Result<()> {
    let now = SystemTime::now();
    let mut deleted = 0u64;

    let synced_files: Vec<_> = WalkDir::new(data_dir)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.file_type().is_file()
                && e.path()
                    .extension()
                    .and_then(|x| x.to_str())
                    .is_some_and(|x| x == "synced")
        })
        .collect();

    for entry in synced_files {
        let marker = entry.path();

        // Derive original parquet path: strip .synced suffix
        let parquet = marker.with_extension("");
        if !parquet.exists() {
            // Parquet already gone — clean up orphan marker
            std::fs::remove_file(marker).ok();
            continue;
        }

        // Use marker mtime (upload time), not parquet mtime (write time).
        // This prevents premature deletion after extended downtime: if
        // fathom-sync was offline >retention and uploads a backlog, those
        // files won't be immediately deleted because the marker is fresh.
        let marker_mtime = marker
            .metadata()
            .and_then(|m| m.modified())
            .map_err(|e| Error::Io {
                path: marker.to_path_buf(),
                source: e,
            })?;

        let age = now.duration_since(marker_mtime).unwrap_or(Duration::ZERO);
        if age < retention {
            continue;
        }

        info!(path = %parquet.display(), age_hours = age.as_secs() / 3600, "deleting");
        std::fs::remove_file(&parquet).map_err(|e| Error::Io {
            path: parquet.clone(),
            source: e,
        })?;
        std::fs::remove_file(marker).ok();
        deleted += 1;

        // Remove empty parent directories up to data_dir
        remove_empty_parents(&parquet, data_dir);
    }

    if deleted > 0 {
        info!(deleted, "cleanup complete");
    }

    Ok(())
}

fn remove_empty_parents(file: &Path, stop_at: &Path) {
    let mut dir = file.parent();
    while let Some(d) = dir {
        if d == stop_at {
            break;
        }
        // try removing — fails silently if non-empty
        if std::fs::remove_dir(d).is_err() {
            break;
        }
        dir = d.parent();
    }
}
