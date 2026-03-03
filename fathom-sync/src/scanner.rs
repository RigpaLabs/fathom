use std::path::{Path, PathBuf};

use chrono::Utc;
use walkdir::WalkDir;

use crate::state;

#[derive(Debug)]
pub struct SyncCandidate {
    pub abs_path: PathBuf,
    pub rel_path: PathBuf,
}

/// Walk `data_dir` and return completed, not-yet-synced Parquet files.
pub fn scan(data_dir: &Path) -> Vec<SyncCandidate> {
    let today = Utc::now().format("%Y-%m-%d").to_string();
    let mut candidates = Vec::new();

    for entry in WalkDir::new(data_dir)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
    {
        let path = entry.path();

        let ext = path.extension().and_then(|e| e.to_str());
        if ext != Some("parquet") {
            continue;
        }

        // Already uploaded
        if state::is_synced(path) {
            continue;
        }

        let rel = match path.strip_prefix(data_dir) {
            Ok(r) => r,
            Err(_) => continue,
        };
        let rel_str = rel.to_string_lossy();

        // Raw files: skip if filename contains _open
        if (rel_str.starts_with("raw/") || rel_str.starts_with("raw\\"))
            && path
                .file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.contains("_open"))
        {
            continue;
        }

        // 1s files: skip today's file (still being written)
        if (rel_str.starts_with("1s/") || rel_str.starts_with("1s\\"))
            && path
                .file_stem()
                .and_then(|n| n.to_str())
                .is_some_and(|s| s == today)
        {
            continue;
        }

        candidates.push(SyncCandidate {
            abs_path: path.to_path_buf(),
            rel_path: rel.to_path_buf(),
        });
    }

    candidates
}
