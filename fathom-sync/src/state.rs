use std::fs;
use std::path::Path;

use crate::error::{Error, Result};

/// Marker path: `foo.parquet` → `foo.parquet.synced`
pub fn marker_path(parquet: &Path) -> std::path::PathBuf {
    let mut s = parquet.as_os_str().to_os_string();
    s.push(".synced");
    s.into()
}

pub fn is_synced(parquet: &Path) -> bool {
    marker_path(parquet).exists()
}

pub fn mark_synced(parquet: &Path) -> Result<()> {
    let mp = marker_path(parquet);
    fs::File::create(&mp).map_err(|e| Error::Io {
        path: mp,
        source: e,
    })?;
    Ok(())
}
