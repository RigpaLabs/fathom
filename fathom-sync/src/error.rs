use std::path::PathBuf;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("config: {0}")]
    Config(String),

    #[error("io: {path}: {source}")]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },

    #[error("walk: {0}")]
    Walk(#[from] walkdir::Error),

    #[error("upload: {key}: {source}")]
    Upload {
        key: String,
        source: object_store::Error,
    },

    #[error("object_store: {0}")]
    ObjectStore(#[from] object_store::Error),
}

pub type Result<T> = std::result::Result<T, Error>;
