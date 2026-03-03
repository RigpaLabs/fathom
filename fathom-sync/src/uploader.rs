use std::path::Path;
use std::time::Instant;

use object_store::ObjectStore;
use object_store::aws::AmazonS3Builder;
use tracing::info;

use crate::config::S3Config;
use crate::error::{Error, Result};
use crate::scanner::SyncCandidate;
use crate::state;

pub struct Uploader {
    store: Box<dyn ObjectStore>,
    prefix: String,
}

impl Uploader {
    pub fn new(cfg: &S3Config) -> Result<Self> {
        let mut builder = AmazonS3Builder::new()
            .with_bucket_name(&cfg.bucket)
            .with_region(&cfg.region);

        if !cfg.endpoint.is_empty() {
            builder = builder.with_endpoint(&cfg.endpoint);
        }

        // Credentials come from env: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
        let store = builder.build()?;

        Ok(Self {
            store: Box::new(store),
            prefix: cfg.prefix.clone(),
        })
    }

    /// Build the S3 object key from prefix + relative path.
    fn object_key(&self, rel_path: &Path) -> String {
        let rel = rel_path.to_string_lossy().replace('\\', "/");
        if self.prefix.is_empty() {
            rel
        } else {
            format!("{}/{}", self.prefix.trim_end_matches('/'), rel)
        }
    }

    pub async fn upload(&self, candidate: &SyncCandidate) -> Result<()> {
        let key = self.object_key(&candidate.rel_path);
        let data = tokio::fs::read(&candidate.abs_path)
            .await
            .map_err(|e| Error::Io {
                path: candidate.abs_path.clone(),
                source: e,
            })?;
        let size = data.len();

        let start = Instant::now();
        let location = object_store::path::Path::from(key.as_str());
        self.store
            .put(&location, data.into())
            .await
            .map_err(|e| Error::Upload {
                key: key.clone(),
                source: e,
            })?;

        let elapsed = start.elapsed();
        info!(
            key,
            size_bytes = size,
            duration_ms = elapsed.as_millis() as u64,
            "uploaded"
        );

        state::mark_synced(&candidate.abs_path)?;
        Ok(())
    }
}
