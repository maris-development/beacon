//! Storage configuration types.
//!
//! These plain data structs describe how the datasets store is backed (local
//! filesystem vs. S3). They live in this crate — the storage layer — so it has
//! no dependency on `beacon-config`; `beacon-config` re-exports them and fills
//! them from the environment.

use object_store::aws::AmazonS3Builder;

use crate::error::StorageError;

/// How Beacon's object storage is configured.
#[derive(Debug, Clone, Default)]
pub struct StorageConfig {
    pub enable_fs_events: bool,
    pub enable_s3_events: bool,
    pub s3: S3Config,
}

/// S3-specific storage settings (used when `s3.data_lake` is set).
///
/// This is the single source of truth for the S3 backend: it drives both store
/// construction ([`Self::amazon_s3_builder`]) and NetCDF URL translation, so the
/// two can never diverge. Credentials are intentionally *not* held here — they
/// flow through the standard AWS environment chain (`AmazonS3Builder::from_env`).
#[derive(Debug, Clone)]
pub struct S3Config {
    pub bucket: Option<String>,
    pub enable_virtual_hosting: bool,
    pub data_lake: bool,
    /// S3-compatible endpoint, e.g. `http://minio:9000`. Captured explicitly so
    /// store-building and NetCDF URL translation use the same value. `None` =>
    /// rely on the AWS endpoint resolution (real AWS).
    pub endpoint: Option<String>,
    /// Optional region; when `None`, `from_env` still reads `AWS_REGION`.
    pub region: Option<String>,
    /// Allow plain HTTP (dev/MinIO). Defaults to `true` (current behavior).
    pub allow_http: bool,
}

impl Default for S3Config {
    fn default() -> Self {
        Self {
            bucket: None,
            enable_virtual_hosting: false,
            data_lake: false,
            endpoint: None,
            region: None,
            allow_http: true,
        }
    }
}

impl S3Config {
    /// Build an [`AmazonS3Builder`] from this config — the single place the S3
    /// backend is configured. Credentials are layered in by
    /// [`AmazonS3Builder::from_env`]; the explicit values here override the
    /// corresponding environment variables so the configured endpoint/region
    /// always win.
    pub(crate) fn amazon_s3_builder(&self) -> Result<AmazonS3Builder, StorageError> {
        let mut builder = AmazonS3Builder::from_env()
            .with_allow_http(self.allow_http)
            .with_virtual_hosted_style_request(self.enable_virtual_hosting);

        if let Some(endpoint) = &self.endpoint {
            builder = builder.with_endpoint(endpoint);
        }
        if let Some(region) = &self.region {
            builder = builder.with_region(region);
        }

        if !self.enable_virtual_hosting {
            // Path-style requests need an explicit bucket name.
            let bucket = self.bucket.as_ref().ok_or(StorageError::MissingConfig {
                key: "BEACON_S3_BUCKET",
            })?;
            builder = builder.with_bucket_name(bucket);
        }

        Ok(builder)
    }
}
