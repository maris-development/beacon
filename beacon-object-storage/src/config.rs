//! Storage configuration types.
//!
//! These plain data structs describe how Beacon's object storage is laid out on
//! local disk and, optionally, how the datasets store is backed by S3. They live
//! in this crate — the storage layer — so it has no dependency on `beacon-config`;
//! `beacon-config` re-exports them and fills them from the environment.

use std::path::PathBuf;

use object_store::aws::AmazonS3Builder;

/// How Beacon's object storage is configured: where data lives on local disk and
/// whether the datasets store is backed by S3 instead of the local filesystem.
#[derive(Debug, Clone, Default)]
pub struct StorageConfig {
    /// Root data directory (parent of the stores below).
    pub data_dir: PathBuf,
    /// Local root of the datasets store. Also used for NetCDF URL translation of
    /// local datasets.
    pub datasets_dir: PathBuf,
    /// Where the tables store — the catalog (`table.json`, `extensions.json`,
    /// crawlers) and managed Lance data — lives. DuckDB-style:
    /// - `None` → an ephemeral in-memory store (nothing persists to disk);
    /// - `Some(path)` → a single-file [`beacon_redb_store::RedbStore`] at `path`,
    ///   held under an exclusive lock for the process lifetime.
    pub db_path: Option<PathBuf>,
    /// Local root of the temporary-files store.
    pub tmp_dir: PathBuf,
    /// Watch the local datasets directory for changes (local backend only).
    pub enable_fs_events: bool,
    /// Reserved: wire S3 change notifications into the event listener.
    pub enable_s3_events: bool,
    /// Maximum size, in bytes, accepted for a single dataset upload through the
    /// admin API. `0` means unlimited. Defaults to `0` so embedders and tests that
    /// build a [`StorageConfig`] directly are not capped; `beacon-config` fills a
    /// concrete default from `BEACON_MAX_UPLOAD_BYTES`.
    pub max_upload_bytes: u64,
    /// Part size, in bytes, advertised to clients for chunked (resumable) uploads.
    /// Clients slice large files into parts of this size. `0` => the built-in
    /// default (8 MiB). Kept ≥ 5 MiB so each part satisfies S3's minimum.
    pub upload_part_size: usize,
    /// Idle timeout, in seconds, after which an in-progress chunked upload session
    /// is aborted and cleaned up. `0` => the built-in default (1 hour).
    pub upload_session_ttl_secs: u64,
    /// S3 backing for the datasets store. `None` => local filesystem; `Some` =>
    /// the datasets store is backed by S3, configured from these settings.
    pub s3: Option<S3Config>,
}

/// S3 settings for the datasets store, present only when datasets are backed by
/// S3 ([`StorageConfig::s3`] is `Some`).
///
/// This is the single source of truth for the S3 backend: it drives both store
/// construction ([`Self::amazon_s3_builder`]) and NetCDF URL translation, so the
/// two can never diverge. Credentials are intentionally *not* held here — they
/// flow through the standard AWS environment chain (`AmazonS3Builder::from_env`).
#[derive(Debug, Clone)]
pub struct S3Config {
    /// Bucket name. Required: `object_store` needs it regardless of addressing
    /// style, and it is never inferred from the endpoint.
    pub bucket: String,
    /// S3-compatible endpoint, e.g. `http://minio:9000`. `None` => the endpoint is
    /// resolved from the region (real AWS).
    pub endpoint: Option<String>,
    /// Region; when `None`, `from_env` still reads `AWS_REGION`.
    pub region: Option<String>,
    /// Use virtual-hosted-style addressing (bucket in the host) instead of
    /// path-style (`{endpoint}/{bucket}/{key}`). Also selects the NetCDF URL form.
    pub enable_virtual_hosting: bool,
    /// Allow plain HTTP (dev/MinIO).
    pub allow_http: bool,
}

impl S3Config {
    /// Build an [`AmazonS3Builder`] from this config — the single place the S3
    /// backend is configured. Credentials are layered in by
    /// [`AmazonS3Builder::from_env`]; the explicit values here override the
    /// corresponding environment variables so the configured endpoint/region/
    /// bucket always win.
    pub(crate) fn amazon_s3_builder(&self) -> AmazonS3Builder {
        let mut builder = AmazonS3Builder::from_env()
            .with_allow_http(self.allow_http)
            .with_virtual_hosted_style_request(self.enable_virtual_hosting)
            .with_bucket_name(&self.bucket);

        if let Some(endpoint) = &self.endpoint {
            builder = builder.with_endpoint(endpoint);
        }
        if let Some(region) = &self.region {
            builder = builder.with_region(region);
        }

        builder
    }
}
