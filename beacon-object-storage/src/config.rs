//! Storage configuration types.
//!
//! These plain data structs describe how the datasets store is backed (local
//! filesystem vs. S3). They live in this crate — the storage layer — so it has
//! no dependency on `beacon-config`; `beacon-config` re-exports them and fills
//! them from the environment.

/// How Beacon's object storage is configured.
#[derive(Debug, Clone, Default)]
pub struct StorageConfig {
    pub enable_fs_events: bool,
    pub enable_s3_events: bool,
    pub s3: S3Config,
}

/// S3-specific storage settings (used when `s3.data_lake` is set).
#[derive(Debug, Clone, Default)]
pub struct S3Config {
    pub bucket: Option<String>,
    pub enable_virtual_hosting: bool,
    pub data_lake: bool,
}
