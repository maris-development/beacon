//! Metadata cache in front of an [`object_store::ObjectStore`].
//!
//! [`CachedObjectStore`] answers `list`, `list_with_offset`,
//! `list_with_delimiter` and `head` from a sorted index in memory. It does
//! not cache object data.
//!
//! The index is one lock-free sorted map with the object path as key. A read
//! takes no lock, so it does not wait for a write or a sync.
//!
//! The cache stays correct in two ways:
//!
//! - A write through the wrapper updates the index at once.
//! - A background sync lists the full source again. This is the source of truth.
//!
//! The first `list` call lists the full source. The store then starts the
//! sync task, and stops it when the store drops. A [`CacheHandle`] reads the
//! status and starts a sync.
//!
//! The crate has no knowledge of tables or queries.

mod config;
mod fill;
mod handle;
mod index;
mod state;
mod store;
mod upload;

pub use config::StoreCacheConfig;
pub use fill::SyncReport;
pub use handle::CacheHandle;
pub use state::{CacheBypass, CacheStatus, head_uncached};
pub use store::CachedObjectStore;
