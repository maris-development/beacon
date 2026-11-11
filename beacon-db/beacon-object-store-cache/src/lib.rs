//! Metadata cache in front of an [`object_store::ObjectStore`].
//!
//! [`CachedObjectStore`] answers `list`, `list_with_offset`,
//! `list_with_delimiter` and `head` from a sorted index in memory. It does
//! not cache object data.
//!
//! The cache stays correct in three ways:
//!
//! - A write through the wrapper updates the index at once.
//! - A periodic sync lists each cached prefix again. This is the source of truth.
//! - A [`StoreEvent`] sent to the [`CacheHandle`] updates a path before the next sync.
//!
//! The crate has no knowledge of tables or queries.

mod config;
mod event;
mod fill;
mod index;
mod state;
mod store;
mod upload;
#[cfg(feature = "watch")]
mod watch;

pub use config::StoreCacheConfig;
pub use event::{ApplyReport, CacheHandle, StoreEvent};
pub use fill::SyncReport;
pub use state::{CacheBypass, CacheStatus, head_uncached};
pub use store::CachedObjectStore;
#[cfg(feature = "watch")]
pub use watch::{LocalWatcher, WatcherState, network_file_system, watch_local};
