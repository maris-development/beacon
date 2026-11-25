//! A local store wrapper that keeps file handles open and caches blocks.

mod handles;
mod options;
mod version;

pub use options::CachedLocalOptions;
