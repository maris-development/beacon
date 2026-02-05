//! Runtime configuration for Beacon Atlas.
//!
//! All environment-variable driven settings live here.

use std::env;

/// Env var for IO cache size in bytes.
pub const IO_CACHE_BYTES_ENV: &str = "BEACON_ATLAS_IO_CACHE_BYTES";
/// Env var for chunk fetch concurrency.
pub const CHUNK_FETCH_CONCURRENCY_ENV: &str = "BEACON_ATLAS_CHUNK_FETCH_CONCURRENCY";

const DEFAULT_IO_CACHE_BYTES: usize = 64 * 1024 * 1024; // 64 MiB
const DEFAULT_CHUNK_FETCH_CONCURRENCY: usize = 16;

/// Returns the configured IO cache size in bytes.
pub fn io_cache_bytes() -> usize {
    env_usize(IO_CACHE_BYTES_ENV, DEFAULT_IO_CACHE_BYTES)
}

/// Returns the configured chunk fetch concurrency.
pub fn chunk_fetch_concurrency() -> usize {
    env_usize(CHUNK_FETCH_CONCURRENCY_ENV, DEFAULT_CHUNK_FETCH_CONCURRENCY)
}

fn env_usize(name: &str, default: usize) -> usize {
    env::var(name)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}