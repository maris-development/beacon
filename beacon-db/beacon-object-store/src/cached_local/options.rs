//! The options of the cached local store.

use std::time::Duration;

/// The smallest block size other than 0.
const MIN_BLOCK_SIZE: u64 = 4096;

/// The largest block size.
const MAX_BLOCK_SIZE: u64 = 16 << 20;

/// The options of the cached local store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CachedLocalOptions {
    /// The maximum number of open file handles.
    pub max_open_files: u64,
    /// The time an unused file handle stays open.
    pub handle_idle: Duration,
    /// The age after which a cached handle checks its file version again.
    pub revalidate_after: Duration,
    /// The block size of the block cache in bytes. 0 turns the block cache off.
    pub block_size: u64,
    /// The capacity of the block cache in bytes.
    pub block_cache_bytes: u64,
    /// The largest gap in bytes between two ranges that one read covers.
    pub coalesce_gap: u64,
    /// The largest piece in bytes of a streamed read.
    pub stream_chunk: u64,
}

impl Default for CachedLocalOptions {
    fn default() -> Self {
        Self {
            max_open_files: 256,
            handle_idle: Duration::from_secs(30),
            revalidate_after: Duration::from_millis(1000),
            block_size: 0,
            block_cache_bytes: 268_435_456,
            coalesce_gap: 1 << 20,
            stream_chunk: 8 << 20,
        }
    }
}

impl CachedLocalOptions {
    /// Checks the options and returns a message for the first bad value.
    pub fn validate(&self) -> Result<(), String> {
        if self.max_open_files == 0 {
            return Err("max_open_files must be at least 1".to_string());
        }
        if self.stream_chunk == 0 {
            return Err("stream_chunk must be at least 1".to_string());
        }
        let b = self.block_size;
        if b != 0 && !(b.is_power_of_two() && (MIN_BLOCK_SIZE..=MAX_BLOCK_SIZE).contains(&b)) {
            return Err(format!(
                "block_size must be 0 or a power of two from {MIN_BLOCK_SIZE} to {MAX_BLOCK_SIZE}, got {b}"
            ));
        }
        Ok(())
    }
}
