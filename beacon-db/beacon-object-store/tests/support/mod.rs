//! Shared helpers for the cached local store tests.

#![allow(dead_code)]

use std::path::Path;

use beacon_object_store::cached_local::{CachedLocalFileSystem, CachedLocalOptions};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

/// Creates a temporary directory with a cached store and a stock store on it.
pub fn temp_store(opts: CachedLocalOptions) -> (TempDir, CachedLocalFileSystem, LocalFileSystem) {
    let dir = tempfile::tempdir().unwrap();
    let ours =
        CachedLocalFileSystem::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap(), opts);
    let stock = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    (dir, ours, stock)
}

/// Writes `len` pseudo-random bytes to `dir/name` and returns them.
pub fn random_file(dir: &Path, name: &str, len: usize) -> Vec<u8> {
    let mut rng = Rng::new(len as u64 ^ 0x9e37_79b9_7f4a_7c15);
    let data: Vec<u8> = (0..len).map(|_| rng.next() as u8).collect();
    std::fs::write(dir.join(name), &data).unwrap();
    data
}

/// A small deterministic xorshift generator.
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        Self(seed | 1)
    }

    pub fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }

    /// A number in `0..n`.
    pub fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }

    /// A non-empty range inside `0..len`.
    pub fn range(&mut self, len: u64) -> std::ops::Range<u64> {
        let start = self.below(len);
        let end = start + 1 + self.below(len - start);
        start..end
    }
}
