//! File-statistics configuration.
//!
//! A plain data struct, in this leaf crate for the same reason as
//! [`CrawlerConfig`](crate::CrawlerConfig): `beacon-server-config` fills it from
//! the environment and re-exports it, and `beacon-core` consumes it, without
//! either depending on the other.

/// How the background file-statistics subsystem behaves.
#[derive(Debug, Clone)]
pub struct FileStatsConfig {
    /// Master switch. When false nothing is discovered, analyzed or stored, and
    /// no background task is spawned.
    ///
    /// The same store holds the schema cache. A server with this flag off reads
    /// the schema of each file again on each cold query.
    pub enable: bool,
    /// Seconds between passes.
    pub interval_secs: u64,
    /// Collect at startup instead of waiting for the first tick.
    ///
    /// The timer's first pass lands one whole interval after boot, so a fresh
    /// server holds no statistics for 15 minutes — and a server that restarts
    /// more often than the interval never collects at all, because the interval
    /// starts again on each boot. This runs a pass as soon as the runtime is up,
    /// in the background, and keeps going until the queue is empty. The timer
    /// takes over from there.
    ///
    /// Off by default. The pass holds the service, and thus the database file,
    /// while it reads a batch. A process that exits does not see this. A caller
    /// that drops a runtime and opens the same file again gets a lock error.
    pub on_startup: bool,
    /// Files analyzed at once.
    ///
    /// Analysis is spawned, so this is real parallelism. It is also a background
    /// job that must not starve queries, hence a fraction of the cores by
    /// default. Raise it well above the core count when datasets live in object
    /// storage, where the work is waiting on round trips rather than parsing.
    pub concurrency: usize,
    /// Files taken off the queue per pass. Bounds the segment builder's memory,
    /// roughly `batch_files x columns-per-file x 50 bytes`.
    pub batch_files: usize,
    /// Files a segment should aim to cover.
    ///
    /// Smaller means narrower segments and sharper skipping for a rare column,
    /// but more segments to read for a column present in all of them.
    pub target_group_files: usize,
    /// Never split a group below this, even across directories. A block costs
    /// framing however few rows it holds.
    pub min_group_files: usize,
    /// Fix the segment grouping at this directory depth instead of deriving it
    /// from each batch's paths.
    ///
    /// Leave unset. The derivation handles a store whose roots have different
    /// shapes, which no single depth can.
    pub prefix_depth: Option<usize>,
    /// Prefix of the datasets store to discover files under. Empty means all.
    pub scan_prefix: String,
    /// Files registered per discovery transaction, so a listing of a large store
    /// does not have to be held whole.
    pub discovery_chunk: usize,
    /// Keep the schema each analysis derives, so a query reads it instead of
    /// deriving it again.
    ///
    /// On by default. The analyzer already computes each file's schema, to
    /// position the statistics, and used to drop it; keeping it costs one write
    /// per batch. Deriving a schema from every file was 83% of a netCDF query
    /// over a hundred thousand files.
    ///
    /// Only a pass writes an entry. This flag does nothing while
    /// [`Self::enable`] is false.
    ///
    /// Turn it off to take the cache out of a query's path while leaving
    /// statistics on. Existing entries are then neither written nor read.
    pub schema_cache: bool,
}

impl Default for FileStatsConfig {
    fn default() -> Self {
        // Mirrors the `BEACON_FILE_STATS_*` environment defaults in
        // `beacon-server-config`.
        Self {
            // On. The Rust readers are the default for netCDF and HDF5, so a
            // pass records a real range. The same store holds the schema
            // cache, so a server with this flag off reads the schema of each
            // file again on each cold query.
            enable: true,
            interval_secs: 900,
            // Off. A fresh server therefore holds no statistics for 15
            // minutes.
            //
            // Teardown blocks a change to on. A pass holds the service, and
            // thus the redb database, while it reads a batch. A drop of the
            // runtime does not release the file lock. An immediate reopen
            // fails. `tables_store_lock_release` shows this.
            //
            // Set this flag to true together with a shutdown that waits for
            // the pass.
            on_startup: false,
            concurrency: default_concurrency(),
            batch_files: 10_000,
            target_group_files: 10_000,
            min_group_files: 500,
            prefix_depth: None,
            scan_prefix: String::new(),
            discovery_chunk: 10_000,
            // On, unlike the subsystem around it. The schema is derived and
            // dropped today, so keeping it adds a write and removes a read.
            schema_cache: true,
        }
    }
}

/// A quarter of the cores, and never fewer than two.
///
/// Analysis is CPU bound for a local netCDF file, so throughput follows the core
/// count. Taking all of them would hand a backfill the whole machine and stall
/// query serving, which is the wrong trade for a background job.
pub fn default_concurrency() -> usize {
    std::thread::available_parallelism()
        .map(|n| (n.get() / 4).max(2))
        .unwrap_or(2)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_default_is_on_and_leaves_room_for_queries() {
        let config = FileStatsConfig::default();
        assert!(config.enable);
        assert!(
            !config.on_startup,
            "a pass at startup holds the database file after a drop"
        );
        assert!(config.concurrency >= 2);
        assert!(
            config.concurrency <= std::thread::available_parallelism().map(|n| n.get()).unwrap_or(2),
            "a background job must not claim more than the machine has"
        );
        assert!(
            config.schema_cache,
            "the schema cache rides on a pass that already derives every schema"
        );
        assert!(config.prefix_depth.is_none(), "grouping is derived by default");
    }
}
