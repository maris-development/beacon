//! File-statistics configuration.
//!
//! A plain data struct, in this leaf crate for the same reason as
//! [`CrawlerConfig`](crate::CrawlerConfig): `beacon-server-config` fills it from
//! the environment and re-exports it, and `beacon-core` consumes it, without
//! either depending on the other.
//!
//! An external table turns statistics on for itself with the option
//! `use_statistics_pruning`, and sets its own refresh interval with
//! `statistics_refresh_interval`. These settings apply to all tables.

/// How the file-statistics engine behaves.
#[derive(Debug, Clone)]
pub struct FileStatsConfig {
    /// Master switch. When false, no table keeps statistics, no background
    /// task is spawned, and `ANALYZE FILES` returns an error.
    pub enable: bool,
    /// The most file analyses that run at the same time, over all tables.
    ///
    /// Analysis is a background job that must not starve queries, hence a
    /// fraction of the cores by default. Raise it well above the core count
    /// when datasets live in object storage, where the work waits on round
    /// trips rather than parsing.
    pub concurrency: usize,
    /// The most tables that refresh their statistics at the same time. The
    /// other tables wait for a free slot.
    pub max_refreshes: usize,
}

impl Default for FileStatsConfig {
    fn default() -> Self {
        // Mirrors the `BEACON_FILE_STATS_*` environment defaults in
        // `beacon-server-config`.
        Self {
            enable: true,
            concurrency: default_concurrency(),
            max_refreshes: 2,
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
        assert!(config.concurrency >= 2);
        assert!(
            config.concurrency
                <= std::thread::available_parallelism()
                    .map(|n| n.get())
                    .unwrap_or(2)
                    .max(2),
            "a background job must not claim more than the machine has"
        );
        assert_eq!(config.max_refreshes, 2);
    }
}
