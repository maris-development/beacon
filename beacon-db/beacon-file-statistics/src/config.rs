//! The settings of the file statistics engine.

/// The settings of the file statistics engine.
///
/// Two limits keep the background work off most cores:
/// [`concurrency`](Self::concurrency) caps the file analyses over all tables,
/// and [`max_refreshes`](Self::max_refreshes) caps the tables that refresh at
/// the same time. The other tables wait for a free slot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    concurrency: usize,
    max_refreshes: usize,
    write_batch: usize,
    compaction_fragments: usize,
}

impl Config {
    /// The most file analyses that run at the same time, over all tables.
    pub fn concurrency(&self) -> usize {
        self.concurrency
    }

    /// The most tables that refresh at the same time.
    pub fn max_refreshes(&self) -> usize {
        self.max_refreshes
    }

    /// Sets the most tables that refresh at the same time. Zero gives one.
    pub fn with_max_refreshes(mut self, max_refreshes: usize) -> Self {
        self.max_refreshes = max_refreshes.max(1);
        self
    }

    /// The number of files that one write to a statistics table holds.
    pub fn write_batch(&self) -> usize {
        self.write_batch
    }

    /// The number of fragments above which a refresh compacts a statistics table.
    pub fn compaction_fragments(&self) -> usize {
        self.compaction_fragments
    }

    /// Sets the most file analyses that run at the same time. Zero gives one.
    pub fn with_concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = concurrency.max(1);
        self
    }

    /// Sets the number of files that one write holds. Zero gives one.
    pub fn with_write_batch(mut self, write_batch: usize) -> Self {
        self.write_batch = write_batch.max(1);
        self
    }

    /// Sets the number of fragments above which a refresh compacts a statistics table.
    pub fn with_compaction_fragments(mut self, compaction_fragments: usize) -> Self {
        self.compaction_fragments = compaction_fragments;
        self
    }
}

impl Default for Config {
    /// One analysis for each 4 cores (at least 2), 2 tables that refresh at a
    /// time, writes of 4096 files, and compaction above 16 fragments.
    fn default() -> Self {
        let cores = std::thread::available_parallelism().map_or(1, |n| n.get());
        Self {
            concurrency: (cores / 4).max(2),
            max_refreshes: 2,
            write_batch: 4096,
            compaction_fragments: 16,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_defaults_leave_most_cores_free() {
        let config = Config::default();
        let cores = std::thread::available_parallelism().map_or(1, |n| n.get());

        assert!(config.concurrency() >= 2);
        assert!(config.concurrency() <= cores.max(2));
        assert_eq!(config.max_refreshes(), 2);
        assert_eq!(Config::default().with_max_refreshes(0).max_refreshes(), 1);
        assert_eq!(Config::default().with_concurrency(0).concurrency(), 1);
    }
}
