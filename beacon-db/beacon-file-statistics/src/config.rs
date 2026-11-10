//! The settings of the file statistics pipeline.

use std::time::Duration;

/// The settings of the file statistics pipeline.
#[derive(Debug, Clone, PartialEq)]
pub struct Config {
    num_threads: usize,
    walk_interval: Duration,
    queue_capacity: usize,
    batch_size: usize,
    batch_age: Duration,
}

impl Config {
    /// The number of analyzer workers.
    pub fn num_threads(&self) -> usize {
        self.num_threads
    }

    /// The time from the start of one store walk to the start of the next.
    pub fn walk_interval(&self) -> Duration {
        self.walk_interval
    }

    /// The number of tasks the queue holds before a walk waits.
    pub fn queue_capacity(&self) -> usize {
        self.queue_capacity
    }

    /// The number of files that makes a full batch for one commit.
    pub fn batch_size(&self) -> usize {
        self.batch_size
    }

    /// The longest time an analyzed file waits before its batch is committed.
    pub fn batch_age(&self) -> Duration {
        self.batch_age
    }

    /// Sets the number of analyzer workers.
    pub fn with_num_threads(mut self, num_threads: usize) -> Self {
        self.num_threads = num_threads;
        self
    }

    /// Sets the time from the start of one store walk to the start of the next.
    pub fn with_walk_interval(mut self, walk_interval: Duration) -> Self {
        self.walk_interval = walk_interval;
        self
    }

    /// Sets the longest time an analyzed file waits before its batch is committed.
    pub fn with_batch_age(mut self, batch_age: Duration) -> Self {
        self.batch_age = batch_age;
        self
    }
}

impl Default for Config {
    /// One worker for each core, a walk each 15 minutes, 1024 queued tasks, and
    /// a commit for each 1024 files or 30 seconds.
    fn default() -> Self {
        Self {
            num_threads: std::thread::available_parallelism().map_or(1, |n| n.get()),
            walk_interval: Duration::from_secs(900),
            queue_capacity: 1024,
            batch_size: 1024,
            batch_age: Duration::from_secs(30),
        }
    }
}
