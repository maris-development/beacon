use std::time::Duration;

/// The settings of the file statistics subsystem.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    num_threads: usize,
    walk_interval: Duration,
    queue_capacity: usize,
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

    /// The number of tasks the analyzer queue holds before a walk waits.
    pub fn queue_capacity(&self) -> usize {
        self.queue_capacity
    }

    /// Sets the time from the start of one store walk to the start of the next.
    pub fn with_walk_interval(mut self, walk_interval: Duration) -> Self {
        self.walk_interval = walk_interval;
        self
    }
}

impl Default for Config {
    /// One analyzer worker for each core, a walk each 15 minutes, and 1024 queued tasks.
    fn default() -> Self {
        Self {
            num_threads: std::thread::available_parallelism().map_or(1, |n| n.get()),
            walk_interval: Duration::from_secs(900),
            queue_capacity: 1024,
        }
    }
}
