//! Helpers for reasoning about the process's open file-descriptor budget.
//!
//! File-format crates use these to bound how many files they open concurrently
//! during schema inference so a wide glob doesn't exhaust the descriptor limit.

/// Get the maximum number of open file descriptors allowed by the system.
/// On Unix systems, this is determined by the NOFILE rlimit. On other systems, it defaults to u64::MAX (running through docker should default to unix though).
pub fn max_open_fd() -> u64 {
    #[cfg(unix)]
    {
        use rlimit::{Resource, getrlimit};
        if let Ok((soft_limit, _)) = getrlimit(Resource::NOFILE) {
            tracing::debug!(
                "Max open file descriptors (NOFILE soft limit): {}",
                soft_limit
            );
            soft_limit
        } else {
            tracing::warn!("Failed to get NOFILE rlimit, defaulting to 1024");
            1024
        }
    }
    #[cfg(not(unix))]
    u64::MAX
}

pub fn file_open_parallelism() -> usize {
    let max = max_open_fd() as usize / 2; // use half of the available file descriptors for parallelism to be safe
    //Make sure max is at least 1 to avoid zero parallelism
    std::cmp::max(max, 1)
}
