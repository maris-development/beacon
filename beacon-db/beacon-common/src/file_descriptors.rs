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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parallelism_is_never_zero_and_stays_under_the_limit() {
        // Callers use this as a concurrency permit count, so a zero would stall
        // schema inference outright — the `max(_, 1)` floor is load-bearing.
        let parallelism = file_open_parallelism();
        assert!(parallelism >= 1);
        // It must also leave headroom: never more than half the descriptor budget.
        let limit = max_open_fd() as usize;
        assert!(
            parallelism <= limit.max(2) / 2 || parallelism == 1,
            "parallelism {parallelism} exceeds half of the {limit} descriptor budget"
        );
    }
}
