//! Crawler configuration.
//!
//! A plain data struct describing how the crawler subsystem behaves. It lives in
//! this leaf crate so both `beacon-config` (which fills it from the environment and
//! re-exports it) and `beacon-core` (which owns the crawler engine and consumes it)
//! can reach it without `beacon-config` depending on the engine — mirroring how
//! `beacon-object-storage` owns `StorageConfig`.

/// How the crawler subsystem behaves.
#[derive(Debug, Clone)]
pub struct CrawlerConfig {
    /// Master switch for crawler scheduling/event triggers. When false, crawlers
    /// can still be defined and run on demand, but no background tasks are spawned.
    pub enable: bool,
    /// Fallback poll interval (seconds) applied to a crawler that requests
    /// event-driven crawling on a deployment where storage events are unavailable.
    pub default_interval_secs: u64,
}

impl Default for CrawlerConfig {
    fn default() -> Self {
        // Mirrors the `BEACON_CRAWLER_ENABLE` / `BEACON_CRAWLER_DEFAULT_INTERVAL_SECS`
        // environment defaults in `beacon-config`.
        Self {
            enable: true,
            default_interval_secs: 900,
        }
    }
}
