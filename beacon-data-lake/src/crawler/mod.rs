//! A Glue-style crawler for Beacon.
//!
//! A crawler scans a prefix in the datasets store, classifies files by format,
//! groups them into candidate external tables (detecting Hive-style partitions),
//! and registers them through the same path `CREATE EXTERNAL TABLE` uses — so a
//! crawled table is indistinguishable from a hand-created one.
//!
//! This module is split so the discovery logic stays pure and unit-testable:
//! - [`definition`]: the persisted [`CrawlerDefinition`].
//! - [`discovery`]: pure grouping + partition detection over classified files.

pub mod definition;
pub mod discovery;
pub mod engine;
pub mod manager;
pub mod persistence;

pub use definition::{CrawlerDefinition, TableNaming, CRAWLER_OWNER_OPTION};
pub use discovery::{assign_table_names, group_into_tables, CandidateTable};
pub use engine::{CrawlEngine, CrawlReport};
pub use manager::{new_crawler_manager_handle, CrawlerManager, CrawlerManagerHandle};
pub use persistence::CrawlerPersistence;
