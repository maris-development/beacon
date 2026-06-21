//! The crawl engine: turn a [`CrawlerDefinition`] into registered external tables.
//!
//! Reuses Beacon's existing primitives end-to-end:
//! - [`FileManager::list_datasets`] for scan + per-format classification,
//! - [`ExternalTableDefinition::build_provider`] for schema inference + partition
//!   validation (the same code path used when loading persisted tables),
//! - [`TableManager::register_table`] for registration + `table.json` persistence.
//!
//! The only crawler-specific behaviour is grouping (`super::discovery`) and an
//! ownership guard so a crawl never overwrites a hand-created table.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::Schema;
use beacon_datafusion_ext::table_ext::{ExternalTable, ExternalTableDefinition, TableDefinition};
use datafusion::prelude::SessionContext;
use serde::{Deserialize, Serialize};

use crate::{FileManager, TableManager, DATASETS_OBJECT_STORE_URL};

use super::definition::{CrawlerDefinition, CRAWLER_OWNER_OPTION};
use super::discovery::{assign_table_names, group_into_tables};

/// Outcome of a single crawl, suitable for logging or returning over the API.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CrawlReport {
    /// Crawler name.
    pub crawler: String,
    /// Candidate tables discovered.
    pub discovered: usize,
    /// Newly registered tables.
    pub created: Vec<String>,
    /// Existing crawler-owned tables that were refreshed.
    pub updated: Vec<String>,
    /// Tables left untouched because they are not owned by this crawler.
    pub skipped: Vec<String>,
    /// Per-table failures (`name`, error message).
    pub failed: Vec<(String, String)>,
    /// Files that did not match any crawlable format.
    pub skipped_files: usize,
}

/// Builds external tables from discovered datasets.
pub struct CrawlEngine {
    session_ctx: Arc<SessionContext>,
    file_manager: Arc<FileManager>,
    table_manager: Arc<TableManager>,
}

impl CrawlEngine {
    pub fn new(
        session_ctx: Arc<SessionContext>,
        file_manager: Arc<FileManager>,
        table_manager: Arc<TableManager>,
    ) -> Self {
        Self {
            session_ctx,
            file_manager,
            table_manager,
        }
    }

    /// Scan, group, and (re)register external tables for `def`.
    pub async fn run(&self, def: &CrawlerDefinition) -> anyhow::Result<CrawlReport> {
        let mut report = CrawlReport {
            crawler: def.name.clone(),
            ..Default::default()
        };

        // 1. Scan + classify (reuses FileManager + per-format discover_datasets).
        let pattern = scan_pattern(&def.target_prefix);
        let datasets = self
            .file_manager
            .list_datasets(None, None, Some(pattern))
            .await
            .map_err(|e| anyhow::anyhow!("crawler '{}' scan failed: {e}", def.name))?;

        // 2. Group into candidate tables + detect partitions (pure logic).
        let (candidates, skipped_files) = group_into_tables(&datasets, def);
        let names = assign_table_names(&candidates, def);
        report.discovered = candidates.len();
        report.skipped_files = skipped_files.len();

        // 3. Build + register each candidate.
        for (cand, name) in candidates.iter().zip(names) {
            // Ownership guard: only (re)write tables this crawler owns.
            let is_update = match self.table_manager.table_provider(&name) {
                None => false,
                Some(provider) => {
                    let owned = provider
                        .as_any()
                        .downcast_ref::<ExternalTable>()
                        .and_then(|ext| ext.definition().options.get(CRAWLER_OWNER_OPTION).cloned())
                        .map(|owner| owner == def.name)
                        .unwrap_or(false);
                    if !owned {
                        tracing::debug!(
                            "crawler '{}' skipping '{}' (not crawler-owned)",
                            def.name,
                            name
                        );
                        report.skipped.push(name);
                        continue;
                    }
                    true
                }
            };

            let mut options = def.options.clone();
            options.insert(CRAWLER_OWNER_OPTION.to_string(), def.name.clone());

            let table_def = ExternalTableDefinition {
                name: name.clone(),
                location: cand.location(),
                file_type: cand.format.clone(),
                // Empty schema -> infer now and keep re-inferring on refresh.
                schema: Arc::new(Schema::empty()),
                definition: None,
                partition_cols: cand.partition_cols.clone(),
                options,
                if_not_exists: false,
            };

            // build_provider infers schema and validates partitions; failures are
            // per-table and must not abort the whole crawl.
            let provider = match table_def
                .build_provider(self.session_ctx.clone(), &DATASETS_OBJECT_STORE_URL)
                .await
            {
                Ok(provider) => provider,
                Err(error) => {
                    report.failed.push((name, error.to_string()));
                    continue;
                }
            };

            match self.table_manager.register_table(name.clone(), provider) {
                Ok(_) if is_update => report.updated.push(name),
                Ok(_) => report.created.push(name),
                Err(error) => report.failed.push((name, error.to_string())),
            }
        }

        tracing::info!(
            "crawler '{}': discovered={} created={} updated={} skipped={} failed={}",
            def.name,
            report.discovered,
            report.created.len(),
            report.updated.len(),
            report.skipped.len(),
            report.failed.len()
        );

        Ok(report)
    }
}

/// Build the recursive scan glob for a target prefix.
fn scan_pattern(target_prefix: &str) -> String {
    let trimmed = target_prefix.trim_matches('/');
    if trimmed.is_empty() {
        "**/*".to_string()
    } else {
        format!("{trimmed}/**/*")
    }
}

#[cfg(test)]
mod tests {
    use super::scan_pattern;

    #[test]
    fn scan_patterns() {
        assert_eq!(scan_pattern("argo/"), "argo/**/*");
        assert_eq!(scan_pattern("argo"), "argo/**/*");
        assert_eq!(scan_pattern("/argo/floats/"), "argo/floats/**/*");
        assert_eq!(scan_pattern(""), "**/*");
        assert_eq!(scan_pattern("/"), "**/*");
    }
}
