//! The crawl engine: turn a [`CrawlerDefinition`] into registered external tables.
//!
//! Reuses Beacon's existing primitives end-to-end:
//! - [`beacon_functions::listing::list_datasets`] for scan +
//!   per-format classification,
//! - [`ExternalTableDefinition::build_provider`] for schema inference + partition
//!   validation (the same code path used when loading persisted tables),
//! - `SessionContext::register_table` (backed by `PersistentSchemaProvider`) for
//!   registration + `table.json` persistence.
//!
//! The only crawler-specific behaviour is grouping (`super::discovery`) and an
//! ownership guard so a crawl never overwrites a hand-created table.

use std::{collections::HashMap, sync::Arc};

use arrow::datatypes::Schema;
use beacon_datafusion_ext::format_ext::FileFormatFactoryExt;
use beacon_datafusion_ext::listing_factory::ListingFactory;
use beacon_datafusion_ext::table_ext::{ExternalTable, ExternalTableDefinition, TableDefinition};
use datafusion::{catalog::TableProvider, prelude::SessionContext};
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};

use beacon_functions::listing::list_datasets;

use crate::statement_plan::{upgrade_session, SessionCell};

use super::definition::{CrawlerDefinition, CRAWLER_OWNER_OPTION};
use super::discovery::{
    assign_table_names, find_directory_tables, group_into_tables, inside_directory_table,
    DirectoryFormat, DirectoryTable, FormatExtensions,
};

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
    /// A weak handle, not an `Arc<SessionContext>`: the session owns the crawler
    /// manager (through a config extension), so a strong reference back would be a
    /// cycle that leaks the session and the tables-store lock it holds.
    session: SessionCell,
    file_formats: Vec<Arc<dyn FileFormatFactoryExt>>,
}

impl CrawlEngine {
    pub(crate) fn new(session: SessionCell, file_formats: Vec<Arc<dyn FileFormatFactoryExt>>) -> Self {
        Self {
            session,
            file_formats,
        }
    }

    fn session(&self) -> anyhow::Result<Arc<SessionContext>> {
        upgrade_session(&self.session, "crawler engine")
    }

    /// Scan, group, and (re)register external tables for `def`.
    pub async fn run(&self, def: &CrawlerDefinition) -> anyhow::Result<CrawlReport> {
        // Upgraded once per crawl: a run is short and must see one consistent
        // session, and failing here (runtime torn down mid-schedule) aborts the
        // whole crawl rather than half-registering its tables.
        let session_ctx = self.session()?;

        let mut report = CrawlReport {
            crawler: def.name.clone(),
            ..Default::default()
        };

        // 1. Scan + classify (reuses list_datasets + per-format discover_datasets).
        // Crawlers run periodically, so the cache-backed registered store is fine.
        let pattern = scan_pattern(&def.target_prefix);
        let mut datasets = list_datasets(&session_ctx, &self.file_formats, &pattern)
            .await
        .map_err(|e| anyhow::anyhow!("crawler '{}' scan failed: {e}", def.name))?;

        // A Delta or Iceberg directory is one table. Its Parquet files include those of old
        // versions, so they must not become a Parquet table of their own.
        let directory_tables = find_directory_tables(&object_paths(&session_ctx, &pattern).await?);
        datasets.retain(|dataset| !inside_directory_table(&dataset.file_path, &directory_tables));

        // 2. Group into candidate tables + detect partitions (pure logic).
        let extensions: FormatExtensions = self
            .file_formats
            .iter()
            .map(|format| {
                (
                    format.file_format_name().to_lowercase(),
                    format.crawlable_extensions(),
                )
            })
            .collect();
        let (candidates, skipped_files) = group_into_tables(&datasets, def, &extensions);
        // One naming pass over both kinds keeps every name unique.
        let mut named = candidates.clone();
        named.extend(directory_tables.iter().map(DirectoryTable::as_candidate));
        let names = assign_table_names(&named, def);
        report.discovered = named.len();
        report.skipped_files = skipped_files.len();

        // 3. Build + register each candidate. Directory tables follow the file groups.
        for (index, name) in names.into_iter().enumerate() {
            // Ownership guard: only (re)write tables this crawler owns.
            let is_update = match session_ctx.table_provider(name.as_str()).await {
                Err(_) => false, // does not exist yet
                Ok(provider) => {
                    let owned = crawler_owner(provider.as_ref()).is_some_and(|owner| owner == def.name);
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

            let owner = (CRAWLER_OWNER_OPTION.to_string(), def.name.clone());
            let table_def: Box<dyn TableDefinition> = match candidates.get(index) {
                Some(cand) => {
                    let mut options = def.options.clone();
                    options.insert(owner.0, owner.1);
                    Box::new(ExternalTableDefinition {
                        name: name.clone(),
                        location: cand.location(),
                        file_type: cand.format.clone(),
                        // Empty schema -> infer now and keep re-inferring on refresh.
                        schema: Arc::new(Schema::empty()),
                        definition: None,
                        partition_cols: cand.partition_cols.clone(),
                        options,
                        if_not_exists: false,
                    })
                }
                None => {
                    let table = &directory_tables[index - candidates.len()];
                    directory_table_definition(table, &name, HashMap::from([owner]))
                }
            };

            // build_provider infers schema and validates partitions; failures are
            // per-table and must not abort the whole crawl.
            let provider = match table_def.build_provider(session_ctx.clone()).await {
                Ok(provider) => provider,
                Err(error) => {
                    report.failed.push((name, error.to_string()));
                    continue;
                }
            };

            // register_table (via PersistentSchemaProvider) persists table.json.
            match session_ctx.register_table(name.as_str(), provider) {
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

/// Every object path under `pattern`, the ones no format claims included.
///
/// The dataset listing drops a `_delta_log/*.json` file, because no format reads it, so the
/// table directories are found in this raw listing.
async fn object_paths(session_ctx: &SessionContext, pattern: &str) -> anyhow::Result<Vec<String>> {
    let state = session_ctx.state();
    let factory = state
        .config()
        .get_extension::<ListingFactory>()
        .ok_or_else(|| anyhow::anyhow!("the listing factory is not registered on the session"))?;
    let objects: Vec<_> = factory.listing(&state, pattern)?.stream().try_collect().await?;
    Ok(objects
        .into_iter()
        .map(|object| object.location.to_string())
        .collect())
}

/// The crawler that registered `provider`, when a crawler did.
fn crawler_owner(provider: &dyn TableProvider) -> Option<String> {
    let options = if let Some(external) = provider.downcast_ref::<ExternalTable>() {
        &external.definition().options
    } else if let Some(delta) = provider.downcast_ref::<beacon_delta::BeaconDeltaTable>() {
        &delta.definition().options
    } else if let Some(iceberg) = provider.downcast_ref::<beacon_iceberg::BeaconIcebergTable>() {
        &iceberg.definition().options
    } else {
        return None;
    };
    options.get(CRAWLER_OWNER_OPTION).cloned()
}

/// The table definition of a Delta or Iceberg directory, registered as `name`.
fn directory_table_definition(
    table: &DirectoryTable,
    name: &str,
    options: HashMap<String, String>,
) -> Box<dyn TableDefinition> {
    match table.format {
        DirectoryFormat::Delta => Box::new(beacon_delta::DeltaTableDefinition {
            name: name.to_string(),
            location: table.base.clone(),
            options,
            definition: None,
        }),
        DirectoryFormat::Iceberg => Box::new(beacon_iceberg::IcebergTableDefinition {
            name: name.to_string(),
            location: table.base.clone(),
            options,
            definition: None,
        }),
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
