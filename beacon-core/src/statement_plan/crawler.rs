//! Side effects for the crawler DDL execution-plan nodes.
//!
//! These recover the [`CrawlerManager`] from the session extension (registered as
//! a late-filled handle in `Runtime::init_ctx`) and delegate to it.

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use arrow::array::{ArrayRef, BooleanArray, StringArray, UInt64Array};
use arrow::record_batch::RecordBatch;
use crate::crawler::{CrawlerDefinition, CrawlerManager, TableNaming};
use datafusion::prelude::SessionContext;

use super::logical::show_crawlers_arrow_schema;

/// Recover the crawler manager from the session extension handle.
fn crawler_manager(session: &Arc<SessionContext>) -> anyhow::Result<Arc<CrawlerManager>> {
    let handle = session
        .state()
        .config()
        .get_extension::<OnceLock<Arc<CrawlerManager>>>()
        .ok_or_else(|| anyhow::anyhow!("crawler subsystem is not available"))?;
    handle
        .get()
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("crawler manager is not initialized yet"))
}

/// `CREATE CRAWLER`: build a definition from the SQL surface, then create it.
pub(crate) async fn create_crawler(
    session: &Arc<SessionContext>,
    name: &str,
    target_prefix: Option<String>,
    options: &[(String, String)],
) -> anyhow::Result<()> {
    let with: HashMap<String, String> = options.iter().cloned().collect();
    let def = CrawlerDefinition::from_sql(name, target_prefix, &with)
        .map_err(|e| anyhow::anyhow!("invalid CREATE CRAWLER: {e}"))?;
    crawler_manager(session)?.create(def).await?;
    tracing::info!("created crawler '{name}'");
    Ok(())
}

/// `RUN CRAWLER`: run once on demand, logging the resulting report.
pub(crate) async fn run_crawler(session: &Arc<SessionContext>, name: &str) -> anyhow::Result<()> {
    let report = crawler_manager(session)?.run(name).await?;
    tracing::info!(
        "ran crawler '{name}': discovered={} created={} updated={} skipped={} failed={}",
        report.discovered,
        report.created.len(),
        report.updated.len(),
        report.skipped.len(),
        report.failed.len()
    );
    Ok(())
}

/// `DROP CRAWLER`: remove the definition and stop its triggers.
pub(crate) async fn drop_crawler(session: &Arc<SessionContext>, name: &str) -> anyhow::Result<()> {
    crawler_manager(session)?.drop_crawler(name).await?;
    tracing::info!("dropped crawler '{name}'");
    Ok(())
}

/// `SHOW CRAWLERS`: one row per defined crawler.
pub(crate) async fn show_crawlers(session: &Arc<SessionContext>) -> anyhow::Result<RecordBatch> {
    let crawlers = crawler_manager(session)?.list();

    let names: StringArray = crawlers.iter().map(|c| Some(c.name.clone())).collect();
    let prefixes: StringArray = crawlers
        .iter()
        .map(|c| Some(c.target_prefix.clone()))
        .collect();
    let format_filter: StringArray = crawlers
        .iter()
        .map(|c| c.format_filter.as_ref().map(|f| f.join(",")))
        .collect();
    let detect_partitions =
        BooleanArray::from(crawlers.iter().map(|c| c.detect_partitions).collect::<Vec<_>>());
    let schedule_secs =
        UInt64Array::from(crawlers.iter().map(|c| c.schedule_secs).collect::<Vec<_>>());
    let event_driven =
        BooleanArray::from(crawlers.iter().map(|c| c.event_driven).collect::<Vec<_>>());
    let table_naming: StringArray = crawlers
        .iter()
        .map(|c| Some(naming_str(c.table_naming)))
        .collect();

    let columns: Vec<ArrayRef> = vec![
        Arc::new(names),
        Arc::new(prefixes),
        Arc::new(format_filter),
        Arc::new(detect_partitions),
        Arc::new(schedule_secs),
        Arc::new(event_driven),
        Arc::new(table_naming),
    ];

    RecordBatch::try_new(show_crawlers_arrow_schema(), columns)
        .map_err(|e| anyhow::anyhow!("failed to build SHOW CRAWLERS batch: {e}"))
}

fn naming_str(n: TableNaming) -> &'static str {
    match n {
        TableNaming::LeafPrefix => "leaf_prefix",
        TableNaming::CrawlerPrefixed => "crawler_prefixed",
    }
}
