//! Removing files from a built scan.
//!
//! This lives here, rather than beside either table provider that uses it, so
//! both can reach it: `SuperListingTable` in `beacon-common` backs every SQL
//! `read_*` function, and `FileCollection` in `beacon-datafusion-ext` backs the
//! JSON query API. Neither of those crates may depend on the other, and this one
//! depends on neither.
//!
//! # Why after the plan, not before
//!
//! Pruning rewrites the file list of a plan `ListingTable` already built. That
//! keeps all of its listing, partitioning and ordering logic and changes only
//! which files survive. Doing it earlier would mean reimplementing that; doing it
//! in a physical optimizer rule is not possible at all, because those are
//! synchronous and reading a segment is not.
//!
//! # Fail open
//!
//! Every uncertain path returns the plan untouched: no store, no statistics, an
//! unsupported predicate, a plan shape this does not recognise, a path the
//! registry has never seen. Pruning may only ever drop a file that provably
//! cannot match.

use std::collections::HashSet;
use std::sync::{Arc, OnceLock};

use arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::common::DFSchema;
use datafusion::datasource::physical_plan::{FileGroup, FileScanConfig};
use datafusion::datasource::source::DataSourceExec;
use datafusion::logical_expr::utils::conjunction;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::metrics::MetricBuilder;
use datafusion::prelude::Expr;

use crate::store::FileStatsStore;
use crate::types::FileId;

/// Shared, late-filled handle to the store.
///
/// The store is built by the runtime, after the session it needs, so the handle
/// is registered empty as a session-config extension and filled once the store
/// exists. Empty means no pruning, which is always a correct answer.
pub type FileStatsHandle = Arc<OnceLock<Arc<FileStatsStore>>>;

/// Create an empty handle to register as a session extension.
pub fn new_file_stats_handle() -> FileStatsHandle {
    Arc::new(OnceLock::new())
}

/// The store this session prunes against, if it has one.
pub fn try_file_stats_from_session(session: &dyn Session) -> Option<Arc<FileStatsStore>> {
    session
        .config()
        .get_extension::<OnceLock<Arc<FileStatsStore>>>()?
        .get()
        .cloned()
}

/// Drop the files `filters` provably rule out from a built scan.
///
/// Returns `plan` unchanged when pruning cannot apply.
pub async fn prune_scan(
    state: &dyn Session,
    plan: Arc<dyn ExecutionPlan>,
    filters: &[Expr],
    schema: SchemaRef,
) -> Arc<dyn ExecutionPlan> {
    match try_prune_scan(state, &plan, filters, schema).await {
        Some(pruned) => pruned,
        None => plan,
    }
}

async fn try_prune_scan(
    state: &dyn Session,
    plan: &Arc<dyn ExecutionPlan>,
    filters: &[Expr],
    schema: SchemaRef,
) -> Option<Arc<dyn ExecutionPlan>> {
    if filters.is_empty() {
        return None;
    }
    let store = try_file_stats_from_session(state)?;

    let exec = plan.as_any().downcast_ref::<DataSourceExec>()?;
    let config = exec
        .data_source()
        .as_any()
        .downcast_ref::<FileScanConfig>()?;

    // The scan holds paths; the store holds ids. One batched lookup rather than
    // one per file: measured at 0.3us each against 7us for the single form,
    // which is 0.3s against 7s on a million-file scan.
    let paths: Vec<String> = config
        .file_groups
        .iter()
        .flat_map(|group| {
            group
                .iter()
                .map(|file| file.object_meta.location.to_string())
        })
        .collect();
    if paths.is_empty() {
        return None;
    }
    let borrowed: Vec<&str> = paths.iter().map(|path| path.as_str()).collect();
    let ids = store.registry().file_ids(&borrowed).ok()?;

    let mut candidates: Vec<FileId> = ids.iter().filter_map(|id| *id).collect();
    if candidates.is_empty() {
        return None; // nothing here is registered, so nothing is prunable
    }
    candidates.sort_unstable();
    candidates.dedup();

    // The provider's filters are logical; the pruning engine wants one physical
    // predicate over the table schema.
    let predicate = conjunction(filters.iter().cloned())?;
    let df_schema = DFSchema::try_from(schema.as_ref().clone()).ok()?;
    let predicate = state.create_physical_expr(predicate, &df_schema).ok()?;

    let file_range = (candidates[0], candidates[candidates.len() - 1]);
    let columns_used =
        crate::pruning::columns_with_statistics(&store, &predicate, &schema, file_range).await;
    let kept: HashSet<FileId> = crate::pruning::prune_files(&store, &predicate, &schema, &candidates)
        .await
        .into_iter()
        .collect();
    if kept.len() == candidates.len() {
        return None; // nothing was dropped, so leave the plan alone
    }

    // Rebuild the groups. A path the registry has never seen has no statistics,
    // so it stays: a partially backfilled store must not lose files.
    let mut position = 0usize;
    let mut groups = Vec::with_capacity(config.file_groups.len());
    let mut dropped = 0usize;
    for group in &config.file_groups {
        let mut files = Vec::with_capacity(group.len());
        for file in group.iter() {
            let keep = match ids[position] {
                Some(id) => kept.contains(&id),
                None => true,
            };
            position += 1;
            if keep {
                files.push(file.clone());
            } else {
                dropped += 1;
            }
        }
        if !files.is_empty() {
            groups.push(FileGroup::new(files));
        }
    }

    tracing::debug!(
        dropped,
        of = paths.len(),
        "file statistics pruned the scan's file list"
    );

    let mut pruned = config.clone();
    pruned.file_groups = groups;

    // Report what happened where people already look. The counts are known at
    // plan time, but `DataSourceExec` shares one `ExecutionPlanMetricsSet` with
    // its `FileSource` through an `Arc`, so registering here surfaces them under
    // the scan node in `EXPLAIN ANALYZE` -- with no extra plan node, and so no
    // risk of blocking a later repartition or limit pushdown.
    //
    // Without this the only evidence pruning happened is a smaller file list in
    // the plan, which tells you the result but never the ratio.
    let metrics = datafusion::datasource::source::DataSource::metrics(&pruned);
    MetricBuilder::new(&metrics)
        .global_counter("file_stats_files_considered")
        .add(paths.len());
    MetricBuilder::new(&metrics)
        .global_counter("file_stats_files_pruned")
        .add(dropped);
    MetricBuilder::new(&metrics)
        .global_counter("file_stats_columns_used")
        .add(columns_used);

    Some(Arc::new(DataSourceExec::new(Arc::new(pruned))))
}
