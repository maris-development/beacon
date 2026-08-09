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
//! # Where the scan is
//!
//! A plain listing scan *is* a `DataSourceExec`. An nd format returns a stack:
//! netCDF and HDF5 hand back `NdBroadcastExec(NdSourceExec(scan))`, because
//! their arrays reach the plan encoded and are decoded and broadcast above the
//! scan. The file list lives under all of that, so this descends the plan's
//! single-child chain to reach it and rebuilds the chain afterwards.
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

/// The nodes standing above a scan, outermost first.
type ScanWrappers = Vec<Arc<dyn ExecutionPlan>>;

/// Split a scan into the nodes above its `DataSourceExec` and that node.
///
/// Returns the wrappers outermost-first. `None` when no `DataSourceExec` sits at
/// the bottom of a single-child chain, which is the fail-open case: a plan shape
/// this does not recognise keeps its file list.
///
/// The descent carries no depth limit. It terminates because a plan is a finite
/// tree and every step moves to a child, and a limit could only ever turn a deep
/// plan into a silently unpruned one. However many nodes a format stacks above
/// its scan, the file list underneath is still the file list.
fn split_at_scan(plan: &Arc<dyn ExecutionPlan>) -> Option<(ScanWrappers, Arc<dyn ExecutionPlan>)> {
    let mut wrappers: ScanWrappers = Vec::new();
    let mut node = Arc::clone(plan);
    loop {
        if node.as_any().is::<DataSourceExec>() {
            return Some((wrappers, node));
        }
        // Only a single-child chain. A join or a union has more than one scan
        // under it, and rewriting one of them here would be guesswork.
        let children = node.children();
        let [child] = children[..] else { return None };
        let child = Arc::clone(child);
        wrappers.push(node);
        node = child;
    }
}

/// Put `scan` back under the `wrappers` that stood above it.
///
/// Each node rebuilds itself from its new child, so an nd source re-derives its
/// decoded schema and an nd broadcast its partitioning, from a child that differs
/// only in which files it lists.
fn rebuild_over_scan(
    wrappers: ScanWrappers,
    scan: Arc<dyn ExecutionPlan>,
) -> Option<Arc<dyn ExecutionPlan>> {
    let mut node = scan;
    for wrapper in wrappers.into_iter().rev() {
        node = wrapper.with_new_children(vec![node]).ok()?;
    }
    Some(node)
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

    let (wrappers, scan) = split_at_scan(plan)?;
    let exec = scan.as_any().downcast_ref::<DataSourceExec>()?;
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

    rebuild_over_scan(wrappers, Arc::new(DataSourceExec::new(Arc::new(pruned))))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::union::UnionExec;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("TEMP", DataType::Float64, true)]))
    }

    /// A `DataSourceExec`. Its data source is a memory one rather than a file
    /// scan: these tests are about the plan shape above the node, not about what
    /// it reads.
    fn scan() -> Arc<dyn ExecutionPlan> {
        MemorySourceConfig::try_new_exec(&[vec![]], schema(), None).unwrap()
    }

    #[test]
    fn a_bare_scan_is_its_own_bottom() {
        let (wrappers, found) = split_at_scan(&scan()).expect("a scan is a scan");
        assert!(wrappers.is_empty());
        assert!(found.as_any().is::<DataSourceExec>());
    }

    /// The nd shape: the file list lives under the nodes that decode and
    /// broadcast it, and pruning has to reach through them. `CoalescePartitions`
    /// stands in for those here, because this crate cannot depend on the crate
    /// they live in.
    #[test]
    fn a_wrapped_scan_is_found_and_put_back() {
        let plan: Arc<dyn ExecutionPlan> = Arc::new(CoalescePartitionsExec::new(Arc::new(
            CoalescePartitionsExec::new(scan()),
        )));

        let (wrappers, found) = split_at_scan(&plan).expect("two wrappers is still a scan");
        assert_eq!(wrappers.len(), 2);
        assert!(found.as_any().is::<DataSourceExec>());

        // Rebuilding restores the shape, with the replacement scan underneath.
        let rebuilt = rebuild_over_scan(wrappers, scan()).expect("the wrappers rebuild");
        assert!(rebuilt.as_any().is::<CoalescePartitionsExec>());
        assert!(rebuilt.children()[0].as_any().is::<CoalescePartitionsExec>());
        assert!(rebuilt.children()[0].children()[0]
            .as_any()
            .is::<DataSourceExec>());
    }

    /// Fail open, both ways: no scan at the bottom, and more than one scan under
    /// a node. Rewriting either would be guesswork, so neither is pruned.
    #[test]
    fn an_unrecognised_shape_is_left_alone() {
        let empty: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(schema()));
        assert!(split_at_scan(&empty).is_none());

        let union = UnionExec::try_new(vec![scan(), scan()]).unwrap();
        assert!(split_at_scan(&union).is_none());
    }

    /// Depth is not a reason to give up. However many nodes stand above a scan,
    /// the file list underneath is still prunable, and a limit here would only
    /// turn a deep plan into a silently unpruned one.
    #[test]
    fn a_deep_chain_is_still_found_and_rebuilt() {
        const DEPTH: usize = 512;

        let mut plan = scan();
        for _ in 0..DEPTH {
            plan = Arc::new(CoalescePartitionsExec::new(plan));
        }

        let (wrappers, found) = split_at_scan(&plan).expect("depth is not a failure");
        assert_eq!(wrappers.len(), DEPTH);
        assert!(found.as_any().is::<DataSourceExec>());

        // And the whole stack comes back, with the replacement scan at the bottom.
        let rebuilt = rebuild_over_scan(wrappers, scan()).expect("the wrappers rebuild");
        let mut node = rebuilt;
        for _ in 0..DEPTH {
            assert!(node.as_any().is::<CoalescePartitionsExec>());
            node = Arc::clone(node.children()[0]);
        }
        assert!(node.as_any().is::<DataSourceExec>());
    }
}
