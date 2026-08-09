//! Planning a scan from the file-statistics registry, without listing the
//! store, opening a file, or enumerating the collection.
//!
//! # Why
//!
//! `ListingTable` lists the object store, reads every file's footer for
//! statistics, and materialises one `PartitionedFile` per file. Each cost is
//! linear in the file count and paid again on every query: at 100 000 local
//! files a plan exhausts the process's descriptors, and at three million the
//! file list alone is over a gigabyte per plan. The registry holds what the
//! plan needs — path, size, last-modified, etag, row count, per-column ranges
//! — indexed for exactly the questions a plan asks.
//!
//! # Two modes, and why there are two
//!
//! **Streaming**, the default. Partitions come from
//! [`shard_prefix`](beacon_file_stats::RegistrySnapshot::shard_prefix), which
//! reads only the path index — no record decoded, no path allocated, no list
//! built — and returns one small path range per partition. A `SELECT *` over
//! three million files therefore plans in constant memory and executes by
//! walking the registry lazily.
//!
//! **Pruned**, when a predicate can actually use the statistics.
//! [`prune_files`](beacon_file_stats::prune_files) evaluates a row per
//! candidate, so the candidates must be named; enumeration is inherent to
//! pruning, not incidental. What the plan then carries is the survivors as
//! 8-byte ids. The choice is made without enumerating: if the predicate names
//! no column the registry has ever interned — one lookup each — pruning cannot
//! drop a file, so the scan streams instead.
//!
//! Both modes read one [`RegistrySnapshot`](beacon_file_stats::RegistrySnapshot)
//! opened here and shared by every partition, so a discovery pass landing
//! mid-query cannot shift the ground under a running scan.
//!
//! # What the registry cannot answer, the listing still does
//!
//! Every uncertain case falls back to the `ListingTable` path, untouched:
//!
//! - the switch is off, or the session has no statistics store;
//! - a table URL under which the registry knows no file — an empty directory
//!   and a never-discovered one look identical, and guessing "empty" would
//!   make files invisible;
//! - a single-file URL the registry has never seen;
//! - hive partition columns and declared sort orders, which live on the
//!   listing path;
//! - a format that stacks decode or broadcast nodes over its scan (netCDF and
//!   friends): those are built by the format's `create_physical_plan` around a
//!   materialised scan. The stack is probed with an empty scan, which costs no
//!   I/O.
//!
//! # What this gives up
//!
//! - **`EXPLAIN` stops listing files**, because the plan no longer holds them.
//!   It prints the mode and the counts, and `EXPLAIN ANALYZE`'s
//!   `file_stats_files_listed` / `_pruned` counters become the primary
//!   evidence that pruning ran rather than a convenience.
//! - **Streaming reports no row-count statistics.** Summing them means reading
//!   every record, which is the enumeration this mode exists to avoid, so they
//!   come back `Absent`. The pruned mode, having enumerated already, sums what
//!   it has.
//! - **Order is by path within a partition**, as before, but partitions are
//!   byte-balanced path ranges rather than DataFusion's file groups.
//!
//! # The visibility trade, and why it is opt-in
//!
//! The listing path sees a file the moment it lands. This path sees it when
//! discovery next runs. So [`RegistryListingSwitch`] defaults to off and is
//! enabled per deployment, where the operator controls the discovery interval.
//! Deletion is the exception: a tombstoned file drops out at once.

use std::ops::Range;
use std::sync::Arc;

use beacon_file_stats::{
    FileId, FileState, FileStatsStore, PathShard, RegistrySnapshot, SharedSnapshot,
};
use datafusion::{
    arrow::datatypes::SchemaRef,
    catalog::{Session, TableProvider},
    common::{DFSchema, Statistics, project_schema, stats::Precision},
    datasource::{
        listing::{ListingTable, ListingTableUrl},
        physical_plan::{FileScanConfig, FileScanConfigBuilder},
        source::{DataSource, DataSourceExec},
        table_schema::TableSchema,
    },
    logical_expr::utils::conjunction,
    physical_expr::utils::collect_columns,
    physical_optimizer::pruning::PruningPredicate,
    physical_plan::{ExecutionPlan, empty::EmptyExec, metrics::MetricBuilder},
    prelude::Expr,
};

use crate::registry_source::{Partitions, RegistryScanSource, ShardQuery};

/// Whether scans may plan their file lists from the registry.
///
/// A session-config extension, registered by the runtime builder from
/// `FileStatsConfig`. Absent or disabled means every scan takes the listing
/// path, which is today's behaviour.
///
/// A switch of its own rather than a field on the statistics-store handle: the
/// store existing means pruning is *possible*, while this means the operator
/// accepted the visibility trade documented on this module.
#[derive(Debug, Clone, Copy, Default)]
pub struct RegistryListingSwitch {
    pub enable: bool,
}

/// Build `table`'s scan from the registry, or say why not with `None`.
///
/// `None` is not an error. It is "the registry cannot serve this table", and
/// the caller runs the listing path exactly as it would have without this
/// module.
pub async fn try_scan_from_registry(
    state: &dyn Session,
    table: &ListingTable,
    projection: Option<&Vec<usize>>,
    filters: &[Expr],
    limit: Option<usize>,
) -> Option<Arc<dyn ExecutionPlan>> {
    let switch = state.config().get_extension::<RegistryListingSwitch>()?;
    if !switch.enable {
        return None;
    }
    let store = beacon_file_stats::try_file_stats_from_session(state)?;

    let options = table.options();
    // Partition columns are extracted from paths, and a declared sort order
    // drives output-ordering machinery; both live on the listing path.
    if !options.table_partition_cols.is_empty() || !options.file_sort_order.is_empty() {
        return None;
    }

    let table_paths = table.table_paths();
    let object_store_url = table_paths.first()?.object_store();
    let schema = table.schema();

    // One view of the registry for the whole query: plan and every partition
    // read the same state, however many discovery passes commit meanwhile.
    let snapshot: SharedSnapshot = Arc::new(store.registry().snapshot().ok()?);

    // The scan's configuration, with an empty file list. Probing the format
    // with it settles two things at no I/O cost: whether the format wraps its
    // scan in decode/broadcast nodes (then only `create_physical_plan` can
    // build the plan, so fall back), and what the configured scan looks like
    // after the format has had its say — that config is what openers are
    // created from.
    let (base, projected_schema) =
        probe_format(state, table, &schema, object_store_url, projection, limit).await?;

    // Enumerate only when pruning can actually drop something. Deciding this
    // costs one lookup per predicate column; deciding it by enumerating would
    // cost the whole collection.
    let prunable = !filters.is_empty()
        && predicate_columns(state, &schema, filters)
            .is_some_and(|columns| worth_pruning(&snapshot, &columns));

    if prunable {
        plan_pruned(
            state,
            &store,
            &snapshot,
            table,
            base,
            projected_schema,
            &schema,
            projection,
            filters,
        )
        .await
    } else {
        plan_streaming(
            state,
            &snapshot,
            table,
            base,
            projected_schema,
            options.target_partitions,
        )
    }
}

/// Ask the format for a scan over no files, and take the configuration and
/// output schema it settles on.
///
/// `None` when the format wraps its scan in other nodes, which the registry
/// source cannot stand in for.
async fn probe_format(
    state: &dyn Session,
    table: &ListingTable,
    schema: &SchemaRef,
    object_store_url: datafusion::execution::object_store::ObjectStoreUrl,
    projection: Option<&Vec<usize>>,
    limit: Option<usize>,
) -> Option<(FileScanConfig, SchemaRef)> {
    let options = table.options();
    let table_schema = TableSchema::new(Arc::clone(schema), vec![]);
    let file_source = options.format.file_source(table_schema);
    let mut builder = FileScanConfigBuilder::new(object_store_url, file_source)
        .with_statistics(Statistics::new_unknown(schema.as_ref()))
        .with_limit(limit);
    if let Some(constraints) = table.constraints() {
        builder = builder.with_constraints(constraints.clone());
    }
    let builder = match builder.with_projection_indices(projection.cloned()) {
        Ok(builder) => builder,
        Err(error) => {
            tracing::debug!(%error, "registry listing could not push the projection; using the listing path");
            return None;
        }
    };
    let probe = match options
        .format
        .create_physical_plan(state, builder.build())
        .await
    {
        Ok(plan) => plan,
        Err(error) => {
            tracing::debug!(%error, "registry listing could not probe the format; using the listing path");
            return None;
        }
    };
    let scan = probe.as_any().downcast_ref::<DataSourceExec>()?;
    let base = scan
        .data_source()
        .as_any()
        .downcast_ref::<FileScanConfig>()?
        .clone();
    Some((base, probe.schema()))
}

/// Plan with partitions as path ranges: nothing is enumerated.
fn plan_streaming(
    state: &dyn Session,
    snapshot: &SharedSnapshot,
    table: &ListingTable,
    base: FileScanConfig,
    projected_schema: SchemaRef,
    target_partitions: usize,
) -> Option<Arc<dyn ExecutionPlan>> {
    let options = table.options();
    let table_paths = table.table_paths();
    let ignore_subdirectory = state
        .config_options()
        .execution
        .listing_table_ignore_subdirectory;

    // The partition budget is chosen, then divided between the URLs; it is
    // never derived from a file count.
    let per_url = (target_partitions / table_paths.len().max(1)).max(1);

    // Each URL is sharded under its own prefix, and the prefix travels with
    // the shard. A shared one would let a URL's last shard — which has no end
    // bound — walk into the next URL's range.
    let mut shards: Vec<ShardQuery> = Vec::new();
    let mut estimate = 0u64;
    for url in table_paths {
        let prefix = url.prefix().as_ref().to_string();
        if !url.is_collection() {
            // One named file: a range of one, so the same walk applies.
            let (_, record) = snapshot.record_by_path(&prefix).ok()??;
            object_store::path::Path::parse(&record.path).ok()?;
            shards.push(ShardQuery {
                prefix: record.path.clone(),
                shard: PathShard {
                    start: record.path.clone().into_bytes(),
                    end: None,
                    files: 1,
                    bytes: record.size,
                },
            });
            estimate += 1;
            continue;
        }
        let sharded = snapshot.shard_prefix(&prefix, per_url).ok()?;
        if sharded.files == 0 {
            // The registry knows nothing here. An empty directory and a
            // never-discovered one are indistinguishable, and treating the
            // second as empty would silently hide its files.
            return None;
        }
        estimate += sharded.files;
        shards.extend(sharded.shards.into_iter().map(|shard| ShardQuery {
            prefix: prefix.clone(),
            shard,
        }));
    }
    if shards.is_empty() {
        return None;
    }

    let partitions = Partitions::Streaming {
        extension: options.file_extension.clone(),
        urls: Arc::new(table_paths.to_vec()),
        ignore_subdirectory,
        shards: Arc::new(shards),
    };

    // Row counts would have to be read per file, which is the enumeration this
    // mode exists to avoid. Absent is the honest answer.
    let statistics = Statistics::new_unknown(projected_schema.as_ref());
    let source = RegistryScanSource::new(
        base,
        projected_schema,
        Arc::clone(snapshot),
        partitions,
        statistics,
        estimate as usize,
        0,
    );
    record_counters(&source, estimate as usize, 0, 0);

    tracing::debug!(
        files = estimate,
        "planned a streaming scan from the file-statistics registry"
    );
    Some(DataSourceExec::from_data_source(source))
}

/// Plan with partitions as id slices, because a predicate had to be evaluated
/// against named candidates.
#[expect(clippy::too_many_arguments)]
async fn plan_pruned(
    state: &dyn Session,
    store: &FileStatsStore,
    snapshot: &SharedSnapshot,
    table: &ListingTable,
    base: FileScanConfig,
    projected_schema: SchemaRef,
    schema: &SchemaRef,
    projection: Option<&Vec<usize>>,
    filters: &[Expr],
) -> Option<Arc<dyn ExecutionPlan>> {
    let options = table.options();
    let table_paths = table.table_paths();
    let ignore_subdirectory = state
        .config_options()
        .execution
        .listing_table_ignore_subdirectory;

    // Ids and sizes only: 16 bytes a file, no record, no path.
    let mut candidates: Vec<(FileId, u64)> = Vec::new();
    for url in table_paths {
        let before = candidates.len();
        if url.is_collection() {
            for (id, size) in snapshot
                .candidates_under_prefix(url.prefix().as_ref())
                .ok()?
            {
                candidates.push((id, size));
            }
            // The glob and extension still decide, but checking them needs the
            // path, so it happens once per candidate below rather than here.
        } else {
            let path = url.prefix().as_ref().to_string();
            let (id, record) = snapshot.record_by_path(&path).ok()??;
            object_store::path::Path::parse(&record.path).ok()?;
            candidates.push((id, record.size));
        }
        if candidates.len() == before {
            return None; // nothing known here; only the store can say why
        }
    }

    // Filter to this table's files, and drop what the predicate rules out.
    // Both need the paths, so one record read serves both.
    let ids: Vec<FileId> = candidates.iter().map(|(id, _)| *id).collect();
    let records = snapshot.records_for_ids(&ids).ok()?;
    let mut matched: Vec<(FileId, u64)> = Vec::with_capacity(candidates.len());
    for ((id, size), record) in candidates.iter().zip(&records) {
        let Some(record) = record else { continue };
        if record.state == FileState::Deleted {
            continue;
        }
        let Ok(location) = object_store::path::Path::parse(&record.path) else {
            return None; // a path that cannot be parsed is the listing's problem
        };
        if record.path.ends_with(&options.file_extension)
            && table_paths
                .iter()
                .any(|url| url.contains(&location, ignore_subdirectory))
        {
            matched.push((*id, *size));
        }
    }
    if matched.is_empty() {
        return None;
    }

    let considered = matched.len();
    let mut columns_used = 0;
    if let Some((kept, columns)) = prune(state, store, schema, filters, &matched).await {
        columns_used = columns;
        matched.retain(|(id, _)| kept.binary_search(id).is_ok());
    }
    let pruned = considered - matched.len();

    if matched.is_empty() {
        // Every candidate was provably ruled out; the listing path's answer to
        // "no files" is the same empty plan.
        let projected = project_schema(schema, projection).ok()?;
        return Some(Arc::new(EmptyExec::new(projected)));
    }

    let statistics = summarize(snapshot, &matched, options.collect_stat, &projected_schema);
    let ranges = partition_ranges(&matched, options.target_partitions);
    let ids: Vec<FileId> = matched.iter().map(|(id, _)| *id).collect();

    let source = RegistryScanSource::new(
        base,
        projected_schema,
        Arc::clone(snapshot),
        Partitions::Ids {
            ids: Arc::new(ids),
            ranges: Arc::new(ranges),
        },
        statistics,
        considered,
        pruned,
    );
    record_counters(&source, considered, pruned, columns_used);

    tracing::debug!(
        considered,
        pruned,
        "planned a pruned scan from the file-statistics registry"
    );
    Some(DataSourceExec::from_data_source(source))
}

/// The counters are known at plan time; the file source's metrics set is
/// shared through an `Arc`, so registering here surfaces them under the scan
/// node in `EXPLAIN ANALYZE`. With no file list in the plan, these are the
/// evidence of what the scan considered.
fn record_counters(
    source: &RegistryScanSource,
    considered: usize,
    pruned: usize,
    columns_used: usize,
) {
    let metrics = DataSource::metrics(source);
    MetricBuilder::new(&metrics)
        .global_counter("file_stats_files_listed")
        .add(considered);
    MetricBuilder::new(&metrics)
        .global_counter("file_stats_files_pruned")
        .add(pruned);
    MetricBuilder::new(&metrics)
        .global_counter("file_stats_columns_used")
        .add(columns_used);
}

/// The columns a predicate compares on, or `None` when the pruning engine
/// cannot use its shape at all.
fn predicate_columns(state: &dyn Session, schema: &SchemaRef, filters: &[Expr]) -> Option<Vec<String>> {
    let predicate = conjunction(filters.iter().cloned())?;
    let df_schema = DFSchema::try_from(schema.as_ref().clone()).ok()?;
    let predicate = state.create_physical_expr(predicate, &df_schema).ok()?;
    let pruning = PruningPredicate::try_new(predicate, Arc::clone(schema)).ok()?;
    let columns: Vec<String> = collect_columns(pruning.orig_expr())
        .into_iter()
        .map(|column| column.name().to_string())
        .collect();
    (!columns.is_empty()).then_some(columns)
}

/// Whether any predicate column has ever been interned.
///
/// One lookup per column. A predicate over columns the registry has never seen
/// cannot drop a file, and enumerating the collection to discover that would
/// be the entire cost of pruning for none of its benefit.
fn worth_pruning(snapshot: &RegistrySnapshot, columns: &[String]) -> bool {
    let names: Vec<&str> = columns.iter().map(String::as_str).collect();
    snapshot.knows_any_column(&names).unwrap_or(false)
}

/// The candidate ids `filters` leave alive (ascending), with how many
/// predicate columns had statistics. `None` when pruning cannot apply.
async fn prune(
    state: &dyn Session,
    store: &FileStatsStore,
    schema: &SchemaRef,
    filters: &[Expr],
    candidates: &[(FileId, u64)],
) -> Option<(Vec<FileId>, usize)> {
    let predicate = conjunction(filters.iter().cloned())?;
    let df_schema = DFSchema::try_from(schema.as_ref().clone()).ok()?;
    let predicate = state.create_physical_expr(predicate, &df_schema).ok()?;

    let mut ids: Vec<FileId> = candidates.iter().map(|(id, _)| *id).collect();
    ids.sort_unstable();
    ids.dedup();
    let range = (ids[0], ids[ids.len() - 1]);

    let columns_used =
        beacon_file_stats::pruning::columns_with_statistics(store, &predicate, schema, range).await;
    // Ascending in, ascending out, so the caller may binary-search it.
    let kept = beacon_file_stats::prune_files(store, &predicate, schema, &ids).await;
    Some((kept, columns_used))
}

/// Aggregate statistics for the survivors, read from their records.
///
/// Only the pruned mode calls this: it has already enumerated, so reading the
/// recorded counts costs one batch fetch rather than a new pass. Sums are
/// exact only while every survivor's counts are trustworthy — an `Analyzed`
/// record — because one unknown makes the total unknowable.
fn summarize(
    snapshot: &RegistrySnapshot,
    survivors: &[(FileId, u64)],
    collect_stat: bool,
    projected_schema: &SchemaRef,
) -> Statistics {
    let mut summary = Statistics::new_unknown(projected_schema.as_ref());
    if !collect_stat {
        return summary;
    }
    let ids: Vec<FileId> = survivors.iter().map(|(id, _)| *id).collect();
    let Ok(records) = snapshot.records_for_ids(&ids) else {
        return summary;
    };

    let mut rows: Option<usize> = Some(0);
    let mut bytes: Option<usize> = Some(0);
    for record in records.into_iter().flatten() {
        let trusted = record.state == FileState::Analyzed;
        rows = rows
            .zip(record.num_rows.filter(|_| trusted))
            .map(|(acc, n)| acc + n as usize);
        bytes = bytes
            .zip(record.total_byte_size.filter(|_| trusted))
            .map(|(acc, n)| acc + n as usize);
    }
    summary.num_rows = rows.map_or(Precision::Absent, Precision::Exact);
    summary.total_byte_size = bytes.map_or(Precision::Absent, Precision::Exact);
    summary
}

/// Shard the path-ordered survivors into at most `target_partitions`
/// contiguous index ranges of roughly equal byte size.
fn partition_ranges(survivors: &[(FileId, u64)], target_partitions: usize) -> Vec<Range<usize>> {
    let target_partitions = target_partitions.max(1);
    if survivors.is_empty() {
        return Vec::new();
    }
    let total: u64 = survivors.iter().map(|(_, size)| *size).sum();
    if total == 0 {
        let per_group = survivors.len().div_ceil(target_partitions);
        return (0..survivors.len())
            .step_by(per_group)
            .map(|start| start..(start + per_group).min(survivors.len()))
            .collect();
    }

    // Close a range once it holds its share of the bytes. Every closed range
    // has at least `share` bytes, so at most `target_partitions` form.
    let share = total.div_ceil(target_partitions as u64);
    let mut ranges = Vec::with_capacity(target_partitions);
    let mut start = 0usize;
    let mut run = 0u64;
    for (index, (_, size)) in survivors.iter().enumerate() {
        run += size;
        if run >= share {
            ranges.push(start..index + 1);
            start = index + 1;
            run = 0;
        }
    }
    if start < survivors.len() {
        ranges.push(start..survivors.len());
    }
    ranges
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The point of sharding by bytes: one large file must not drag every
    /// small one into its partition.
    #[test]
    fn sharding_by_size_isolates_the_large_file() {
        let files = vec![(0, 1000), (1, 10), (2, 10), (3, 10)];
        assert_eq!(partition_ranges(&files, 4), vec![0..1, 1..4]);
    }

    /// Ranges stay contiguous, cover every file once, and never exceed the
    /// target.
    #[test]
    fn sharding_respects_the_partition_target() {
        let files: Vec<(FileId, u64)> = (0..10).map(|i| (i, 100)).collect();
        let ranges = partition_ranges(&files, 4);
        assert!(ranges.len() <= 4, "got {} ranges", ranges.len());
        assert_eq!(ranges.iter().map(|r| r.len()).sum::<usize>(), 10);
        for pair in ranges.windows(2) {
            assert_eq!(pair[0].end, pair[1].start, "ranges must be contiguous");
        }
    }

    /// Sizeless files fall back to the count split rather than one giant
    /// range.
    #[test]
    fn sharding_zero_bytes_falls_back_to_count() {
        let files: Vec<(FileId, u64)> = (0..8).map(|i| (i, 0)).collect();
        let ranges = partition_ranges(&files, 4);
        assert_eq!(ranges.len(), 4);
        assert!(ranges.iter().all(|r| r.len() == 2));
    }

    #[test]
    fn an_empty_list_shards_into_nothing() {
        assert!(partition_ranges(&[], 4).is_empty());
    }
}
