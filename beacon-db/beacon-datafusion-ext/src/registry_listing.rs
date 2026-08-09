//! Planning a scan's file list from the file-statistics registry, without
//! listing the store, opening a file, or materialising the list.
//!
//! # Why
//!
//! `ListingTable` builds its file list by listing the object store, reads
//! every file's footer for statistics, and materialises one `PartitionedFile`
//! per file. Every one of those costs is linear in the file count and is paid
//! again on every query: at 100 000 local files a plan exhausts the process's
//! file descriptors, and at three million the file list alone is more than a
//! gigabyte per plan. The registry already holds everything the plan needs —
//! path, size, last-modified, etag, row count and per-column ranges — keyed
//! for exactly the lookups a plan performs: "every file under this prefix" is
//! one B-tree range walk, and "which of these can match the predicate" is
//! [`prune_files`](beacon_file_stats::prune_files).
//!
//! This module plans from those lookups instead. The walk keeps 8-byte file
//! ids rather than records; pruning runs on the ids *before* anything is
//! built, so a `WHERE` clause sizes the plan to the survivors; and the ids go
//! into a [`RegistryScanSource`](crate::registry_source::RegistryScanSource),
//! which fetches records back in small chunks at execute time. The file list
//! never exists in memory as objects.
//!
//! # What the registry cannot answer, the listing still does
//!
//! Every uncertain case falls back to the `ListingTable` path, untouched:
//!
//! - the switch is off, or the session has no statistics store;
//! - a table URL under which the registry knows no matching file — an empty
//!   directory and a never-discovered one look identical, and guessing "empty"
//!   would make files invisible;
//! - a single-file URL the registry has never seen;
//! - hive partition columns and declared sort orders, which live on the
//!   listing path;
//! - a format that stacks decode or broadcast nodes over its scan (netCDF and
//!   friends): those nodes are built by the format's `create_physical_plan`
//!   around a materialised scan, so the registry source cannot yet stand in.
//!   The stack is probed with an empty scan, which costs no I/O.
//!
//! # The trade this makes, and why it is opt-in
//!
//! The listing path sees a file the moment it lands in the store. This path
//! sees it when discovery next runs, so a freshly copied file is invisible
//! until then — and a file *changed* in place serves its recorded size until
//! rediscovery, which a reader that trusts sizes may trip over. That is a real
//! behaviour change, so the switch ([`RegistryListingSwitch`]) defaults to off
//! and is enabled per deployment, where the operator controls the discovery
//! interval.
//!
//! Deletion is the exception: a tombstoned file drops out of this list at
//! once, while the listing path would only lose it when the store stops
//! reporting it.
//!
//! # What `EXPLAIN` shows
//!
//! The scan node prints `RegistryScanExec: files=N pruned=M partitions=K`, and
//! `EXPLAIN ANALYZE` reports `file_stats_files_listed`,
//! `file_stats_files_pruned` and `file_stats_columns_used` under it. With no
//! materialised list there is no file list to print, so the counts are the
//! evidence of what the plan considered and dropped.

use std::ops::Range;
use std::sync::Arc;

use beacon_file_stats::{FileId, FileState, FileStatsStore};
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
    physical_plan::{ExecutionPlan, empty::EmptyExec, metrics::MetricBuilder},
    prelude::Expr,
};

use crate::registry_source::RegistryScanSource;

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

/// What the planner keeps per candidate file: 32 bytes, no path, no record.
///
/// `rows` and `bytes` are `None` unless the record is `Analyzed` — a pending
/// file has no statistics, and a stale or failed one has numbers describing
/// content that changed underneath them.
struct PlanFile {
    id: FileId,
    /// Object size, for sharding partitions by cumulative bytes.
    size: u64,
    rows: Option<u64>,
    bytes: Option<u64>,
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

    // The scan's configuration, with an empty file list. Probing the format
    // with it settles two things at no I/O cost: whether the format wraps its
    // scan in decode/broadcast nodes (then only `create_physical_plan` can
    // build the plan, so fall back), and what the configured scan looks like
    // after the format has had its say — that config is what openers are
    // created from.
    let table_schema = TableSchema::new(Arc::clone(&schema), vec![]);
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
    let bare_scan = probe.as_any().downcast_ref::<DataSourceExec>()?;
    let base = bare_scan
        .data_source()
        .as_any()
        .downcast_ref::<FileScanConfig>()?
        .clone();
    let projected_schema = probe.schema();

    // The file list, as ids: table-path order, then path order within each,
    // straight off the registry's path B-tree.
    let ignore_subdirectory = state
        .config_options()
        .execution
        .listing_table_ignore_subdirectory;
    let mut candidates: Vec<PlanFile> = Vec::new();
    for url in table_paths {
        let before = candidates.len();
        plan_files_for_url(
            &store,
            url,
            &options.file_extension,
            ignore_subdirectory,
            &mut candidates,
        )?;
        if candidates.len() == before {
            // The registry knows nothing that matches here. An empty directory
            // and a never-discovered one are indistinguishable, and treating
            // the second as empty would silently hide its files. Only the
            // store can tell them apart, so ask it.
            return None;
        }
    }

    let considered = candidates.len();
    let mut pruned = 0usize;
    let mut columns_used = 0usize;

    // Prune on the ids, before anything is built: the plan only ever carries
    // the survivors. Every uncertain case keeps every file.
    if !filters.is_empty() {
        if let Some((kept, columns)) =
            prune_candidates(state, &store, &schema, filters, &candidates).await
        {
            columns_used = columns;
            candidates.retain(|file| kept.binary_search(&file.id).is_ok());
            pruned = considered - candidates.len();
        }
    }

    if candidates.is_empty() {
        // Every candidate was provably ruled out; the listing path's answer to
        // "no files" is the same empty plan.
        let projected = project_schema(&schema, projection).ok()?;
        return Some(Arc::new(EmptyExec::new(projected)));
    }

    // Mirror `ListingTable`: a limit may cut the file list short only when
    // there is no predicate left to apply above the scan, and only while every
    // row count so far is trustworthy.
    let mut cut_short = false;
    if filters.is_empty()
        && let Some(limit) = limit
    {
        let mut rows = 0u64;
        let mut keep = candidates.len();
        for (index, file) in candidates.iter().enumerate() {
            if rows > limit as u64 {
                keep = index;
                break;
            }
            match file.rows {
                Some(n) => rows += n,
                None => break, // unknown rows: nothing more can be proven unnecessary
            }
        }
        if keep < candidates.len() {
            candidates.truncate(keep);
            cut_short = true;
        }
    }

    let statistics = summarize(
        &candidates,
        options.collect_stat,
        cut_short,
        projected_schema.as_ref(),
    );
    let partitions = partition_ranges(&candidates, options.target_partitions);
    let ids: Vec<FileId> = candidates.iter().map(|file| file.id).collect();
    drop(candidates);

    let source = RegistryScanSource::new(
        base,
        projected_schema,
        Arc::clone(store.registry()),
        ids,
        partitions,
        statistics,
        considered,
        pruned,
    );

    // The counters are known at plan time; the file source's metrics set is
    // shared through an `Arc`, so registering here surfaces them under the
    // scan node in `EXPLAIN ANALYZE`. With no materialised file list, these
    // counters are the evidence of what the plan considered.
    let metrics = DataSource::metrics(&source);
    MetricBuilder::new(&metrics)
        .global_counter("file_stats_files_listed")
        .add(considered);
    MetricBuilder::new(&metrics)
        .global_counter("file_stats_files_pruned")
        .add(pruned);
    MetricBuilder::new(&metrics)
        .global_counter("file_stats_columns_used")
        .add(columns_used);

    tracing::debug!(
        considered,
        pruned,
        "planned a scan's file list from the file-statistics registry"
    );
    Some(DataSourceExec::from_data_source(source))
}

/// Append the registered files matching one table URL to `out`, or say the
/// registry cannot answer for this URL with `None`.
fn plan_files_for_url(
    store: &FileStatsStore,
    url: &ListingTableUrl,
    file_extension: &str,
    ignore_subdirectory: bool,
    out: &mut Vec<PlanFile>,
) -> Option<()> {
    let registry = store.registry();
    if url.is_collection() {
        // A recorded path that does not parse cannot be scanned, and skipping
        // it would silently hide a file the store may well serve; poison the
        // walk instead and let the listing path decide.
        let mut poisoned = false;
        registry
            .for_each_under_prefix(url.prefix().as_ref(), |id, record| {
                let Ok(location) = object_store::path::Path::parse(&record.path) else {
                    poisoned = true;
                    return;
                };
                if record.path.ends_with(file_extension)
                    && url.contains(&location, ignore_subdirectory)
                {
                    out.push(plan_file(id, &record));
                }
            })
            .ok()?;
        (!poisoned).then_some(())
    } else {
        // A URL naming one file. The registry either knows it or the listing
        // path must go look; an empty answer here would hide the file.
        let path = url.prefix().as_ref().to_string();
        let id = registry.file_id(&path).ok()??;
        let record = registry.record(id).ok()??;
        if record.state == FileState::Deleted {
            return None;
        }
        object_store::path::Path::parse(&record.path).ok()?;
        out.push(plan_file(id, &record));
        Some(())
    }
}

fn plan_file(id: FileId, record: &beacon_file_stats::FileRecord) -> PlanFile {
    let trusted = record.state == FileState::Analyzed;
    PlanFile {
        id,
        size: record.size,
        rows: record.num_rows.filter(|_| trusted),
        bytes: record.total_byte_size.filter(|_| trusted),
    }
}

/// The candidate ids `filters` leave alive (ascending), with how many
/// predicate columns had statistics. `None` when pruning cannot apply, which
/// keeps every file.
async fn prune_candidates(
    state: &dyn Session,
    store: &FileStatsStore,
    schema: &SchemaRef,
    filters: &[Expr],
    candidates: &[PlanFile],
) -> Option<(Vec<FileId>, usize)> {
    let predicate = conjunction(filters.iter().cloned())?;
    let df_schema = DFSchema::try_from(schema.as_ref().clone()).ok()?;
    let predicate = state.create_physical_expr(predicate, &df_schema).ok()?;

    let mut ids: Vec<FileId> = candidates.iter().map(|file| file.id).collect();
    ids.sort_unstable();
    ids.dedup();
    let range = (ids[0], ids[ids.len() - 1]);

    let columns_used =
        beacon_file_stats::pruning::columns_with_statistics(store, &predicate, schema, range).await;
    // Ascending in, ascending out, so the caller may binary-search it.
    let kept = beacon_file_stats::prune_files(store, &predicate, schema, &ids).await;
    Some((kept, columns_used))
}

/// Aggregate statistics for the surviving files, in output-schema terms.
///
/// Row and byte sums are exact only while every survivor's counts are
/// trustworthy; one unknown makes them absent, and a limit-cut list reports
/// what it kept, inexactly, because files were left out.
fn summarize(
    candidates: &[PlanFile],
    collect_stat: bool,
    cut_short: bool,
    projected_schema: &datafusion::arrow::datatypes::Schema,
) -> Statistics {
    let mut summary = Statistics::new_unknown(projected_schema);
    if !collect_stat {
        return summary;
    }

    let mut rows: Option<usize> = Some(0);
    let mut bytes: Option<usize> = Some(0);
    for file in candidates {
        rows = rows.zip(file.rows).map(|(acc, n)| acc + n as usize);
        bytes = bytes.zip(file.bytes).map(|(acc, n)| acc + n as usize);
    }
    summary.num_rows = rows.map_or(Precision::Absent, Precision::Exact);
    summary.total_byte_size = bytes.map_or(Precision::Absent, Precision::Exact);
    if cut_short {
        summary = summary.to_inexact();
    }
    summary
}

/// Shard the path-ordered candidates into at most `target_partitions`
/// contiguous index ranges of roughly equal byte size.
///
/// By bytes rather than by count, because the registry hands both over for
/// free and a count split can put every large file in one partition.
fn partition_ranges(candidates: &[PlanFile], target_partitions: usize) -> Vec<Range<usize>> {
    let target_partitions = target_partitions.max(1);
    if candidates.is_empty() {
        return Vec::new();
    }
    let total: u64 = candidates.iter().map(|file| file.size).sum();
    if total == 0 {
        // Nothing to balance by; split evenly by count instead.
        let per_group = candidates.len().div_ceil(target_partitions);
        return (0..candidates.len())
            .step_by(per_group)
            .map(|start| start..(start + per_group).min(candidates.len()))
            .collect();
    }

    // Close a range once it holds its share of the bytes. Every closed range
    // has at least `share` bytes, so at most `target_partitions` ranges form.
    let share = total.div_ceil(target_partitions as u64);
    let mut ranges = Vec::with_capacity(target_partitions);
    let mut start = 0usize;
    let mut current_bytes = 0u64;
    for (index, file) in candidates.iter().enumerate() {
        current_bytes += file.size;
        if current_bytes >= share {
            ranges.push(start..index + 1);
            start = index + 1;
            current_bytes = 0;
        }
    }
    if start < candidates.len() {
        ranges.push(start..candidates.len());
    }
    ranges
}

#[cfg(test)]
mod tests {
    use super::*;

    fn file(id: FileId, size: u64) -> PlanFile {
        PlanFile {
            id,
            size,
            rows: None,
            bytes: None,
        }
    }

    /// The point of sharding by bytes: one large file must not drag every
    /// small one into its partition.
    #[test]
    fn sharding_by_size_isolates_the_large_file() {
        let files = vec![file(0, 1000), file(1, 10), file(2, 10), file(3, 10)];
        let ranges = partition_ranges(&files, 4);
        assert_eq!(ranges, vec![0..1, 1..4]);
    }

    /// Ranges stay contiguous, cover every file once, and never exceed the
    /// target.
    #[test]
    fn sharding_respects_the_partition_target() {
        let files: Vec<PlanFile> = (0..10).map(|i| file(i, 100)).collect();
        let ranges = partition_ranges(&files, 4);
        assert!(ranges.len() <= 4, "got {} ranges", ranges.len());
        let covered: usize = ranges.iter().map(|r| r.len()).sum();
        assert_eq!(covered, 10);
        for pair in ranges.windows(2) {
            assert_eq!(pair[0].end, pair[1].start, "ranges must be contiguous");
        }
    }

    /// Sizeless files fall back to the count split rather than one giant
    /// range.
    #[test]
    fn sharding_zero_bytes_falls_back_to_count() {
        let files: Vec<PlanFile> = (0..8).map(|i| file(i, 0)).collect();
        let ranges = partition_ranges(&files, 4);
        assert_eq!(ranges.len(), 4);
        assert!(ranges.iter().all(|r| r.len() == 2));
    }

    #[test]
    fn an_empty_list_shards_into_nothing() {
        assert!(partition_ranges(&[], 4).is_empty());
    }

    /// Row and byte sums are exact only while every survivor's counts are
    /// trustworthy.
    #[test]
    fn the_summary_is_exact_only_when_every_count_is_known() {
        let schema = datafusion::arrow::datatypes::Schema::empty();
        let known = |id: u64, rows: u64| PlanFile {
            id,
            size: 10,
            rows: Some(rows),
            bytes: Some(100),
        };

        let summary = summarize(&[known(0, 5), known(1, 7)], true, false, &schema);
        assert_eq!(summary.num_rows, Precision::Exact(12));
        assert_eq!(summary.total_byte_size, Precision::Exact(200));

        // One unknown poisons the sums.
        let summary = summarize(&[known(0, 5), file(1, 10)], true, false, &schema);
        assert_eq!(summary.num_rows, Precision::Absent);

        // A limit-cut list reports what it kept, inexactly.
        let summary = summarize(&[known(0, 5)], true, true, &schema);
        assert_eq!(summary.num_rows, Precision::Inexact(5));

        // With statistics collection off, nothing is claimed.
        let summary = summarize(&[known(0, 5)], false, false, &schema);
        assert_eq!(summary.num_rows, Precision::Absent);
    }
}
