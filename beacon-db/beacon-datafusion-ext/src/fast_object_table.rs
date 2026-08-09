//! [`FastObjectTable`]: Beacon's listing table.
//!
//! A `TableProvider` in its own right — there is no `ListingTable` inside it.
//! It owns the four things such a table needs (a format, its URLs, a schema, a
//! partition target) and answers `scan` by listing the store once and handing
//! the result to a [`FastObjectDataSource`], which reads it a chunk at a time.
//!
//! # Why not `ListingTable`
//!
//! `ListingTable` turns its listing into one `PartitionedFile` per file —
//! ~280 bytes plus a path, fixed at plan time, and there is no way to give it a
//! list or to make it lazy. At three million files that is over a gigabyte,
//! per plan, per concurrent query. This provider keeps the store's own
//! `ObjectMeta`s instead and builds a `PartitionedFile` only at the moment a
//! file is opened.
//!
//! # Pruning
//!
//! When the file-statistics store can answer a predicate, the scan carries it
//! and applies it *while reading* — see [`StreamPruning`]. The planner compiles
//! the predicate and stops; no segment is read to build the plan. Deciding
//! whether it is worth carrying costs one index lookup per predicate column,
//! because a predicate naming no recorded column cannot drop a file.
//!
//! # Formats that own their file list
//!
//! Zarr's `create_physical_plan` expands a store *directory* into partitions,
//! so it has to see the objects itself. Those formats take
//! [`plan_materialized`](FastObjectTable::plan_materialized), which hands the
//! list over and prunes afterwards — pruning objects first could drop a store's
//! `zarr.json` and orphan its children.
//!
//! # What `EXPLAIN` shows
//!
//! `FastObjectScan: files=N, partitions=K[, prune=stream]`. How many files a
//! predicate removes is not known until the scan runs, so the
//! `file_stats_files_considered` and `_pruned` counters under the node report
//! it in `EXPLAIN ANALYZE`.

use std::any::Any;
use std::ops::Range;
use std::sync::Arc;

use beacon_file_stats::FileStatsStore;
use datafusion::{
    arrow::datatypes::SchemaRef,
    catalog::{Session, TableProvider},
    common::{DFSchema, Statistics, plan_datafusion_err, project_schema},
    datasource::{
        TableType,
        file_format::FileFormat,
        physical_plan::{FileGroup, FileScanConfig, FileScanConfigBuilder, FileSource},
        listing::{ListingTableUrl, PartitionedFile},
        source::DataSourceExec,
        table_schema::TableSchema,
    },
    error::DataFusionError,
    execution::{SessionState, object_store::ObjectStoreUrl},
    logical_expr::{TableProviderFilterPushDown, utils::conjunction},
    physical_expr::utils::collect_columns,
    physical_optimizer::pruning::PruningPredicate,
    physical_plan::{ExecutionPlan, empty::EmptyExec},
    prelude::Expr,
};
use futures::TryStreamExt;
use object_store::ObjectMeta;

use crate::fast_object_data_source::{FastObjectDataSource, StreamPruning, projected_schema_of};
use crate::type_widening::{ArrowTypeWidening, SuperTypeWidening};

/// A table over objects, read through a streaming scan.
#[derive(Debug)]
pub struct FastObjectTable {
    format: Arc<dyn FileFormat>,
    urls: Vec<ListingTableUrl>,
    schema: SchemaRef,
    /// Suffix a file must carry. Empty means the format decides.
    extension: String,
    target_partitions: usize,
}

impl FastObjectTable {
    /// Build a table over `urls`, inferring and merging their schemas.
    pub async fn try_new(
        state: &SessionState,
        format: Arc<dyn FileFormat>,
        urls: Vec<ListingTableUrl>,
    ) -> Result<Self, DataFusionError> {
        let target_partitions = state.config_options().execution.target_partitions;

        let mut schemas = Vec::with_capacity(urls.len());
        for url in &urls {
            tracing::debug!("Infer schema for table/file url: {}", url);
            schemas.push(infer_schema(state, format.as_ref(), url, "").await?);
        }

        // The session decides how a column's diverging types merge. Beacon
        // registers super typing at startup; a session without the extension
        // (a test, an embedded use) gets the same strategy rather than a
        // different behaviour.
        let widening = state
            .config()
            .get_extension::<ArrowTypeWidening>()
            .unwrap_or_else(|| Arc::new(ArrowTypeWidening::new(Arc::new(SuperTypeWidening))));
        let schema = widening
            .merge_schemas(&schemas)
            .map_err(|e| plan_datafusion_err!("Failed to merge schemas for object table: {}", e))?;

        Ok(Self {
            format,
            urls,
            schema,
            extension: String::new(),
            target_partitions,
        })
    }

    /// The URLs (including any globs) backing this table.
    ///
    /// Used by query-time authorization to resolve the dataset paths a
    /// `read_*` scan reads.
    pub fn table_paths(&self) -> &[ListingTableUrl] {
        &self.urls
    }
}

/// Infer one URL's schema from the files it currently lists.
async fn infer_schema(
    state: &SessionState,
    format: &dyn FileFormat,
    url: &ListingTableUrl,
    extension: &str,
) -> Result<SchemaRef, DataFusionError> {
    let store = state.runtime_env().object_store(url)?;
    let objects: Vec<ObjectMeta> = url
        .list_all_files(state, store.as_ref(), extension)
        .await?
        // An empty file cannot affect the schema but may throw when read for it.
        .try_filter(|meta| futures::future::ready(meta.size > 0))
        .try_collect()
        .await?;

    format.infer_schema(state, &store, &objects).await
}

impl FastObjectTable {
    /// Everything the scan needs that is not the file list.
    fn scan_parts(
        &self,
        projection: Option<&Vec<usize>>,
    ) -> Result<(Arc<dyn FileSource>, SchemaRef), DataFusionError> {
        let table_schema = TableSchema::new(Arc::clone(&self.schema), vec![]);
        let mut file_source = self.format.file_source(table_schema);

        // A projection is pushed into the file source itself, which is what
        // makes a narrow `SELECT` read narrow.
        if let Some(indices) = projection {
            let exprs = datafusion::physical_expr::projection::ProjectionExprs::from_indices(
                indices,
                &self.schema,
            );
            // A format that cannot narrow itself keeps the full schema, and
            // the plan projects above the scan instead.
            if let Some(projected) = file_source.try_pushdown_projection(&exprs)? {
                file_source = projected;
            }
        }
        let projected = projected_schema_of(&file_source)?;
        Ok((file_source, projected))
    }
}

#[async_trait::async_trait]
impl TableProvider for FastObjectTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let Some(object_store_url) = self.urls.first().map(ListingTableUrl::object_store) else {
            return Ok(Arc::new(EmptyExec::new(project_schema(
                &self.schema,
                projection,
            )?)));
        };
        let (file_source, _) = self.scan_parts(projection)?;

        if self.format_owns_file_list() {
            return self
                .plan_materialized(state, &object_store_url, file_source, projection, filters)
                .await;
        }

        self.plan_from_listing(
            state,
            &object_store_url,
            file_source,
            projection,
            filters,
            limit,
        )
        .await
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::error::Result<Vec<TableProviderFilterPushDown>> {
        // Inexact: a predicate may drop whole files here, but the rows that
        // survive still have to be filtered above the scan.
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }

    fn statistics(&self) -> Option<Statistics> {
        None
    }
}

impl FastObjectTable {
    /// Whether the format rewrites its own file list at plan time.
    ///
    /// Zarr does: a store is a *directory*, and `create_physical_plan` expands
    /// each one into partitions after finding its top-level `zarr.json`. A
    /// format like that has to see the objects, so a cursor that produces them
    /// lazily cannot serve it — and it does not need to, because such stores
    /// are counted in hundreds, not millions.
    ///
    /// Keyed on the format's extension naming a metadata document, which is
    /// what makes a format directory-oriented. A second such format would
    /// justify a trait; one does not.
    fn format_owns_file_list(&self) -> bool {
        self.format.get_ext().ends_with(".json")
    }

    /// Hand the format a materialised file list and let it plan.
    ///
    /// The shape [`format_owns_file_list`](Self::format_owns_file_list)
    /// describes: the objects are listed, grouped by cumulative bytes, and
    /// given to `create_physical_plan`, which is the only code that knows how
    /// to turn them into partitions.
    async fn plan_materialized(
        &self,
        state: &dyn Session,
        object_store_url: &ObjectStoreUrl,
        file_source: Arc<dyn FileSource>,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let store = state.runtime_env().object_store(object_store_url)?;
        let mut objects: Vec<ObjectMeta> = Vec::new();
        for url in &self.urls {
            let listed: Vec<ObjectMeta> = url
                .list_all_files(state, store.as_ref(), &self.extension)
                .await?
                .try_collect()
                .await?;
            objects.extend(listed);
        }
        objects.sort_by(|a, b| a.location.cmp(&b.location));
        if objects.is_empty() {
            return Ok(Arc::new(EmptyExec::new(project_schema(
                &self.schema,
                projection,
            )?)));
        }

        let groups: Vec<FileGroup> =
            cost_balanced_ranges(objects.iter().map(|meta| meta.size), self.target_partitions)
                .into_iter()
                .map(|range| {
                    FileGroup::new(
                        objects[range]
                            .iter()
                            .cloned()
                            .map(PartitionedFile::from)
                            .collect(),
                    )
                })
                .collect();

        let config = FileScanConfigBuilder::new(object_store_url.clone(), file_source)
            .with_file_groups(groups)
            .build();
        let plan = self.format.create_physical_plan(state, config).await?;

        // Pruning runs *after* the format has planned, not before. These
        // formats rewrite their file list — Zarr expands a store directory into
        // partitions — so an object dropped beforehand can take a store's
        // metadata with it and leave its children orphaned. Once the plan
        // exists, its files are the ones the scan will read, and dropping from
        // that list is safe.
        Ok(beacon_file_stats::prune_scan(state, plan, filters, self.schema()).await)
    }

    /// The pruning a partition will apply as it reads, or `None` when there is
    /// none worth applying.
    ///
    /// Compiling a predicate is pure CPU. The check that follows is one index
    /// lookup per predicate column: a predicate naming no column the registry
    /// has ever interned cannot drop a file, and setting up pruning for it
    /// would buy a segment read per chunk for nothing.
    fn stream_pruning(
        &self,
        state: &dyn Session,
        store: &Arc<FileStatsStore>,
        filters: &[Expr],
    ) -> Option<StreamPruning> {
        if filters.is_empty() {
            return None;
        }
        let columns = predicate_columns(state, &self.schema, filters)?;
        if !knows_any(store, &columns) {
            return None;
        }
        Some(StreamPruning {
            store: Arc::clone(store),
            predicate: physical_predicate(state, &self.schema, filters)?,
            table_schema: Arc::clone(&self.schema),
        })
    }

    /// Build the scan, under whatever nodes the format wants above it.
    ///
    /// netCDF, HDF5 and Zarr hand back a stack — their arrays reach the plan
    /// encoded and are decoded and broadcast above the scan — and only
    /// `create_physical_plan` knows how to build it. So the format is asked
    /// for its shape over a scan of no files, and this source takes the place
    /// of the scan at the bottom.
    ///
    /// The file source and schema come from *that* node rather than from this
    /// table: a format may hand its scan a different source, and an nd format's
    /// scan emits encoded columns that the nodes above it decode. Computing the
    /// schema here instead would hand those nodes columns they cannot read.
    #[expect(clippy::too_many_arguments)]
    async fn build_plan(
        &self,
        state: &dyn Session,
        object_store_url: &ObjectStoreUrl,
        file_source: Arc<dyn FileSource>,
        objects: Arc<Vec<ObjectMeta>>,
        ranges: Arc<Vec<Range<usize>>>,
        pruning: Option<StreamPruning>,
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let probe =
            FileScanConfigBuilder::new(object_store_url.clone(), Arc::clone(&file_source)).build();
        let shape = self.format.create_physical_plan(state, probe).await?;

        // Descend the single-child chain to the format's own scan.
        let mut wrappers: Vec<Arc<dyn ExecutionPlan>> = Vec::new();
        let mut node = shape;
        loop {
            if node.as_any().is::<DataSourceExec>() {
                break;
            }
            let children = node.children();
            let [child] = children[..] else {
                // More than one scan under this node: substituting one of them
                // would be guesswork. Say so rather than plan something wrong.
                return Err(DataFusionError::NotImplemented(format!(
                    "the {} format builds a branching plan over its scan, which FastObjectTable cannot substitute into",
                    self.format.get_ext()
                )));
            };
            let child = Arc::clone(child);
            wrappers.push(node);
            node = child;
        }

        // Take the source and schema the format settled on.
        let scan_schema = node.schema();
        let effective_source = node
            .as_any()
            .downcast_ref::<DataSourceExec>()
            .and_then(|exec| {
                exec.data_source()
                    .as_any()
                    .downcast_ref::<FileScanConfig>()
                    .map(|config| Arc::clone(&config.file_source))
            })
            .unwrap_or(file_source);

        // Row and byte counts would have to be read per file, which is the
        // enumeration this scan exists to avoid. Absent is the honest answer.
        let statistics = Statistics::new_unknown(scan_schema.as_ref());

        let source = FastObjectDataSource::new(
            effective_source,
            object_store_url.clone(),
            scan_schema,
            objects,
            ranges,
            pruning,
            limit,
            statistics,
        );

        let mut plan: Arc<dyn ExecutionPlan> = DataSourceExec::from_data_source(source);
        for wrapper in wrappers.into_iter().rev() {
            plan = wrapper.with_new_children(vec![plan])?;
        }
        Ok(plan)
    }

    /// Plan from a store listing: the shape a collection with no registry
    /// gets. The objects are materialised, as on any listing path, but nothing
    /// becomes a `PartitionedFile`.
    async fn plan_from_listing(
        &self,
        state: &dyn Session,
        object_store_url: &ObjectStoreUrl,
        file_source: Arc<dyn FileSource>,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let store = state.runtime_env().object_store(object_store_url)?;
        let mut objects: Vec<ObjectMeta> = Vec::new();
        for url in &self.urls {
            let listed: Vec<ObjectMeta> = url
                .list_all_files(state, store.as_ref(), &self.extension)
                .await?
                .try_collect()
                .await?;
            objects.extend(listed);
        }
        if objects.is_empty() {
            return Ok(Arc::new(EmptyExec::new(project_schema(
                &self.schema,
                projection,
            )?)));
        }
        // A listing is not ordered across pages; sorting keeps a scan's output
        // reproducible run to run, as the listing path's path sort does.
        objects.sort_by(|a, b| a.location.cmp(&b.location));

        // The registry may know these files' column ranges even when it
        // cannot supply the file list — a store discovered but not opted into
        // registry listing. Pruning on them is worth having here too, and it
        // runs in the stream for the same reason it does there: the planner
        // should not block on segment reads.
        let pruning = beacon_file_stats::try_file_stats_from_session(state)
            .and_then(|store| self.stream_pruning(state, &store, filters));

        let ranges =
            cost_balanced_ranges(objects.iter().map(|meta| meta.size), self.target_partitions);
        self.build_plan(
            state,
            object_store_url,
            file_source,
            Arc::new(objects),
            Arc::new(ranges),
            pruning,
            limit,
        )
        .await
    }

}

/// The predicate `filters` form, as one physical expression over `schema`.
fn physical_predicate(
    state: &dyn Session,
    schema: &SchemaRef,
    filters: &[Expr],
) -> Option<Arc<dyn datafusion::physical_expr::PhysicalExpr>> {
    let predicate = conjunction(filters.iter().cloned())?;
    let df_schema = DFSchema::try_from(schema.as_ref().clone()).ok()?;
    state.create_physical_expr(predicate, &df_schema).ok()
}

/// The columns a predicate compares on, or `None` when the pruning engine
/// cannot use its shape at all.
fn predicate_columns(
    state: &dyn Session,
    schema: &SchemaRef,
    filters: &[Expr],
) -> Option<Vec<String>> {
    let predicate = physical_predicate(state, schema, filters)?;
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
fn knows_any(store: &FileStatsStore, columns: &[String]) -> bool {
    columns
        .iter()
        .any(|name| matches!(store.registry().column_id(name), Ok(Some(_))))
}

/// What opening one file is worth, in bytes, when balancing partitions.
///
/// Opening a file is a round trip and a metadata read whatever it holds, so a
/// partition of a thousand small files is not cheap merely because it holds few
/// bytes. A megabyte is the order of magnitude that makes a file's fixed cost
/// comparable to reading it: below that, count dominates the split; well above
/// it, size does.
const FILE_OPEN_COST: u64 = 1024 * 1024;

/// Divide `sizes` into at most `target` contiguous ranges of roughly equal
/// *cost*, where a file costs its size plus [`FILE_OPEN_COST`].
///
/// Balancing on bytes alone strands files: one large file among small ones
/// takes a whole partition's byte share by itself, so every remaining file
/// falls into the next — and once the budget is spent on such partitions, the
/// last one absorbs the rest. Measured on 12 000 files with 11 large ones at
/// the front, that produced partitions of 3, 3, 3, 27, seven of 1000, and one
/// of 4964. Counting files alone has the mirror problem: one partition draws
/// every large file. Adding the two together handles both, and gives sizeless
/// input a plain count split for free.
///
/// Ranges are cut against cumulative boundaries rather than a per-partition
/// budget that resets, because a budget re-rounds on every cut and the error
/// accumulates — four files of near-equal cost over four partitions came out
/// as 1, 2, 1.
fn cost_balanced_ranges(
    sizes: impl Iterator<Item = u64> + Clone,
    target: usize,
) -> Vec<Range<usize>> {
    let target = target.max(1);
    let count = sizes.clone().count();
    if count == 0 {
        return Vec::new();
    }
    let total: u128 = sizes
        .clone()
        .map(|size| (size + FILE_OPEN_COST) as u128)
        .sum();

    let mut ranges = Vec::with_capacity(target);
    let mut start = 0usize;
    let mut cumulative = 0u128;
    for (index, size) in sizes.enumerate() {
        cumulative += (size + FILE_OPEN_COST) as u128;
        // `cumulative >= (closed + 1) / target * total`, without dividing.
        let full = cumulative * target as u128 >= (ranges.len() as u128 + 1) * total;
        // Never cut after the last file: that would leave a trailing range
        // covering nothing.
        if full && ranges.len() + 1 < target {
            ranges.push(start..index + 1);
            start = index + 1;
        }
    }
    if start < count {
        ranges.push(start..count);
    }
    ranges
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ranges(sizes: &[u64], target: usize) -> Vec<Range<usize>> {
        cost_balanced_ranges(sizes.iter().copied(), target)
    }

    /// Files far below the open cost split by count.
    #[test]
    fn small_files_split_by_count() {
        assert_eq!(ranges(&[1_000; 4], 4), vec![0..1, 1..2, 2..3, 3..4]);
        assert_eq!(ranges(&[1_000; 4], 2), vec![0..2, 2..4]);
    }

    /// A file large enough to dominate its own open cost stands alone.
    #[test]
    fn a_large_file_takes_its_own_partition() {
        let mut sizes = vec![64 * 1024 * 1024];
        sizes.extend([1_000u64; 11]);
        let ranges = ranges(&sizes, 4);
        assert_eq!(ranges[0], 0..1, "64 MiB outweighs eleven small files");
    }

    /// Front-loaded skew must not strand the rest in one partition — the
    /// regression that produced 3, 3, 3, 27, ..., 4964.
    #[test]
    fn front_loaded_skew_spreads_across_the_budget() {
        let mut sizes = vec![900_000u64; 11];
        sizes.extend(vec![1_000u64; 1_189]);
        let ranges = ranges(&sizes, 12);
        assert_eq!(ranges.len(), 12, "every partition gets work");

        let largest = ranges.iter().map(|r| r.len()).max().unwrap();
        let smallest = ranges.iter().map(|r| r.len()).min().unwrap();
        assert!(
            largest <= smallest * 2,
            "partitions must stay within a factor of two: {:?}",
            ranges.iter().map(|r| r.len()).collect::<Vec<_>>()
        );
    }

    /// Ranges are contiguous, cover every file once, and never exceed the
    /// target.
    #[test]
    fn ranges_tile_the_listing() {
        let ranges = ranges(&[100; 10], 4);
        assert!(ranges.len() <= 4);
        assert_eq!(ranges.iter().map(|r| r.len()).sum::<usize>(), 10);
        for pair in ranges.windows(2) {
            assert_eq!(pair[0].end, pair[1].start);
        }
    }

    #[test]
    fn an_empty_listing_yields_no_ranges() {
        assert!(ranges(&[], 4).is_empty());
    }
}
