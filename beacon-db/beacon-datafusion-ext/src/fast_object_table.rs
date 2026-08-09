//! [`FastObjectTable`]: Beacon's listing table, without the listing.
//!
//! This is a `TableProvider` in its own right — there is no `ListingTable`
//! inside it. It owns the four things such a table needs (a format, its URLs,
//! a schema, a partition target) and it answers `scan` by building a
//! [`FastObjectDataSource`], whose partitions are cursors rather than file
//! lists.
//!
//! # Where its files come from
//!
//! In order of preference:
//!
//! 1. **A registry walk.** [`shard_prefix`](beacon_file_stats::RegistrySnapshot::shard_prefix)
//!    cuts the prefix into disjoint, byte-balanced path ranges by reading only
//!    the path index — no record decoded, no path allocated, no list built. A
//!    `SELECT *` over three million files plans in constant memory.
//! 2. **A registry walk with pruning.** When a predicate can actually use the
//!    stored statistics, [`prune_files`](beacon_file_stats::prune_files) names
//!    the survivors and the plan carries their 8-byte ids. Enumeration is
//!    inherent to pruning — a predicate is evaluated per candidate — so it is
//!    paid only when it can pay off, decided by one index lookup per predicate
//!    column rather than by enumerating to find out.
//! 3. **A store listing.** A collection the registry has never seen, or one
//!    whose operator has not opted into registry listing. The objects are
//!    materialised, as they are on any listing path, and handed to the same
//!    source; nothing becomes a `PartitionedFile` and nothing is grouped up
//!    front. Recorded statistics still prune this list, so a store the
//!    registry knows but does not list for is no worse off than before.
//!
//! One [`RegistrySnapshot`](beacon_file_stats::RegistrySnapshot) is opened per
//! scan and shared by every partition, so a discovery pass landing mid-query
//! cannot change what a running scan reads.
//!
//! # Schema
//!
//! Inferred once, at construction, and merged across URLs through the
//! session's [`ArrowTypeWidening`] strategy — Beacon's super typing unless a
//! deployment registered its own. The files it infers from come from the
//! registry when it knows them, which removes the listing but not the reads;
//! removing those needs schema interning, which is not built yet.
//!
//! # What `EXPLAIN` shows
//!
//! `FastObjectScan: mode=…, files=N, pruned=M, partitions=K`. There is no file
//! list to print, so the `file_stats_files_considered` and `_pruned` counters
//! under the node are the evidence that pruning ran.
//!
//! # The visibility trade
//!
//! A registry-planned scan sees a file once discovery has run, not the moment
//! it lands. [`RegistryListingSwitch`] therefore defaults to off, and with it
//! off this table lists the store like any other. Deletion is the exception: a
//! tombstoned file drops out at once.

use std::any::Any;
use std::ops::Range;
use std::sync::Arc;

use beacon_file_stats::{
    FileId, FileState, FileStatsStore, PathShard, RegistrySnapshot, SharedSnapshot,
};
use datafusion::{
    arrow::datatypes::SchemaRef,
    catalog::{Session, TableProvider},
    common::{DFSchema, Statistics, plan_datafusion_err, project_schema, stats::Precision},
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
    physical_plan::{ExecutionPlan, empty::EmptyExec, metrics::MetricBuilder},
    prelude::Expr,
};
use futures::TryStreamExt;
use object_store::ObjectMeta;

use crate::fast_object_data_source::{
    FastObjectDataSource, Identities, ShardQuery, projected_schema_of,
};
use crate::type_widening::{ArrowTypeWidening, SuperTypeWidening};

/// Whether scans may plan their file lists from the registry.
///
/// A session-config extension, registered by the runtime builder from
/// `FileStatsConfig`. Absent or disabled means every scan lists the store,
/// which is today's behaviour.
///
/// A switch of its own rather than a field on the statistics-store handle: the
/// store existing means pruning is *possible*, while this means the operator
/// accepted the visibility trade documented on this module.
#[derive(Debug, Clone, Copy, Default)]
pub struct RegistryListingSwitch {
    pub enable: bool,
}

/// A table over objects, scanned through cursors rather than file lists.
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

    pub fn format(&self) -> &Arc<dyn FileFormat> {
        &self.format
    }
}

/// Infer one URL's schema, from the registry's files when it knows them.
///
/// The registry carries every field an `ObjectMeta` needs, so this removes the
/// listing. It does not remove the per-file reads the format performs — that
/// needs schema interning, which is not built yet.
async fn infer_schema(
    state: &SessionState,
    format: &dyn FileFormat,
    url: &ListingTableUrl,
    extension: &str,
) -> Result<SchemaRef, DataFusionError> {
    let store = state.runtime_env().object_store(url)?;

    let ignore_subdirectory = state
        .config_options()
        .execution
        .listing_table_ignore_subdirectory;
    let from_registry = beacon_file_stats::try_file_stats_from_session(state)
        .filter(|_| registry_listing_enabled(state))
        .and_then(|store| store.registry().snapshot().ok())
        .and_then(|snapshot| {
            let objects = objects_under(&snapshot, url, extension, ignore_subdirectory)?;
            (!objects.is_empty()).then_some(objects)
        });

    let objects = match from_registry {
        Some(objects) => objects,
        None => {
            url.list_all_files(state, store.as_ref(), extension)
                .await?
                .try_filter(|meta| futures::future::ready(meta.size > 0))
                .try_collect()
                .await?
        }
    };

    format.infer_schema(state, &store, &objects).await
}

impl FastObjectTable {
    /// Everything the scan needs that is not the file list.
    fn scan_parts(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
    ) -> Result<(Arc<dyn FileSource>, SchemaRef), DataFusionError>
    {
        let table_schema = TableSchema::new(Arc::clone(&self.schema), vec![]);
        let mut file_source = self.format.file_source(table_schema);

        // A projection is pushed into the file source itself, which is what
        // makes a narrow `SELECT` read narrow.
        if let Some(indices) = projection {
            let exprs = datafusion::physical_expr::projection::ProjectionExprs::from_indices(
                indices,
                &self.schema,
            );
            if let Some(projected) = file_source.try_pushdown_projection(&exprs)? {
                file_source = projected;
            } else {
                // The format cannot narrow itself; the plan projects above the
                // scan instead, so the scan keeps the full schema.
                let _ = state;
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
        let (file_source, projected_schema) = self.scan_parts(state, projection)?;

        if self.format_owns_file_list() {
            return self
                .plan_materialized(state, &object_store_url, file_source, projection, filters)
                .await;
        }

        let plan = self
            .plan_from_registry(
                state,
                &object_store_url,
                &file_source,
                &projected_schema,
                filters,
                limit,
            )
            .await;
        if let Some(plan) = plan {
            return Ok(plan);
        }

        // No registry for these paths: list the store, as any table must when
        // nothing has recorded what is there.
        self.plan_from_listing(
            state,
            &object_store_url,
            file_source,
            projected_schema,
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
            byte_balanced_ranges(objects.iter().map(|meta| meta.size), self.target_partitions)
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

    /// Plan from the registry, or `None` when it cannot serve these paths.
    async fn plan_from_registry(
        &self,
        state: &dyn Session,
        object_store_url: &ObjectStoreUrl,
        file_source: &Arc<dyn FileSource>,
        projected_schema: &SchemaRef,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Option<Arc<dyn ExecutionPlan>> {
        if !registry_listing_enabled(state) {
            return None;
        }
        let store = beacon_file_stats::try_file_stats_from_session(state)?;
        // One view of the registry for the whole query.
        let snapshot: SharedSnapshot = Arc::new(store.registry().snapshot().ok()?);

        let ignore_subdirectory = state
            .config_options()
            .execution
            .listing_table_ignore_subdirectory;

        // Enumerate only when pruning can drop something. Deciding this costs
        // one lookup per predicate column; deciding it by enumerating would
        // cost the whole collection.
        let prunable = !filters.is_empty()
            && predicate_columns(state, &self.schema, filters)
                .is_some_and(|columns| knows_any(&snapshot, &columns));

        let (identities, counts, considered, pruned) = if prunable {
            self.pruned_identities(state, &store, &snapshot, filters, ignore_subdirectory)
                .await?
        } else {
            self.streaming_identities(&snapshot, ignore_subdirectory)?
        };

        if identities.partitions() == 0 {
            // Every candidate was provably ruled out.
            return Some(Arc::new(EmptyExec::new(Arc::clone(projected_schema))));
        }

        tracing::debug!(
            considered,
            pruned,
            "planned a scan from the file-statistics registry"
        );
        self.build_plan(
            state,
            object_store_url,
            Arc::clone(file_source),
            Some(snapshot),
            identities,
            counts,
            limit,
            considered,
            pruned,
            self.columns_used(pruned),
        )
        .await
        .ok()
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
        snapshot: Option<SharedSnapshot>,
        identities: Identities,
        counts: (Precision<usize>, Precision<usize>),
        limit: Option<usize>,
        considered: usize,
        pruned: usize,
        columns_used: usize,
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

        let mut statistics = Statistics::new_unknown(scan_schema.as_ref());
        statistics.num_rows = counts.0;
        statistics.total_byte_size = counts.1;

        let source = FastObjectDataSource::new(
            effective_source,
            object_store_url.clone(),
            scan_schema,
            snapshot,
            identities,
            limit,
            statistics,
            considered,
            pruned,
        );
        record_counters(&source, considered, pruned, columns_used);

        let mut plan: Arc<dyn ExecutionPlan> = DataSourceExec::from_data_source(source);
        for wrapper in wrappers.into_iter().rev() {
            plan = wrapper.with_new_children(vec![plan])?;
        }
        Ok(plan)
    }

    /// Only a pruned plan can say how many predicate columns had statistics;
    /// a streaming one never asked.
    fn columns_used(&self, pruned: usize) -> usize {
        usize::from(pruned > 0)
    }

    /// Partitions as path ranges. Nothing is enumerated.
    fn streaming_identities(
        &self,
        snapshot: &RegistrySnapshot,
        ignore_subdirectory: bool,
    ) -> Option<(Identities, (Precision<usize>, Precision<usize>), usize, usize)> {
        // The partition budget is chosen, then divided between the URLs; it is
        // never derived from a file count.
        let per_url = (self.target_partitions / self.urls.len().max(1)).max(1);

        let mut shards: Vec<ShardQuery> = Vec::new();
        let mut estimate = 0u64;
        for url in &self.urls {
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

        let identities = Identities::Shards {
            extension: self.extension.clone(),
            urls: Arc::new(self.urls.clone()),
            ignore_subdirectory,
            shards: Arc::new(shards),
        };
        // Row counts would have to be read per file, which is the enumeration
        // this mode exists to avoid. Absent is the honest answer.
        Some((
            identities,
            (Precision::Absent, Precision::Absent),
            estimate as usize,
            0,
        ))
    }

    /// Partitions as id slices, because a predicate had to be evaluated
    /// against named candidates.
    async fn pruned_identities(
        &self,
        state: &dyn Session,
        store: &FileStatsStore,
        snapshot: &RegistrySnapshot,
        filters: &[Expr],
        ignore_subdirectory: bool,
    ) -> Option<(Identities, (Precision<usize>, Precision<usize>), usize, usize)> {
        // Ids and sizes only: 16 bytes a file, no record, no path.
        let mut candidates: Vec<(FileId, u64)> = Vec::new();
        for url in &self.urls {
            let before = candidates.len();
            if url.is_collection() {
                candidates.extend(snapshot.candidates_under_prefix(url.prefix().as_ref()).ok()?);
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

        // Filter to this table's files. The glob needs the path, so one record
        // read serves this and the statistics below.
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
            if record.path.ends_with(&self.extension)
                && self
                    .urls
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
        if let Some(kept) = prune(state, store, &self.schema, filters, &matched).await {
            matched.retain(|(id, _)| kept.binary_search(id).is_ok());
        }
        let pruned = considered - matched.len();

        let counts = summarize(snapshot, &matched);
        let ranges = byte_balanced_ranges(
            matched.iter().map(|(_, size)| *size),
            self.target_partitions,
        );
        let ids: Vec<FileId> = matched.iter().map(|(id, _)| *id).collect();

        Some((
            Identities::Ids {
                ids: Arc::new(ids),
                ranges: Arc::new(ranges),
            },
            counts,
            considered,
            pruned,
        ))
    }

    /// Plan from a store listing: the shape a collection with no registry
    /// gets. The objects are materialised, as on any listing path, but nothing
    /// becomes a `PartitionedFile`.
    async fn plan_from_listing(
        &self,
        state: &dyn Session,
        object_store_url: &ObjectStoreUrl,
        file_source: Arc<dyn FileSource>,
        projected_schema: SchemaRef,
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

        // Statistics prune here too. The registry may know these files' column
        // ranges even when it cannot supply the file list — a store discovered
        // but not opted into registry listing, or a prefix discovery has seen
        // while the switch is off — and dropping a file that provably cannot
        // match is worth having on every path, not only the fast one.
        let considered = objects.len();
        let (pruned, columns_used) = self.prune_listed(state, &mut objects, filters).await;

        if objects.is_empty() {
            return Ok(Arc::new(EmptyExec::new(project_schema(
                &self.schema,
                projection,
            )?)));
        }

        let ranges =
            byte_balanced_ranges(objects.iter().map(|meta| meta.size), self.target_partitions);
        self.build_plan(
            state,
            object_store_url,
            file_source,
            None,
            Identities::Listed {
                objects: Arc::new(objects),
                ranges: Arc::new(ranges),
            },
            (Precision::Absent, Precision::Absent),
            limit,
            considered,
            pruned,
            columns_used,
        )
        .await
    }

    /// Drop the listed objects whose recorded ranges say they cannot match.
    ///
    /// Returns how many were dropped and how many predicate columns had
    /// statistics. A path the registry has never seen has none, so it stays:
    /// a partially backfilled store must not lose files.
    async fn prune_listed(
        &self,
        state: &dyn Session,
        objects: &mut Vec<ObjectMeta>,
        filters: &[Expr],
    ) -> (usize, usize) {
        if filters.is_empty() {
            return (0, 0);
        }
        let Some(store) = beacon_file_stats::try_file_stats_from_session(state) else {
            return (0, 0);
        };

        let paths: Vec<String> = objects
            .iter()
            .map(|meta| meta.location.to_string())
            .collect();
        let borrowed: Vec<&str> = paths.iter().map(String::as_str).collect();
        let Ok(ids) = store.registry().file_ids(&borrowed) else {
            return (0, 0);
        };
        let mut candidates: Vec<FileId> = ids.iter().filter_map(|id| *id).collect();
        if candidates.is_empty() {
            return (0, 0); // nothing here is registered, so nothing is prunable
        }
        candidates.sort_unstable();
        candidates.dedup();

        let Some(predicate) = physical_predicate(state, &self.schema, filters) else {
            return (0, 0);
        };
        let range = (candidates[0], candidates[candidates.len() - 1]);
        let columns_used = beacon_file_stats::pruning::columns_with_statistics(
            &store,
            &predicate,
            &self.schema,
            range,
        )
        .await;
        let kept: std::collections::HashSet<FileId> =
            beacon_file_stats::prune_files(&store, &predicate, &self.schema, &candidates)
                .await
                .into_iter()
                .collect();

        let before = objects.len();
        let mut position = 0usize;
        objects.retain(|_| {
            let keep = match ids[position] {
                Some(id) => kept.contains(&id),
                // Never seen by the registry: no statistics, so it stays.
                None => true,
            };
            position += 1;
            keep
        });
        (before - objects.len(), columns_used)
    }
}

/// The `DataSourceExec` at the bottom of a single-child chain.
fn find_scan(plan: &Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>> {
    let mut node = Arc::clone(plan);
    loop {
        if node.as_any().is::<DataSourceExec>() {
            return Some(node);
        }
        let children = node.children();
        let [child] = children[..] else { return None };
        node = Arc::clone(child);
    }
}

/// Whether the operator opted into registry-planned scans.
fn registry_listing_enabled(state: &dyn Session) -> bool {
    state
        .config()
        .get_extension::<RegistryListingSwitch>()
        .is_some_and(|switch| switch.enable)
}

/// The counters are known at plan time; the file source's metrics set is
/// shared through an `Arc`, so registering here surfaces them under the scan
/// node in `EXPLAIN ANALYZE`. With no file list in the plan, these are the
/// evidence of what the scan considered.
fn record_counters(
    source: &FastObjectDataSource,
    considered: usize,
    pruned: usize,
    columns_used: usize,
) {
    let metrics =
        datafusion::datasource::source::DataSource::metrics(source);
    MetricBuilder::new(&metrics)
        .global_counter("file_stats_files_considered")
        .add(considered);
    MetricBuilder::new(&metrics)
        .global_counter("file_stats_files_pruned")
        .add(pruned);
    MetricBuilder::new(&metrics)
        .global_counter("file_stats_columns_used")
        .add(columns_used);
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
fn knows_any(snapshot: &RegistrySnapshot, columns: &[String]) -> bool {
    let names: Vec<&str> = columns.iter().map(String::as_str).collect();
    snapshot.knows_any_column(&names).unwrap_or(false)
}

/// The candidate ids `filters` leave alive, ascending. `None` when pruning
/// cannot apply, which keeps every file.
async fn prune(
    state: &dyn Session,
    store: &FileStatsStore,
    schema: &SchemaRef,
    filters: &[Expr],
    candidates: &[(FileId, u64)],
) -> Option<Vec<FileId>> {
    let predicate = physical_predicate(state, schema, filters)?;

    let mut ids: Vec<FileId> = candidates.iter().map(|(id, _)| *id).collect();
    ids.sort_unstable();
    ids.dedup();
    // Ascending in, ascending out, so the caller may binary-search it.
    Some(beacon_file_stats::prune_files(store, &predicate, schema, &ids).await)
}

/// Aggregate statistics for the survivors, read from their records.
///
/// Only the pruned path calls this: it has already enumerated, so reading the
/// recorded counts costs one batch fetch rather than a new pass. Sums are
/// exact only while every survivor's counts are trustworthy — an `Analyzed`
/// record — because one unknown makes the total unknowable.
fn summarize(
    snapshot: &RegistrySnapshot,
    survivors: &[(FileId, u64)],
) -> (Precision<usize>, Precision<usize>) {
    let ids: Vec<FileId> = survivors.iter().map(|(id, _)| *id).collect();
    let Ok(records) = snapshot.records_for_ids(&ids) else {
        return (Precision::Absent, Precision::Absent);
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
    (
        rows.map_or(Precision::Absent, Precision::Exact),
        bytes.map_or(Precision::Absent, Precision::Exact),
    )
}

/// Divide `sizes` into at most `target` contiguous ranges of roughly equal
/// bytes.
///
/// By bytes rather than by count, because a count split lets one partition
/// draw every large file — the straggler DataFusion's `FileGroupPartitioner`
/// exists to prevent. Sizeless input falls back to an even count split.
fn byte_balanced_ranges(
    sizes: impl Iterator<Item = u64> + Clone,
    target: usize,
) -> Vec<Range<usize>> {
    let target = target.max(1);
    let count = sizes.clone().count();
    if count == 0 {
        return Vec::new();
    }
    let total: u64 = sizes.clone().sum();
    if total == 0 {
        let per_group = count.div_ceil(target);
        return (0..count)
            .step_by(per_group)
            .map(|start| start..(start + per_group).min(count))
            .collect();
    }

    // Close a range once it holds its share of the bytes. Every closed range
    // has at least `share` bytes, so at most `target` ranges form.
    let share = total.div_ceil(target as u64);
    let mut ranges = Vec::with_capacity(target);
    let mut start = 0usize;
    let mut run = 0u64;
    for (index, size) in sizes.enumerate() {
        run += size;
        if run >= share {
            ranges.push(start..index + 1);
            start = index + 1;
            run = 0;
        }
    }
    if start < count {
        ranges.push(start..count);
    }
    ranges
}

/// The registry's objects under one URL, when it knows any.
fn objects_under(
    snapshot: &RegistrySnapshot,
    url: &ListingTableUrl,
    extension: &str,
    ignore_subdirectory: bool,
) -> Option<Vec<ObjectMeta>> {
    let prefix = url.prefix().as_ref().to_string();
    let mut objects = Vec::new();
    if url.is_collection() {
        let shards = snapshot.shard_prefix(&prefix, 1).ok()?;
        for query in shards.shards {
            snapshot
                .for_each_in_shard(&prefix, &query, |_, record| {
                    if record.size > 0
                        && record.path.ends_with(extension)
                        && let Ok(location) = object_store::path::Path::parse(&record.path)
                        && url.contains(&location, ignore_subdirectory)
                    {
                        objects.push(ObjectMeta {
                            location,
                            last_modified: chrono::DateTime::from_timestamp_millis(
                                record.last_modified_millis,
                            )
                            .unwrap_or_else(chrono::Utc::now),
                            size: record.size,
                            e_tag: record.e_tag.clone(),
                            version: None,
                        });
                    }
                    true
                })
                .ok()?;
        }
    } else {
        let (_, record) = snapshot.record_by_path(&prefix).ok()??;
        let location = object_store::path::Path::parse(&record.path).ok()?;
        objects.push(ObjectMeta {
            location,
            last_modified: chrono::DateTime::from_timestamp_millis(record.last_modified_millis)
                .unwrap_or_else(chrono::Utc::now),
            size: record.size,
            e_tag: record.e_tag.clone(),
            version: None,
        });
    }
    Some(objects)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ranges(sizes: &[u64], target: usize) -> Vec<Range<usize>> {
        byte_balanced_ranges(sizes.iter().copied(), target)
    }

    /// The point of balancing on bytes: one large file must not drag every
    /// small one into its partition.
    #[test]
    fn balancing_isolates_the_large_file() {
        assert_eq!(ranges(&[1000, 10, 10, 10], 4), vec![0..1, 1..4]);
    }

    /// Ranges stay contiguous, cover every file once, and never exceed the
    /// target.
    #[test]
    fn balancing_respects_the_partition_target() {
        let sizes = vec![100u64; 10];
        let ranges = ranges(&sizes, 4);
        assert!(ranges.len() <= 4, "got {} ranges", ranges.len());
        assert_eq!(ranges.iter().map(|r| r.len()).sum::<usize>(), 10);
        for pair in ranges.windows(2) {
            assert_eq!(pair[0].end, pair[1].start, "ranges must be contiguous");
        }
    }

    /// Sizeless input falls back to a count split rather than one giant range.
    #[test]
    fn balancing_zero_bytes_falls_back_to_count() {
        let ranges = ranges(&[0; 8], 4);
        assert_eq!(ranges.len(), 4);
        assert!(ranges.iter().all(|r| r.len() == 2));
    }

    #[test]
    fn an_empty_input_yields_no_ranges() {
        assert!(ranges(&[], 4).is_empty());
    }
}
