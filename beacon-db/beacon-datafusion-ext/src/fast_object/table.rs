//! [`FastObjectTable`]: the `TableProvider`.
//!
//! Owns the four things such a table needs (a format, its URLs, a schema, a
//! partition target) and answers `scan` by listing the store once. See the
//! [module docs](super) for why it does not wrap a `ListingTable`.

use std::any::Any;
use std::sync::Arc;

use beacon_file_stats::FileStatsStore;
use datafusion::{
    arrow::datatypes::SchemaRef,
    catalog::{Session, TableProvider},
    common::{DFSchema, Statistics, plan_datafusion_err, project_schema},
    datasource::{
        TableType,
        file_format::FileFormat,
        listing::ListingTableUrl,
        physical_plan::{FileScanConfig, FileScanConfigBuilder, FileSource},
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

use super::data_source::{FastObjectDataSource, projected_schema_of};
use super::plan::StreamPruning;
use crate::type_widening::ArrowTypeWidening;

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
        let widening = state.config().get_extension::<ArrowTypeWidening>().expect(
            "ArrowTypeWidening extension missing from session config; this is a bug in Beacon",
        );
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
    /// The object that stands for a whole store, when this format is
    /// directory-oriented.
    ///
    /// Told apart by the shape of the extension: a marker format names a file
    /// (`zarr.json`, `atlas.json`), a file format names a suffix (`parquet`,
    /// `txt`, `bbf`). Nothing else in the trait distinguishes them.
    fn store_marker(&self) -> Option<String> {
        let ext = self.format.get_ext();
        ext.contains('.').then_some(ext)
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
    async fn build_plan(
        &self,
        state: &dyn Session,
        object_store_url: &ObjectStoreUrl,
        file_source: Arc<dyn FileSource>,
        objects: Arc<Vec<ObjectMeta>>,
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
            self.target_partitions,
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

        // A listing is not ordered across pages. Sorting is what lets a limited
        // scan hand out files in a reproducible order, so `LIMIT` without an
        // `ORDER BY` returns the same rows run to run.
        objects.sort_by(|a, b| a.location.cmp(&b.location));

        // A directory-oriented format is handed one object per store, not the
        // store's contents. See `outermost_markers`.
        if let Some(marker) = self.store_marker() {
            objects = outermost_markers(objects, &marker);
            if objects.is_empty() {
                return Ok(Arc::new(EmptyExec::new(project_schema(
                    &self.schema,
                    projection,
                )?)));
            }
        }

        // The registry may know these files' column ranges even when it
        // cannot supply the file list — a store discovered but not opted into
        // registry listing. Pruning on them is worth having here too, and it
        // runs inside the scan for the same reason it does there: the planner
        // should not block on segment reads.
        let pruning = beacon_file_stats::try_file_stats_from_session(state)
            .and_then(|store| self.stream_pruning(state, &store, filters));

        self.build_plan(
            state,
            object_store_url,
            file_source,
            Arc::new(objects),
            pruning,
            limit,
        )
        .await
    }
}

/// Reduce a listing to the outermost marker object of each store.
///
/// Zarr and Atlas call a store a directory but never open one: the reader takes
/// the marker at its root and resolves the store from there. A listing reports
/// everything under that directory — the arrays' own markers, every chunk — and
/// all of it would be rejected by the reader, which is what
/// `create_physical_plan` used to strip before it was handed an empty config.
///
/// A marker nested inside a store already taken belongs to that store, so it is
/// dropped. Markers are considered shallowest first, so a store's own marker
/// always decides before its arrays'.
///
/// A listing with no marker at all is returned untouched, so the format reports
/// its own "no metadata found" rather than this silently scanning nothing.
fn outermost_markers(objects: Vec<ObjectMeta>, marker: &str) -> Vec<ObjectMeta> {
    let mut markers: Vec<ObjectMeta> = objects
        .iter()
        .filter(|meta| meta.location.filename() == Some(marker))
        .cloned()
        .collect();
    if markers.is_empty() {
        return objects;
    }
    markers.sort_by_key(|meta| meta.location.parts().count());

    let mut roots: Vec<Vec<String>> = Vec::new();
    let mut kept: Vec<ObjectMeta> = Vec::with_capacity(markers.len());
    for meta in markers {
        let mut store: Vec<String> = meta
            .location
            .parts()
            .map(|part| part.as_ref().to_string())
            .collect();
        store.pop(); // the marker itself
        if roots.iter().any(|root| store.starts_with(root)) {
            continue; // inside a store already taken
        }
        roots.push(store);
        kept.push(meta);
    }
    kept.sort_by(|a, b| a.location.cmp(&b.location));
    kept
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

#[cfg(test)]
mod tests {
    use super::*;

    fn meta(path: &str) -> ObjectMeta {
        ObjectMeta {
            location: object_store::path::Path::from(path),
            last_modified: chrono::Utc::now(),
            size: 1,
            e_tag: None,
            version: None,
        }
    }

    fn paths(objects: Vec<ObjectMeta>) -> Vec<String> {
        objects
            .into_iter()
            .map(|meta| meta.location.to_string())
            .collect()
    }

    /// A store is one object to the reader: its root marker. Everything else it
    /// contains — the arrays' own markers, every chunk — would be rejected.
    #[test]
    fn a_store_reduces_to_its_root_marker() {
        let listing = vec![
            meta("sst/north.zarr/zarr.json"),
            meta("sst/north.zarr/lat/zarr.json"),
            meta("sst/north.zarr/lat/c/0"),
            meta("sst/south.zarr/zarr.json"),
            meta("sst/south.zarr/lat/zarr.json"),
            meta("sst/south.zarr/lat/c/0"),
        ];
        assert_eq!(
            paths(outermost_markers(listing, "zarr.json")),
            vec!["sst/north.zarr/zarr.json", "sst/south.zarr/zarr.json"],
            "one marker per store, and nothing from inside one"
        );
    }

    /// A nested marker sorts before its store's own by path, so depth decides
    /// rather than order.
    #[test]
    fn depth_decides_which_marker_wins() {
        let listing = vec![
            meta("sst/store.zarr/lat/zarr.json"),
            meta("sst/store.zarr/zarr.json"),
        ];
        assert_eq!(
            paths(outermost_markers(listing, "zarr.json")),
            vec!["sst/store.zarr/zarr.json"]
        );
    }

    /// A URL naming the marker itself resolves to that store.
    #[test]
    fn a_marker_named_directly_is_kept() {
        let listing = vec![meta("sst/north.zarr/zarr.json")];
        assert_eq!(
            paths(outermost_markers(listing, "zarr.json")),
            vec!["sst/north.zarr/zarr.json"]
        );
    }

    /// With no marker anywhere the listing passes through, so the format
    /// reports its own error instead of this scanning nothing.
    #[test]
    fn a_listing_without_a_marker_is_untouched() {
        let listing = vec![meta("sst/not-a-store/data.bin")];
        assert_eq!(
            paths(outermost_markers(listing, "zarr.json")),
            vec!["sst/not-a-store/data.bin"]
        );
    }
}
