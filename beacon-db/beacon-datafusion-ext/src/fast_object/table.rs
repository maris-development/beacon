//! [`FastObjectTable`]: the `TableProvider`.
//!
//! Wraps a `ListingTable` and prunes inside `scan`. See the
//! [module docs](super).

use std::any::Any;
use std::borrow::Cow;
use std::sync::Arc;

use beacon_file_stats::FileStatsStore;
use datafusion::{
    arrow::datatypes::{Schema, SchemaRef},
    catalog::{Session, TableProvider},
    common::{Constraints, DFSchema, Statistics, plan_datafusion_err},
    datasource::{
        TableType,
        file_format::FileFormat,
        listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl},
        physical_plan::FileScanConfig,
        source::DataSourceExec,
    },
    error::DataFusionError,
    execution::SessionState,
    logical_expr::{LogicalPlan, TableProviderFilterPushDown, dml::InsertOp, utils::conjunction},
    physical_expr::utils::collect_columns,
    physical_optimizer::pruning::PruningPredicate,
    physical_plan::{ExecutionPlan, metrics::MetricBuilder},
    prelude::Expr,
};

use super::prune::{Pruning, prune_plan};
use crate::type_widening::{ArrowTypeWidening, ArrowTypeWideningStrategy};

/// A table over objects: a listing table that prunes before it scans.
///
/// `Clone` is as cheap as the listing table's own: a configuration and some
/// `Arc`s, no files touched. `MaterializedView` holds one by value and needs it.
#[derive(Clone, Debug)]
pub struct FastObjectTable {
    inner: ListingTable,
}

impl FastObjectTable {
    /// Build a table over `urls`, inferring and merging their schemas.
    ///
    /// The session decides how a column's diverging types merge. Callers that
    /// need a particular rule regardless of the session use
    /// [`try_new_with_widening`](Self::try_new_with_widening).
    pub async fn try_new(
        state: &SessionState,
        format: Arc<dyn FileFormat>,
        urls: Vec<ListingTableUrl>,
    ) -> Result<Self, DataFusionError> {
        let widening = state.config().get_extension::<ArrowTypeWidening>().expect(
            "ArrowTypeWidening extension missing from session config; this is a bug in Beacon",
        );
        Self::try_new_with_widening(state, format, urls, widening.strategy.as_ref()).await
    }

    /// The same, with the merge rule named rather than taken from the session.
    ///
    /// The JSON query API has always merged its schemas by Beacon's super
    /// typing, which widens a column two files disagree on instead of refusing
    /// it. SQL `read_*` follows whatever the session registered, which defaults
    /// to the stricter union. Both reach this, and say which they want.
    pub async fn try_new_with_widening(
        state: &SessionState,
        format: Arc<dyn FileFormat>,
        urls: Vec<ListingTableUrl>,
        widening: &dyn ArrowTypeWideningStrategy,
    ) -> Result<Self, DataFusionError> {
        let options = ListingOptions::new(format)
            // The format identifies its own files. A suffix here would also
            // have to match a directory-oriented format's marker.
            .with_file_extension("")
            .with_target_partitions(state.config_options().execution.target_partitions)
            .with_collect_stat(false); // We rely on the statistics store, not the listing table, to collect stats.

        let mut schemas = Vec::with_capacity(urls.len());
        for url in &urls {
            tracing::debug!("Infer schema for table/file url: {}", url);
            schemas.push(options.infer_schema(state, url).await?);
        }

        let schema = widening
            .merge_schemas(&schemas)
            .map_err(|e| plan_datafusion_err!("Failed to merge schemas for object table: {}", e))?;

        let config = ListingTableConfig::new_with_multi_paths(urls)
            .with_listing_options(options)
            .with_schema(schema);
        Ok(Self {
            inner: ListingTable::try_new(config)?,
        })
    }

    /// Wrap a listing table the caller has already configured.
    ///
    /// `try_new` covers a `read_*` scan, which knows only a format and some
    /// URLs. A `CREATE EXTERNAL TABLE` knows more — a declared schema, partition
    /// columns, a sort order, constraints, column defaults, a statistics cache —
    /// and builds its own listing table to hold them. This adds pruning to that
    /// one without taking any of it apart.
    pub fn from_listing_table(inner: ListingTable) -> Self {
        Self { inner }
    }

    /// The URLs (including any globs) backing this table.
    ///
    /// Used by query-time authorization to resolve the dataset paths a `read_*`
    /// scan reads.
    pub fn table_paths(&self) -> &[ListingTableUrl] {
        self.inner.table_paths()
    }

    /// The listing table underneath. For diagnostics and tests.
    pub fn inner(&self) -> &ListingTable {
        &self.inner
    }

    /// The same table with its schema narrowed to `projection`.
    ///
    /// The JSON query API names its columns up front, and narrowing the schema
    /// before planning keeps a wide collection from carrying columns nobody
    /// asked for. A name that is not in the schema is ignored, and a projection
    /// that selects nothing leaves the schema alone rather than producing a
    /// table with no columns.
    ///
    /// Rebuilds from the paths and options, so it suits a table built by
    /// [`try_new`](Self::try_new). A table wrapped by
    /// [`from_listing_table`](Self::from_listing_table) would lose the
    /// constraints and column defaults its caller attached.
    pub fn with_pushdown_projection(
        &self,
        projection: Vec<String>,
    ) -> Result<Self, DataFusionError> {
        let mut schema = self.inner.schema();
        if !projection.is_empty() {
            let kept: Vec<_> = schema
                .fields()
                .iter()
                .filter(|field| projection.contains(field.name()))
                .map(|field| field.as_ref().clone())
                .collect();
            if !kept.is_empty() {
                schema = Arc::new(Schema::new(kept));
            }
        }

        let config = ListingTableConfig::new_with_multi_paths(self.inner.table_paths().to_vec())
            .with_listing_options(self.inner.options().clone())
            .with_schema(schema);
        Ok(Self {
            inner: ListingTable::try_new(config)?,
        })
    }
}

#[async_trait::async_trait]
impl TableProvider for FastObjectTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn table_type(&self) -> TableType {
        self.inner.table_type()
    }

    fn constraints(&self) -> Option<&Constraints> {
        self.inner.constraints()
    }

    fn get_table_definition(&self) -> Option<&str> {
        self.inner.get_table_definition()
    }

    fn get_logical_plan(&self) -> Option<Cow<'_, LogicalPlan>> {
        self.inner.get_logical_plan()
    }

    fn get_column_default(&self, column: &str) -> Option<&Expr> {
        self.inner.get_column_default(column)
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        // The listing table plans the scan. Reimplementing it here would mean
        // reimplementing everything it decides on the way: which filters prune
        // partition directories, the ordering a `WITH ORDER` table promises,
        // splitting groups by statistics to keep that ordering, the expression
        // adapter, and the predicate a format pushes into its own reader.
        let plan = self.inner.scan(state, projection, filters, limit).await?;

        // Then drop the files the predicate rules out, from the list the format
        // settled on. It decides that list itself — netCDF and HDF5 stack decode
        // and broadcast nodes over their scan, and Zarr and Atlas expand a store
        // directory into the groups their reader opens — so pruning the listing
        // beforehand would drop a store's analysed root marker, leave its
        // unanalysed children behind, and the format would read one of those as
        // a store.
        let Some(pruning) = self.pruning(state, filters) else {
            return Ok(plan);
        };
        let (plan, counts) = prune_plan(plan, &pruning).await;
        if let Some((considered, dropped)) = counts {
            record_counters(&plan, considered, dropped);
        }
        Ok(plan)
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::error::Result<Vec<TableProviderFilterPushDown>> {
        // The listing table's answer, unchanged. It reports `Exact` for a filter
        // that partition directories alone settle, and `Inexact` otherwise.
        // Pruning here only removes whole files, never rows, so it cannot turn
        // an exact filter into an inexact one.
        self.inner.supports_filters_pushdown(filters)
    }

    fn statistics(&self) -> Option<Statistics> {
        self.inner.statistics()
    }

    /// Writing goes straight to the listing table.
    ///
    /// Pruning is a read-side concern: it decides which existing files a scan
    /// opens, and has nothing to say about where new rows land. The listing
    /// table already knows how its format writes.
    async fn insert_into(
        &self,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        self.inner.insert_into(state, input, insert_op).await
    }
}

impl FastObjectTable {
    /// The pruning this scan will apply, or `None` when there is none worth
    /// applying.
    ///
    /// Compiling a predicate is pure CPU. The check that follows is one index
    /// lookup per predicate column: a predicate naming no column the registry
    /// has ever interned cannot drop a file, and setting up pruning for it
    /// would buy a segment read for nothing.
    fn pruning(&self, state: &dyn Session, filters: &[Expr]) -> Option<Pruning> {
        if filters.is_empty() {
            return None;
        }
        let store = beacon_file_stats::try_file_stats_from_session(state)?;
        let schema = self.schema();
        let columns = predicate_columns(state, &schema, filters)?;
        if !knows_any(&store, &columns) {
            return None;
        }
        Some(Pruning {
            store,
            predicate: physical_predicate(state, &schema, filters)?,
            table_schema: schema,
        })
    }
}

/// Report what pruning did where people already look.
///
/// `DataSourceExec` shares one `ExecutionPlanMetricsSet` with its `FileSource`
/// through an `Arc`, so registering on the built scan surfaces these under that
/// node in `EXPLAIN ANALYZE` — with no extra plan node, and so no risk of
/// blocking a later repartition or limit pushdown.
///
/// The scan is found by descending the single-child chain, because an nd format
/// returns a stack: netCDF, HDF5, Zarr and GeoTIFF hand back decode and
/// broadcast nodes above their scan.
fn record_counters(plan: &Arc<dyn ExecutionPlan>, considered: usize, dropped: usize) {
    let mut node: &dyn ExecutionPlan = plan.as_ref();
    loop {
        if let Some(exec) = node.as_any().downcast_ref::<DataSourceExec>() {
            let Some(config) = exec.data_source().as_any().downcast_ref::<FileScanConfig>() else {
                return;
            };
            let metrics = datafusion::datasource::source::DataSource::metrics(config);
            MetricBuilder::new(&metrics)
                .global_counter("file_stats_files_considered")
                .add(considered);
            MetricBuilder::new(&metrics)
                .global_counter("file_stats_files_pruned")
                .add(dropped);
            return;
        }
        let children = node.children();
        let [child] = children[..] else { return };
        node = child.as_ref();
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
/// cannot drop a file, and reading a segment to discover that would be the
/// entire cost of pruning for none of its benefit.
fn knows_any(store: &FileStatsStore, columns: &[String]) -> bool {
    columns
        .iter()
        .any(|name| matches!(store.registry().column_id(name), Ok(Some(_))))
}
