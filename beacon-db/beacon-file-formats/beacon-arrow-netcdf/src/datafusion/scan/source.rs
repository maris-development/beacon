//! The DataFusion [`FileSource`] of the NetCDF scan.

use std::sync::Arc;

use beacon_nd_array::arrow::metrics::ReadMetrics;
use datafusion::common::plan_err;
use datafusion::config::ConfigOptions;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{FileOpener, FileScanConfig, FileSource};
use datafusion::datasource::table_schema::TableSchema;
use datafusion::error::Result;
use datafusion::physical_expr::projection::ProjectionExprs;
use datafusion::physical_expr::{conjunction, LexOrdering, PhysicalExpr};
use datafusion::physical_plan::filter_pushdown::{FilterPushdownPropagation, PushedDown};
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion_datasource::morsel::Morselizer;
use object_store::ObjectStore;

use super::morsel::{NetcdfMorselizer, ScanContext, SkipFiles};
use super::slices::{slice_files, spread_files};

/// DataFusion [`FileSource`] for NetCDF (`.nc`) files.
///
/// It reads on the morsel API: one morsel per chunk of a file.
#[derive(Debug, Clone)]
pub struct NetcdfSource {
    table_schema: TableSchema,
    metrics: ExecutionPlanMetricsSet,
    read_dimensions: Option<Vec<String>>,
    skip: SkipFiles,
    batch_size: usize,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    /// The projection the scan pushed down, on top of the table schema.
    projection: Option<ProjectionExprs>,
}

impl NetcdfSource {
    pub fn new(read_dimensions: Option<Vec<String>>, table_schema: TableSchema) -> Self {
        Self {
            table_schema,
            metrics: ExecutionPlanMetricsSet::new(),
            read_dimensions,
            skip: SkipFiles::default(),
            batch_size: usize::MAX,
            predicate: None,
            projection: None,
        }
    }

    /// The same source, skipping the files that do not fit `read_dimensions` when `skip`.
    pub fn with_skip_unbroadcastable(mut self, skip: bool) -> Self {
        self.skip.unbroadcastable = skip;
        self
    }

    /// The same source, skipping the files the reader cannot open when `skip`.
    pub fn with_skip_invalid_files(mut self, skip: bool) -> Self {
        self.skip.invalid = skip;
        self
    }

    /// The same source with `projection`. The format keeps a pushed projection this way.
    pub fn with_projection(mut self, projection: Option<ProjectionExprs>) -> Self {
        self.projection = projection;
        self
    }
}

impl FileSource for NetcdfSource {
    fn create_file_opener(
        &self,
        _object_store: Arc<dyn ObjectStore>,
        _base_config: &FileScanConfig,
        _partition: usize,
    ) -> Result<Arc<dyn FileOpener>> {
        datafusion::common::not_impl_err!("NetCDF scans use create_morselizer")
    }

    fn create_morselizer(
        &self,
        object_store: Arc<dyn ObjectStore>,
        base_config: &FileScanConfig,
        partition: usize,
    ) -> Result<Box<dyn Morselizer>> {
        let projected_schema = base_config.projected_schema()?;
        let partition_fields = base_config.table_partition_cols().clone();
        let file_columns = projected_schema
            .fields()
            .iter()
            .filter(|field| !partition_fields.iter().any(|p| p.name() == field.name()))
            .count();
        if file_columns == 0 {
            return plan_err!(
                "a NetCDF scan needs at least one file column; use COUNT(column) in place of COUNT(*)"
            );
        }

        Ok(Box::new(NetcdfMorselizer::new(ScanContext {
            store: object_store,
            projected_schema,
            partition_fields,
            read_dimensions: self.read_dimensions.clone(),
            skip: self.skip,
            batch_size: self.batch_size,
            predicate: self.predicate.clone(),
            metrics: ReadMetrics::new(&self.metrics, partition),
        })))
    }

    fn table_schema(&self) -> &TableSchema {
        &self.table_schema
    }

    fn with_batch_size(&self, batch_size: usize) -> Arc<dyn FileSource> {
        Arc::new(Self {
            batch_size,
            ..self.clone()
        })
    }

    fn supports_repartitioning(&self) -> bool {
        true
    }

    /// Spread the files over `target_partitions` groups, and slice a file when
    /// there are fewer files than partitions. A file is never cut by byte range.
    fn repartitioned(
        &self,
        target_partitions: usize,
        _repartition_file_min_size: usize,
        output_ordering: Option<LexOrdering>,
        config: &FileScanConfig,
    ) -> Result<Option<FileScanConfig>> {
        if output_ordering.is_some() || target_partitions <= 1 {
            return Ok(None);
        }
        let entries: Vec<PartitionedFile> = config
            .file_groups
            .iter()
            .flat_map(|group| group.files().iter().cloned())
            .collect();

        // A scan with a row limit stops early, so slices would only add opens.
        let entries = if config.limit.is_some() {
            entries
        } else {
            slice_files(entries, target_partitions)
        };
        let mut config = config.clone();
        config.file_groups = spread_files(entries, target_partitions);
        Ok(Some(config))
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.metrics
    }

    fn file_type(&self) -> &str {
        "netcdf"
    }

    fn projection(&self) -> Option<&ProjectionExprs> {
        self.projection.as_ref()
    }

    fn try_pushdown_projection(
        &self,
        projection: &ProjectionExprs,
    ) -> Result<Option<Arc<dyn FileSource>>> {
        let merged = match &self.projection {
            Some(existing) => existing.try_merge(projection)?,
            None => projection.clone(),
        };
        Ok(Some(Arc::new(Self {
            projection: Some(merged),
            ..self.clone()
        })))
    }

    fn try_pushdown_filters(
        &self,
        filters: Vec<Arc<dyn PhysicalExpr>>,
        _config: &ConfigOptions,
    ) -> Result<FilterPushdownPropagation<Arc<dyn FileSource>>> {
        let predicate = match self.predicate.clone() {
            Some(existing) => conjunction(std::iter::once(existing).chain(filters.clone())),
            None => conjunction(filters.clone()),
        };
        let source = Self {
            predicate: Some(predicate),
            ..self.clone()
        };
        // The scan only prunes chunks, so the FilterExec above it stays.
        Ok(FilterPushdownPropagation::with_parent_pushdown_result(vec![
            PushedDown::No;
            filters.len()
        ])
        .with_updated_node(Arc::new(source)))
    }
}
