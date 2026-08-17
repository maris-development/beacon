//! [`FileSource`] implementation for reading GeoParquet datasets.

use std::sync::Arc;

use datafusion::{
    config::ConfigOptions,
    datasource::{
        physical_plan::{FileOpener, FileScanConfig, FileSource},
        schema_adapter::SchemaAdapterFactory,
        table_schema::TableSchema,
    },
    physical_expr::{conjunction, projection::ProjectionExprs},
    physical_plan::{
        PhysicalExpr,
        filter_pushdown::{FilterPushdownPropagation, PushedDown},
        metrics::ExecutionPlanMetricsSet,
    },
};
use datafusion_datasource::projection::{ProjectionOpener, SplitProjection};
use object_store::ObjectStore;

use crate::datafusion::{
    bbox::{self, QueryBox},
    opener::GeoParquetOpener,
};

/// A [`FileSource`] that produces [`GeoParquetOpener`]s for the scanned files.
#[derive(Debug, Clone)]
pub struct GeoParquetSource {
    schema_adapter_factory: Option<Arc<dyn SchemaAdapterFactory>>,
    /// The table schema (file schema + partition columns).
    table_schema: TableSchema,
    execution_plan_metrics: ExecutionPlanMetricsSet,
    batch_size: usize,
    /// The projection the scan pushed down, split into the file columns the
    /// Parquet reader selects and a remainder applied on top of them.
    ///
    /// A `FileSource` that accepts a projection must apply it in full, so this
    /// source only reads plain columns and leaves everything else — aliases,
    /// computed expressions, partition columns — to [`ProjectionOpener`].
    projection: SplitProjection,
    /// The box a pushed-down spatial predicate states, when it states one.
    ///
    /// The reader drops a row group whose own box misses it. Nothing else about
    /// the predicate is kept: the box test is necessary and never sufficient, so
    /// the filter itself stays above the scan and decides every surviving row.
    query_box: Option<QueryBox>,
}

impl GeoParquetSource {
    pub fn new(table_schema: TableSchema) -> Self {
        Self {
            schema_adapter_factory: None,
            projection: SplitProjection::unprojected(&table_schema),
            table_schema,
            execution_plan_metrics: ExecutionPlanMetricsSet::new(),
            batch_size: 128 * 1024,
            query_box: None,
        }
    }

    /// Returns a copy of this source carrying the given projection. Used to
    /// preserve a pushed-down projection when the format rebuilds the source
    /// in `create_physical_plan`.
    pub fn with_projection(mut self, projection: Option<ProjectionExprs>) -> Self {
        self.projection = match projection {
            Some(projection) => SplitProjection::new(self.table_schema.file_schema(), &projection),
            None => SplitProjection::unprojected(&self.table_schema),
        };
        self
    }
}

impl FileSource for GeoParquetSource {
    fn create_file_opener(
        &self,
        object_store: Arc<dyn ObjectStore>,
        _base_config: &FileScanConfig,
        _partition: usize,
    ) -> datafusion::error::Result<Arc<dyn FileOpener>> {
        let file_schema = self.table_schema.file_schema();
        // The columns the reader selects, in file order. `ProjectionOpener`
        // derives its input schema the same way, so the two always agree.
        let read_schema = Arc::new(file_schema.project(&self.projection.file_indices)?);

        let opener = Arc::new(GeoParquetOpener::new(
            object_store,
            read_schema,
            self.batch_size,
            self.query_box.clone(),
            &self.execution_plan_metrics,
        )) as Arc<dyn FileOpener>;

        ProjectionOpener::try_new(self.projection.clone(), opener, file_schema)
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
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

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.execution_plan_metrics
    }

    fn file_type(&self) -> &str {
        "geoparquet"
    }

    fn schema_adapter_factory(&self) -> Option<Arc<dyn SchemaAdapterFactory>> {
        self.schema_adapter_factory.clone()
    }

    fn with_schema_adapter_factory(
        &self,
        factory: Arc<dyn SchemaAdapterFactory>,
    ) -> datafusion::error::Result<Arc<dyn FileSource>> {
        Ok(Arc::new(Self {
            schema_adapter_factory: Some(factory),
            ..self.clone()
        }))
    }

    fn projection(&self) -> Option<&ProjectionExprs> {
        Some(&self.projection.source)
    }

    fn try_pushdown_projection(
        &self,
        projection: &ProjectionExprs,
    ) -> datafusion::error::Result<Option<Arc<dyn FileSource>>> {
        let merged = self.projection.source.try_merge(projection)?;
        let source = Self {
            projection: SplitProjection::new(self.table_schema.file_schema(), &merged),
            ..self.clone()
        };
        Ok(Some(Arc::new(source)))
    }

    /// Keep a spatial predicate's bounding box, so the reader can drop a row
    /// group that lies outside it.
    ///
    /// Every filter is reported as `PushedDown::No`. A row group box only says
    /// that a row *may* match, so the exact predicate has to run above the scan
    /// on every row that survives. Reporting otherwise would return rows the
    /// query excludes.
    fn try_pushdown_filters(
        &self,
        filters: Vec<Arc<dyn PhysicalExpr>>,
        _config: &ConfigOptions,
    ) -> datafusion::error::Result<FilterPushdownPropagation<Arc<dyn FileSource>>> {
        let predicate = conjunction(filters.clone());
        let query_box = bbox::query_box(&predicate, self.table_schema.table_schema());

        let source = Self {
            // A second pushdown only narrows: the filters compose as a
            // conjunction, so their boxes intersect. Keeping the wider of the
            // two would still be correct, just slower.
            query_box: match (self.query_box.clone(), query_box) {
                (Some(held), Some(next)) => Some(held.narrowed_by(next)),
                (held, next) => held.or(next),
            },
            ..self.clone()
        };

        Ok(FilterPushdownPropagation::with_parent_pushdown_result(vec![
            PushedDown::No;
            filters.len()
        ])
        .with_updated_node(Arc::new(source)))
    }
}
