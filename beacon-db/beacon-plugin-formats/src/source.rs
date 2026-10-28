//! The DataFusion file source of a plugin format.

use std::sync::Arc;

use beacon_plugin_sdk::file_format::{FormatReader, ScanRequest};
use datafusion::config::ConfigOptions;
use datafusion::datasource::physical_plan::{FileOpener, FileScanConfig, FileSource};
use datafusion::datasource::projection::{ProjectionOpener, SplitProjection};
use datafusion::datasource::table_schema::TableSchema;
use datafusion::error::Result;
use datafusion::physical_expr::projection::ProjectionExprs;
use datafusion::physical_expr::{PhysicalExpr, conjunction};
use datafusion::physical_plan::filter_pushdown::{FilterPushdownPropagation, PushedDown};
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use object_store::ObjectStore;

use crate::opener::{ContextOpener, PluginFileOpener, filters_for};
use beacon_datafusion_ext::scan_adapt::AdaptingOpener;
use beacon_datafusion_ext::type_widening::ArrowTypeWideningStrategy;

/// Reads the files of a flat plugin format: one plugin `open` call per file.
///
/// The batches of the plugin go through [`AdaptingOpener`] (cast, null fill, column
/// order) and `ProjectionOpener` (projection, partition values).
#[derive(Clone)]
pub struct PluginFileSource {
    name: String,
    reader: Arc<dyn FormatReader>,
    table_schema: TableSchema,
    projection: SplitProjection,
    metrics: ExecutionPlanMetricsSet,
    strategy: Arc<dyn ArrowTypeWideningStrategy>,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    batch_size: usize,
}

impl PluginFileSource {
    pub fn new(
        name: String,
        reader: Arc<dyn FormatReader>,
        table_schema: TableSchema,
        strategy: Arc<dyn ArrowTypeWideningStrategy>,
    ) -> Self {
        Self {
            name,
            reader,
            projection: SplitProjection::unprojected(&table_schema),
            table_schema,
            metrics: ExecutionPlanMetricsSet::new(),
            strategy,
            predicate: None,
            batch_size: 8192,
        }
    }

    /// The same source with a projection that the scan pushed down.
    pub fn with_projection(mut self, projection: Option<&ProjectionExprs>) -> Self {
        self.projection = match projection {
            Some(projection) => SplitProjection::new(self.table_schema.file_schema(), projection),
            None => SplitProjection::unprojected(&self.table_schema),
        };
        self
    }
}

impl std::fmt::Debug for PluginFileSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PluginFileSource")
            .field("name", &self.name)
            .field("predicate", &self.predicate)
            .field("batch_size", &self.batch_size)
            .finish()
    }
}

impl FileSource for PluginFileSource {
    fn create_file_opener(
        &self,
        object_store: Arc<dyn ObjectStore>,
        base_config: &FileScanConfig,
        _partition: usize,
    ) -> Result<Arc<dyn FileOpener>> {
        let file_schema = self.table_schema.file_schema();
        let read_schema = Arc::new(file_schema.project(&self.projection.file_indices)?);
        let request = ScanRequest {
            filters: filters_for(self.predicate.as_ref(), &read_schema),
            schema: Arc::clone(&read_schema),
            limit: base_config.limit,
            batch_size: self.batch_size,
        };
        let plugin: Arc<dyn FileOpener> = Arc::new(PluginFileOpener {
            reader: Arc::clone(&self.reader),
            store: object_store,
            request,
        });
        let adapting = AdaptingOpener::wrap(plugin, read_schema, Arc::clone(&self.strategy));
        let projected = ProjectionOpener::try_new(self.projection.clone(), adapting, file_schema)?;
        Ok(Arc::new(ContextOpener {
            inner: projected,
            format: self.name.clone(),
        }))
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
        &self.metrics
    }

    fn file_type(&self) -> &str {
        &self.name
    }

    fn supports_repartitioning(&self) -> bool {
        false
    }

    fn projection(&self) -> Option<&ProjectionExprs> {
        Some(&self.projection.source)
    }

    fn try_pushdown_projection(
        &self,
        projection: &ProjectionExprs,
    ) -> Result<Option<Arc<dyn FileSource>>> {
        let merged = self.projection.source.try_merge(projection)?;
        Ok(Some(Arc::new(Self {
            projection: SplitProjection::new(self.table_schema.file_schema(), &merged),
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
        // Filters are hints for the plugin: Beacon applies them again.
        Ok(FilterPushdownPropagation::with_parent_pushdown_result(vec![
            PushedDown::No;
            filters.len()
        ])
        .with_updated_node(Arc::new(source)))
    }
}
