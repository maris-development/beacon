//! The scan of an nd plugin format: `beacon.nd`-encoded batches.

use std::sync::Arc;

use beacon_datafusion_ext::nd::encoding::{
    decode_nd_record_batch_row, logical_schema, nd_batch_count, nd_value_type,
};
use beacon_plugin_sdk::file_format::{FormatReader, ScanRequest};
use datafusion::arrow::array::{RecordBatch, RecordBatchOptions};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::config::ConfigOptions;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{
    FileOpenFuture, FileOpener, FileScanConfig, FileSource,
};
use datafusion::datasource::projection::{ProjectionOpener, SplitProjection};
use datafusion::datasource::table_schema::TableSchema;
use datafusion::error::Result;
use datafusion::physical_expr::projection::ProjectionExprs;
use datafusion::physical_expr::{PhysicalExpr, conjunction};
use datafusion::physical_plan::filter_pushdown::{FilterPushdownPropagation, PushedDown};
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use futures::{StreamExt, TryStreamExt};
use object_store::ObjectStore;

use crate::nd_partition::NdPartitionOpener;
use crate::opener::{ContextOpener, filters_for};
use beacon_datafusion_ext::scan_adapt::BatchAdapter;
use beacon_datafusion_ext::type_widening::DefaultArrowTypeWidening;

/// Reads the files of an nd plugin format.
///
/// Each column is a `beacon.nd` struct, one row per nd batch. `ProjectionOpener` adds
/// the partition columns, and [`NdPartitionOpener`] makes their values rank-0 structs.
#[derive(Clone)]
pub struct NdPluginSource {
    name: String,
    reader: Arc<dyn FormatReader>,
    table_schema: TableSchema,
    projection: SplitProjection,
    metrics: ExecutionPlanMetricsSet,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    batch_size: usize,
}

impl std::fmt::Debug for NdPluginSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NdPluginSource")
            .field("name", &self.name)
            .field("predicate", &self.predicate)
            .finish()
    }
}

impl NdPluginSource {
    /// `table_schema` holds the encoded file schema and partition columns.
    pub fn new(name: String, reader: Arc<dyn FormatReader>, table_schema: TableSchema) -> Self {
        Self {
            name,
            reader,
            projection: SplitProjection::unprojected(&table_schema),
            table_schema,
            metrics: ExecutionPlanMetricsSet::new(),
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

impl FileSource for NdPluginSource {
    fn create_file_opener(
        &self,
        object_store: Arc<dyn ObjectStore>,
        base_config: &FileScanConfig,
        _partition: usize,
    ) -> Result<Arc<dyn FileOpener>> {
        let file_schema = self.table_schema.file_schema();
        let opener: Arc<dyn FileOpener> = Arc::new(NdPluginOpener {
            reader: Arc::clone(&self.reader),
            store: object_store,
            file_schema: Arc::clone(file_schema),
            projected: Arc::new(file_schema.project(&self.projection.file_indices)?),
            predicate: self.predicate.clone(),
            limit: base_config.limit,
            batch_size: self.batch_size,
        });
        let projected = ProjectionOpener::try_new(self.projection.clone(), opener, file_schema)?;
        let types = self
            .table_schema
            .table_partition_cols()
            .iter()
            .map(|field| nd_value_type(field.data_type()))
            .collect::<Result<_>>()?;
        let partitioned = Arc::new(NdPartitionOpener {
            inner: projected,
            types,
            no_file_columns: self.projection.file_indices.is_empty(),
        });
        Ok(Arc::new(ContextOpener {
            inner: partitioned,
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

/// Opens one file of an nd plugin format, for the file columns of the scan.
struct NdPluginOpener {
    reader: Arc<dyn FormatReader>,
    store: Arc<dyn ObjectStore>,
    /// The encoded file schema of the table.
    file_schema: SchemaRef,
    /// The encoded file columns of the scan.
    projected: SchemaRef,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    limit: Option<usize>,
    batch_size: usize,
}

impl FileOpener for NdPluginOpener {
    fn open(&self, file: PartitionedFile) -> Result<FileOpenFuture> {
        let projected = Arc::clone(&self.projected);
        // A scan of no column (`COUNT(*)`) still needs the row count of each grid, so it
        // reads every column of the file.
        let counting = projected.fields().is_empty();
        let read_target = if counting {
            Arc::clone(&self.file_schema)
        } else {
            Arc::clone(&projected)
        };
        let logical = logical_schema(&read_target)?;
        let request = ScanRequest {
            filters: filters_for(self.predicate.as_ref(), &logical),
            schema: logical,
            limit: self.limit,
            batch_size: self.batch_size,
        };
        let reader = Arc::clone(&self.reader);
        let store = Arc::clone(&self.store);
        let object = file.object_meta;
        Ok(Box::pin(async move {
            let stream = reader.open(store, &object, request).await?;
            let mut adapter: Option<(SchemaRef, BatchAdapter)> = None;
            let batches = stream.map(move |batch| -> Result<Vec<RecordBatch>> {
                let batch = batch?;
                if counting {
                    return count_batches(&batch, &projected);
                }
                let source = batch.schema();
                if adapter.as_ref().is_none_or(|(held, _)| held != &source) {
                    let built = BatchAdapter::try_new(
                        Arc::clone(&projected),
                        &source,
                        &DefaultArrowTypeWidening::new(),
                    )?;
                    adapter = Some((source, built));
                }
                let (_, adapter) = adapter.as_ref().expect("just settled");
                Ok(vec![adapter.adapt(&batch)?])
            });
            Ok(batches
                .map_ok(|batches| futures::stream::iter(batches.into_iter().map(Ok)))
                .try_flatten()
                .boxed())
        }))
    }
}

/// The batches of a scan of no column: one batch of no column per nd batch in `batch`,
/// with the grid row count as its row count.
fn count_batches(batch: &RecordBatch, projected: &SchemaRef) -> Result<Vec<RecordBatch>> {
    let mut out = Vec::new();
    if batch.num_rows() == 0 {
        return Ok(out);
    }
    for row in 0..nd_batch_count(batch) {
        let rows = decode_nd_record_batch_row(batch, row)?.num_rows();
        let options = RecordBatchOptions::new().with_row_count(Some(rows));
        out.push(RecordBatch::try_new_with_options(
            Arc::clone(projected),
            Vec::new(),
            &options,
        )?);
    }
    Ok(out)
}
