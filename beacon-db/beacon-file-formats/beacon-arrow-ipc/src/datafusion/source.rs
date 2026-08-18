//! [`FileSource`] for Arrow IPC, adapting each file to the merged schema.
//!
//! DataFusion's own `ArrowSource` reads a file with the file's own schema and
//! passes the batches on unchanged. That holds for a collection whose files
//! agree. It fails for one whose files do not: the scan reports the merged
//! schema and then produces a batch that does not carry it.
//!
//! This source keeps every other decision of `ArrowSource` and adds one step.
//! It wraps the reader in [`AdaptingOpener`], which maps a file's columns onto
//! the schema the scan reports.

use std::any::Any;
use std::sync::Arc;

use beacon_datafusion_ext::scan_adapt::AdaptingOpener;
use datafusion::datasource::physical_plan::{
    ArrowOpener, FileGroupPartitioner, FileOpener, FileScanConfig, FileSource,
};
use datafusion::datasource::table_schema::TableSchema;
use datafusion::physical_expr::LexOrdering;
use datafusion::physical_expr::projection::ProjectionExprs;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion_datasource::projection::{ProjectionOpener, SplitProjection};
use object_store::ObjectStore;

/// Which Arrow IPC container a collection holds.
///
/// The file container carries a footer, so a reader can address its record
/// batches and a scan can split one file across partitions. The stream
/// container carries none, so it reads from the front, once.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IpcContainer {
    /// The IPC *file* container: magic, then blocks, then a footer.
    File,
    /// The IPC *stream* container: a schema message, then messages.
    Stream,
}

/// A [`FileSource`] that reads Arrow IPC and produces the merged schema.
#[derive(Debug, Clone)]
pub struct BeaconArrowSource {
    container: IpcContainer,
    /// The table schema: the file schema, plus the partition columns.
    table_schema: TableSchema,
    /// The projection the scan pushed down, split into the file columns to read
    /// and a remainder applied on top of them.
    ///
    /// A `FileSource` that accepts a projection must apply it in full, so this
    /// source selects plain columns and leaves everything else — aliases,
    /// computed expressions, partition columns — to [`ProjectionOpener`].
    projection: SplitProjection,
    metrics: ExecutionPlanMetricsSet,
}

impl BeaconArrowSource {
    /// A source over `table_schema` for `container`.
    pub fn new(container: IpcContainer, table_schema: TableSchema) -> Self {
        Self {
            container,
            projection: SplitProjection::unprojected(&table_schema),
            table_schema,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    /// The container this source reads.
    pub fn container(&self) -> IpcContainer {
        self.container
    }

    /// A copy of this source that carries `projection`.
    ///
    /// The format rebuilds the source in `create_physical_plan`, once it knows
    /// which container the collection holds. A projection the optimizer already
    /// pushed down has to survive that rebuild.
    pub fn with_projection(mut self, projection: Option<&ProjectionExprs>) -> Self {
        self.projection = match projection {
            Some(projection) => SplitProjection::new(self.table_schema.file_schema(), projection),
            None => SplitProjection::unprojected(&self.table_schema),
        };
        self
    }
}

impl FileSource for BeaconArrowSource {
    fn create_file_opener(
        &self,
        object_store: Arc<dyn ObjectStore>,
        _base_config: &FileScanConfig,
        _partition: usize,
    ) -> datafusion::error::Result<Arc<dyn FileOpener>> {
        let file_schema = self.table_schema.file_schema();
        // The columns the scan reads, in table order. `ProjectionOpener` derives
        // its input schema the same way, so the two always agree.
        let read_schema = Arc::new(file_schema.project(&self.projection.file_indices)?);

        // The reader selects no column of its own. An index here addresses the
        // physical schema of the file, and `file_indices` addresses the merged
        // schema of the table; the two agree only where the files do. The
        // adapter selects by name instead, which holds for every file.
        //
        // The cost of that is small. An IPC reader reads a record batch block
        // whole, whichever columns it then builds, so a column it skips saves no
        // read. It saves one array, and an IPC array is a view on bytes the
        // reader already holds.
        let inner: Arc<dyn FileOpener> = match self.container {
            IpcContainer::File => Arc::new(ArrowOpener::new_file_opener(object_store, None)),
            IpcContainer::Stream => {
                Arc::new(ArrowOpener::new_stream_file_opener(object_store, None))
            }
        };

        let adapting = AdaptingOpener::wrap(inner, read_schema);
        ProjectionOpener::try_new(self.projection.clone(), adapting, file_schema)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_schema(&self) -> &TableSchema {
        &self.table_schema
    }

    fn with_batch_size(&self, _batch_size: usize) -> Arc<dyn FileSource> {
        // An IPC file states its own batches. A reader hands them on as they
        // are, so a batch size has no effect here.
        Arc::new(self.clone())
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.metrics
    }

    fn file_type(&self) -> &str {
        match self.container {
            IpcContainer::File => "arrow",
            IpcContainer::Stream => "arrow_stream",
        }
    }

    /// Split the files across partitions, where the container allows it.
    ///
    /// The stream container carries no footer. A reader therefore cannot address
    /// a record batch without reading every byte before it, so a split buys
    /// nothing and this source keeps one partition.
    fn repartitioned(
        &self,
        target_partitions: usize,
        repartition_file_min_size: usize,
        output_ordering: Option<LexOrdering>,
        config: &FileScanConfig,
    ) -> datafusion::error::Result<Option<FileScanConfig>> {
        if self.container == IpcContainer::Stream || config.file_compression_type.is_compressed() {
            return Ok(None);
        }

        let repartitioned = FileGroupPartitioner::new()
            .with_target_partitions(target_partitions)
            .with_repartition_file_min_size(repartition_file_min_size)
            .with_preserve_order_within_groups(output_ordering.is_some())
            .repartition_file_groups(&config.file_groups);

        Ok(repartitioned.map(|file_groups| {
            let mut config = config.clone();
            config.file_groups = file_groups;
            config
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
}
