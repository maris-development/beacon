use std::sync::Arc;

use arrow::{array::RecordBatch, datatypes::SchemaRef};
use datafusion::{
    catalog::Session,
    common::{Statistics, exec_datafusion_err, not_impl_err},
    datasource::{
        file_format::{FileFormat, file_compression_type::FileCompressionType},
        physical_plan::{FileScanConfig, FileSource},
        sink::DataSink,
    },
    execution::{SendableRecordBatchStream, TaskContext},
    physical_expr::LexRequirement,
    physical_plan::{
        DisplayAs, ExecutionPlan, metrics::MetricsSet, stream::RecordBatchStreamAdapter,
    },
};
use object_store::{ObjectMeta, ObjectStore};

pub struct DeferredBatchStream {
    pub schema: tokio::sync::oneshot::Receiver<SchemaRef>,
    pub batches_stream: flume::Receiver<datafusion::error::Result<RecordBatch>>,
}

impl DeferredBatchStream {
    pub async fn into_stream(self) -> datafusion::error::Result<SendableRecordBatchStream> {
        let schema = self
            .schema
            .await
            .map_err(|e| exec_datafusion_err!("Failed to receive schema: {}", e))?;

        let adapter = RecordBatchStreamAdapter::new(schema, self.batches_stream.into_stream());

        Ok(Box::pin(adapter) as SendableRecordBatchStream)
    }
}

#[derive(Debug)]
pub struct ArrowBatchesStreamSender {
    pub schema: SchemaRef,
    pub schema_tx: tokio::sync::oneshot::Sender<SchemaRef>,
    pub inner_sender: flume::Sender<datafusion::error::Result<RecordBatch>>,
}

impl DisplayAs for ArrowBatchesStreamSender {
    fn fmt_as(
        &self,
        t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "ArrowBatchesStreamSender")
    }
}

pub struct ArrowBatchesStreamFormatFactory;

#[derive(Debug)]
pub struct ArrowBatchesStreamFormat {
    pub sender: ArrowBatchesStreamSender,
}

#[async_trait::async_trait]
impl DataSink for ArrowBatchesStreamSender {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    /// Return a snapshot of the [MetricsSet] for this
    /// [DataSink].
    ///
    /// See [ExecutionPlan::metrics()] for more details
    fn metrics(&self) -> Option<MetricsSet> {
        None
    }

    /// Returns the sink schema
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    // TODO add desired input ordering
    // How does this sink want its input ordered?

    /// Writes the data to the sink, returns the number of values written
    ///
    /// This method will be called exactly once during each DML
    /// statement. Thus prior to return, the sink should do any commit
    /// or rollback required.
    async fn write_all(
        &self,
        data: SendableRecordBatchStream,
        context: &Arc<TaskContext>,
    ) -> datafusion::error::Result<u64> {
        todo!()
    }
}

impl ArrowBatchesStreamFormat {
    pub fn new() -> (Self, DeferredBatchStream) {
        let (schema_tx, schema_rx) = tokio::sync::oneshot::channel();
        let (batch_tx, batch_rx) = flume::unbounded();

        let sender = ArrowBatchesStreamSender {
            schema: schema_tx,
            inner_sender: batch_tx,
        };

        let format = ArrowBatchesStreamFormat { sender };

        let deferred_stream = DeferredBatchStream {
            schema: schema_rx,
            batches_stream: batch_rx,
        };

        (format, deferred_stream)
    }
}

#[async_trait::async_trait]
impl FileFormat for ArrowBatchesStreamFormat {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn get_ext(&self) -> String {
        "arrow_stream".to_string()
    }

    fn compression_type(&self) -> Option<FileCompressionType> {
        None
    }

    fn get_ext_with_compression(
        &self,
        _file_compression_type: &FileCompressionType,
    ) -> datafusion::error::Result<String> {
        Ok(self.get_ext())
    }

    async fn infer_schema(
        &self,
        state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> datafusion::error::Result<SchemaRef> {
        not_impl_err!("ArrowBatchesStreamFormat does not support schema inference")
    }

    /// Infer the statistics for the provided object. The cost and accuracy of the
    /// estimated statistics might vary greatly between file formats.
    ///
    /// `table_schema` is the (combined) schema of the overall table
    /// and may be a superset of the schema contained in this file.
    ///
    /// TODO: should the file source return statistics for only columns referred to in the table schema?
    async fn infer_stats(
        &self,
        state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        object: &ObjectMeta,
    ) -> datafusion::error::Result<Statistics> {
        not_impl_err!("ArrowBatchesStreamFormat does not support statistics inference")
    }

    /// Take a list of files and convert it to the appropriate executor
    /// according to this file format.
    async fn create_physical_plan(
        &self,
        state: &dyn Session,
        conf: FileScanConfig,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        not_impl_err!("ArrowStreamFormat does not support physical plan creation")
    }

    /// Take a list of files and the configuration to convert it to the
    /// appropriate writer executor according to this file format.
    async fn create_writer_physical_plan(
        &self,
        input: Arc<dyn ExecutionPlan>,
        _state: &dyn Session,
        _conf: datafusion::datasource::physical_plan::FileSinkConfig,
        _order_requirements: Option<LexRequirement>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        todo!()
    }

    /// Return the related FileSource such as `CsvSource`, `JsonSource`, etc.
    fn file_source(&self) -> Arc<dyn FileSource> {
        unimplemented!("ArrowStreamFormat does not have a FileSource implementation")
    }
}
