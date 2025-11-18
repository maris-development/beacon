use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use beacon_common::util::async_once::AsyncOnce;
use datafusion::{
    catalog::Session,
    common::{GetExt, Statistics, not_impl_err},
    datasource::{
        file_format::{FileFormat, FileFormatFactory, file_compression_type::FileCompressionType},
        physical_plan::{FileScanConfig, FileSource},
        sink::DataSinkExec,
    },
    physical_expr::LexRequirement,
    physical_plan::{ExecutionPlan, metrics::MetricsSet},
};
use object_store::{ObjectMeta, ObjectStore};

use crate::arrow_stream::{
    sink::ArrowBatchesStreamSink,
    stream::{ArrowBatchesStreamSender, DeferredBatchStream},
};

pub mod sink;
pub mod stream;

#[derive(Debug)]
pub struct ArrowBatchesStreamFormatFactory {
    pub sender: Arc<ArrowBatchesStreamSender>,
}

impl ArrowBatchesStreamFormatFactory {
    pub fn new() -> (Self, DeferredBatchStream) {
        let async_once = Arc::new(AsyncOnce::new());
        let (batch_tx, batch_rx) = flume::bounded(beacon_config::CONFIG.deferred_stream_capacity);

        let sender = ArrowBatchesStreamSender {
            inner_sender: batch_tx,
            schema: async_once.clone(),
        };

        let factory = ArrowBatchesStreamFormatFactory {
            sender: Arc::new(sender),
        };

        let deferred_stream = DeferredBatchStream {
            schema: async_once,
            batches_stream: batch_rx,
        };

        (factory, deferred_stream)
    }
}

impl GetExt for ArrowBatchesStreamFormatFactory {
    fn get_ext(&self) -> String {
        "arrow_stream".to_string()
    }
}

impl FileFormatFactory for ArrowBatchesStreamFormatFactory {
    fn create(
        &self,
        state: &dyn Session,
        format_options: &std::collections::HashMap<String, String>,
    ) -> datafusion::error::Result<Arc<dyn FileFormat>> {
        Ok(self.default())
    }

    fn default(&self) -> Arc<dyn FileFormat> {
        Arc::new(ArrowBatchesStreamFormat {
            sender: self.sender.clone(),
        })
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug)]
pub struct ArrowBatchesStreamFormat {
    pub sender: Arc<ArrowBatchesStreamSender>,
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

    async fn infer_stats(
        &self,
        state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        object: &ObjectMeta,
    ) -> datafusion::error::Result<Statistics> {
        not_impl_err!("ArrowBatchesStreamFormat does not support statistics inference")
    }

    async fn create_physical_plan(
        &self,
        state: &dyn Session,
        conf: FileScanConfig,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        not_impl_err!("ArrowStreamFormat does not support physical plan creation")
    }

    async fn create_writer_physical_plan(
        &self,
        input: Arc<dyn ExecutionPlan>,
        _state: &dyn Session,
        _conf: datafusion::datasource::physical_plan::FileSinkConfig,
        order_requirements: Option<LexRequirement>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let sink = ArrowBatchesStreamSink::new(input.schema(), self.sender.clone()).await?;
        let data_sink_exec = DataSinkExec::new(input, Arc::new(sink), order_requirements);
        Ok(Arc::new(data_sink_exec))
    }

    fn file_source(&self) -> Arc<dyn FileSource> {
        unimplemented!("ArrowStreamFormat does not have a FileSource implementation")
    }
}
