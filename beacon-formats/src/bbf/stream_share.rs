use std::{future::Future, sync::Arc};

use arrow::datatypes::SchemaRef;
use beacon_binary_format::reader::async_reader::stream::AsyncStreamProducer;
use datafusion::datasource::schema_adapter::SchemaMapper;
use nd_arrow_array::batch::NdRecordBatch;

#[derive(Debug)]
pub struct StreamShare {
    inner: tokio::sync::OnceCell<(
        AsyncStreamProducer<NdRecordBatch>,
        Arc<dyn SchemaMapper>,
        SchemaRef,
    )>,
}

impl StreamShare {
    pub fn new() -> Self {
        Self {
            inner: tokio::sync::OnceCell::new(),
        }
    }

    pub fn get_or_try_init<F, Fut>(
        &self,
        f: F,
    ) -> impl Future<
        Output = Result<
            &(
                AsyncStreamProducer<NdRecordBatch>,
                Arc<dyn SchemaMapper>,
                SchemaRef,
            ),
            datafusion::error::DataFusionError,
        >,
    >
    where
        F: FnOnce() -> Fut,
        Fut: Future<
            Output = Result<
                (
                    AsyncStreamProducer<NdRecordBatch>,
                    Arc<dyn SchemaMapper>,
                    SchemaRef,
                ),
                datafusion::error::DataFusionError,
            >,
        >,
    {
        self.inner.get_or_try_init(f)
    }
}

impl Default for StreamShare {
    fn default() -> Self {
        Self::new()
    }
}
