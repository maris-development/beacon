use std::future::Future;

use beacon_binary_format::reader::async_reader::stream::AsyncStreamProducer;
use nd_arrow_array::batch::NdRecordBatch;

/// One reader stream per file, shared by every partition that opens the file.
#[derive(Debug)]
pub struct StreamShare {
    inner: tokio::sync::OnceCell<AsyncStreamProducer<NdRecordBatch>>,
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
        Output = Result<&AsyncStreamProducer<NdRecordBatch>, datafusion::error::DataFusionError>,
    >
    where
        F: FnOnce() -> Fut,
        Fut: Future<
            Output = Result<AsyncStreamProducer<NdRecordBatch>, datafusion::error::DataFusionError>,
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
