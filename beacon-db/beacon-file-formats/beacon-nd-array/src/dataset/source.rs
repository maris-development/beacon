use beacon_datafusion_ext::nd::NdRecordBatch;
use std::{any::Any, sync::Arc};

#[async_trait::async_trait]
pub trait DatasetSource: Send + Sync + std::fmt::Debug {
    fn chunks(&self) -> Vec<Arc<dyn Any + Send + Sync>>;

    /// How many rows `chunk` holds, when that is known without a read.
    ///
    /// A read that projects no column counts rows and reads nothing else.
    /// `None` says the count is not known ahead: read the chunk and count
    /// what comes back.
    fn chunk_rows(&self, _chunk: &Arc<dyn Any + Send + Sync>) -> Option<usize> {
        None
    }

    async fn poll_next(
        &self,
        chunk: Arc<dyn Any + Send + Sync>,
    ) -> anyhow::Result<Option<NdRecordBatch>>;
}
