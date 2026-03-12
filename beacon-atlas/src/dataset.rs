use std::sync::Arc;

use arrow::array::ArrayRef;
use futures::{StreamExt, stream::BoxStream};
use object_store::ObjectStore;

use crate::column::ColumnReader;

pub struct DatasetView {
    pub id: u32,
    pub name: String,
    pub column_readers: Vec<Arc<ColumnReader<Arc<dyn ObjectStore>>>>,
}

impl DatasetView {
    pub fn new(
        id: u32,
        name: String,
        column_readers: Vec<Arc<ColumnReader<Arc<dyn ObjectStore>>>>,
    ) -> Self {
        Self {
            id,
            name,
            column_readers,
        }
    }

    pub fn as_arrow_batch_stream(
        &self,
    ) -> Option<anyhow::Result<BoxStream<'static, anyhow::Result<arrow::record_batch::RecordBatch>>>>
    {
        todo!()
    }
}
