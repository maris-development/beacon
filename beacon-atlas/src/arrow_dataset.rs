use std::sync::Arc;

use arrow::datatypes::SchemaRef;

pub struct ArrowDataset {
    pub name: String,
    pub schema: SchemaRef,
    pub columns_readers: Arc<[Arc<dyn ColumnReader + Send + Sync>]>,
}
