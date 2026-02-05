use std::sync::Arc;

use arrow::{array::Scalar, datatypes::SchemaRef};
use futures::{Stream, StreamExt};

use crate::{column::Column, column_reader::ColumnReader};

pub struct ArrowDataset {
    pub name: String,
    pub dataset_index: u32,
    pub schema: SchemaRef,
    pub columns_readers: Arc<[Arc<dyn ColumnReader + Send + Sync>]>,
}

impl ArrowDataset {
    pub fn new(
        name: String,
        dataset_index: u32,
        schema: SchemaRef,
        columns_readers: Arc<[Arc<dyn ColumnReader + Send + Sync>]>,
    ) -> anyhow::Result<Self> {
        // The number of column readers must match the number of columns in the schema.
        if schema.fields().len() != columns_readers.len() {
            return Err(anyhow::anyhow!(
                "number of column readers ({}) does not match number of columns in schema ({})",
                columns_readers.len(),
                schema.fields().len()
            ));
        }

        Ok(Self {
            name,
            dataset_index,
            schema,
            columns_readers,
        })
    }

    pub async fn stream_as_arrow_batches(
        &self,
        batch_size: usize,
    ) -> impl Stream<Item = anyhow::Result<arrow::record_batch::RecordBatch>> + '_ {
        let mut columns = vec![];
        for (idx, reader) in self.columns_readers.iter().enumerate() {
            let column = match reader.read(self.dataset_index).await {
                Ok(Some(column)) => column,
                Ok(None) => {
                    // Create a scalar null column with the same data type as the schema field.
                    let field = self.schema.field(idx);
                    let null_array = arrow::array::new_null_array(field.data_type(), 1);
                    Column::Attribute(Scalar::new(Arc::new(null_array)))
                }
                Err(e) => {
                    return futures::stream::iter(vec![Err(anyhow::anyhow!(
                        "error reading column {}: {e}",
                        idx
                    ))])
                    .boxed();
                }
            };
            columns.push(column);
        }

        todo!()
    }
}
