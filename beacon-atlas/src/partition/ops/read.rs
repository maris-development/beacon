use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use beacon_nd_arrow::{NdRecordBatch, array::NdArrowArray};
use object_store::ObjectStore;
use tokio::sync::OnceCell;

use crate::{
    array::io_cache::IoCache,
    column::ColumnReader,
    consts::DEFAULT_IO_CACHE_BYTES,
    partition::{Partition, column_name_to_path},
};

pub struct ReaderBuilder<S: ObjectStore + Clone> {
    object_store: S,
    partition: Partition,
    column_readers: Vec<OnceCell<Arc<ColumnReader<S>>>>,
    io_cache: Arc<crate::array::io_cache::IoCache>,
}

impl<S: ObjectStore + Clone> ReaderBuilder<S> {
    pub fn new(object_store: S, partition: Partition) -> Self {
        let column_readers = partition
            .schema()
            .columns
            .iter()
            .map(|_| OnceCell::new())
            .collect();

        Self {
            object_store,
            partition,
            column_readers,
            io_cache: Arc::new(crate::array::io_cache::IoCache::new(DEFAULT_IO_CACHE_BYTES)),
        }
    }

    pub fn arrow_schema(&self) -> SchemaRef {
        Arc::new(self.partition.arrow_schema())
    }

    pub async fn dataset(self, dataset_index: usize) -> anyhow::Result<Dataset> {
        let mut columns = Vec::with_capacity(self.partition.schema().columns.len());

        for (column_index, column) in self.partition.schema().columns.iter().enumerate() {
            let reader_cell = self
                .column_readers
                .get(column_index)
                .ok_or_else(|| anyhow::anyhow!("column reader missing at index {column_index}"))?;

            let reader = reader_cell
                .get_or_try_init(|| async {
                    init_column_reader(
                        self.object_store.clone(),
                        self.partition.directory(),
                        &column.name,
                        self.io_cache.clone(),
                    )
                    .await
                })
                .await?;

            let array = reader.read_column_array(dataset_index as u32).transpose()?;

            columns.push(array);
        }

        let dataset = Dataset::new_unaligned("dataset", self.arrow_schema(), columns)?;

        Ok(dataset)
    }
}

pub(crate) async fn init_column_reader<S: ObjectStore + Clone>(
    object_store: S,
    partition_path: &object_store::path::Path,
    column_name: &str,
    io_cache: Arc<IoCache>,
) -> anyhow::Result<Arc<ColumnReader<S>>> {
    let column_reader = ColumnReader::new(
        object_store,
        column_name_to_path(partition_path.clone(), column_name),
        io_cache,
    )
    .await?;
    Ok(Arc::new(column_reader))
}

pub(crate) fn read_dataset_columns_by_index<S: ObjectStore + Clone>(
    readers: &[Arc<ColumnReader<S>>],
    dataset_index: u32,
) -> anyhow::Result<Vec<Option<Arc<dyn NdArrowArray>>>> {
    readers
        .iter()
        .map(|reader| reader.read_column_array(dataset_index).transpose())
        .collect::<Result<Vec<_>, _>>()
}

pub struct Dataset(pub NdRecordBatch);

impl Dataset {
    pub fn new(
        name: &str,
        schema: SchemaRef,
        columns: Vec<Arc<dyn NdArrowArray>>,
    ) -> anyhow::Result<Self> {
        if columns.len() != schema.fields().len() {
            anyhow::bail!(
                "number of columns ({}) does not match number of schema fields ({})",
                columns.len(),
                schema.fields().len()
            );
        }

        Ok(Self(NdRecordBatch::new(name.to_string(), schema, columns)?))
    }

    pub fn new_unaligned(
        name: &str,
        schema: SchemaRef,
        columns: Vec<Option<Arc<dyn NdArrowArray>>>,
    ) -> anyhow::Result<Self> {
        if columns.len() != schema.fields().len() {
            anyhow::bail!(
                "number of columns ({}) does not match number of schema fields ({})",
                columns.len(),
                schema.fields().len()
            );
        }

        let mut arrays = Vec::with_capacity(columns.len());
        let mut schema_fields = Vec::with_capacity(columns.len());

        for (i, field) in schema.fields().iter().enumerate() {
            if let Some(array) = columns.get(i).and_then(|opt| opt.as_ref()) {
                schema_fields.push(arrow::datatypes::Field::new(
                    field.name(),
                    array.data_type().clone(),
                    true,
                ));
                arrays.push(array.clone());
            }
        }

        Ok(Self(NdRecordBatch::new(
            name.to_string(),
            Arc::new(arrow::datatypes::Schema::new(schema_fields)),
            arrays,
        )?))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int32Array, StringArray};
    use futures::stream;
    use object_store::{ObjectStore, memory::InMemory, path::Path};

    use super::ReaderBuilder;
    use crate::{column::Column, partition::ops::write::PartitionWriter};

    #[tokio::test]
    async fn reader_builder_reads_dataset_by_index() -> anyhow::Result<()> {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let partition_path = Path::from("collections/example/partitions/part-00000");
        let mut writer = PartitionWriter::new(store.clone(), partition_path, "part-00000", None)?;

        writer
            .write_dataset_columns(
                "dataset-0",
                stream::iter(vec![Column::new_from_vec(
                    "temperature".to_string(),
                    vec![10i32],
                    vec![1],
                    vec!["x".to_string()],
                    None,
                )?]),
            )
            .await?;
        writer
            .write_dataset_columns(
                "dataset-1",
                stream::iter(vec![Column::new_from_vec(
                    "temperature".to_string(),
                    vec![20i32],
                    vec![1],
                    vec!["x".to_string()],
                    None,
                )?]),
            )
            .await?;

        let partition = writer.finish().await?;
        let schema = ReaderBuilder::new(store.clone(), partition.clone()).arrow_schema();
        assert_eq!(schema.fields().len(), 2);

        let arrays = ReaderBuilder::new(store, partition).dataset(1).await?;
        assert_eq!(arrays.0.arrays.len(), 2);

        let entry_values = arrays.0.arrays[0].as_arrow_array_ref().await?;
        let entry_values = entry_values.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(entry_values.value(0), "dataset-1");

        let temp_values = arrays.0.arrays[1].as_arrow_array_ref().await?;
        let temp_values = temp_values.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(temp_values.value(0), 20);

        Ok(())
    }
}
