use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use beacon_nd_arrow::array::NdArrowArray;
use object_store::ObjectStore;
use tokio::sync::OnceCell;

use crate::{
    array::io_cache::{self, IoCache},
    column::{Column, ColumnReader},
    partition::{Partition, column_name_to_path},
};

pub struct ReaderBuilder<S: ObjectStore + Clone> {
    object_store: S,
    dataset_index: Option<u32>,
    dataset_name: Option<String>,
    partition: Partition,
    dataset_arrow_schema: OnceCell<SchemaRef>,
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
            dataset_index: None,
            dataset_name: None,
            dataset_arrow_schema: OnceCell::new(),
            column_readers,
            io_cache: Arc::new(crate::array::io_cache::IoCache::new(256 * 1024 * 1024)),
        }
    }

    pub fn with_dataset_index(mut self, dataset_index: u32) -> Self {
        self.dataset_index = Some(dataset_index);
        self
    }

    pub fn with_dataset_name(mut self, dataset_name: impl Into<String>) -> Self {
        self.dataset_name = Some(dataset_name.into());
        self
    }

    pub async fn dataset_arrow_schema(&self) -> anyhow::Result<SchemaRef> {
        let schema = self
            .dataset_arrow_schema
            .get_or_try_init::<anyhow::Error, _, _>(|| async {
                Ok(Arc::new(self.partition.arrow_schema()))
            })
            .await?;
        Ok(schema.clone())
    }

    pub async fn execute(self) -> anyhow::Result<Vec<Arc<dyn NdArrowArray>>> {
        let dataset_index = self.resolve_dataset_index()?;
        let mut columns = Vec::with_capacity(self.partition.schema().columns.len());

        for (column_index, column) in self.partition.schema().columns.iter().enumerate() {
            let reader_cell = self
                .column_readers
                .get(column_index)
                .ok_or_else(|| anyhow::anyhow!("column reader missing at index {column_index}"))?;

            let reader = reader_cell
                .get_or_try_init(|| async {
                    init_column_readers(
                        self.object_store.clone(),
                        self.partition.directory(),
                        &column.name,
                        self.io_cache.clone(),
                    )
                    .await
                })
                .await?;

            let array = reader
                .read_column_array(dataset_index)
                .transpose()?
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "dataset index {dataset_index} missing column '{}' in partition '{}'",
                        column.name,
                        self.partition.name()
                    )
                })?;
            columns.push(array);
        }

        Ok(columns)
    }

    fn resolve_dataset_index(&self) -> anyhow::Result<u32> {
        match (self.dataset_index, self.dataset_name.as_deref()) {
            (Some(dataset_index), Some(dataset_name)) => {
                let actual_name = self
                    .partition
                    .entry_keys()
                    .get(dataset_index as usize)
                    .ok_or_else(|| {
                        anyhow::anyhow!("dataset index out of bounds: {dataset_index}")
                    })?;
                anyhow::ensure!(
                    actual_name == dataset_name,
                    "dataset selection mismatch: index {dataset_index} resolves to '{}', not '{dataset_name}'",
                    actual_name
                );
                Ok(dataset_index)
            }
            (Some(dataset_index), None) => {
                anyhow::ensure!(
                    (dataset_index as usize) < self.partition.entry_keys().len(),
                    "dataset index out of bounds for partition '{}': {dataset_index}",
                    self.partition.name()
                );
                Ok(dataset_index)
            }
            (None, Some(dataset_name)) => {
                let matches = self
                    .partition
                    .entry_keys()
                    .iter()
                    .enumerate()
                    .filter_map(|(index, name)| (name == dataset_name).then_some(index as u32))
                    .collect::<Vec<_>>();

                match matches.as_slice() {
                    [] => anyhow::bail!(
                        "dataset name not found in partition '{}': {dataset_name}",
                        self.partition.name()
                    ),
                    [dataset_index] => Ok(*dataset_index),
                    _ => anyhow::bail!(
                        "dataset name '{dataset_name}' is ambiguous because it appears multiple times in partition '{}'",
                        self.partition.name()
                    ),
                }
            }
            (None, None) => match self.partition.undeleted_dataset_indexes().as_slice() {
                [dataset_index] => Ok(*dataset_index),
                [] => anyhow::bail!(
                    "partition '{}' has no readable datasets",
                    self.partition.name()
                ),
                _ => anyhow::bail!(
                    "dataset selection is required for partition '{}' because it contains multiple datasets",
                    self.partition.name()
                ),
            },
        }
    }
}

pub(crate) async fn init_column_readers<S: ObjectStore + Clone>(
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int32Array, StringArray};
    use futures::stream;
    use object_store::{ObjectStore, memory::InMemory, path::Path};

    use super::ReaderBuilder;
    use crate::{column::Column, partition::ops::write::PartitionWriter};

    #[tokio::test]
    async fn reader_builder_reads_dataset_by_name() -> anyhow::Result<()> {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let partition_path = Path::from("collections/example/partitions/part-00000");
        let mut writer = PartitionWriter::new(store.clone(), partition_path, "part-00000", None)?;

        writer
            .write_dataset(
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
            .write_dataset(
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
        let schema = ReaderBuilder::new(store.clone(), partition.clone())
            .dataset_arrow_schema()
            .await?;
        assert_eq!(schema.fields().len(), 2);

        let mut arrays = ReaderBuilder::new(store, partition)
            .with_dataset_name("dataset-1")
            .execute()
            .await?;
        assert_eq!(arrays.len(), 2);

        let entry_values = arrays.remove(0).as_arrow_array_ref().await?;
        let entry_values = entry_values.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(entry_values.value(0), "dataset-1");

        let temp_values = arrays.remove(0).as_arrow_array_ref().await?;
        let temp_values = temp_values.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(temp_values.value(0), 20);

        Ok(())
    }
}
