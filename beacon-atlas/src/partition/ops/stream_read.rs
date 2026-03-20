use beacon_nd_arrow::array::NdArrowArray;
use object_store::ObjectStore;
use std::sync::Arc;

use crate::{
    array::io_cache::IoCache,
    column::ColumnReader,
    consts::DEFAULT_IO_CACHE_BYTES,
    partition::{
        Partition,
        ops::read::{Dataset, init_column_reader, read_dataset_columns_by_index},
    },
};

pub struct PartitionStreamReaderBuilder<S: ObjectStore + Clone> {
    object_store: S,
    partition: Arc<Partition<S>>,
    io_cache: Option<Arc<IoCache>>,
    projection: Option<Vec<usize>>,
    dataset_indexes: Vec<u32>,
}

impl<S: ObjectStore + Clone> PartitionStreamReaderBuilder<S> {
    pub fn new(object_store: S, partition: Arc<Partition<S>>) -> Self {
        Self {
            object_store,
            partition: partition.clone(),
            projection: None,
            dataset_indexes: partition.undeleted_dataset_indexes(),
            io_cache: None,
        }
    }

    pub fn with_projection(mut self, projection: Vec<usize>) -> Self {
        self.projection = Some(projection);
        self
    }

    pub fn with_io_cache(mut self, io_cache: Arc<IoCache>) -> Self {
        self.io_cache = Some(io_cache);
        self
    }

    pub fn with_dataset_indexes(mut self, dataset_indexes: Vec<u32>) -> Self {
        self.dataset_indexes = dataset_indexes;
        self
    }

    pub async fn create_shareable_stream(
        self,
        buffer_size: usize,
    ) -> anyhow::Result<ShareableCollectionStream<S>> {
        // ToDo: Maybe move to array queue?
        let dataset_indexes = crossbeam::queue::SegQueue::new();
        for dataset_index in &self.dataset_indexes {
            dataset_indexes.push(*dataset_index);
        }
        let io_cache = self
            .io_cache
            .unwrap_or_else(|| Arc::new(IoCache::new(DEFAULT_IO_CACHE_BYTES)));

        let mut column_readers: Vec<Arc<ColumnReader<S>>> = Vec::new();
        for column in &self.partition.schema().columns {
            let column_name = column.name.clone();
            let object_store = self.object_store.clone();
            let partition_directory = self.partition.directory().clone();
            let column_reader = init_column_reader(
                object_store,
                &partition_directory,
                &column_name,
                io_cache.clone(),
            )
            .await?;
            column_readers.push(column_reader);
        }

        Ok(ShareableCollectionStream {
            dataset_indexes_queue: Arc::new(dataset_indexes),
            columns_readers: column_readers,
            schema: Arc::new(self.partition.arrow_schema()),
            _buffer_size: buffer_size.max(1),
        })
    }
}

pub type DatasetReadResult = anyhow::Result<Vec<Option<Arc<dyn NdArrowArray>>>>;

#[derive(Clone)]
pub struct ShareableCollectionStream<S: ObjectStore + Clone> {
    dataset_indexes_queue: Arc<crossbeam::queue::SegQueue<u32>>,
    columns_readers: Vec<Arc<ColumnReader<S>>>,
    schema: Arc<arrow::datatypes::Schema>,
    _buffer_size: usize,
}

impl<S: ObjectStore + Clone + 'static> ShareableCollectionStream<S> {
    pub fn stream_ref(&self) -> Box<dyn Iterator<Item = anyhow::Result<Dataset>>> {
        let dataset_indexes_queue = self.dataset_indexes_queue.clone();
        let column_readers = self.columns_readers.clone();
        let schema = self.schema.clone();

        Box::new(std::iter::from_fn(move || {
            let dataset_index = dataset_indexes_queue.pop();
            dataset_index.map(|dataset_index| {
                let columns = read_dataset_columns_by_index(&column_readers, dataset_index)?;
                let dataset = Dataset::new_unaligned("dataset", schema.clone(), columns)?;
                Ok(dataset)
            })
        }))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Float32Array, StringArray};
    use futures::stream;
    use object_store::{ObjectStore, memory::InMemory, path::Path};

    use super::PartitionStreamReaderBuilder;
    use crate::{column::Column, partition::ops::write::PartitionWriter};

    #[tokio::test]
    async fn stream_reads_multiple_datasets_with_missing_partition_columns() -> anyhow::Result<()> {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let partition_path = Path::from("collections/example/partitions/part-00000");
        let mut writer = PartitionWriter::new(store.clone(), partition_path, "part-00000", None)?;

        writer
            .write_dataset_columns(
                "dataset-0",
                stream::iter(vec![Column::new_from_vec(
                    "temperature".to_string(),
                    vec![10.0f32],
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
                    "salinity".to_string(),
                    vec![35.0f32],
                    vec![1],
                    vec!["x".to_string()],
                    None,
                )?]),
            )
            .await?;

        let partition = Arc::new(writer.finish().await?);
        let shareable_stream = PartitionStreamReaderBuilder::new(store, partition)
            .with_dataset_indexes(vec![0, 1])
            .create_shareable_stream(2)
            .await?;

        let mut stream = shareable_stream.stream_ref();

        let dataset_0 = stream.next().expect("first dataset")?;
        assert_eq!(dataset_0.0.schema.fields().len(), 2);
        assert_eq!(dataset_0.0.schema.field(0).name(), "__entry_key");
        assert_eq!(dataset_0.0.schema.field(1).name(), "temperature");

        let entry_0 = dataset_0.0.arrays[0].as_arrow_array_ref().await?;
        let entry_0 = entry_0.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(entry_0.value(0), "dataset-0");

        let temp_0 = dataset_0.0.arrays[1].as_arrow_array_ref().await?;
        let temp_0 = temp_0.as_any().downcast_ref::<Float32Array>().unwrap();
        assert_eq!(temp_0.value(0), 10.0);

        let dataset_1 = stream.next().expect("second dataset")?;
        assert_eq!(dataset_1.0.schema.fields().len(), 2);
        assert_eq!(dataset_1.0.schema.field(0).name(), "__entry_key");
        assert_eq!(dataset_1.0.schema.field(1).name(), "salinity");

        let entry_1 = dataset_1.0.arrays[0].as_arrow_array_ref().await?;
        let entry_1 = entry_1.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(entry_1.value(0), "dataset-1");

        let sal_1 = dataset_1.0.arrays[1].as_arrow_array_ref().await?;
        let sal_1 = sal_1.as_any().downcast_ref::<Float32Array>().unwrap();
        assert_eq!(sal_1.value(0), 35.0);

        assert!(stream.next().is_none());

        Ok(())
    }
}
