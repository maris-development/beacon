use std::sync::Arc;

use beacon_nd_arrow::array::NdArrowArray;
use futures::StreamExt;
use futures::stream::BoxStream;
use object_store::ObjectStore;
use tokio::sync::OnceCell;

use crate::{
    array::io_cache::IoCache, column::ColumnReader, partition::Partition, schema::AtlasSchema,
};

pub struct PartitionStreamReaderBuilder<S: ObjectStore + Clone> {
    object_store: S,
    partition: Arc<Partition>,
    io_cache: Option<Arc<IoCache>>,
    projection: Option<Vec<usize>>,
    dataset_indexes: Vec<u32>,
}

impl<S: ObjectStore + Clone> PartitionStreamReaderBuilder<S> {
    pub fn new(object_store: S, partition: Arc<Partition>) -> Self {
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

    pub fn stream(self, buffer_size: usize) -> ShareableCollectionStream<S> {
        let (dataset_indexes_tx, dataset_indexes_rx) = flume::unbounded();
        for dataset_index in self.dataset_indexes {
            let _ = dataset_indexes_tx.send(dataset_index);
        }
        drop(dataset_indexes_tx);

        let column_readers = self
            .partition
            .schema()
            .columns
            .iter()
            .map(|_| OnceCell::new())
            .collect::<Vec<_>>();

        let io_cache = self
            .io_cache
            .unwrap_or_else(|| Arc::new(IoCache::new(256 * 1024 * 1024)));

        ShareableCollectionStream {
            partition: self.partition.clone(),
            dataset_indexes_rx,
            columns_readers: column_readers,
            schema: Arc::new(self.partition.schema().clone()),
            io_cache,
            buffer_size: buffer_size.max(1),
        }
    }
}

pub type DatasetReadResult = anyhow::Result<Vec<Option<Arc<dyn NdArrowArray>>>>;
pub type ShareableDatasetStream = BoxStream<'static, DatasetReadResult>;

#[derive(Clone)]
pub struct ShareableCollectionStream<S: ObjectStore + Clone> {
    partition: Arc<Partition>,
    dataset_indexes_rx: flume::Receiver<u32>,
    columns_readers: Vec<Arc<ColumnReader<S>>>,
    schema: Arc<AtlasSchema>,
    buffer_size: usize,
    io_cache: Arc<crate::array::io_cache::IoCache>,
}

impl<S: ObjectStore + Clone + 'static> ShareableCollectionStream<S> {
    pub fn with_io_cache(mut self, io_cache: Arc<crate::array::io_cache::IoCache>) -> Self {
        self.io_cache = io_cache;
        self
    }

    pub fn with_buffer_size(mut self, buffer_size: usize) -> Self {
        self.buffer_size = buffer_size.max(1);
        self
    }

    pub fn buffer_size(&self) -> usize {
        self.buffer_size
    }

    pub fn stream(&self) -> ShareableDatasetStream {
        let partition = self.partition.clone();
        let dataset_indexes_rx = self.dataset_indexes_rx.clone();
        let column_readers = self.columns_readers.clone();
        let schema = self.schema.clone();
        let io_cache = self.io_cache.clone();

        futures::stream::unfold(
            (
                dataset_indexes_rx,
                column_readers,
                schema,
                io_cache,
                partition.directory().clone(),
            ),
            |(dataset_indexes_rx, column_readers, schema, io_cache, partition_path)| async move {
                let dataset_index = dataset_indexes_rx.recv_async().await.ok()?;

                let mut arrays = Vec::new();
                for column_reader_cell in &column_readers {
                    let task = column_reader_cell
                        .read_column_array(dataset_index)
                        .transpose()
                        .unwrap();
                    arrays.push(task);
                }

                Some((
                    Ok(arrays),
                    (
                        dataset_indexes_rx,
                        column_readers,
                        schema,
                        io_cache,
                        partition_path,
                    ),
                ))
            },
        )
        .boxed()
    }
}
