use std::sync::Arc;

use beacon_nd_arrow::{
    dataset::Dataset,
    datatypes::{NdArrayDataType, TimestampNanosecond},
};
use chrono::{DateTime, Utc};
use futures::{Stream, StreamExt};
use object_store::ObjectStore;

use crate::{
    cache::Cache,
    column::{Column, ColumnWriter, writer::ColumnWriterD},
    consts::{ENTRIES_COLUMN_NAME, PARTITION_METADATA_FILE, chunk_size_by_type},
    partition::{
        Partition, PartitionMetadata, PartitionState, column_name_to_path,
        ops::write_partition_entries,
    },
    schema::{AtlasColumn, AtlasSchema},
};

pub struct PartitionWriter<S: ObjectStore + Clone> {
    object_store: S,
    partition_directory: object_store::path::Path,
    column_writers: indexmap::IndexMap<String, Box<dyn ColumnWriterD>>,
    metadata: PartitionMetadata,
    dataset_names: Vec<String>,
    dataset_insert_timestamps: Vec<DateTime<Utc>>,
}

impl<S: object_store::ObjectStore + Clone> PartitionWriter<S> {
    pub fn new(
        object_store: S,
        partition_directory: object_store::path::Path,
        name: &str,
        description: Option<&str>,
    ) -> anyhow::Result<Self> {
        let mut column_writers = indexmap::IndexMap::new();
        column_writers.insert(
            ENTRIES_COLUMN_NAME.to_string(),
            Box::new(ColumnWriter::<String, S>::new(
                object_store.clone(),
                column_name_to_path(partition_directory.clone(), ENTRIES_COLUMN_NAME),
                32 * 1024, // TODO: make this configurable
            )?) as Box<dyn ColumnWriterD>,
        );

        Ok(Self {
            object_store,
            partition_directory,
            column_writers,
            metadata: PartitionMetadata {
                schema: AtlasSchema::empty(),
                description: description.map(|value| value.to_string()),
                name: name.to_string(),
            },
            dataset_names: vec![],
            dataset_insert_timestamps: vec![],
        })
    }

    #[cfg(test)]
    pub(crate) fn column_writer(&self, name: &str) -> Option<&Box<dyn ColumnWriterD>> {
        self.column_writers.get(name)
    }

    pub async fn write_dataset(&mut self, dataset: Dataset) -> anyhow::Result<()> {
        // self.write_dataset_columns(
        //     &dataset.name,
        //     futures::stream::iter(dataset.0.arrays.into_iter().enumerate().map(|(i, array)| {
        //         let column_name = dataset.0.schema.field(i).name().to_string();
        //         Column::new(column_name, array)
        //     })),
        // )
        // .await
        todo!()
    }

    pub async fn write_dataset_columns<C: Stream<Item = Column>>(
        &mut self,
        name: &str,
        columns: C,
    ) -> anyhow::Result<()> {
        let dataset_index = self.dataset_names.len() as u32;
        self.dataset_names.push(name.to_string());
        self.dataset_insert_timestamps.push(Utc::now());

        let entry_name_column = Column::new_from_vec(
            ENTRIES_COLUMN_NAME.to_string(),
            vec![name.to_string()],
            vec![],
            vec![],
            None,
        )?;
        self.write_column(dataset_index, entry_name_column).await?;

        let mut pinned = std::pin::pin!(columns);
        while let Some(column) = pinned.next().await {
            self.write_column(dataset_index, column).await?;
        }

        Ok(())
    }

    async fn write_column(&mut self, dataset_index: u32, column: Column) -> anyhow::Result<()> {
        let column_name = column.name().to_string();
        let array = column.array();
        let data_type = column.datatype();

        let column_writer = match self.column_writers.entry(column_name.clone()) {
            indexmap::map::Entry::Occupied(entry) => entry.into_mut(),
            indexmap::map::Entry::Vacant(entry) => {
                let store = self.object_store.clone();
                let column_path =
                    column_name_to_path(self.partition_directory.clone(), &column_name);
                entry.insert(Self::create_column_writer(&store, column_path, data_type)?)
            }
        };

        column_writer.write_column_array(dataset_index, array).await
    }

    fn create_column_writer(
        store: &S,
        column_path: object_store::path::Path,
        data_type: NdArrayDataType,
    ) -> anyhow::Result<Box<dyn ColumnWriterD>> {
        match data_type {
            NdArrayDataType::Bool => Ok(Box::new(ColumnWriter::<bool, S>::new(
                store.clone(),
                column_path,
                chunk_size_by_type(&data_type),
            )?)),
            NdArrayDataType::I8 => Ok(Box::new(ColumnWriter::<i8, S>::new(
                store.clone(),
                column_path,
                chunk_size_by_type(&data_type),
            )?)),
            NdArrayDataType::I16 => Ok(Box::new(ColumnWriter::<i16, S>::new(
                store.clone(),
                column_path,
                chunk_size_by_type(&data_type),
            )?)),
            NdArrayDataType::I32 => Ok(Box::new(ColumnWriter::<i32, S>::new(
                store.clone(),
                column_path,
                chunk_size_by_type(&data_type),
            )?)),
            NdArrayDataType::I64 => Ok(Box::new(ColumnWriter::<i64, S>::new(
                store.clone(),
                column_path,
                chunk_size_by_type(&data_type),
            )?)),
            NdArrayDataType::U8 => Ok(Box::new(ColumnWriter::<u8, S>::new(
                store.clone(),
                column_path,
                chunk_size_by_type(&data_type),
            )?)),
            NdArrayDataType::U16 => Ok(Box::new(ColumnWriter::<u16, S>::new(
                store.clone(),
                column_path,
                chunk_size_by_type(&data_type),
            )?)),
            NdArrayDataType::U32 => Ok(Box::new(ColumnWriter::<u32, S>::new(
                store.clone(),
                column_path,
                chunk_size_by_type(&data_type),
            )?)),
            NdArrayDataType::U64 => Ok(Box::new(ColumnWriter::<u64, S>::new(
                store.clone(),
                column_path,
                chunk_size_by_type(&data_type),
            )?)),
            NdArrayDataType::F32 => Ok(Box::new(ColumnWriter::<f32, S>::new(
                store.clone(),
                column_path,
                chunk_size_by_type(&data_type),
            )?)),
            NdArrayDataType::F64 => Ok(Box::new(ColumnWriter::<f64, S>::new(
                store.clone(),
                column_path,
                chunk_size_by_type(&data_type),
            )?)),
            NdArrayDataType::Timestamp => {
                Ok(Box::new(ColumnWriter::<TimestampNanosecond, S>::new(
                    store.clone(),
                    column_path,
                    chunk_size_by_type(&data_type),
                )?))
            }
            NdArrayDataType::Binary => Ok(Box::new(ColumnWriter::<Vec<u8>, S>::new(
                store.clone(),
                column_path,
                chunk_size_by_type(&data_type),
            )?)),
            NdArrayDataType::String => Ok(Box::new(ColumnWriter::<String, S>::new(
                store.clone(),
                column_path,
                chunk_size_by_type(&data_type),
            )?)),
        }
    }

    pub async fn finish(mut self, cache: Arc<Cache>) -> anyhow::Result<Partition<S>> {
        self.metadata.schema.columns = self
            .column_writers
            .iter()
            .map(|(name, writer)| AtlasColumn {
                name: name.clone(),
                data_type: writer.data_type().clone(),
            })
            .collect::<Vec<_>>();

        let metadata_path = self.partition_directory.child(PARTITION_METADATA_FILE);
        self.object_store
            .put(&metadata_path, serde_json::to_vec(&self.metadata)?.into())
            .await?;

        for (_, writer) in self.column_writers {
            writer.finish().await?;
        }

        let deletion_flags = vec![false; self.dataset_names.len()];
        let local_indexes = (0..self.dataset_names.len() as u32).collect::<Vec<_>>();
        write_partition_entries(
            &self.object_store,
            &self.partition_directory,
            &self.dataset_names,
            &local_indexes,
            &deletion_flags,
            &self.dataset_insert_timestamps,
        )
        .await?;

        let partiton_state = PartitionState {
            dataset_indexes: local_indexes,
            deletion_flags,
            entry_keys: self.dataset_names.clone(),
            insert_timestamps: self.dataset_insert_timestamps.clone(),
        };

        Ok(Partition::new(
            self.object_store.clone(),
            self.metadata.name.clone(),
            self.partition_directory.clone(),
            self.metadata.clone(),
            partiton_state,
            cache,
        ))
    }
}

// #[cfg(test)]
// mod tests {
//     use std::sync::Arc;

//     use arrow::array::{Int32Array, StringArray};
//     use arrow::datatypes::{DataType, Field, Schema};
//     use futures::stream;
//     use object_store::{ObjectStore, memory::InMemory, path::Path};

//     use super::PartitionWriter;
//     use crate::array::io_cache::IoCache;
//     use crate::consts::DEFAULT_IO_CACHE_BYTES;
//     use crate::{
//         column::Column,
//         partition::{
//             load_partition,
//             ops::read::{Dataset, ReaderBuilder},
//         },
//     };

//     #[tokio::test]
//     async fn write_column_uses_nested_directory_for_dotted_names() -> anyhow::Result<()> {
//         let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
//         let partition_path = Path::from("collections/example/part-00000");
//         let mut writer = PartitionWriter::new(store, partition_path, "part-00000", None)?;
//         let column = Column::new_from_vec(
//             "attributes.color".to_string(),
//             vec!["blue".to_string()],
//             vec![],
//             vec![],
//             None,
//         )?;

//         writer.write_column(0, column).await?;

//         let column_writer = writer.column_writer("attributes.color").unwrap();
//         assert_eq!(
//             column_writer.column_directory(),
//             &Path::from("collections/example/part-00000/columns/attributes/color")
//         );
//         Ok(())
//     }

//     #[tokio::test]
//     async fn write_column_uses_reserved_directory_for_global_attributes() -> anyhow::Result<()> {
//         let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
//         let partition_path = Path::from("collections/example/part-00000");
//         let mut writer = PartitionWriter::new(store, partition_path, "part-00000", None)?;
//         let column = Column::new_from_vec(
//             ".platform".to_string(),
//             vec!["argo".to_string()],
//             vec![],
//             vec![],
//             None,
//         )?;

//         writer.write_column(0, column).await?;

//         let column_writer = writer.column_writer(".platform").unwrap();
//         assert_eq!(
//             column_writer.column_directory(),
//             &Path::from("collections/example/part-00000/columns/__platform")
//         );
//         Ok(())
//     }

//     #[tokio::test]
//     async fn finish_writes_loadable_partition_metadata_and_entries() -> anyhow::Result<()> {
//         let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
//         let partition_path = Path::from("collections/example/partitions/part-00000");
//         let mut writer =
//             PartitionWriter::new(store.clone(), partition_path.clone(), "part-00000", None)?;

//         writer
//             .write_dataset_columns(
//                 "dataset-0",
//                 stream::iter(vec![Column::new_from_vec(
//                     "temperature".to_string(),
//                     vec![10i32],
//                     vec![1],
//                     vec!["x".to_string()],
//                     None,
//                 )?]),
//             )
//             .await?;

//         let io_cache = Arc::new(IoCache::new(DEFAULT_IO_CACHE_BYTES));
//         let partition = writer.finish(io_cache.clone()).await?;
//         let loaded = load_partition(store.clone(), partition_path, io_cache).await?;

//         assert_eq!(partition.name(), "part-00000");
//         assert_eq!(loaded.name(), "part-00000");
//         assert_eq!(loaded.logical_entries(), vec!["dataset-0"]);
//         assert_eq!(loaded.dataset_indexes(), &[0]);
//         assert_eq!(loaded.deletion_flags(), &[false]);
//         assert_eq!(loaded.insert_timestamps().len(), 1);
//         assert_eq!(partition.insert_timestamps(), loaded.insert_timestamps());

//         let schema = loaded.arrow_schema();
//         assert_eq!(schema.fields().len(), 2);
//         assert_eq!(schema.field(0).name(), "__entry_key");
//         assert_eq!(schema.field(1).name(), "temperature");

//         let arrays = ReaderBuilder::new(store, loaded).dataset(0).await?;
//         assert_eq!(arrays.0.arrays.len(), 2);

//         let entry_values = arrays.0.arrays[0].as_arrow_array_ref().await?;
//         let entry_values = entry_values.as_any().downcast_ref::<StringArray>().unwrap();
//         assert_eq!(entry_values.value(0), "dataset-0");

//         let temp_values = arrays.0.arrays[1].as_arrow_array_ref().await?;
//         let temp_values = temp_values.as_any().downcast_ref::<Int32Array>().unwrap();
//         assert_eq!(temp_values.value(0), 10);

//         Ok(())
//     }

//     #[tokio::test]
//     async fn finish_creates_partition_with_name_description_and_directory() -> anyhow::Result<()> {
//         let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
//         let io_cache = Arc::new(IoCache::new(DEFAULT_IO_CACHE_BYTES));
//         let partition_path = Path::from("collections/example/partitions/part-00123");
//         let description = "test partition";

//         let writer = PartitionWriter::new(
//             store.clone(),
//             partition_path.clone(),
//             "part-00123",
//             Some(description),
//         )?;

//         let partition = writer.finish(io_cache.clone()).await?;
//         let loaded = load_partition(store, partition_path.clone(), io_cache).await?;

//         assert_eq!(partition.name(), "part-00123");
//         assert_eq!(partition.directory(), &partition_path);
//         assert_eq!(partition.metadata().name, "part-00123");
//         assert_eq!(
//             partition.metadata().description.as_deref(),
//             Some(description)
//         );

//         assert_eq!(loaded.name(), "part-00123");
//         assert_eq!(loaded.directory(), &partition_path);
//         assert_eq!(loaded.metadata().name, "part-00123");
//         assert_eq!(loaded.metadata().description.as_deref(), Some(description));
//         assert_eq!(loaded.logical_entries(), Vec::<&str>::new());
//         assert_eq!(loaded.dataset_indexes(), &[] as &[u32]);
//         assert_eq!(loaded.deletion_flags(), &[] as &[bool]);
//         assert_eq!(
//             loaded.insert_timestamps(),
//             &[] as &[chrono::DateTime<chrono::Utc>]
//         );

//         Ok(())
//     }

//     #[tokio::test]
//     async fn write_dataset_uses_dataset_name_and_assigns_sequential_indexes() -> anyhow::Result<()>
//     {
//         let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
//         let io_cache = Arc::new(IoCache::new(DEFAULT_IO_CACHE_BYTES));
//         let partition_path = Path::from("collections/example/partitions/part-00042");
//         let mut writer =
//             PartitionWriter::new(store.clone(), partition_path.clone(), "part-00042", None)?;

//         let temperature_0 = Column::new_from_vec(
//             "temperature".to_string(),
//             vec![10i32],
//             vec![1],
//             vec!["x".to_string()],
//             None,
//         )?;
//         let schema_0 = Arc::new(Schema::new(vec![Field::new(
//             "temperature",
//             DataType::Int32,
//             true,
//         )]));
//         let dataset_0 = Dataset::new(
//             "dataset-a",
//             schema_0,
//             vec![temperature_0.array() as Arc<dyn NdArrowArray>],
//         )?;

//         let temperature_1 = Column::new_from_vec(
//             "temperature".to_string(),
//             vec![20i32],
//             vec![1],
//             vec!["x".to_string()],
//             None,
//         )?;
//         let schema_1 = Arc::new(Schema::new(vec![Field::new(
//             "temperature",
//             DataType::Int32,
//             true,
//         )]));
//         let dataset_1 = Dataset::new(
//             "dataset-b",
//             schema_1,
//             vec![temperature_1.array() as Arc<dyn NdArrowArray>],
//         )?;

//         writer.write_dataset(dataset_0).await?;
//         writer.write_dataset(dataset_1).await?;

//         let partition = writer.finish(io_cache.clone()).await?;
//         let loaded = load_partition(store, partition_path, io_cache).await?;

//         assert_eq!(partition.logical_entries(), vec!["dataset-a", "dataset-b"]);
//         assert_eq!(partition.dataset_indexes(), &[0, 1]);
//         assert_eq!(partition.deletion_flags(), &[false, false]);
//         assert_eq!(partition.insert_timestamps().len(), 2);

//         assert_eq!(loaded.logical_entries(), vec!["dataset-a", "dataset-b"]);
//         assert_eq!(loaded.dataset_indexes(), &[0, 1]);
//         assert_eq!(loaded.deletion_flags(), &[false, false]);
//         assert_eq!(loaded.insert_timestamps().len(), 2);
//         assert_eq!(partition.insert_timestamps(), loaded.insert_timestamps());

//         Ok(())
//     }
// }
