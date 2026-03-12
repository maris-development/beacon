use std::sync::Arc;

use arrow::array::AsArray;
use arrow::array::BooleanArray;
use arrow::array::StringArray;
use arrow::datatypes::Field;
use arrow::datatypes::SchemaRef;
use beacon_nd_arrow::array::NdArrowArray;
use futures::Stream;
use futures::StreamExt;
use futures::stream::BoxStream;

use crate::IPC_WRITE_OPTS;
use crate::column::Column;
use crate::column::ColumnReader;
use crate::column::ColumnWriter;
use crate::consts::arrow_chunk_size_by_type;
use crate::schema::AtlasSchema;

type DatasetReadResult = anyhow::Result<Vec<Option<Arc<dyn NdArrowArray>>>>;
type ShareableDatasetStream = BoxStream<'static, DatasetReadResult>;

#[derive(Clone)]
pub struct ShareableCollectionStream<S: object_store::ObjectStore + Clone> {
    dataset_indexes_rx: flume::Receiver<u32>,
    projected_readers: Arc<[Arc<ColumnReader<S>>]>,
    buffer_size: usize,
}

impl<S: object_store::ObjectStore + Clone + 'static> ShareableCollectionStream<S> {
    pub fn with_buffer_size(mut self, buffer_size: usize) -> Self {
        self.buffer_size = buffer_size.max(1);
        self
    }

    pub fn buffer_size(&self) -> usize {
        self.buffer_size
    }

    pub fn stream(&self) -> ShareableDatasetStream {
        self.stream_with_buffer(self.buffer_size)
    }

    pub fn stream_with_buffer(&self, buffer_size: usize) -> ShareableDatasetStream {
        let dataset_indexes_rx = self.dataset_indexes_rx.clone();
        let projected_readers = self.projected_readers.clone();
        let buffer_size = buffer_size.max(1);

        futures::stream::unfold(
            (
                dataset_indexes_rx,
                projected_readers,
                std::collections::VecDeque::<DatasetReadResult>::new(),
                buffer_size,
            ),
            |(dataset_indexes_rx, projected_readers, mut buffered_results, buffer_size)| async move {
                if buffered_results.is_empty() {
                    let first_dataset_index = dataset_indexes_rx.recv_async().await.ok()?;
                    buffered_results.push_back(read_dataset_from_readers(
                        &projected_readers,
                        first_dataset_index,
                    ));

                    while buffered_results.len() < buffer_size {
                        let dataset_index = match dataset_indexes_rx.try_recv() {
                            Ok(dataset_index) => dataset_index,
                            Err(flume::TryRecvError::Empty)
                            | Err(flume::TryRecvError::Disconnected) => break,
                        };
                        buffered_results.push_back(read_dataset_from_readers(
                            &projected_readers,
                            dataset_index,
                        ));
                    }
                }

                let next_result = buffered_results
                    .pop_front()
                    .expect("buffered result exists after fill");

                Some((
                    next_result,
                    (
                        dataset_indexes_rx,
                        projected_readers,
                        buffered_results,
                        buffer_size,
                    ),
                ))
            },
        )
        .boxed()
    }
}

fn read_dataset_from_readers<S: object_store::ObjectStore + Clone>(
    projected_readers: &[Arc<ColumnReader<S>>],
    dataset_index: u32,
) -> DatasetReadResult {
    projected_readers
        .iter()
        .map(|reader| reader.read_column_array(dataset_index).transpose())
        .collect()
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CollectionMetadata {
    pub name: String,
    pub description: Option<String>,
    pub schema: AtlasSchema,
    pub entries: Vec<String>,
}

const COLLECTION_METADATA_FILE: &str = "atlas.json";
const ENTRIES_FILE: &str = "entries.arrow";
const ENTRIES_COLUMN_NAME: &str = "__entry_key";

pub struct CollectionReader<S: object_store::ObjectStore + Clone> {
    object_store: S,
    collection_directory: object_store::path::Path,
    metadata: CollectionMetadata,
    io_cache: Arc<crate::array::io_cache::IoCache>,
    columns_readers: indexmap::IndexMap<String, tokio::sync::OnceCell<Arc<ColumnReader<S>>>>,
}

impl<S: object_store::ObjectStore + Clone> CollectionReader<S> {
    pub async fn new(
        object_store: S,
        collection_directory: object_store::path::Path,
        io_cache: Option<Arc<crate::array::io_cache::IoCache>>,
        io_cache_size: Option<usize>,
    ) -> anyhow::Result<Self> {
        // Read collection metadata from object store
        let metadata_path = collection_directory.child(COLLECTION_METADATA_FILE);
        // Check if metadata file exists
        let metafile = object_store.get(&metadata_path).await;

        match metafile {
            Ok(metafile) => {
                // Metadata file exists, proceed to read it
                let metadata_bytes = metafile.bytes().await?;
                let metadata: CollectionMetadata = serde_json::from_slice(&metadata_bytes)?;
                let columns_readers = metadata
                    .schema
                    .columns
                    .iter()
                    .map(|column| (column.name.clone(), tokio::sync::OnceCell::new()))
                    .collect();

                Ok(Self {
                    object_store,
                    collection_directory,
                    metadata,
                    io_cache: io_cache.unwrap_or_else(|| {
                        Arc::new(crate::array::io_cache::IoCache::new(
                            io_cache_size.unwrap_or(256 * 1024 * 1024),
                        ))
                    }),
                    columns_readers,
                })
            }
            Err(object_store::Error::NotFound { .. }) => {
                anyhow::bail!(
                    "Collection metadata file not found at path: {}",
                    metadata_path
                );
            }
            Err(e) => {
                anyhow::bail!(
                    "Error accessing collection metadata file at path {}: {}",
                    metadata_path,
                    e
                );
            }
        }
    }

    pub fn metadata(&self) -> &CollectionMetadata {
        &self.metadata
    }

    pub fn arrow_schema(&self) -> SchemaRef {
        let fields: Vec<_> = self
            .metadata
            .schema
            .columns
            .iter()
            .map(|col| arrow::datatypes::Field::new(&col.name, col.data_type.clone(), true))
            .collect();

        Arc::new(arrow::datatypes::Schema::new(fields))
    }

    pub async fn entries_vector(&self) -> anyhow::Result<(StringArray, BooleanArray)> {
        let file_bytes = self
            .object_store
            .get(&self.collection_directory.child(ENTRIES_FILE))
            .await?
            .bytes()
            .await?;

        let cursor = std::io::Cursor::new(file_bytes);
        let file_reader = arrow::ipc::reader::FileReader::try_new(cursor, None)?;

        let schema = file_reader.schema();
        let batches = file_reader.collect::<Result<Vec<_>, arrow::error::ArrowError>>()?;

        // Concat all the batches into one batch (assuming the entries file is small, which it should be since it only contains entry keys and deletion flags)
        let concatenated_batch = arrow::compute::concat_batches(&schema, &batches)?;

        // Extract the entry keys and deletion flags from the concatenated batch
        let entry_keys = concatenated_batch.column(0).as_string().clone();
        let deletion_flags = concatenated_batch.column(1).as_boolean().clone();

        Ok((entry_keys, deletion_flags))
    }

    pub async fn physical_entries(&self) -> anyhow::Result<Vec<String>> {
        let entries = self.entries_vector().await?;

        Ok(entries
            .0
            .iter()
            .map(|entry| entry.unwrap_or_default().to_string())
            .collect())
    }

    pub async fn deletion_entries_vector(&self) -> anyhow::Result<Vec<bool>> {
        let entries = self.entries_vector().await?;

        Ok(entries
            .1
            .iter()
            .map(|deletion_flag| deletion_flag.unwrap_or(false))
            .collect())
    }

    pub async fn logical_entries(&self) -> anyhow::Result<Vec<String>> {
        let entry_keys = self.entries_vector().await?.0;
        let deletion_flags = self.entries_vector().await?.1;

        let logical_entries = entry_keys
            .iter()
            .zip(deletion_flags.iter())
            .filter_map(|(entry, deletion_flag)| {
                if !deletion_flag.unwrap_or(false) {
                    Some(entry.unwrap_or_default().to_string())
                } else {
                    None
                }
            })
            .collect();

        Ok(logical_entries)
    }

    pub async fn shareable_stream<P: AsRef<[usize]>>(
        &self,
        projection: Option<P>,
        dataset_indexes: Option<Vec<u32>>,
    ) -> anyhow::Result<ShareableCollectionStream<S>> {
        let projected_readers = self.projected_column_readers(projection).await?;
        let dataset_indexes = match dataset_indexes {
            Some(dataset_indexes) => dataset_indexes,
            None => self
                .deletion_entries_vector()
                .await?
                .into_iter()
                .enumerate()
                .filter_map(|(dataset_index, deleted)| (!deleted).then_some(dataset_index as u32))
                .collect(),
        };

        let (dataset_indexes_tx, dataset_indexes_rx) = flume::unbounded();
        for dataset_index in dataset_indexes {
            dataset_indexes_tx
                .send(dataset_index)
                .map_err(|error| anyhow::anyhow!("failed to enqueue dataset index: {error}"))?;
        }
        drop(dataset_indexes_tx);

        Ok(ShareableCollectionStream {
            dataset_indexes_rx,
            projected_readers: projected_readers.into(),
            buffer_size: 1,
        })
    }

    pub async fn read_dataset<P: AsRef<[usize]>>(
        &self,
        projection: Option<P>,
        dataset_index: u32,
    ) -> anyhow::Result<Vec<Option<Arc<dyn NdArrowArray>>>> {
        Ok(read_dataset_from_readers(
            &self.projected_column_readers(projection).await?,
            dataset_index,
        )?)
    }

    async fn projected_column_readers<P: AsRef<[usize]>>(
        &self,
        projection: Option<P>,
    ) -> anyhow::Result<Vec<Arc<ColumnReader<S>>>> {
        let projection_indexes = projection
            .map(|indexes| indexes.as_ref().to_vec())
            .unwrap_or_else(|| (0..self.metadata.schema.columns.len()).collect());

        let mut projected_readers = Vec::with_capacity(projection_indexes.len());

        for column_index in projection_indexes {
            let column = self
                .metadata
                .schema
                .columns
                .get(column_index)
                .ok_or_else(|| anyhow::anyhow!("projection index out of bounds: {column_index}"))?;

            let reader_cell = self.columns_readers.get(&column.name).ok_or_else(|| {
                anyhow::anyhow!(
                    "column reader cell missing from collection schema: {}",
                    column.name
                )
            })?;

            let column_reader = reader_cell
                .get_or_try_init(|| async {
                    ColumnReader::new(
                        self.object_store.clone(),
                        column_name_to_path(self.collection_directory.clone(), &column.name),
                        self.io_cache.clone(),
                    )
                    .await
                    .map(Arc::new)
                })
                .await?
                .clone();

            projected_readers.push(column_reader);
        }

        Ok(projected_readers)
    }
}

pub struct CollectionWriter<S: object_store::ObjectStore + Clone> {
    object_store: S,
    collection_directory: object_store::path::Path,
    column_writers: indexmap::IndexMap<String, ColumnWriter<S>>,
    metadata: CollectionMetadata,
}

impl<S: object_store::ObjectStore + Clone> CollectionWriter<S> {
    pub fn new(
        object_store: S,
        collection_directory: object_store::path::Path,
        name: &str,
        description: Option<&str>,
    ) -> anyhow::Result<Self> {
        let mut column_writers = indexmap::IndexMap::new();

        // Add by default the "__entry_key" column for storing the entry keys of the collection.
        column_writers.insert(
            ENTRIES_COLUMN_NAME.to_string(),
            ColumnWriter::new(
                object_store.clone(),
                column_name_to_path(collection_directory.clone(), ENTRIES_COLUMN_NAME),
                arrow::datatypes::DataType::Utf8,
                arrow_chunk_size_by_type(&arrow::datatypes::DataType::Utf8),
            )?,
        );

        Ok(Self {
            object_store,
            collection_directory,
            column_writers,
            metadata: CollectionMetadata {
                schema: crate::schema::AtlasSchema { columns: vec![] },
                entries: vec![],
                description: description.map(|s| s.to_string()),
                name: name.to_string(),
            },
        })
    }

    pub async fn write_dataset<C: Stream<Item = Column>>(
        &mut self,
        name: &str,
        columns: C,
    ) -> anyhow::Result<()> {
        self.metadata.entries.push(name.to_string());

        // Create scalar for __entry_name column
        let entry_name_vec = vec![name.to_string()];
        let entry_name_column = Column::new_from_vec(
            "__entry_name".to_string(),
            entry_name_vec,
            vec![],
            vec![],
            None,
        )?;

        // Write __entry_name column first
        self.write_column(0, entry_name_column).await?;

        let mut pinned = std::pin::pin!(columns);

        // Write other columns
        while let Some(column) = pinned.next().await {
            self.write_column(0, column).await?;
        }

        Ok(())
    }

    async fn write_column(&mut self, dataset_index: u32, column: Column) -> anyhow::Result<()> {
        let column_name = column.name().to_string();
        let array = column.array();
        let data_type = column.data_type();

        let column_writer = match self.column_writers.entry(column_name.clone()) {
            indexmap::map::Entry::Occupied(entry) => entry.into_mut(),
            indexmap::map::Entry::Vacant(entry) => entry.insert(ColumnWriter::new(
                self.object_store.clone(),
                column_name_to_path(self.collection_directory.clone(), &column_name),
                data_type.clone(),
                arrow_chunk_size_by_type(&data_type),
            )?),
        };

        column_writer.write_column_array(dataset_index, array).await
    }

    pub async fn finish(mut self) -> anyhow::Result<()> {
        // Write all the columns as fields in the collection schema
        self.metadata.schema.columns = self
            .column_writers
            .iter()
            .map(|(name, writer)| crate::schema::AtlasColumn {
                name: name.clone(),
                data_type: writer.data_type().clone(),
            })
            .collect();

        // Write collection metadata to object store
        let metadata_path = self.collection_directory.child(COLLECTION_METADATA_FILE);

        let metadata_bytes = serde_json::to_vec(&self.metadata)?;
        self.object_store
            .put(&metadata_path, metadata_bytes.into())
            .await?;

        // Finish writing all column writers
        for (_, writer) in self.column_writers {
            writer.finalize().await?;
        }

        // Write entries file (currently empty, to be implemented)
        let entries_path = self.collection_directory.child(ENTRIES_FILE);
        let schema = arrow::datatypes::Schema::new(vec![
            Field::new("__entry_key", arrow::datatypes::DataType::Utf8, false),
            Field::new("deletion", arrow::datatypes::DataType::Boolean, false),
        ]); // Empty schema for now, to be implemented

        let mut temp_file_buffer = Vec::new();
        let mut file_writer = arrow::ipc::writer::FileWriter::try_new_with_options(
            &mut temp_file_buffer,
            &schema,
            IPC_WRITE_OPTS.clone(),
        )?;

        // Write a batch with all entries and a deletion column (currently all false, to be implemented)
        let entry_keys: Vec<String> = self.metadata.entries.clone();
        let deletion_flags: Vec<bool> = vec![false; entry_keys.len()];
        let entry_key_array = arrow::array::StringArray::from(entry_keys);
        let deletion_array = arrow::array::BooleanArray::from(deletion_flags);

        let batch = arrow::record_batch::RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(entry_key_array) as Arc<dyn arrow::array::Array>,
                Arc::new(deletion_array) as Arc<dyn arrow::array::Array>,
            ],
        )?;

        file_writer.write(&batch)?;
        file_writer.finish()?;

        // Upload entries file to object store
        self.object_store
            .put(&entries_path, temp_file_buffer.into())
            .await?;

        Ok(())
    }
}

fn column_name_to_path(
    collection_path: object_store::path::Path,
    column_name: &str,
) -> object_store::path::Path {
    if column_name.starts_with('.') {
        // Reserver for global attributes and will be replaced with '__' prefix in the actual column name to avoid conflicts with potential future reserved names.
        // For example, '.entry_key' will be stored as '__entry_key' in the column directory, but can be accessed with the reserved name '.entry_key' in the API.
        let reserved_name = column_name.trim_start_matches('.');
        collection_path
            .child("columns")
            .child(format!("__{}", reserved_name))
    } else {
        // Check if the column name contains a '.' which should split the column into a directory structure. For example, 'attributes.color' will be stored in 'columns/attributes/color'.
        if column_name.contains('.') {
            let parts: Vec<&str> = column_name.split('.').collect();
            let mut path = collection_path.child("columns");
            for part in parts {
                path = path.child(part);
            }
            path
        } else {
            collection_path.child("columns").child(column_name)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{CollectionReader, CollectionWriter, column_name_to_path};
    use crate::column::Column;
    use arrow::array::Int32Array;
    use beacon_nd_arrow::array::NdArrowArray;
    use futures::StreamExt;
    use object_store::{ObjectStore, memory::InMemory, path::Path};

    #[test]
    fn maps_reserved_column_names_to_double_underscore_paths() {
        let collection_path = Path::from("collections/example");

        let path = column_name_to_path(collection_path, ".entry_key");

        assert_eq!(path, Path::from("collections/example/columns/__entry_key"));
    }

    #[test]
    fn maps_dotted_column_names_to_nested_paths() {
        let collection_path = Path::from("collections/example");

        let path = column_name_to_path(collection_path, "attributes.color");

        assert_eq!(
            path,
            Path::from("collections/example/columns/attributes/color")
        );
    }

    #[test]
    fn maps_plain_column_names_to_direct_column_paths() {
        let collection_path = Path::from("collections/example");

        let path = column_name_to_path(collection_path, "temperature");

        assert_eq!(path, Path::from("collections/example/columns/temperature"));
    }

    #[tokio::test]
    async fn write_column_uses_nested_directory_for_dotted_names() -> anyhow::Result<()> {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let collection_path = Path::from("collections/example");
        let mut writer = CollectionWriter::new(store, collection_path, "example", None)?;
        let column = Column::new_from_vec(
            "attributes.color".to_string(),
            vec!["blue".to_string()],
            vec![],
            vec![],
            None,
        )?;

        writer.write_column(0, column).await?;

        let column_writer = writer.column_writers.get("attributes.color").unwrap();
        assert_eq!(
            column_writer.column_directory(),
            &Path::from("collections/example/columns/attributes/color")
        );
        Ok(())
    }

    #[tokio::test]
    async fn write_column_uses_reserved_directory_for_global_attributes() -> anyhow::Result<()> {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let collection_path = Path::from("collections/example");
        let mut writer = CollectionWriter::new(store, collection_path, "example", None)?;
        let column = Column::new_from_vec(
            ".platform".to_string(),
            vec!["argo".to_string()],
            vec![],
            vec![],
            None,
        )?;

        writer.write_column(0, column).await?;

        let column_writer = writer.column_writers.get(".platform").unwrap();
        assert_eq!(
            column_writer.column_directory(),
            &Path::from("collections/example/columns/__platform")
        );
        Ok(())
    }

    #[tokio::test]
    async fn read_dataset_returns_none_for_schema_columns_missing_in_dataset() -> anyhow::Result<()>
    {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let collection_path = Path::from("collections/example");
        let mut writer =
            CollectionWriter::new(store.clone(), collection_path.clone(), "example", None)?;

        writer.metadata.entries = vec!["dataset-0".to_string(), "dataset-1".to_string()];
        writer
            .write_column(
                0,
                Column::new_from_vec(
                    "temperature".to_string(),
                    vec![10i32, 11, 12],
                    vec![3],
                    vec!["x".to_string()],
                    None,
                )?,
            )
            .await?;
        writer
            .write_column(
                1,
                Column::new_from_vec(
                    "salinity".to_string(),
                    vec![30i32, 31],
                    vec![2],
                    vec!["x".to_string()],
                    None,
                )?,
            )
            .await?;
        writer.finish().await?;

        let reader = CollectionReader::new(store, collection_path, None, None).await?;

        let dataset_zero = reader.read_dataset(Some(vec![1, 2]), 0).await?;
        assert_eq!(dataset_zero.len(), 2);
        let temperature = dataset_zero[0].as_ref().expect("temperature array present");
        let temperature_values = temperature.as_arrow_array_ref().await?;
        let temperature_values = temperature_values
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("int32 array");
        assert_eq!(temperature_values.values(), &[10, 11, 12]);
        assert!(dataset_zero[1].is_none());

        let dataset_one = reader.read_dataset(Some(vec![1, 2]), 1).await?;
        assert_eq!(dataset_one.len(), 2);
        assert!(dataset_one[0].is_none());
        let salinity = dataset_one[1].as_ref().expect("salinity array present");
        let salinity_values = salinity.as_arrow_array_ref().await?;
        let salinity_values = salinity_values
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("int32 array");
        assert_eq!(salinity_values.values(), &[30, 31]);

        Ok(())
    }

    #[tokio::test]
    async fn read_dataset_without_projection_returns_full_merged_schema() -> anyhow::Result<()> {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let collection_path = Path::from("collections/full-schema");
        let mut writer =
            CollectionWriter::new(store.clone(), collection_path.clone(), "example", None)?;

        writer.metadata.entries = vec!["dataset-0".to_string()];
        writer
            .write_column(
                0,
                Column::new_from_vec(
                    "temperature".to_string(),
                    vec![1i32, 2],
                    vec![2],
                    vec!["x".to_string()],
                    None,
                )?,
            )
            .await?;
        writer.finish().await?;

        let reader = CollectionReader::new(store, collection_path, None, None).await?;
        let arrays = reader.read_dataset::<Vec<usize>>(None, 0).await?;

        assert_eq!(arrays.len(), reader.metadata.schema.columns.len());
        assert!(arrays[0].is_none());
        assert!(arrays[1].is_some());

        Ok(())
    }

    async fn scalar_value_from_dataset(
        dataset: Vec<Option<Arc<dyn NdArrowArray>>>,
    ) -> anyhow::Result<i32> {
        let array = dataset[0].as_ref().expect("projected array present");
        let values = array.as_arrow_array_ref().await?;
        let values = values
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("int32 array");
        Ok(values.value(0))
    }

    #[tokio::test]
    async fn shareable_stream_distributes_entries_across_tasks() -> anyhow::Result<()> {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let collection_path = Path::from("collections/shareable-stream");
        let mut writer =
            CollectionWriter::new(store.clone(), collection_path.clone(), "example", None)?;

        writer.metadata.entries = vec![
            "dataset-0".to_string(),
            "dataset-1".to_string(),
            "dataset-2".to_string(),
        ];
        writer
            .write_column(
                0,
                Column::new_from_vec(
                    "temperature".to_string(),
                    vec![10i32],
                    vec![1],
                    vec!["x".to_string()],
                    None,
                )?,
            )
            .await?;
        writer
            .write_column(
                1,
                Column::new_from_vec(
                    "temperature".to_string(),
                    vec![20i32],
                    vec![1],
                    vec!["x".to_string()],
                    None,
                )?,
            )
            .await?;
        writer
            .write_column(
                2,
                Column::new_from_vec(
                    "temperature".to_string(),
                    vec![30i32],
                    vec![1],
                    vec!["x".to_string()],
                    None,
                )?,
            )
            .await?;
        writer.finish().await?;

        let reader = CollectionReader::new(store, collection_path, None, None).await?;
        let shared_stream = reader
            .shareable_stream(Some(vec![1]), Some(vec![0, 1, 2]))
            .await?;

        let worker_one = {
            let shared_stream = shared_stream.clone();
            tokio::spawn(async move {
                let mut stream = shared_stream.stream();
                let dataset = stream.next().await.expect("worker one result")?;
                scalar_value_from_dataset(dataset).await
            })
        };
        let worker_two = {
            let shared_stream = shared_stream.clone();
            tokio::spawn(async move {
                let mut stream = shared_stream.stream();
                let dataset = stream.next().await.expect("worker two result")?;
                scalar_value_from_dataset(dataset).await
            })
        };
        let mut main_stream = shared_stream.stream();
        let third_dataset = main_stream.next().await.expect("main worker result")?;
        let third_value = scalar_value_from_dataset(third_dataset).await?;

        let mut values = vec![worker_one.await??, worker_two.await??, third_value];
        values.sort_unstable();

        assert_eq!(values, vec![10, 20, 30]);
        assert!(main_stream.next().await.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn shareable_stream_supports_configurable_buffer_size() -> anyhow::Result<()> {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let collection_path = Path::from("collections/buffered-stream");
        let mut writer =
            CollectionWriter::new(store.clone(), collection_path.clone(), "example", None)?;

        writer.metadata.entries = vec![
            "dataset-0".to_string(),
            "dataset-1".to_string(),
            "dataset-2".to_string(),
        ];
        for (dataset_index, value) in [(0, 10i32), (1, 20i32), (2, 30i32)] {
            writer
                .write_column(
                    dataset_index,
                    Column::new_from_vec(
                        "temperature".to_string(),
                        vec![value],
                        vec![1],
                        vec!["x".to_string()],
                        None,
                    )?,
                )
                .await?;
        }
        writer.finish().await?;

        let reader = CollectionReader::new(store, collection_path, None, None).await?;
        let shared_stream = reader
            .shareable_stream(Some(vec![1]), Some(vec![0, 1, 2]))
            .await?
            .with_buffer_size(2);

        assert_eq!(shared_stream.buffer_size(), 2);

        let mut stream = shared_stream.stream();
        let first = scalar_value_from_dataset(stream.next().await.expect("first result")?).await?;
        let second =
            scalar_value_from_dataset(stream.next().await.expect("second result")?).await?;
        let third = scalar_value_from_dataset(stream.next().await.expect("third result")?).await?;

        assert_eq!([first, second, third], [10, 20, 30]);
        assert!(stream.next().await.is_none());

        Ok(())
    }
}
