use std::sync::Arc;

use arrow::array::BooleanArray;
use arrow::array::StringArray;
use arrow::datatypes::Field;
use arrow::datatypes::SchemaRef;
use futures::Stream;
use futures::StreamExt;

use crate::IPC_WRITE_OPTS;
use crate::column::{Column, ColumnWriter};
use crate::consts::arrow_chunk_size_by_type;
use crate::schema::AtlasSchema;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CollectionMetadata {
    pub name: String,
    pub description: Option<String>,
    pub schema: AtlasSchema,
    pub entries: Vec<String>,
}

const COLLECTION_METADATA_FILE: &str = "atlas.json";
const ENTRIES_FILE: &str = "entries.arrow";

pub struct CollectionReader<S: object_store::ObjectStore + Clone> {
    object_store: S,
    collection_directory: object_store::path::Path,

    metadata: CollectionMetadata,
}

impl<S: object_store::ObjectStore + Clone> CollectionReader<S> {
    pub async fn new(
        object_store: S,
        collection_directory: object_store::path::Path,
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

                Ok(Self {
                    object_store,
                    collection_directory,
                    metadata,
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
        let entry_keys = concatenated_batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .clone();
        let deletion_flags = concatenated_batch
            .column(1)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .clone();

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

    pub async fn as_streamable(
        &self,
        entries_mask: Option<&[bool]>,
        projection: Option<&[usize]>,
    ) -> anyhow::Result<()> {
        let mask = match entries_mask {
            Some(mask) => mask.to_vec(), // Use provided mask to filter entries, e.g. for reading only a subset of entries or for implementing deletion by masking deleted entries
            None => vec![false; self.metadata.entries.len()], // Don't mask any entries by default, i.e. read all entries
        };

        // Ensure the mask has the same length as the number of entries
        if mask.len() != self.metadata.entries.len() {
            anyhow::bail!(
                "Entries mask length {} does not match number of entries {}",
                mask.len(),
                self.metadata.entries.len()
            );
        }

        todo!()
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
    ) -> Self {
        let mut column_writers = indexmap::IndexMap::new();

        // Add by default the "__entry_key" column for storing the entry keys of the collection.
        column_writers.insert(
            "__entry_key".to_string(),
            ColumnWriter::new(
                object_store.clone(),
                collection_directory.child("columns").child("__entry_key"),
                arrow::datatypes::DataType::Utf8,
                arrow_chunk_size_by_type(&arrow::datatypes::DataType::Utf8),
            ),
        );

        Self {
            object_store,
            collection_directory,
            column_writers,
            metadata: CollectionMetadata {
                schema: crate::schema::AtlasSchema { columns: vec![] },
                entries: vec![],
                description: description.map(|s| s.to_string()),
                name: name.to_string(),
            },
        }
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
            entry_name_vec,
            vec!["entry".to_string()],
            vec![],
            "__entry_key".to_string(),
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
        let column_writer = self
            .column_writers
            .entry(column.name.clone())
            .or_insert_with(|| {
                ColumnWriter::new(
                    self.object_store.clone(),
                    self.collection_directory
                        .child("columns")
                        .child(&*column.name),
                    column.array.array_datatype.clone(),
                    arrow_chunk_size_by_type(&column.array.array_datatype),
                )
            });

        column_writer
            .write_column_array(dataset_index, column.array)
            .await
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
