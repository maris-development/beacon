// use std::{collections::HashMap, io::Cursor, sync::Arc};

// use arrow::{
//     array::{Array, BooleanArray, StringArray},
//     ipc::reader::FileReader,
//     ipc::writer::FileWriter,
// };
// use moka::future::Cache;
// use object_store::{ObjectStore, PutPayload};

// use crate::{
//     IPC_WRITE_OPTS,
//     attribute::AttributeWriter,
//     dataset::{DatasetReadContext, DatasetReader, DatasetWriter},
//     schema::{AtlasSchema, AttributeField, VariableField},
//     variable::VariableWriter,
// };

// /// Writes partition datasets, variables, and global attributes to object storage.
// ///
// /// A partition owns a collection of dataset entries and persists a compact
// /// `entries.arrow` index alongside a boolean `entries_mask.arrow`.
// pub struct PartitionWriter<S: ObjectStore + Clone> {
//     store: S,
//     partition_path: object_store::path::Path,

//     /// Names of datasets in this partition.
//     entries: Vec<String>,

//     /// Writers for each variable in the partition.
//     variable_writers: HashMap<String, VariableWriter<S>>,
//     /// Writers for each global attribute in the partition.
//     global_attributes_writer: HashMap<String, AttributeWriter<S>>,
// }

// /// Reads partition datasets, variables, and global attributes from object storage.
// ///
// /// The reader loads `schema.json`, `entries.arrow`, and `entries_mask.arrow`,
// /// and provides dataset readers by logical index. A shared Moka cache is used
// /// across all underlying array readers.
// #[derive(Clone)]
// pub struct PartitionReader<S: ObjectStore + Clone + Send + Sync + 'static> {
//     store: S,
//     partition_path: object_store::path::Path,
//     entries: Arc<Vec<String>>,
//     entries_mask: Vec<bool>,
//     schema: Arc<AtlasSchema>,
//     context: Arc<DatasetReadContext<S>>,
// }

// const DEFAULT_BATCH_CACHE_CAPACITY: u64 = 256;

// impl<S: ObjectStore + Clone> PartitionWriter<S> {
//     /// Create a new partition writer at `collection_path/partition_name`.
//     pub fn new(store: S, partition_name: &str, collection_path: &object_store::path::Path) -> Self {
//         let partition_path = collection_path.child(partition_name);
//         Self {
//             entries: Vec::new(),
//             partition_path,
//             store,
//             variable_writers: HashMap::new(),
//             global_attributes_writer: HashMap::new(),
//         }
//     }

//     /// Returns the full partition path in object storage.
//     pub fn path(&self) -> &object_store::path::Path {
//         &self.partition_path
//     }

//     /// Returns the backing object store.
//     pub fn store(&self) -> &S {
//         &self.store
//     }

//     /// Returns the dataset entries tracked for this partition.
//     pub fn entries(&self) -> &Vec<String> {
//         &self.entries
//     }

//     /// Append a dataset entry and return a dataset writer scoped to this partition.
//     pub fn append_dataset(&mut self, dataset_name: &str) -> DatasetWriter<'_, S> {
//         if !self.entries.iter().any(|name| name == dataset_name) {
//             self.entries.push(dataset_name.to_string());
//         }
//         DatasetWriter::new(self.store.clone(), &self.partition_path.clone(), self)
//     }

//     pub(crate) fn variable_writers_mut(&mut self) -> &mut HashMap<String, VariableWriter<S>> {
//         &mut self.variable_writers
//     }

//     pub(crate) fn global_attributes_writer_mut(
//         &mut self,
//     ) -> &mut HashMap<String, AttributeWriter<S>> {
//         &mut self.global_attributes_writer
//     }

//     /// Finalize all writers and persist partition entry index files.
//     pub async fn finish(self) -> anyhow::Result<()> {
//         let mut schema = AtlasSchema::empty();
//         for (name, mut writer) in self.variable_writers {
//             let mut variable_field = VariableField::new(&name, writer.data_type().clone());
//             for (key, attr_writer) in writer.attribute_writers_mut() {
//                 variable_field
//                     .add_attribute(AttributeField::new(key, attr_writer.data_type().clone()));
//             }
//             schema.add_variable(variable_field);
//             writer.finish().await?;
//         }
//         for (name, writer) in self.global_attributes_writer {
//             schema.add_global_attribute(AttributeField::new(&name, writer.data_type().clone()));
//             writer.finish().await?;
//         }

//         // Write the schema file as a JSON file
//         let schema_path = self.partition_path.child("schema.json");
//         let schema_json = serde_json::to_vec_pretty(&schema)?;
//         self.store
//             .put(&schema_path, PutPayload::from_bytes(schema_json.into()))
//             .await?;

//         // Write the entries as: entries.arrow IPC file
//         let entries_path = self.partition_path.child("entries.arrow");
//         let entries_array = arrow::array::StringArray::from(self.entries);
//         let entries_batch = arrow::record_batch::RecordBatch::try_new(
//             arrow::datatypes::Schema::new(vec![arrow::datatypes::Field::new(
//                 "entry",
//                 arrow::datatypes::DataType::Utf8,
//                 false,
//             )])
//             .into(),
//             vec![std::sync::Arc::new(entries_array)],
//         )?;

//         let buffer = Cursor::new(Vec::new());
//         let mut writer = FileWriter::try_new_with_options(
//             buffer,
//             &entries_batch.schema(),
//             IPC_WRITE_OPTS.clone(),
//         )?;
//         writer.write(&entries_batch)?;
//         let buffer = writer.into_inner()?;

//         self.store
//             .put(
//                 &entries_path,
//                 PutPayload::from_bytes(buffer.into_inner().into()),
//             )
//             .await?;

//         // Write an entries_mask.arrow IPC file with all true values
//         let entries_mask_path = self.partition_path.child("entries_mask.arrow");
//         let entries_mask_array =
//             arrow::array::BooleanArray::from(vec![true; entries_batch.num_rows()]);
//         let entries_mask_batch = arrow::record_batch::RecordBatch::try_new(
//             arrow::datatypes::Schema::new(vec![arrow::datatypes::Field::new(
//                 "mask",
//                 arrow::datatypes::DataType::Boolean,
//                 false,
//             )])
//             .into(),
//             vec![std::sync::Arc::new(entries_mask_array)],
//         )?;

//         let buffer = Cursor::new(Vec::new());
//         let mut writer = FileWriter::try_new_with_options(
//             buffer,
//             &entries_mask_batch.schema(),
//             IPC_WRITE_OPTS.clone(),
//         )?;
//         writer.write(&entries_mask_batch)?;
//         let buffer = writer.into_inner()?;

//         self.store
//             .put(
//                 &entries_mask_path,
//                 PutPayload::from_bytes(buffer.into_inner().into()),
//             )
//             .await?;

//         Ok(())
//     }
// }

// impl<S: ObjectStore + Clone + Send + Sync + 'static> PartitionReader<S> {
//     /// Open a partition reader rooted at the given path, creating a shared cache.
//     pub async fn open(store: S, partition_path: object_store::path::Path) -> anyhow::Result<Self> {
//         // let cache = Arc::new(Cache::new(DEFAULT_BATCH_CACHE_CAPACITY));
//         // Self::open_with_cache(store, partition_path, cache).await
//         todo!()
//     }

//     /// Open a partition reader with a shared cache instance.
//     // pub async fn open_with_cache(
//     //     store: S,
//     //     partition_path: object_store::path::Path,
//     //     cache: Arc<BatchCache>,
//     // ) -> anyhow::Result<Self> {
//     //     let schema = Arc::new(Self::read_schema(&store, &partition_path).await?);
//     //     let entries = Arc::new(Self::read_entries(&store, &partition_path).await?);
//     //     let entries_mask = Self::read_entries_mask(&store, &partition_path, entries.len()).await?;
//     //     let context = Arc::new(DatasetReadContext {
//     //         entries: Arc::clone(&entries),
//     //         cache: Arc::clone(&cache),
//     //         partition_prefix: partition_path.clone(),
//     //         schema: Arc::clone(&schema),
//     //         variable_readers: Cache::new(256),
//     //         global_attribute_readers: Cache::new(256),
//     //     });
//     //     Ok(Self {
//     //         store,
//     //         partition_path,
//     //         entries,
//     //         entries_mask,
//     //         schema,
//     //         context,
//     //     })
//     // }

//     /// Returns the full partition path in object storage.
//     pub fn path(&self) -> &object_store::path::Path {
//         &self.partition_path
//     }

//     /// Returns the backing object store.
//     pub fn store(&self) -> &S {
//         &self.store
//     }

//     /// Returns the partition schema.
//     pub fn schema(&self) -> &AtlasSchema {
//         &self.schema
//     }

//     /// Returns the entries list for this partition.
//     pub fn entries(&self) -> &Vec<String> {
//         &self.entries
//     }

//     /// Returns the entries mask for this partition.
//     pub fn entries_mask(&self) -> &Vec<bool> {
//         &self.entries_mask
//     }

//     /// Get a variable reader by name.
//     pub async fn variable_reader(
//         &self,
//         name: &str,
//     ) -> anyhow::Result<Option<crate::variable::VariableReader<S>>> {
//         // Check if the variable exists in the schema
//         if !self.schema.variables().iter().any(|var| var.name() == name) {
//             return Ok(None);
//         }

//         Ok(Some(
//             crate::variable::VariableReader::open(
//                 self.store.clone(),
//                 self.partition_path.child("variables").child(name),
//             )
//             .await?,
//         ))
//     }

//     /// Get a global attribute reader by name.
//     pub async fn global_attribute_reader(
//         &self,
//         name: &str,
//     ) -> anyhow::Result<Option<crate::attribute::AttributeReader>> {
//         // Check if the attribute exists in the schema
//         if !self
//             .schema
//             .global_attributes()
//             .iter()
//             .any(|attr| attr.name() == name)
//         {
//             return Ok(None);
//         }
//         Ok(Some(
//             crate::attribute::AttributeReader::open(
//                 self.store.clone(),
//                 self.partition_path.child("__global_attributes"),
//                 name,
//             )
//             .await?,
//         ))
//     }

//     /// Read a dataset by logical index.
//     pub async fn dataset_by_index(
//         &self,
//         index: usize,
//     ) -> anyhow::Result<Option<Arc<DatasetReader<S>>>> {
//         if index >= self.entries.len() {
//             return Ok(None);
//         }
//         if !self.entries_mask.get(index).copied().unwrap_or(false) {
//             return Ok(None);
//         }

//         let store = self.store.clone();
//         let context = Arc::clone(&self.context);
//         let reader = Arc::new(DatasetReader::new(store, index, context));
//         Ok(Some(reader))
//     }

//     async fn read_schema(
//         store: &S,
//         partition_path: &object_store::path::Path,
//     ) -> anyhow::Result<AtlasSchema> {
//         let schema_path = partition_path.child("schema.json");
//         let schema_bytes = store.get(&schema_path).await?.bytes().await?;
//         Ok(serde_json::from_slice(&schema_bytes)?)
//     }

//     async fn read_entries(
//         store: &S,
//         partition_path: &object_store::path::Path,
//     ) -> anyhow::Result<Vec<String>> {
//         let entries_path = partition_path.child("entries.arrow");
//         let entries_bytes = store.get(&entries_path).await?.bytes().await?;
//         let reader = FileReader::try_new(std::io::Cursor::new(entries_bytes.to_vec()), None)?;
//         let mut entries = Vec::new();
//         for batch in reader {
//             let batch = batch?;
//             let column = batch
//                 .column(0)
//                 .as_any()
//                 .downcast_ref::<StringArray>()
//                 .ok_or_else(|| anyhow::anyhow!("entries column must be Utf8"))?;
//             for i in 0..column.len() {
//                 if column.is_null(i) {
//                     return Err(anyhow::anyhow!("entries contains null at index {i}"));
//                 }
//                 entries.push(column.value(i).to_string());
//             }
//         }
//         Ok(entries)
//     }

//     async fn read_entries_mask(
//         store: &S,
//         partition_path: &object_store::path::Path,
//         entries_len: usize,
//     ) -> anyhow::Result<Vec<bool>> {
//         let entries_mask_path = partition_path.child("entries_mask.arrow");
//         let entries_mask_bytes = match store.get(&entries_mask_path).await {
//             Ok(result) => result.bytes().await?,
//             Err(object_store::Error::NotFound { .. }) => {
//                 return Ok(vec![true; entries_len]);
//             }
//             Err(err) => return Err(anyhow::anyhow!(err)),
//         };
//         let reader = FileReader::try_new(std::io::Cursor::new(entries_mask_bytes.to_vec()), None)?;
//         let mut mask = Vec::new();
//         for batch in reader {
//             let batch = batch?;
//             let column = batch
//                 .column(0)
//                 .as_any()
//                 .downcast_ref::<BooleanArray>()
//                 .ok_or_else(|| anyhow::anyhow!("entries_mask column must be Boolean"))?;
//             for i in 0..column.len() {
//                 if column.is_null(i) {
//                     return Err(anyhow::anyhow!("entries_mask contains null at index {i}"));
//                 }
//                 mask.push(column.value(i));
//             }
//         }

//         if mask.len() != entries_len {
//             return Err(anyhow::anyhow!(
//                 "entries_mask length {mask_len} does not match entries length {entries_len}",
//                 mask_len = mask.len()
//             ));
//         }

//         Ok(mask)
//     }
// }

// #[cfg(test)]
// mod tests {}
