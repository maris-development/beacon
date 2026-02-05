// use std::collections::HashMap;
// use std::sync::Arc;

// use beacon_nd_arrow::NdArrowArray;
// use object_store::ObjectStore;
// use parking_lot::Mutex;

// use crate::{
//     array_chunked::{ChunkedArray, ChunkedArrayProvider, ChunkedArrayWriter},
//     attribute::{AttributeReader, AttributeValue, AttributeWriter},
// };

// pub struct VariableWriter<S: ObjectStore + Clone> {
//     store: S,
//     variable_prefix: object_store::path::Path,
//     variable_array_writer: ChunkedArrayWriter<S>,
//     variable_attribute_writers: HashMap<String, AttributeWriter<S>>,
//     count: usize,
// }

// /// Reads variable arrays and attributes by logical index.
// pub struct VariableReader<S: ObjectStore + Clone> {
//     store: S,
//     attribute_prefix: object_store::path::Path,
//     attribute_readers: Mutex<HashMap<String, AttributeReader>>,
// }

// impl<S: ObjectStore + Clone> VariableWriter<S> {
//     pub fn new(
//         store: S,
//         variable_prefix: object_store::path::Path,
//         data_type: arrow::datatypes::DataType,
//         pre_length: usize,
//     ) -> Self {
//         Self {
//             variable_array_writer: ChunkedArrayWriter::new(
//                 store.clone(),
//                 variable_prefix.clone(),
//                 data_type,
//             ),
//             store,
//             variable_prefix,
//             variable_attribute_writers: HashMap::new(),
//             count: pre_length,
//         }
//     }

//     pub fn count(&self) -> usize {
//         self.count
//     }

//     pub async fn write_array<C: ChunkedArrayProvider>(
//         &mut self,
//         dataset_index: u32,
//         array: ChunkedArray<C>,
//     ) -> anyhow::Result<()> {
//         self.variable_array_writer
//             .append_chunked_array(dataset_index, array)
//             .await?;
//         self.count += 1;
//         Ok(())
//     }

//     pub async fn write_null(&mut self) -> anyhow::Result<()> {
//         self.variable_array_writer.append_null().await?;
//         self.count += 1;
//         Ok(())
//     }

//     pub fn write_attributes(
//         &mut self,
//         attributes: HashMap<String, AttributeValue>,
//     ) -> anyhow::Result<()> {
//         let existing_keys: Vec<String> = self.variable_attribute_writers.keys().cloned().collect();
//         for key in existing_keys {
//             if !attributes.contains_key(&key) {
//                 self.variable_attribute_writers
//                     .get_mut(&key)
//                     .unwrap()
//                     .append_null()?;
//             }
//         }
//         for (key, value) in attributes {
//             let writer = self
//                 .variable_attribute_writers
//                 .entry(key.clone())
//                 .or_insert_with(|| {
//                     AttributeWriter::new(
//                         self.store.clone(),
//                         self.variable_prefix.child("attributes"),
//                         &key,
//                         value.arrow_datatype(),
//                     )
//                     .unwrap()
//                 });
//             writer.append(value)?;
//         }
//         Ok(())
//     }

//     pub fn attribute_writers_mut(&mut self) -> &mut HashMap<String, AttributeWriter<S>> {
//         &mut self.variable_attribute_writers
//     }

//     pub fn data_type(&self) -> &arrow::datatypes::DataType {
//         self.variable_array_writer.data_type()
//     }

//     /// Finalize and persist array + attribute IPC files.
//     pub async fn finish(mut self) -> anyhow::Result<()> {
//         self.variable_array_writer.finalize().await?;
//         for (_, writer) in self.variable_attribute_writers.drain() {
//             writer.finish().await?;
//         }
//         Ok(())
//     }
// }

// impl<S: ObjectStore + Clone> VariableReader<S> {
//     /// Open a variable reader rooted at the given prefix.
//     pub async fn open(store: S, variable_prefix: object_store::path::Path) -> anyhow::Result<Self> {
//         // Self::open_with_cache(store, variable_prefix, None).await
//         todo!()
//     }

//     /// Open a variable reader with an optional shared array cache.
//     pub async fn open_with_cache(
//         store: S,
//         variable_prefix: object_store::path::Path,
//     ) -> anyhow::Result<Self> {
//         // let array_reader =
//         //     ArrayReader::open_with_cache(store.clone(), variable_prefix.clone(), cache).await?;
//         // let attribute_prefix = variable_prefix.child("attributes");
//         // Ok(Self {
//         //     store,
//         //     array_reader,
//         //     attribute_prefix,
//         //     attribute_readers: Mutex::new(HashMap::new()),
//         // })
//         todo!()
//     }

//     // pub async fn variable_pruning_index(&self) -> anyhow::Result<Option<PruningIndex>> {
//     //     self.array_reader.pruning_index().await
//     // }

//     pub async fn attribute_pruning_index(&self) -> anyhow::Result<()> {
//         todo!()
//     }

//     /// Read the ND array at the logical index.
//     pub async fn read_array(&self, index: usize) -> anyhow::Result<Option<NdArrowArray>> {
//         // self.array_reader.read_index(index).await
//         todo!()
//     }

//     /// Read a single attribute value by name at the logical index.
//     pub async fn read_attribute(
//         &self,
//         name: &str,
//         index: usize,
//     ) -> anyhow::Result<Option<arrow::array::ArrayRef>> {
//         let reader = match self.get_attribute_reader(name).await? {
//             Some(reader) => reader,
//             None => return Ok(None),
//         };
//         reader.read_index(index)
//     }

//     /// Read multiple attributes for a logical index.
//     ///
//     /// Missing attributes are returned as `None` values in the map.
//     pub async fn read_attributes(
//         &self,
//         index: usize,
//         names: &[String],
//     ) -> anyhow::Result<HashMap<String, Option<arrow::array::ArrayRef>>> {
//         let mut values = HashMap::with_capacity(names.len());
//         for name in names {
//             let value = match self.get_attribute_reader(name).await? {
//                 Some(reader) => reader.read_index(index)?,
//                 None => None,
//             };
//             values.insert(name.clone(), value);
//         }
//         Ok(values)
//     }

//     async fn get_attribute_reader(&self, name: &str) -> anyhow::Result<Option<AttributeReader>> {
//         if let Some(reader) = self.attribute_readers.lock().get(name).cloned() {
//             return Ok(Some(reader));
//         }

//         let reader =
//             AttributeReader::open_optional(self.store.clone(), self.attribute_prefix.clone(), name)
//                 .await?;

//         if let Some(reader) = reader.clone() {
//             self.attribute_readers
//                 .lock()
//                 .insert(name.to_string(), reader);
//         }
//         Ok(reader)
//     }
// }

// #[cfg(test)]
// mod tests {
//     use std::sync::Arc;

//     use arrow::{array::Int32Array, datatypes::DataType};
//     use beacon_nd_arrow::dimensions::{Dimension, Dimensions};
//     use moka::future::Cache;
//     use object_store::{ObjectStore, memory::InMemory, path::Path};

//     use super::*;

//     // #[tokio::test]
//     // async fn variable_reader_reads_arrays_and_attributes() {
//     //     let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
//     //     let prefix = Path::from("variables/temp");
//     //     let mut writer = VariableWriter::new(store.clone(), prefix.clone(), DataType::Int32, 0);

//     //     let dims = Dimensions::new(vec![Dimension::try_new("x", 2).unwrap()]);
//     //     let arr = NdArrowArray::new(Arc::new(Int32Array::from(vec![1, 2])), dims).unwrap();
//     //     writer.write_array(arr).unwrap();
//     //     let mut attrs = HashMap::new();
//     //     attrs.insert("unit".to_string(), AttributeValue::Utf8("m".to_string()));
//     //     attrs.insert("quality".to_string(), AttributeValue::Int32(1));
//     //     writer.write_attributes(attrs).unwrap();

//     //     let dims = Dimensions::new(vec![Dimension::try_new("x", 3).unwrap()]);
//     //     let arr = NdArrowArray::new(Arc::new(Int32Array::from(vec![3, 4, 5])), dims).unwrap();
//     //     writer.write_array(arr).unwrap();
//     //     let mut attrs = HashMap::new();
//     //     attrs.insert("unit".to_string(), AttributeValue::Utf8("cm".to_string()));
//     //     writer.write_attributes(attrs).unwrap();

//     //     writer.finish().await.unwrap();

//     //     let cache = Arc::new(Cache::new(16));
//     //     let reader = VariableReader::open_with_cache(store.clone(), prefix, Some(cache))
//     //         .await
//     //         .unwrap();
//     //     let nd = reader.read_array(1).await.unwrap().unwrap();
//     //     assert_eq!(nd.values().len(), 3);

//     //     let unit = reader.read_attribute("unit", 1).await.unwrap().unwrap();
//     //     let unit_arr = unit
//     //         .as_any()
//     //         .downcast_ref::<arrow::array::StringArray>()
//     //         .unwrap();
//     //     assert_eq!(unit_arr.value(0), "cm");

//     //     let quality = reader.read_attribute("quality", 1).await.unwrap().unwrap();
//     //     assert_eq!(quality.null_count(), 1);
//     //     assert!(reader.read_attribute("missing", 1).await.unwrap().is_none());
//     // }

//     // #[tokio::test]
//     // async fn variable_reader_reads_multiple_attributes() {
//     //     let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
//     //     let prefix = Path::from("variables/multi");
//     //     let mut writer = VariableWriter::new(store.clone(), prefix.clone(), DataType::Int32, 0);

//     //     let dims = Dimensions::new(vec![Dimension::try_new("x", 1).unwrap()]);
//     //     let arr = NdArrowArray::new(Arc::new(Int32Array::from(vec![10])), dims).unwrap();
//     //     writer.write_array(arr).unwrap();
//     //     let mut attrs = HashMap::new();
//     //     attrs.insert("unit".to_string(), AttributeValue::Utf8("m".to_string()));
//     //     attrs.insert("quality".to_string(), AttributeValue::Int32(7));
//     //     writer.write_attributes(attrs).unwrap();
//     //     writer.finish().await.unwrap();

//     //     let reader = VariableReader::open(store.clone(), prefix).await.unwrap();
//     //     let names = vec![
//     //         "unit".to_string(),
//     //         "quality".to_string(),
//     //         "missing".to_string(),
//     //     ];
//     //     let values = reader.read_attributes(0, &names).await.unwrap();

//     //     let unit = values.get("unit").unwrap().as_ref().unwrap();
//     //     let unit_arr = unit
//     //         .as_any()
//     //         .downcast_ref::<arrow::array::StringArray>()
//     //         .unwrap();
//     //     assert_eq!(unit_arr.value(0), "m");

//     //     let quality = values.get("quality").unwrap().as_ref().unwrap();
//     //     let quality_arr = quality
//     //         .as_any()
//     //         .downcast_ref::<arrow::array::Int32Array>()
//     //         .unwrap();
//     //     assert_eq!(quality_arr.value(0), 7);

//     //     assert!(values.get("missing").unwrap().is_none());
//     // }
// }
