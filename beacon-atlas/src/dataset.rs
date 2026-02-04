use std::sync::Arc;

use moka::future::Cache;
use object_store::ObjectStore;

use crate::{
    array::BatchCache,
    attribute::{AttributeReader, AttributeValue, AttributeWriter},
    partition::PartitionWriter,
    schema::AtlasSchema,
    variable::{VariableReader, VariableWriter},
};

pub struct Dataset {}

/// Writes datasets within a partition and coordinates variable/global attribute writers.
pub struct DatasetWriter<'s, S: ObjectStore + Clone> {
    store: S,
    partition_prefix: object_store::path::Path,
    partition_writer_ref: &'s mut PartitionWriter<S>,
}

pub(crate) struct DatasetReadContext<S: ObjectStore + Clone + Send + Sync + 'static> {
    pub(crate) partition_prefix: object_store::path::Path,
    pub(crate) entries: Arc<Vec<String>>,
    pub(crate) schema: Arc<AtlasSchema>,
    pub(crate) cache: Arc<BatchCache>,
    pub(crate) variable_readers: Cache<String, Arc<VariableReader<S>>>,
    pub(crate) global_attribute_readers: Cache<String, AttributeReader>,
}

/// Reads dataset variables and global attributes for a single partition entry.
pub struct DatasetReader<S: ObjectStore + Clone + Send + Sync + 'static> {
    store: S,
    dataset_index: usize,
    context: Arc<DatasetReadContext<S>>,
}

impl<'s, S: ObjectStore + Clone> DatasetWriter<'s, S> {
    /// Create a dataset writer for the given partition path.
    pub fn new(
        store: S,
        partition_prefix: &object_store::path::Path,
        partition_writer_ref: &'s mut PartitionWriter<S>,
    ) -> Self {
        Self {
            store,
            partition_prefix: partition_prefix.clone(),
            partition_writer_ref,
        }
    }

    /// Returns the backing object store.
    pub fn store(&self) -> &S {
        &self.store
    }

    /// Returns the partition prefix for this dataset.
    pub fn partition_prefix(&self) -> &object_store::path::Path {
        &self.partition_prefix
    }

    /// Returns the parent partition writer.
    pub fn partition_writer(&mut self) -> &mut PartitionWriter<S> {
        self.partition_writer_ref
    }

    /// Append (or retrieve) a variable writer for the dataset.
    pub fn append_variable(
        &mut self,
        variable_name: &str,
        datatype: arrow::datatypes::DataType,
    ) -> &mut VariableWriter<S> {
        let pre_length = self.partition_writer_ref.entries().len();
        let variable_writers = self.partition_writer_ref.variable_writers_mut();
        if !variable_writers.contains_key(variable_name) {
            variable_writers.insert(
                variable_name.to_string(),
                VariableWriter::new(
                    self.store.clone(),
                    self.partition_prefix
                        .child("variables")
                        .child(variable_name),
                    datatype.clone(),
                    pre_length - 1,
                ),
            );
        }
        variable_writers.get_mut(variable_name).unwrap()
    }

    /// Append a global attribute for the dataset.
    pub fn append_global_attribute(
        &mut self,
        name: &str,
        value: impl Into<AttributeValue>,
    ) -> anyhow::Result<()> {
        let value: AttributeValue = value.into();
        let value_dtype = value.arrow_datatype();
        let global_attributes_writer = self.partition_writer_ref.global_attributes_writer_mut();

        if !global_attributes_writer.contains_key(name) {
            global_attributes_writer.insert(
                name.to_string(),
                AttributeWriter::new(
                    self.store.clone(),
                    self.partition_prefix.child("__global_attributes").clone(),
                    name,
                    value_dtype,
                )?,
            );
        }

        global_attributes_writer
            .get_mut(name)
            .unwrap()
            .append(value)?;

        Ok(())
    }
}

impl<S: ObjectStore + Clone + Send + Sync + 'static> DatasetReader<S> {
    pub(crate) fn new(store: S, dataset_index: usize, context: Arc<DatasetReadContext<S>>) -> Self {
        Self {
            store,
            dataset_index,
            context,
        }
    }

    /// Returns the dataset index within the partition.
    pub fn index(&self) -> usize {
        self.dataset_index
    }

    pub fn name(&self) -> &str {
        &self.context.entries[self.dataset_index]
    }

    /// Returns the partition prefix in which this dataset resides.
    pub fn prefix(&self) -> &object_store::path::Path {
        &self.context.partition_prefix
    }

    /// Returns the partition schema.
    pub fn schema(&self) -> &AtlasSchema {
        &self.context.schema
    }

    /// Read a variable array by name for this dataset entry.
    pub async fn read_variable(
        &self,
        name: &str,
    ) -> anyhow::Result<Option<beacon_nd_arrow::NdArrowArray>> {
        let reader = self.get_variable_reader(name).await?;
        reader.read_array(self.dataset_index).await
    }

    /// Read a variable attribute by name for this dataset entry.
    pub async fn read_variable_attribute(
        &self,
        variable: &str,
        attribute: &str,
    ) -> anyhow::Result<Option<arrow::array::ArrayRef>> {
        let reader = self.get_variable_reader(variable).await?;
        reader.read_attribute(attribute, self.dataset_index).await
    }

    /// Read a global attribute value for this dataset entry.
    pub async fn read_global_attribute(
        &self,
        name: &str,
    ) -> anyhow::Result<Option<arrow::array::ArrayRef>> {
        if !self
            .context
            .schema
            .global_attributes()
            .iter()
            .any(|attr| attr.name() == name)
        {
            return Ok(None);
        }

        let reader = self.get_global_attribute_reader(name).await?;
        reader.read_index(self.dataset_index)
    }

    pub fn variable_names(&self) -> Vec<String> {
        self.context
            .schema
            .variables()
            .iter()
            .map(|var| var.name().to_string())
            .collect()
    }

    pub fn global_attribute_names(&self) -> Vec<String> {
        self.context
            .schema
            .global_attributes()
            .iter()
            .map(|attr| attr.name().to_string())
            .collect()
    }

    async fn get_variable_reader(&self, name: &str) -> anyhow::Result<Arc<VariableReader<S>>> {
        let name = name.to_string();
        let store = self.store.clone();
        let context = Arc::clone(&self.context);
        let variable_prefix = self
            .context
            .partition_prefix
            .child("variables")
            .child(name.clone());
        let reader = self
            .context
            .variable_readers
            .try_get_with(name.clone(), async move {
                let cache = Arc::clone(&context.cache);

                let reader =
                    VariableReader::open_with_cache(store, variable_prefix, Some(cache)).await?;
                Ok::<_, anyhow::Error>(Arc::new(reader))
            })
            .await
            .map_err(|err| anyhow::anyhow!(err))?;
        Ok(reader)
    }

    async fn get_global_attribute_reader(&self, name: &str) -> anyhow::Result<AttributeReader> {
        let name = name.to_string();
        let store = self.store.clone();
        let global_attr_prefix = self.context.partition_prefix.child("__global_attributes");
        let reader = self
            .context
            .global_attribute_readers
            .try_get_with(name.clone(), async move {
                let prefix = global_attr_prefix.clone();
                AttributeReader::open(store, prefix, &name)
                    .await
                    .map_err(|err| anyhow::anyhow!(err))
            })
            .await
            .map_err(|err| anyhow::anyhow!(err))?;
        Ok(reader)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow::array::{Array, Int32Array, StringArray};
    use arrow::datatypes::DataType;
    use beacon_nd_arrow::dimensions::{Dimension, Dimensions};
    use object_store::{ObjectStore, memory::InMemory, path::Path};

    use crate::{
        array::ArrayReader,
        attribute::{AttributeReader, AttributeValue},
        partition::{PartitionReader, PartitionWriter},
    };

    // #[tokio::test]
    // async fn dataset_writer_persists_variable_and_attribute() {
    //     let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    //     let collection = Path::from("collections");
    //     let mut partition = PartitionWriter::new(store.clone(), "p0", &collection);

    //     {
    //         let mut dataset = partition.append_dataset("ds_a");
    //         let dims = Dimensions::new(vec![Dimension::try_new("x", 2).unwrap()]);
    //         let arr =
    //             beacon_nd_arrow::NdArrowArray::new(Arc::new(Int32Array::from(vec![10, 11])), dims)
    //                 .unwrap();
    //         dataset
    //             .append_variable("temperature", DataType::Int32)
    //             .write_array(arr)
    //             .unwrap();
    //         dataset.append_global_attribute("title", "Demo").unwrap();
    //     }

    //     partition.finish().await.unwrap();

    //     let global_attrs_path = Path::from("collections/p0/__global_attributes");
    //     let attr_reader = AttributeReader::open(store.clone(), global_attrs_path.clone(), "title")
    //         .await
    //         .unwrap();
    //     let attr = attr_reader.read_index(0).unwrap().unwrap();
    //     let attr_arr = attr.as_any().downcast_ref::<StringArray>().unwrap();
    //     assert_eq!(attr_arr.value(0), "Demo");

    //     let variable_path = Path::from("collections/p0/variables");
    //     let array_reader = ArrayReader::open(store.clone(), variable_path.child("temperature"))
    //         .await
    //         .unwrap();
    //     let nd = array_reader.read_index(0).await.unwrap().unwrap();
    //     let values = nd.values().as_any().downcast_ref::<Int32Array>().unwrap();
    //     assert_eq!(values.values(), &[10, 11]);
    // }

    // #[tokio::test]
    // async fn dataset_writer_reuses_variable_writer() {
    //     let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    //     let collection = Path::from("collections");
    //     let mut partition = PartitionWriter::new(store.clone(), "p0", &collection);

    //     {
    //         let mut dataset = partition.append_dataset("ds_a");
    //         let dims = Dimensions::new(vec![Dimension::try_new("x", 1).unwrap()]);
    //         let arr1 = beacon_nd_arrow::NdArrowArray::new(
    //             Arc::new(Int32Array::from(vec![1])),
    //             dims.clone(),
    //         )
    //         .unwrap();
    //         let arr2 =
    //             beacon_nd_arrow::NdArrowArray::new(Arc::new(Int32Array::from(vec![2])), dims)
    //                 .unwrap();

    //         dataset
    //             .append_variable("salinity", DataType::Int32)
    //             .write_array(arr1)
    //             .unwrap();
    //         dataset
    //             .append_variable("salinity", DataType::Int32)
    //             .write_array(arr2)
    //             .unwrap();
    //     }

    //     partition.finish().await.unwrap();

    //     let variable_path = Path::from("collections/p0/variables");
    //     let array_reader = ArrayReader::open(store.clone(), variable_path.child("salinity"))
    //         .await
    //         .unwrap();
    //     let first = array_reader.read_index(0).await.unwrap().unwrap();
    //     let first_arr = first
    //         .values()
    //         .as_any()
    //         .downcast_ref::<Int32Array>()
    //         .unwrap();
    //     assert_eq!(first_arr.value(0), 1);

    //     let second = array_reader.read_index(1).await.unwrap().unwrap();
    //     let second_arr = second
    //         .values()
    //         .as_any()
    //         .downcast_ref::<Int32Array>()
    //         .unwrap();
    //     assert_eq!(second_arr.value(0), 2);
    // }

    // #[tokio::test]
    // async fn dataset_reader_reads_variables_and_globals() {
    //     let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    //     let collection = Path::from("collections");
    //     let mut partition = PartitionWriter::new(store.clone(), "p0", &collection);

    //     {
    //         let mut dataset = partition.append_dataset("ds_a");
    //         let dims = Dimensions::new(vec![Dimension::try_new("x", 2).unwrap()]);
    //         let arr =
    //             beacon_nd_arrow::NdArrowArray::new(Arc::new(Int32Array::from(vec![10, 11])), dims)
    //                 .unwrap();
    //         dataset
    //             .append_variable("temperature", DataType::Int32)
    //             .write_array(arr)
    //             .unwrap();
    //         dataset.append_global_attribute("title", "Demo A").unwrap();
    //     }

    //     {
    //         let mut dataset = partition.append_dataset("ds_b");
    //         let dims = Dimensions::new(vec![Dimension::try_new("x", 1).unwrap()]);
    //         let arr =
    //             beacon_nd_arrow::NdArrowArray::new(Arc::new(Int32Array::from(vec![42])), dims)
    //                 .unwrap();
    //         dataset
    //             .append_variable("temperature", DataType::Int32)
    //             .write_array(arr)
    //             .unwrap();
    //         dataset.append_global_attribute("title", "Demo B").unwrap();
    //     }

    //     partition.finish().await.unwrap();

    //     let reader = PartitionReader::open(store.clone(), collection.child("p0"))
    //         .await
    //         .unwrap();
    //     let ds_b = reader.dataset_by_index(1).await.unwrap().unwrap();
    //     assert_eq!(ds_b.name(), "ds_b");

    //     let nd = ds_b.read_variable("temperature").await.unwrap().unwrap();
    //     let values = nd.values().as_any().downcast_ref::<Int32Array>().unwrap();
    //     assert_eq!(values.values(), &[42]);

    //     let title = ds_b.read_global_attribute("title").await.unwrap().unwrap();
    //     let title_arr = title.as_any().downcast_ref::<StringArray>().unwrap();
    //     assert_eq!(title_arr.value(0), "Demo B");
    // }

    // #[tokio::test]
    // async fn dataset_reader_reads_variable_attributes() {
    //     let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    //     let collection = Path::from("collections");
    //     let mut partition = PartitionWriter::new(store.clone(), "p0", &collection);

    //     {
    //         let mut dataset = partition.append_dataset("ds_a");
    //         let dims = Dimensions::new(vec![Dimension::try_new("x", 1).unwrap()]);
    //         let arr = beacon_nd_arrow::NdArrowArray::new(Arc::new(Int32Array::from(vec![7])), dims)
    //             .unwrap();
    //         let var = dataset.append_variable("salinity", DataType::Int32);
    //         var.write_array(arr).unwrap();
    //         var.write_attributes(HashMap::from([(
    //             "unit".to_string(),
    //             AttributeValue::Utf8("psu".to_string()),
    //         )]))
    //         .unwrap();
    //     }

    //     partition.finish().await.unwrap();

    //     let reader = PartitionReader::open(store.clone(), collection.child("p0"))
    //         .await
    //         .unwrap();
    //     let dataset = reader.dataset_by_index(0).await.unwrap().unwrap();
    //     let unit = dataset
    //         .read_variable_attribute("salinity", "unit")
    //         .await
    //         .unwrap()
    //         .unwrap();
    //     let unit_arr = unit.as_any().downcast_ref::<StringArray>().unwrap();
    //     assert_eq!(unit_arr.value(0), "psu");
    // }
}
