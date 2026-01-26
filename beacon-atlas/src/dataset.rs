use object_store::ObjectStore;

use crate::{
    attribute::{AttributeValue, AttributeWriter},
    partition::PartitionWriter,
    variable::VariableWriter,
};

/// Writes datasets within a partition and coordinates variable/global attribute writers.
pub struct DatasetWriter<'s, S: ObjectStore + Clone> {
    store: S,
    partition_prefix: object_store::path::Path,
    partition_writer_ref: &'s mut PartitionWriter<S>,
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
                    self.partition_prefix.child(variable_name),
                    datatype.clone(),
                    pre_length,
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
                    self.partition_prefix.clone(),
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Array, Int32Array, StringArray};
    use arrow::datatypes::DataType;
    use beacon_nd_arrow::dimensions::{Dimension, Dimensions};
    use object_store::{ObjectStore, memory::InMemory, path::Path};

    use crate::{array::ArrayReader, attribute::AttributeReader, partition::PartitionWriter};

    #[tokio::test]
    async fn dataset_writer_persists_variable_and_attribute() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let collection = Path::from("collections/datasets");
        let mut partition = PartitionWriter::new(store.clone(), "p0", &collection);

        {
            let mut dataset = partition.append_dataset("ds_a");
            let dims = Dimensions::new(vec![Dimension::try_new("x", 2).unwrap()]);
            let arr =
                beacon_nd_arrow::NdArrowArray::new(Arc::new(Int32Array::from(vec![10, 11])), dims)
                    .unwrap();
            dataset
                .append_variable("temperature", DataType::Int32)
                .write_array(arr)
                .unwrap();
            dataset.append_global_attribute("title", "Demo").unwrap();
        }

        partition.finish().await.unwrap();

        let dataset_path = Path::from("collections/datasets/p0/ds_a");
        let attr_reader = AttributeReader::open(store.clone(), dataset_path.clone(), "title")
            .await
            .unwrap();
        let attr = attr_reader.read_index(0).unwrap().unwrap();
        let attr_arr = attr.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(attr_arr.value(0), "Demo");

        let array_reader = ArrayReader::open(store.clone(), dataset_path.child("temperature"))
            .await
            .unwrap();
        let nd = array_reader.read_index(1).await.unwrap().unwrap();
        let values = nd.values().as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(values.values(), &[10, 11]);
    }

    #[tokio::test]
    async fn dataset_writer_reuses_variable_writer() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let collection = Path::from("collections/datasets_reuse");
        let mut partition = PartitionWriter::new(store.clone(), "p0", &collection);

        {
            let mut dataset = partition.append_dataset("ds_a");
            let dims = Dimensions::new(vec![Dimension::try_new("x", 1).unwrap()]);
            let arr1 = beacon_nd_arrow::NdArrowArray::new(
                Arc::new(Int32Array::from(vec![1])),
                dims.clone(),
            )
            .unwrap();
            let arr2 =
                beacon_nd_arrow::NdArrowArray::new(Arc::new(Int32Array::from(vec![2])), dims)
                    .unwrap();

            dataset
                .append_variable("salinity", DataType::Int32)
                .write_array(arr1)
                .unwrap();
            dataset
                .append_variable("salinity", DataType::Int32)
                .write_array(arr2)
                .unwrap();
        }

        partition.finish().await.unwrap();

        let dataset_path = Path::from("collections/datasets_reuse/p0/ds_a");
        let array_reader = ArrayReader::open(store.clone(), dataset_path.child("salinity"))
            .await
            .unwrap();
        let first = array_reader.read_index(1).await.unwrap().unwrap();
        let first_arr = first
            .values()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(first_arr.value(0), 1);

        let second = array_reader.read_index(2).await.unwrap().unwrap();
        let second_arr = second
            .values()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(second_arr.value(0), 2);
    }
}
