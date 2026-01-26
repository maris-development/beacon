use object_store::ObjectStore;

use crate::{
    attribute::{AttributeValue, AttributeWriter},
    partition::PartitionWriter,
    variable::VariableWriter,
};

pub struct DatasetWriter<'s, S: ObjectStore + Clone> {
    store: S,
    partition_prefix: object_store::path::Path,
    partition_writer_ref: &'s mut PartitionWriter<S>,
}

impl<'s, S: ObjectStore + Clone> DatasetWriter<'s, S> {
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

    pub fn store(&self) -> &S {
        &self.store
    }

    pub fn partition_prefix(&self) -> &object_store::path::Path {
        &self.partition_prefix
    }

    pub fn partition_writer(&mut self) -> &mut PartitionWriter<S> {
        self.partition_writer_ref
    }

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
