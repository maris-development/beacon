use std::collections::HashMap;

use object_store::ObjectStore;

use crate::{array::ArrayWriter, attribute::AttributeWriter};

pub struct VariableWriter<S: ObjectStore + Clone> {
    store: S,
    variable_prefix: object_store::path::Path,
    variable_array_writer: ArrayWriter<S>,
    variable_attribute_writers: HashMap<String, AttributeWriter<S>>,
    count: usize,
}

impl<S: ObjectStore + Clone> VariableWriter<S> {
    pub fn new(
        store: S,
        variable_prefix: object_store::path::Path,
        data_type: arrow::datatypes::DataType,
        pre_length: usize,
    ) -> Self {
        Self {
            variable_array_writer: ArrayWriter::new(
                store.clone(),
                variable_prefix.clone(),
                data_type,
                pre_length,
            )
            .unwrap(),
            store,
            variable_prefix,
            variable_attribute_writers: HashMap::new(),
            count: pre_length,
        }
    }

    pub fn count(&self) -> usize {
        self.count
    }
}
