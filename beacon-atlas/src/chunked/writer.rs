use std::{collections::HashMap, sync::Arc};

use object_store::ObjectStore;

use crate::chunked::{
    array::{CHUNKED_ARRAY_FILE_NAME, writer::ChunkedArrayWriter},
    schema::{ChunkedAttributeValue, ChunkedDatasetSchema, ChunkedSchema, ChunkedVariableSchema},
};

pub struct ChunkedWriter<S: ObjectStore> {
    pub store: Arc<S>,
    pub chunked_dir: object_store::path::Path,
    pub datasets: Vec<String>,
    pub dataset_writers: HashMap<String, ChunkedDatasetWriter<S>>,
}

impl<S> ChunkedWriter<S>
where
    S: ObjectStore,
{
    pub const CHUNKED_DIR_NAME: &'static str = "chunked";
    pub fn new(store: Arc<S>, base_dir: object_store::path::Path) -> Self {
        let chunked_dir = base_dir.child(Self::CHUNKED_DIR_NAME);
        Self {
            datasets: Vec::new(),
            store: store.clone(),
            chunked_dir,
            dataset_writers: HashMap::new(),
        }
    }

    pub fn store(&self) -> Arc<S> {
        self.store.clone()
    }

    // Additional methods for writing chunked data would go here
    pub async fn write_chunked_dataset(
        &mut self,
        dataset_name: &str,
    ) -> &mut ChunkedDatasetWriter<S> {
        // Create writer for the chunked dataset
        let dataset_path = self.chunked_dir.child(dataset_name);
        // Implementation for creating and returning a ChunkedDatasetWriter

        let dataset_writer =
            ChunkedDatasetWriter::new(self.store.clone(), dataset_path, dataset_name);
        self.datasets.push(dataset_name.to_string());
        self.dataset_writers
            .insert(dataset_name.to_string(), dataset_writer);

        self.dataset_writers.get_mut(dataset_name).unwrap()
    }

    pub async fn finalize(self) -> Result<(), object_store::Error> {
        // Update the chunked schema JSON
        let mut dataset_schemas = Vec::new();
        for (_ds_name, ds_writer) in self.dataset_writers {
            let ds_schema = ds_writer.finalize().await;
            dataset_schemas.push(ds_schema);
        }

        let json = serde_json::to_string(&ChunkedSchema {
            datasets: dataset_schemas,
        })
        .unwrap();

        // Write the schema JSON to the object store
        let schema_path = self.chunked_dir.child("schemas.json");
        self.store
            .put(&schema_path, json.into_bytes().into())
            .await?;

        Ok(())
    }
}

pub struct ChunkedDatasetWriter<S: ObjectStore> {
    pub store: Arc<S>,
    pub dataset_path: object_store::path::Path,
    pub dataset_name: String,
    pub variables: HashMap<String, ChunkedVariableWriter<S>>,
    pub global_attributes: HashMap<String, ChunkedAttributeValue>,
}

impl<S> ChunkedDatasetWriter<S>
where
    S: ObjectStore,
{
    pub fn new(store: Arc<S>, dataset_path: object_store::path::Path, dataset_name: &str) -> Self {
        Self {
            store: store.clone(),
            dataset_path,
            dataset_name: dataset_name.to_string(),
            variables: HashMap::new(),
            global_attributes: HashMap::new(),
        }
    }

    pub fn store(&self) -> Arc<S> {
        self.store.clone()
    }

    pub async fn write_variable(
        &mut self,
        variable_name: &str,
        array_type: arrow::datatypes::DataType,
        chunk_shape: Vec<usize>,
        dimensions: Vec<String>,
        array_shape: Vec<usize>,
    ) -> &mut ChunkedVariableWriter<S> {
        // Create writer for the chunked array
        let variables_path = self.dataset_path.child("variables");
        let variable_writer = ChunkedVariableWriter::new(
            self.store.clone(),
            variables_path,
            variable_name,
            array_type,
            dimensions,
            chunk_shape,
            array_shape,
        );
        self.variables
            .insert(variable_name.to_string(), variable_writer);
        self.variables.get_mut(variable_name).unwrap()
    }

    pub async fn set_global_attribute(&mut self, key: &str, value: ChunkedAttributeValue) {
        self.global_attributes.insert(key.to_string(), value);
    }

    pub async fn finalize(self) -> ChunkedDatasetSchema {
        // Update the dataset schema JSON
        let mut variable_schemas = HashMap::new();
        for (var_name, var_writer) in self.variables {
            let var_schema = var_writer.finalize().await;
            variable_schemas.insert(var_name, var_schema);
        }

        let dataset_schema = ChunkedDatasetSchema {
            name: self.dataset_name.clone(),
            arrow_schema: arrow::datatypes::Schema::empty(),
            global_attributes: self.global_attributes.clone(),
            variables: variable_schemas,
        };

        // Write the dataset schema JSON to the object store
        let schema_path = self.dataset_path.child("schema.json");
        let json = serde_json::to_string(&dataset_schema).unwrap();
        self.store
            .put(&schema_path, json.into_bytes().into())
            .await
            .unwrap();
        dataset_schema
    }
}

pub struct ChunkedVariableWriter<S: ObjectStore> {
    pub store: Arc<S>,
    pub variable_path: object_store::path::Path,
    pub variable_name: String,
    pub data_type: arrow::datatypes::DataType,
    pub dimensions: Vec<String>,
    pub array_shape: Vec<usize>,
    pub chunk_shape: Vec<usize>,
    pub attributes: HashMap<String, ChunkedAttributeValue>,
    pub array_writer: ChunkedArrayWriter<S>,
}

impl<S: ObjectStore> ChunkedVariableWriter<S> {
    pub fn new(
        store: Arc<S>,
        variables_path: object_store::path::Path,
        variable_name: &str,
        data_type: arrow::datatypes::DataType,
        dimensions: Vec<String>,
        array_shape: Vec<usize>,
        chunk_shape: Vec<usize>,
    ) -> Self {
        let variable_path = variables_path.child(variable_name);
        Self {
            store: store.clone(),
            variable_path: variable_path.clone(),
            variable_name: variable_name.to_string(),
            data_type: data_type.clone(),
            dimensions: dimensions.clone(),
            array_shape: array_shape.clone(),
            chunk_shape: chunk_shape.clone(),
            attributes: HashMap::new(),
            array_writer: ChunkedArrayWriter::new(
                store.clone(),
                variable_path,
                data_type.clone(),
                chunk_shape.clone(),
                dimensions.clone(),
                array_shape.clone(),
            ),
        }
    }

    pub fn write_array(&mut self) -> &mut ChunkedArrayWriter<S> {
        &mut self.array_writer
    }

    pub fn set_attribute(&mut self, key: &str, value: ChunkedAttributeValue) {
        self.attributes.insert(key.to_string(), value);
    }

    pub fn store(&self) -> Arc<S> {
        self.store.clone()
    }

    pub async fn finalize(self) -> ChunkedVariableSchema {
        // Update the dataset schema JSON
        let variable_schema = ChunkedVariableSchema {
            name: self.variable_name.clone(),
            data_type: self.data_type.clone(),
            shape: self.array_shape.clone(),
            dimensions: self.dimensions.clone(),
            chunked_shape: self.chunk_shape.clone(),
            attributes: self.attributes.clone(),
        };

        self.array_writer.finalize().await.unwrap();

        variable_schema
    }
}
