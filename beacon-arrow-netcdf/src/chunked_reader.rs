use std::{collections::HashMap, panic, path::Path, sync::Arc};

use arrow::{array::ArrayRef, datatypes::SchemaRef};
use indexmap::IndexMap;
use nd_arrow_array::batch::NdRecordBatch;
use netcdf::Variable;

use crate::{
    error::ArrowNetCDFError,
    reader::{arrow_schema, global_attribute, read_variable, variable_attribute},
    NcResult,
};

pub struct ChunkedNetCDFArrowReader {
    file: Arc<netcdf::File>,
    file_schema: SchemaRef,
    chunk_sizes: HashMap<String, usize>,
}

impl ChunkedNetCDFArrowReader {
    pub fn new<P: AsRef<Path>>(path: P, chunks: HashMap<String, usize>) -> NcResult<Self> {
        let file = netcdf::open(path)?;
        let file_schema = Arc::new(arrow_schema(&file)?);

        // go through all the fields in the schema that don't contain a '.'
        // Then check if the field is a scalar, or if it contains one of the chunked dimensions or all of them
        let mut chunked_fields = Vec::new();
        let mut scalar_fields = Vec::new();

        for field in file_schema.fields() {
            if !field.name().contains('.') {
                // Get the dimension for that variable
                let variable = file.variable(field.name()).unwrap();
                let dimensions = variable.dimensions();

                match dimensions.len() {
                    0 => scalar_fields.push(field.clone()),
                    1 => {
                        let dimension_name = dimensions[0].name();
                        if chunks.contains_key(&dimension_name) {
                            chunked_fields.push(field.clone());
                        }
                    }
                    _ => {
                        // All the dimension names should be in the chunked dimensions map
                        let dimension_names =
                            dimensions.iter().map(|d| d.name()).collect::<Vec<_>>();
                        if dimension_names.iter().all(|name| chunks.contains_key(name)) {
                            chunked_fields.push(field.clone());
                        }
                    }
                }
            } else {
                // Append to scalar_fields
                scalar_fields.push(field.clone());
            }
        }

        todo!()
    }

    pub fn read_chunked(&self, projection: Option<Vec<usize>>) -> NcResult<ChunkedStream> {
        todo!()
    }
}

pub struct ChunkedStream {
    chunk_sizes: IndexMap<String, usize>,
    chunk_step_state: IndexMap<String, usize>,
    file: Arc<netcdf::File>,
    projected_schema: SchemaRef,
}

impl ChunkedStream {
    pub fn new(
        file: netcdf::File,
        projected_schema: SchemaRef,
        chunk_sizes: IndexMap<String, usize>,
    ) -> Self {
        Self {
            chunk_step_state: chunk_sizes.iter().map(|c| (c.0.clone(), 0)).collect(),
            chunk_sizes,
            file: Arc::new(file),
            projected_schema,
        }
    }

    pub fn read_next(&mut self) -> Option<NcResult<NdRecordBatch>> {
        if self.is_done() {
            return None;
        }

        let mut fields = Vec::new();
        let mut arrays = Vec::new();

        for field in self.projected_schema.fields() {
            if field.name().contains('.') {
                // It is a attribute
                let parts = field.name().split('.').collect::<Vec<_>>();
                if parts.len() != 2 {
                    return Some(Err(ArrowNetCDFError::InvalidFieldName(
                        field.name().to_string(),
                    )));
                }
                if parts[0].is_empty() {
                    //Global attribute
                    let attr_name = parts[1];
                    let attr_value = global_attribute(&self.file, attr_name)
                        .unwrap()
                        .expect("Attribute not found but was in schema.");

                    fields.push(field.clone());
                    arrays.push(attr_value.into_nd_arrow_array().unwrap());
                } else {
                    //Variable attribute
                    let variable = self
                        .file
                        .variable(parts[0])
                        .expect("Variable not found but was in schema.");
                    let nd_array = variable_attribute(&variable, parts[1])
                        .unwrap()
                        .expect("Attribute not found but was in schema.")
                        .into_nd_arrow_array()
                        .unwrap();
                    fields.push(field.clone());
                    arrays.push(nd_array);
                }
            } else {
                let var = self.file.variable(field.name()).unwrap();
                // Build start/count slices for hyperslab
                let mut start: Vec<usize> = Vec::new();
                let mut count: Vec<usize> = Vec::new();

                for dim in var.dimensions() {
                    let dim_name = dim.name();
                    let dim_len = dim.len();

                    if let Some(&chunk_size) = self.chunk_sizes.get(&dim_name) {
                        let offset = *self.chunk_step_state.get(&dim_name).unwrap_or(&0);
                        start.push(offset);
                        count.push(chunk_size.min(dim_len - offset));
                    } else {
                        panic!("Chunk size not found for dimension {}", dim_name);
                    }
                }

                let values = read_variable(&var, Some((start, count))).unwrap();
                let array = values.into_nd_arrow_array().unwrap();
                fields.push(field.clone());
                arrays.push(array);
            }
        }

        self.advance_state();

        let nd_batch = nd_arrow_array::batch::NdRecordBatch::new(
            fields.into_iter().map(|f| f.as_ref().clone()).collect(),
            arrays,
        )
        .unwrap();

        Some(Ok(nd_batch))
    }

    fn is_done(&self) -> bool {
        // Done if every dimension’s offset has reached/exceeded its length
        self.chunk_step_state.iter().all(|(dim, &offset)| {
            if let Some(d) = self.file.dimension(dim) {
                offset >= d.len()
            } else {
                true
            }
        })
    }

    fn advance_state(&mut self) {
        // Advance like a multi-dimensional counter
        for (dim, step) in self.chunk_step_state.iter_mut() {
            if let Some(d) = self.file.dimension(dim) {
                let size = self.chunk_sizes[dim];
                *step += size;
                if *step < d.len() {
                    return; // still within bounds
                } else {
                    *step = 0; // reset and carry to next dimension
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::Schema;

    use super::*;

    #[test]
    fn test_stream_advance() {
        let file = netcdf::open("test_files/gridded-example.nc").unwrap();
        let mut chunk_sizes = IndexMap::new();

        chunk_sizes.insert("time".to_string(), 1);
        chunk_sizes.insert("lat".to_string(), 400);
        chunk_sizes.insert("lon".to_string(), 400);

        let mut reader = ChunkedStream::new(file, Arc::new(Schema::empty()), chunk_sizes);

        println!("State: {:?}", reader.chunk_step_state);
        reader.advance_state();
        println!("State: {:?}", reader.chunk_step_state);
        reader.advance_state();
        println!("State: {:?}", reader.chunk_step_state);
        reader.advance_state();
        println!("State: {:?}", reader.chunk_step_state);
        reader.advance_state();
        println!("State: {:?}", reader.chunk_step_state);
        reader.advance_state();
        println!("State: {:?}", reader.chunk_step_state);
        reader.advance_state();
        println!("State: {:?}", reader.chunk_step_state);
        reader.advance_state();
        println!("State: {:?}", reader.chunk_step_state);
        reader.advance_state();
        println!("State: {:?}", reader.chunk_step_state);
        reader.advance_state();
        println!("State: {:?}", reader.chunk_step_state);
        reader.advance_state();
        println!("State: {:?}", reader.chunk_step_state);
        reader.advance_state();
        println!("State: {:?}", reader.chunk_step_state);
        reader.advance_state();
        println!("State: {:?}", reader.chunk_step_state);
        println!("State: {:?}", reader.chunk_step_state);
        reader.advance_state();
        println!("State: {:?}", reader.chunk_step_state);
        reader.advance_state();
        println!("State: {:?}", reader.chunk_step_state);
        reader.advance_state();
        println!("State: {:?}", reader.chunk_step_state);
        reader.advance_state();
        println!("State: {:?}", reader.chunk_step_state);
        reader.advance_state();
        println!("State: {:?}", reader.chunk_step_state);
        reader.advance_state();
        println!("State: {:?}", reader.chunk_step_state);
    }
}
