use std::{collections::HashMap, panic, pin::Pin, sync::Arc};

use arrow::datatypes::SchemaRef;
use indexmap::IndexMap;
use nd_arrow_array::{batch::NdRecordBatch, NdArrowArray};
use netcdf::Variable;

use crate::{
    error::ArrowNetCDFError,
    reader::{global_attribute, read_variable, variable_attribute},
    NcResult,
};

fn is_string_dimension(dimension_name: &str) -> bool {
    let dimension_name = dimension_name.to_lowercase();
    dimension_name.starts_with("string")
        || dimension_name.starts_with("strlen")
        || dimension_name.starts_with("strnlen")
}

pub struct Stream {
    chunk_sizes: IndexMap<String, usize>,
    chunk_step_state: IndexMap<String, usize>,
    dimension_lengths: IndexMap<String, usize>,
    file: Arc<netcdf::File>,
    projected_schema: SchemaRef,
    is_done: bool,
}

#[derive(Debug, Clone, Default)]
pub enum Chunking {
    Auto {
        target_chunk_size: usize,
    },
    ChunkSizes(HashMap<String, usize>),
    #[default]
    None,
}

impl Stream {
    pub fn new(
        file: netcdf::File,
        chunking: Option<Chunking>,
        projected_schema: SchemaRef,
    ) -> NcResult<Self> {
        let chunking = chunking.unwrap_or_default();

        // Get all the dimensions used by the variables in the projected schema
        let variables = projected_schema
            .fields()
            .iter()
            .filter_map(|f| {
                if f.name().contains('.') {
                    None
                } else {
                    file.variable(f.name())
                }
            })
            .collect::<Vec<Variable>>();

        // For each variable, get its dimensions and lengths. Create a unique list of dimensions.
        let mut dimensions: Vec<(String, usize)> = Vec::new();
        let mut dim_set: HashMap<String, usize> = HashMap::new();

        for variable in &variables {
            let var_dims = variable.dimensions();
            for (i, dim) in var_dims.iter().enumerate() {
                let dim_name = dim.name();
                let dim_len = dim.len();
                if i == var_dims.len() - 1 && is_string_dimension(&dim_name) {
                    // Skip string length dimensions at the end
                    continue;
                }
                dimensions.push((dim_name.clone(), dim_len));
                dim_set.insert(dim_name, dim_len);
            }
        }

        // Validate that each variable shares the same dimensions or consists of only 1 dimension in the list
        for variable in &variables {
            let var_dims = variable.dimensions();
            if var_dims.len() > 1 {
                if var_dims.len() == dimensions.len() {
                    for dim in var_dims {
                        let dim_name = dim.name();
                        if !dim_set.contains_key(&dim_name) {
                            return Err(ArrowNetCDFError::Stream(format!(
                                "Variable {} has dimension {} which is not in the dimension list.",
                                variable.name(),
                                dim_name
                            )));
                        }
                    }
                } else {
                    return Err(ArrowNetCDFError::Stream(format!(
                        "Variable {} has dimensions {:?} which do not match the overall dimension list {:?}.",
                        variable.name(),
                        var_dims.iter().map(|d| d.name()).collect::<Vec<_>>(),
                        dimensions.iter().map(|(n, _)| n).collect::<Vec<_>>(),
                    )));
                }
            } else if var_dims.len() == 1 {
                let dim_name = var_dims[0].name();
                if !dim_set.contains_key(&dim_name) {
                    return Err(ArrowNetCDFError::Stream(format!(
                        "Variable {} has dimension {} which is not in the dimension list.",
                        variable.name(),
                        dim_name
                    )));
                }
            } // Scalars are always valid
        }

        let chunk_sizes = match chunking {
            Chunking::Auto { target_chunk_size } => {
                Self::balanced_chunk_sizes(&dimensions, target_chunk_size)
            }
            Chunking::ChunkSizes(hash_map) => {
                // Validate that all dimensions in hash_map are in dimensions
                for dim_name in hash_map.keys() {
                    if !dim_set.contains_key(dim_name) {
                        return Err(ArrowNetCDFError::Stream(format!(
                            "Chunk size specified for dimension {} which is not in the dimension list.",
                            dim_name
                        )));
                    }
                }
                hash_map
            }
            Chunking::None => {
                // By default, set chunk size to full dimension length
                dimensions
                    .iter()
                    .map(|(n, l)| (n.clone(), *l))
                    .collect::<HashMap<String, usize>>()
            }
        };

        Ok(Self {
            chunk_step_state: chunk_sizes.keys().map(|n| (n.clone(), 0)).collect(),
            chunk_sizes: IndexMap::from_iter(chunk_sizes),
            dimension_lengths: IndexMap::from_iter(dimensions),
            file: Arc::new(file),
            projected_schema,
            is_done: false,
        })
    }

    fn balanced_chunk_sizes(
        dimensions: &[(String, usize)],
        target_chunk_size: usize,
    ) -> HashMap<String, usize> {
        let total_volume: usize = dimensions.iter().map(|(_, len)| *len).product();
        if total_volume == 0 {
            return dimensions.iter().map(|(d, _)| (d.clone(), 0)).collect();
        }

        // Only count dimensions > 1 for balancing
        let n_active = dimensions.iter().filter(|(_, l)| *l > 1).count().max(1);

        // Scale factor for balancing
        let scale = (target_chunk_size as f64 / total_volume as f64).powf(1.0 / n_active as f64);

        let mut chunk_sizes = HashMap::new();
        for (dim_name, len) in dimensions {
            let chunk_size = if *len <= 1 {
                1
            } else {
                let est = (*len as f64 * scale).ceil() as usize;
                est.clamp(1, *len)
            };
            chunk_sizes.insert(dim_name.clone(), chunk_size);
        }

        chunk_sizes
    }

    fn is_done(&self) -> bool {
        self.is_done
    }

    fn advance_chunk_state(&mut self) {
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
        self.is_done = true; // All dimensions have been processed
    }

    fn generate_hyper_slab(&self, dimensions: &[String]) -> Vec<DimensionHyperSlab> {
        let mut hyper_slab = Vec::new();
        for dim in dimensions {
            let chunk_count = self.chunk_sizes[dim];
            let step = self.chunk_step_state[dim];
            let min_chunk_count = self.dimension_lengths[dim]
                .saturating_sub(step)
                .min(chunk_count);

            hyper_slab.push(DimensionHyperSlab {
                start: step,
                count: min_chunk_count,
            });
        }
        hyper_slab
    }

    fn read_variable_scalar(variable: &Variable) -> NcResult<NdArrowArray> {
        let values = read_variable(variable, None)?;
        let array = values.into_nd_arrow_array().unwrap();
        Ok(array)
    }

    fn read_variable_attribute(
        variable: &Variable,
        attribute_name: &str,
    ) -> NcResult<NdArrowArray> {
        let variable_attribute = variable_attribute(variable, attribute_name)?;
        if let Some(attr) = variable_attribute {
            Ok(attr.into_nd_arrow_array().unwrap())
        } else {
            Err(ArrowNetCDFError::Stream(format!(
                "Attribute {} not found for variable {}",
                attribute_name,
                variable.name()
            )))
        }
    }

    fn read_global_attribute(file: &netcdf::File, attribute_name: &str) -> NcResult<NdArrowArray> {
        let global_attribute = global_attribute(file, attribute_name)?;
        if let Some(attr) = global_attribute {
            Ok(attr.into_nd_arrow_array().unwrap())
        } else {
            Err(ArrowNetCDFError::Stream(format!(
                "Global attribute {} not found",
                attribute_name
            )))
        }
    }

    fn read_variable_hyper_slab(
        variable: &Variable,
        hyper_slab: &[DimensionHyperSlab],
    ) -> NcResult<NdArrowArray> {
        let mut start: Vec<usize> = Vec::new();
        let mut count: Vec<usize> = Vec::new();

        for dim_slab in hyper_slab {
            start.push(dim_slab.start);
            count.push(dim_slab.count);
        }

        let values = read_variable(variable, Some((start, count)))?;
        let array = values.into_nd_arrow_array().unwrap();

        Ok(array)
    }
}

struct DimensionHyperSlab {
    start: usize,
    count: usize,
}

impl Iterator for Stream {
    type Item = NcResult<NdRecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.is_done() {
            return None;
        }

        let mut fields = Vec::new();
        let mut arrays = Vec::new();

        for field in self.projected_schema.fields() {
            // Check if field is an attribute (contains a dot)
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
                // Build start/count slices for hyper_slab
                let mut start: Vec<usize> = Vec::new();
                let mut count: Vec<usize> = Vec::new();

                let var_dims = var
                    .dimensions()
                    .iter()
                    .map(|d| d.name())
                    .collect::<Vec<_>>();
                let hyper_slab = self.generate_hyper_slab(&var_dims);
                for dim_slab in hyper_slab {
                    start.push(dim_slab.start);
                    count.push(dim_slab.count);
                }

                let values = read_variable(&var, Some((start, count))).unwrap();
                let array = values.into_nd_arrow_array().unwrap();
                fields.push(field.clone());
                arrays.push(array);
            }
        }

        self.advance_chunk_state();

        let nd_batch = nd_arrow_array::batch::NdRecordBatch::new(
            fields.into_iter().map(|f| f.as_ref().clone()).collect(),
            arrays,
        )
        .unwrap();

        Some(Ok(nd_batch))
    }
}

impl futures::Stream for Stream {
    type Item = NcResult<NdRecordBatch>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = Pin::get_mut(self);
        std::task::Poll::Ready(this.next())
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::Schema;

    use super::*;

    #[test]
    fn test_auto_chunk() {
        let chunks = ChunkedNetCDFArrowReader::balanced_chunk_sizes(
            &[("lat".to_string(), 500), ("lon".to_string(), 1280000)],
            128000,
        );
        println!("Chunk sizes: {:?}", chunks);
    }

    #[test]
    fn test_stream_advance() {
        let file = netcdf::open("test_files/gridded-example.nc").unwrap();
        let mut chunk_sizes = IndexMap::new();

        chunk_sizes.insert("time".to_string(), 1);
        chunk_sizes.insert("lat".to_string(), 400);
        chunk_sizes.insert("lon".to_string(), 400);

        // let mut reader = Stream::new(file, Arc::new(Schema::empty()), chunk_sizes);

        // println!("State: {:?}", reader.chunk_step_state);
        // reader.advance_state();
        // println!("State: {:?}", reader.chunk_step_state);
        // reader.advance_state();
        // println!("State: {:?}", reader.chunk_step_state);
        // reader.advance_state();
        // println!("State: {:?}", reader.chunk_step_state);
        // reader.advance_state();
        // println!("State: {:?}", reader.chunk_step_state);
        // reader.advance_state();
        // println!("State: {:?}", reader.chunk_step_state);
        // reader.advance_state();
        // println!("State: {:?}", reader.chunk_step_state);
        // reader.advance_state();
        // println!("State: {:?}", reader.chunk_step_state);
        // reader.advance_state();
        // println!("State: {:?}", reader.chunk_step_state);
        // reader.advance_state();
        // println!("State: {:?}", reader.chunk_step_state);
        // reader.advance_state();
        // println!("State: {:?}", reader.chunk_step_state);
        // reader.advance_state();
        // println!("State: {:?}", reader.chunk_step_state);
        // reader.advance_state();
        // println!("State: {:?}", reader.chunk_step_state);
        // reader.advance_state();
        // println!("State: {:?}", reader.chunk_step_state);
        // reader.advance_state();
        // println!("State: {:?}", reader.chunk_step_state);
        // reader.advance_state();
        // println!("State: {:?}", reader.chunk_step_state);
        // reader.advance_state();
        // println!("State: {:?}", reader.chunk_step_state);
        // reader.advance_state();
        // println!("State: {:?}", reader.chunk_step_state);
        // reader.advance_state();
        // println!("State: {:?}", reader.chunk_step_state);
        // reader.advance_state();
        // println!("State: {:?}", reader.chunk_step_state);
        // println!("Is done: {}", reader.is_done());
        // reader.advance_state();
        // println!("State: {:?}", reader.chunk_step_state);
        // reader.advance_state();
        // println!("State: {:?}", reader.chunk_step_state);
        // reader.advance_state();
        // println!("State: {:?}", reader.chunk_step_state);
        // reader.advance_state();
        // println!("State: {:?}", reader.chunk_step_state);
        // println!("Is done: {}", reader.is_done());
    }
}
