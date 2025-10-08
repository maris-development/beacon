use std::sync::Arc;

use arrow::array::BooleanArray;
use indexmap::IndexMap;
use nd_arrow_array::{
    NdArrowArray,
    dimensions::{Dimension, Dimensions},
};
use zarrs::array_subset::ArraySubset;

use crate::reader::ArrowGroupReader;

pub struct ArrowZarrStream {
    group_reader: Arc<crate::reader::ArrowGroupReader>,
    projected_schema: arrow::datatypes::SchemaRef,

    // Streaming State
    is_done: bool,
    dimension_lengths: IndexMap<String, u64>,
    chunk_sizes: IndexMap<String, u64>,
    chunk_indices: IndexMap<String, u64>,
}

impl ArrowZarrStream {
    pub fn new(
        group_reader: Arc<ArrowGroupReader>,
        projected_schema: arrow::datatypes::SchemaRef,
    ) -> Result<Self, String> {
        // Get all the variables
        let variables = projected_schema
            .fields()
            .iter()
            .filter_map(|f| {
                if !f.name().contains('.') {
                    Some(f.name().clone())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        // Find the variable with the most dimensions
        let max_dims_var = variables
            .iter()
            .max_by_key(|var_name| {
                group_reader
                    .arrays()
                    .get(var_name.as_str())
                    .map(|array| array.shape().len())
                    .unwrap_or(0)
            })
            .cloned();

        let mut chunk_sizes = IndexMap::new();
        let mut dimension_lengths = IndexMap::new();
        if let Some(var_name) = max_dims_var {
            let var = group_reader.arrays().get(var_name.as_str()).unwrap();
            let chunk_shape = var.chunk_grid_shape();
            if let Some(dimensions) = var.dimension_names() {
                for (i, maybe_dimension) in dimensions.iter().enumerate() {
                    if let Some(dimension) = maybe_dimension {
                        chunk_sizes.insert(dimension.clone(), chunk_shape[i]);
                        dimension_lengths.insert(dimension.clone(), var.shape()[i]);
                    } else {
                        return Err(format!(
                            "Array {} has unnamed dimensions, cannot stream.",
                            var_name
                        ));
                    }
                }
            }
        }

        let mut chunk_indices = IndexMap::new();
        for (dim_name, _) in chunk_sizes.iter() {
            chunk_indices.insert(dim_name.clone(), 0);
        }

        Ok(Self {
            group_reader,
            projected_schema,
            is_done: false,
            dimension_lengths,
            chunk_sizes,
            chunk_indices,
        })
    }

    fn advance_chunk_state(&mut self) {
        // Advance like a multi-dimensional counter
        for (dim, step) in self.chunk_indices.iter_mut() {
            if let Some(d) = self.dimension_lengths.get(dim) {
                let size = self.chunk_sizes[dim];
                *step += size;
                if *step < *d {
                    return; // still within bounds
                } else {
                    *step = 0; // reset and carry to next dimension
                }
            }
        }
        self.is_done = true; // All dimensions have been processed
    }

    fn generate_array_subset(&self, dimensions: &[String]) -> ArraySubset {
        let mut ranges = vec![];
        for dim in dimensions {
            let chunk_count = self.chunk_sizes.get(dim).cloned().unwrap_or_else(|| {
                self.dimension_lengths.get(dim).cloned().unwrap() // Default to full length
            });
            let step = self.chunk_indices.get(dim).cloned().unwrap_or(0);
            let min_chunk_count = self.dimension_lengths[dim]
                .saturating_sub(step)
                .min(chunk_count);

            ranges.push(step..step + min_chunk_count);
        }

        ArraySubset::new_with_ranges(&ranges)
    }

    fn read_attribute(&self, attribute_name: &str) -> Option<NdArrowArray> {
        self.group_reader
            .attributes()
            .get(attribute_name)
            .map(|attr| attr.as_nd_arrow_array())
    }

    async fn read_array(
        &self,
        array_name: &str,
        subset: &ArraySubset,
    ) -> Result<Option<NdArrowArray>, String> {
        let array_reader = if let Some(array) = self.group_reader.arrays().get(array_name) {
            array
        } else {
            return Ok(None);
        };

        let dimensions: Vec<nd_arrow_array::dimensions::Dimension> = array_reader
            .dimension_names()
            .as_ref()
            .unwrap()
            .iter()
            .zip(subset.shape().iter())
            .map(|(dim, &len)| Dimension {
                name: dim.clone().unwrap(),
                size: len as usize,
            })
            .collect::<Vec<_>>();
        let dimensions = Dimensions::new(dimensions);

        match array_reader.data_type() {
            zarrs::array::DataType::Bool => {
                let array = array_reader
                    .async_retrieve_array_subset_ndarray::<bool>(&subset)
                    .await
                    .map_err(|e| e.to_string())?;

                let arrow_boolean_array = BooleanArray::from(array.into_raw_vec_and_offset().0);

                NdArrowArray::new(Arc::new(arrow_boolean_array), dimensions);

                todo!()
            }
            zarrs::array::DataType::Int8 => todo!(),
            zarrs::array::DataType::Int16 => todo!(),
            zarrs::array::DataType::Int32 => todo!(),
            zarrs::array::DataType::Int64 => todo!(),
            zarrs::array::DataType::UInt8 => todo!(),
            zarrs::array::DataType::UInt16 => todo!(),
            zarrs::array::DataType::UInt32 => todo!(),
            zarrs::array::DataType::UInt64 => todo!(),
            zarrs::array::DataType::Float32 => todo!(),
            zarrs::array::DataType::Float64 => todo!(),
            zarrs::array::DataType::String => todo!(),
            zarrs::array::DataType::Bytes => todo!(),
            _ => todo!(),
        }
    }
}
