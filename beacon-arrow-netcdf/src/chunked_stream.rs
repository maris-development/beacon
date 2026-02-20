use std::{collections::HashMap, pin::Pin, sync::Arc};

use arrow::datatypes::SchemaRef;
use beacon_nd_arrow::NdArrowArray;
use indexmap::IndexMap;
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

#[derive(Debug, Clone, Default, serde::Deserialize, serde::Serialize)]
pub enum Chunking {
    Auto {
        target_chunk_size: usize,
    },
    ChunkSizes(IndexMap<String, usize>),
    #[default]
    None,
}

impl Stream {
    pub fn new(
        file: Arc<netcdf::File>,
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

        if let Chunking::ChunkSizes(map) = &chunking {
            // Validate that all dimensions in hash_map are in variables
            for (dim_name, chunk_size) in map {
                let file_dim = file.dimension(dim_name).ok_or_else(|| {
                    ArrowNetCDFError::Stream(format!(
                        "Chunk size specified for dimension {} which is not in the variable dimensions.",
                        dim_name
                    ))
                })?;
                dimensions.push((dim_name.clone(), file_dim.len()));
                dim_set.insert(dim_name.clone(), file_dim.len());
            }
        }

        for variable in &variables {
            let var_dims = variable.dimensions();
            for (i, dim) in var_dims.iter().enumerate() {
                let dim_name = dim.name();
                let dim_len = dim.len();
                if i == var_dims.len() - 1 && is_string_dimension(&dim_name) {
                    // Skip string length dimensions at the end
                    continue;
                }
                if dim_set.contains_key(&dim_name) {
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
            Chunking::ChunkSizes(map) => {
                // Validate that all dimensions in hash_map are in dimensions
                for dim_name in map.keys() {
                    if !dim_set.contains_key(dim_name) {
                        return Err(ArrowNetCDFError::Stream(format!(
                            "Chunk size specified for dimension {} which is not in the dimension list.",
                            dim_name
                        )));
                    }
                }
                map
            }
            Chunking::None => {
                // By default, set chunk size to full dimension length
                dimensions
                    .iter()
                    .map(|(n, l)| (n.clone(), *l))
                    .collect::<IndexMap<String, usize>>()
            }
        };

        Ok(Self {
            chunk_step_state: chunk_sizes.keys().map(|n| (n.clone(), 0)).collect(),
            chunk_sizes: IndexMap::from_iter(chunk_sizes),
            dimension_lengths: IndexMap::from_iter(dimensions),
            file: file.clone(),
            projected_schema,
            is_done: false,
        })
    }

    fn balanced_chunk_sizes(
        dimensions: &[(String, usize)],
        target_chunk_size: usize,
    ) -> IndexMap<String, usize> {
        let total_volume: usize = dimensions.iter().map(|(_, len)| *len).product();
        if total_volume == 0 {
            return dimensions.iter().map(|(d, _)| (d.clone(), 0)).collect();
        }

        // Only count dimensions > 1 for balancing
        let n_active = dimensions.iter().filter(|(_, l)| *l > 1).count().max(1);

        // Scale factor for balancing
        let scale = (target_chunk_size as f64 / total_volume as f64).powf(1.0 / n_active as f64);

        let mut chunk_sizes = IndexMap::new();
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
            let chunk_count = self.chunk_sizes.get(dim).cloned().unwrap_or_else(|| {
                self.dimension_lengths.get(dim).cloned().unwrap() // Default to full length
            });
            let step = self.chunk_step_state.get(dim).cloned().unwrap_or(0);
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

#[derive(Debug, Clone, Copy)]
struct DimensionHyperSlab {
    start: usize,
    count: usize,
}

impl Iterator for Stream {
    type Item = NcResult<Vec<NdArrowArray>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.is_done() {
            return None;
        }

        let mut arrays = Vec::new();
        let mut fields = Vec::new();

        for field in self.projected_schema.fields() {
            fields.push(field.clone());
            let array_result = if let Some((var_name, attr_name)) = field.name().split_once('.') {
                if var_name.is_empty() {
                    // Global attribute
                    Self::read_global_attribute(&self.file, attr_name)
                } else {
                    // Variable attribute
                    match self.file.variable(var_name) {
                        Some(variable) => Self::read_variable_attribute(&variable, attr_name),
                        None => {
                            return Some(Err(ArrowNetCDFError::InvalidFieldName(
                                field.name().to_string(),
                            )))
                        }
                    }
                }
            } else {
                match self.file.variable(field.name()) {
                    Some(variable) => {
                        let var_dims: Vec<_> =
                            variable.dimensions().iter().map(|d| d.name()).collect();
                        if var_dims.is_empty() {
                            Self::read_variable_scalar(&variable)
                        } else {
                            let hyper_slab = self.generate_hyper_slab(&var_dims);
                            // println!(
                            //     "Reading variable {} with hyper slab: {:?}",
                            //     variable.name(),
                            //     hyper_slab
                            // );
                            Self::read_variable_hyper_slab(&variable, &hyper_slab)
                        }
                    }
                    None => {
                        return Some(Err(ArrowNetCDFError::InvalidFieldName(
                            field.name().to_string(),
                        )))
                    }
                }
            };

            match array_result {
                Ok(array) => arrays.push(array),
                Err(e) => return Some(Err(e)),
            }
        }

        self.advance_chunk_state();

        let arrays = 

        Some(maybe_nd_batch)
    }
}

impl futures::Stream for Stream {
    type Item = NcResult<NdRecordBatch>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = Pin::get_mut(self);
        std::task::Poll::Ready(this.next())
    }
}

#[cfg(test)]
mod tests {
    use std::vec;

    use crate::reader::NetCDFArrowReader;

    use super::*;

    #[test]
    fn test_auto_chunk_with_aligned_dimensions() {
        let reader = NetCDFArrowReader::new_with_aligned_dimensions(
            "test_files/gridded-example.nc",
            vec!["time".to_string(), "lat".to_string(), "lon".to_string()],
        )
        .unwrap();

        let stream = reader.read_as_stream::<Vec<_>>(None, None);
        assert!(stream.is_ok());
    }

    #[test]
    fn test_stream_read_auto_chunk() {
        let reader = NetCDFArrowReader::new("test_files/gridded-example.nc").unwrap();

        let temp_idx = reader.schema().index_of("analysed_sst").unwrap();
        let lat = reader.schema().index_of("lat").unwrap();
        let lon = reader.schema().index_of("lon").unwrap();

        let mut stream = reader
            .read_as_stream::<Vec<_>>(
                Some(vec![temp_idx, lat, lon]),
                Some(Chunking::Auto {
                    target_chunk_size: 128000,
                }),
            )
            .unwrap();

        let mut total_rows = 0;
        while let Some(batch_result) = stream.next() {
            match batch_result {
                Ok(batch) => {
                    let batch = batch.to_arrow_record_batch().unwrap();
                    println!("Batch: {:?}", batch);
                    total_rows += batch.num_rows();
                }
                Err(e) => {
                    eprintln!("Error reading batch: {}", e);
                }
            }
        }
        println!("Total rows read: {}", total_rows);
    }

    #[test]
    fn test_stream_read_no_chunking() {
        let reader = NetCDFArrowReader::new("test_files/gridded-example.nc").unwrap();

        let temp_idx = reader.schema().index_of("analysed_sst").unwrap();
        let lat = reader.schema().index_of("lat").unwrap();
        let lon = reader.schema().index_of("lon").unwrap();

        let mut stream = reader
            .read_as_stream::<Vec<_>>(Some(vec![temp_idx, lat, lon]), Some(Chunking::None))
            .unwrap();

        let mut total_rows = 0;
        while let Some(batch_result) = stream.next() {
            match batch_result {
                Ok(batch) => {
                    let batch = batch.to_arrow_record_batch().unwrap();
                    println!("Batch: {:?}", batch);
                    total_rows += batch.num_rows();
                }
                Err(e) => {
                    eprintln!("Error reading batch: {}", e);
                }
            }
        }
        println!("Total rows read: {}", total_rows);
    }

    #[test]
    fn test_stream_read_custom_chunking() {
        let reader = NetCDFArrowReader::new("test_files/gridded-example.nc").unwrap();

        let temp = reader.schema().index_of("analysed_sst").unwrap();
        let temp_units = reader.schema().index_of("analysed_sst.units").unwrap();
        let lat = reader.schema().index_of("lat").unwrap();
        let lon = reader.schema().index_of("lon").unwrap();
        let processing_level = reader.schema().index_of(".processing_level").unwrap();

        let mut stream = reader
            .read_as_stream::<Vec<_>>(
                Some(vec![temp, temp_units, lat, lon, processing_level]),
                Some(Chunking::ChunkSizes(
                    [("lat".to_string(), 500), ("lon".to_string(), 500)]
                        .into_iter()
                        .collect(),
                )),
            )
            .unwrap();

        let mut total_rows = 0;
        while let Some(batch_result) = stream.next() {
            match batch_result {
                Ok(batch) => {
                    let batch = batch.to_arrow_record_batch().unwrap();
                    println!("Batch: {:?}", batch);
                    total_rows += batch.num_rows();
                }
                Err(e) => {
                    eprintln!("Error reading batch: {}", e);
                }
            }
        }
        println!("Total rows read: {}", total_rows);
    }

    #[test]
    fn test_flat_read() {
        let reader = NetCDFArrowReader::new("test_files/gridded-example.nc").unwrap();

        let temp = reader.schema().index_of("analysed_sst").unwrap();
        let temp_units = reader.schema().index_of("analysed_sst.units").unwrap();
        let lat = reader.schema().index_of("lat").unwrap();
        let lon = reader.schema().index_of("lon").unwrap();
        let processing_level = reader.schema().index_of(".processing_level").unwrap();

        let mut batch = reader
            .read_as_batch::<Vec<_>>(Some(vec![temp, temp_units, lat, lon, processing_level]))
            .unwrap();

        println!("Total rows read: {}", batch.num_rows());
    }

    #[test]
    fn test_chunked_column_read() {
        let reader = NetCDFArrowReader::new("test_files/gridded-example.nc").unwrap();

        let chunking = Chunking::ChunkSizes(
            [("lat".to_string(), 500), ("lon".to_string(), 500)]
                .into_iter()
                .collect(),
        );
        let mut temp_chunked_column = reader
            .read_column_as_stream("analysed_sst", Some(chunking.clone()))
            .unwrap();

        while let Some(batch_result) = temp_chunked_column.next() {
            match batch_result {
                Ok(batch) => {
                    let batch = batch.to_arrow_record_batch().unwrap();
                    // println!("Batch: {:?}", batch);
                    println!("Batch rows: {}", batch.num_rows());
                }
                Err(e) => {
                    eprintln!("Error reading batch: {}", e);
                }
            }
        }

        let mut lon_chunked_column = reader.read_column_as_stream("lon", Some(chunking)).unwrap();

        while let Some(batch_result) = lon_chunked_column.next() {
            match batch_result {
                Ok(batch) => {
                    let batch = batch.to_arrow_record_batch().unwrap();
                    // println!("Batch: {:?}", batch);
                    println!("Batch rows: {}", batch.num_rows());
                }
                Err(e) => {
                    eprintln!("Error reading batch: {}", e);
                }
            }
        }
    }
}
