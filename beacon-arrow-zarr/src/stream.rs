use std::{
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    task::Poll,
};

use arrow::array::BooleanArray;
use futures::{Future, Stream};
use indexmap::IndexMap;
use nd_arrow_array::{
    NdArrowArray,
    batch::NdRecordBatch,
    dimensions::{Dimension, Dimensions},
};
use zarrs::array_subset::ArraySubset;

use crate::{decoder::Decoder, reader::ArrowGroupReader};

#[derive(Clone)]
pub struct ArrowZarrStreamComposer {
    group_reader: Arc<crate::reader::ArrowGroupReader>,
    projected_schema: arrow::datatypes::SchemaRef,
    decoders: Vec<Arc<dyn Decoder>>,
    // Streaming State
    state: Arc<parking_lot::Mutex<ArrowZarrStreamState>>,
}

impl ArrowZarrStreamComposer {
    pub fn new(
        group_reader: Arc<ArrowGroupReader>,
        projected_schema: arrow::datatypes::SchemaRef,
        decoders: Vec<Arc<dyn Decoder>>,
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
            chunk_indices.insert(dim_name.clone(), Arc::new(AtomicU64::new(0)));
        }

        Ok(Self {
            decoders,
            group_reader,
            projected_schema,
            state: todo!(),
        })
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
                    .async_retrieve_array_subset_ndarray::<bool>(subset)
                    .await
                    .map_err(|e| e.to_string())?;

                let arrow_boolean_array = BooleanArray::from(array.into_raw_vec_and_offset().0);

                let nd_array = NdArrowArray::new(Arc::new(arrow_boolean_array), dimensions)
                    .map_err(|e| e.to_string())?;
                Ok(Some(nd_array))
            }
            zarrs::array::DataType::Int8 => {
                let array = array_reader
                    .async_retrieve_array_subset_ndarray::<i8>(subset)
                    .await
                    .map_err(|e| e.to_string())?;

                let arrow_int8_array =
                    arrow::array::Int8Array::from(array.into_raw_vec_and_offset().0);

                let nd_array = NdArrowArray::new(Arc::new(arrow_int8_array), dimensions)
                    .map_err(|e| e.to_string())?;
                Ok(Some(nd_array))
            }
            zarrs::array::DataType::Int16 => {
                let array = array_reader
                    .async_retrieve_array_subset_ndarray::<i16>(subset)
                    .await
                    .map_err(|e| e.to_string())?;

                let arrow_int16_array =
                    arrow::array::Int16Array::from(array.into_raw_vec_and_offset().0);

                let nd_array = NdArrowArray::new(Arc::new(arrow_int16_array), dimensions)
                    .map_err(|e| e.to_string())?;
                Ok(Some(nd_array))
            }
            zarrs::array::DataType::Int32 => {
                let array = array_reader
                    .async_retrieve_array_subset_ndarray::<i32>(subset)
                    .await
                    .map_err(|e| e.to_string())?;

                let arrow_int32_array =
                    arrow::array::Int32Array::from(array.into_raw_vec_and_offset().0);

                let nd_array = NdArrowArray::new(Arc::new(arrow_int32_array), dimensions)
                    .map_err(|e| e.to_string())?;
                Ok(Some(nd_array))
            }
            zarrs::array::DataType::Int64 => {
                let array = array_reader
                    .async_retrieve_array_subset_ndarray::<i64>(subset)
                    .await
                    .map_err(|e| e.to_string())?;

                let arrow_int64_array =
                    arrow::array::Int64Array::from(array.into_raw_vec_and_offset().0);

                let nd_array = NdArrowArray::new(Arc::new(arrow_int64_array), dimensions)
                    .map_err(|e| e.to_string())?;
                Ok(Some(nd_array))
            }
            zarrs::array::DataType::UInt8 => {
                let array = array_reader
                    .async_retrieve_array_subset_ndarray::<u8>(subset)
                    .await
                    .map_err(|e| e.to_string())?;

                let arrow_uint8_array =
                    arrow::array::UInt8Array::from(array.into_raw_vec_and_offset().0);

                let nd_array = NdArrowArray::new(Arc::new(arrow_uint8_array), dimensions)
                    .map_err(|e| e.to_string())?;
                Ok(Some(nd_array))
            }
            zarrs::array::DataType::UInt16 => {
                let array = array_reader
                    .async_retrieve_array_subset_ndarray::<u16>(subset)
                    .await
                    .map_err(|e| e.to_string())?;

                let arrow_uint16_array =
                    arrow::array::UInt16Array::from(array.into_raw_vec_and_offset().0);

                let nd_array = NdArrowArray::new(Arc::new(arrow_uint16_array), dimensions)
                    .map_err(|e| e.to_string())?;
                Ok(Some(nd_array))
            }
            zarrs::array::DataType::UInt32 => {
                let array = array_reader
                    .async_retrieve_array_subset_ndarray::<u32>(subset)
                    .await
                    .map_err(|e| e.to_string())?;

                let arrow_uint32_array =
                    arrow::array::UInt32Array::from(array.into_raw_vec_and_offset().0);

                let nd_array = NdArrowArray::new(Arc::new(arrow_uint32_array), dimensions)
                    .map_err(|e| e.to_string())?;
                Ok(Some(nd_array))
            }
            zarrs::array::DataType::UInt64 => {
                let array = array_reader
                    .async_retrieve_array_subset_ndarray::<u64>(subset)
                    .await
                    .map_err(|e| e.to_string())?;

                let arrow_uint64_array =
                    arrow::array::UInt64Array::from(array.into_raw_vec_and_offset().0);

                let nd_array = NdArrowArray::new(Arc::new(arrow_uint64_array), dimensions)
                    .map_err(|e| e.to_string())?;
                Ok(Some(nd_array))
            }
            zarrs::array::DataType::Float32 => {
                let array = array_reader
                    .async_retrieve_array_subset_ndarray::<f32>(subset)
                    .await
                    .map_err(|e| e.to_string())?;

                let arrow_float32_array =
                    arrow::array::Float32Array::from(array.into_raw_vec_and_offset().0);

                let nd_array = NdArrowArray::new(Arc::new(arrow_float32_array), dimensions)
                    .map_err(|e| e.to_string())?;
                Ok(Some(nd_array))
            }
            zarrs::array::DataType::Float64 => {
                let array = array_reader
                    .async_retrieve_array_subset_ndarray::<f64>(subset)
                    .await
                    .map_err(|e| e.to_string())?;

                let arrow_float64_array =
                    arrow::array::Float64Array::from(array.into_raw_vec_and_offset().0);

                let nd_array = NdArrowArray::new(Arc::new(arrow_float64_array), dimensions)
                    .map_err(|e| e.to_string())?;
                Ok(Some(nd_array))
            }
            zarrs::array::DataType::String => {
                let array = array_reader
                    .async_retrieve_array_subset_ndarray::<String>(subset)
                    .await
                    .map_err(|e| e.to_string())?;

                let arrow_string_array =
                    arrow::array::StringArray::from(array.into_raw_vec_and_offset().0);

                let nd_array = NdArrowArray::new(Arc::new(arrow_string_array), dimensions)
                    .map_err(|e| e.to_string())?;
                Ok(Some(nd_array))
            }
            zarrs::array::DataType::Bytes => {
                let array = array_reader
                    .async_retrieve_array_subset_ndarray::<Vec<u8>>(subset)
                    .await
                    .map_err(|e| e.to_string())?;

                let arrow_binary_array =
                    arrow::array::BinaryArray::from_iter_values(array.into_raw_vec_and_offset().0);

                let nd_array = NdArrowArray::new(Arc::new(arrow_binary_array), dimensions)
                    .map_err(|e| e.to_string())?;
                Ok(Some(nd_array))
            }
            zarr_data_type => Err(format!("Unsupported Zarrs data type: {:?}", zarr_data_type)),
        }
    }

    pub fn next_state(&self) -> ArrowZarrStreamState {
        todo!()
    }
}

#[derive(Debug, Clone)]
pub struct ArrowZarrStreamState {
    is_done: bool,
    dimension_lengths: Arc<IndexMap<String, u64>>,
    chunk_sizes: Arc<IndexMap<String, u64>>,
    chunk_indices: IndexMap<String, u64>,
}

impl ArrowZarrStreamState {
    pub fn new(
        dimension_lengths: IndexMap<String, u64>,
        chunk_sizes: IndexMap<String, u64>,
    ) -> Self {
        let mut chunk_indices = IndexMap::new();
        for (dim_name, _) in chunk_sizes.iter() {
            chunk_indices.insert(dim_name.clone(), 0);
        }

        Self {
            is_done: false,
            dimension_lengths: Arc::new(dimension_lengths),
            chunk_sizes: Arc::new(chunk_sizes),
            chunk_indices,
        }
    }

    fn is_done(&self) -> bool {
        self.is_done
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
        self.is_done = true; // All dimensions exhausted
    }

    fn generate_array_subset(&self, dimensions: &[String]) -> ArraySubset {
        let mut ranges = vec![];
        for dim in dimensions {
            let chunk_count = self.chunk_sizes.get(dim).cloned().unwrap_or_else(|| {
                self.dimension_lengths.get(dim).cloned().unwrap() // Default to full length
            });
            let step = self.chunk_indices.get(dim).map(|s| *s).unwrap_or(0);
            let min_chunk_count = self.dimension_lengths[dim]
                .saturating_sub(step)
                .min(chunk_count);

            ranges.push(step..step + min_chunk_count);
        }

        ArraySubset::new_with_ranges(&ranges)
    }
}

#[pin_project::pin_project]
pub struct ArrowZarrStream {
    composer: Arc<ArrowZarrStreamComposer>,
    // Pinned state
    #[pin]
    ongoing: Option<Pin<Box<dyn Future<Output = Option<Result<NdRecordBatch, String>>> + Send>>>,
}

impl ArrowZarrStream {
    pub async fn next_chunk(
        composer: Arc<ArrowZarrStreamComposer>,
    ) -> Option<Result<NdRecordBatch, String>> {
        let state = composer.next_state();
        if state.is_done() {
            return None;
        }

        let mut arrays = vec![];
        let mut fields = vec![];
        for column in composer.projected_schema.fields().iter() {
            let array_name = column.name();
            fields.push(column.as_ref().clone());

            composer.read_array(array_name, subset)
        }

        todo!()
    }
}

impl Stream for ArrowZarrStream {
    type Item = Result<NdRecordBatch, String>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let mut pinned_stream = self.project();
        let composer = pinned_stream.composer.clone();

        if pinned_stream.ongoing.is_none() {
            *pinned_stream.ongoing = Some(Box::pin(Self::next_chunk(composer)) as _);
        }

        let fut = pinned_stream.ongoing.as_mut().as_pin_mut().unwrap();
        match fut.poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Some(value)) => {
                *pinned_stream.ongoing = None;
                Poll::Ready(Some(value))
            }
            Poll::Ready(None) => Poll::Ready(None),
        }
    }
}
