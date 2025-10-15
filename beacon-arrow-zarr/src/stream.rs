use std::{collections::HashMap, pin::Pin, sync::Arc, task::Poll};

use futures::{Future, Stream};
use indexmap::IndexMap;
use nd_arrow_array::{NdArrowArray, batch::NdRecordBatch};
use zarrs::{array::ChunkGrid, array_subset::ArraySubset};

use crate::{array_slice_pushdown::ArraySlicePushDown, reader::AsyncArrowZarrGroupReader};

pub type ArrowZarrStreamComposerRef = Arc<ArrowZarrStreamComposer>;

pub struct ArrowZarrStreamComposer {
    group_reader: crate::reader::AsyncArrowZarrGroupReader,
    projected_schema: arrow::datatypes::SchemaRef,
    // Streaming State
    chunk_grid: Option<ChunkGrid>,
    dimension_names: Vec<String>,
    dimension_sizes: Vec<u64>,
    readable_chunks: crossbeam::queue::SegQueue<ChunkIndices>,
    array_slice_pushdowns: Option<HashMap<String, ArraySlicePushDown>>,
}

impl ArrowZarrStreamComposer {
    pub fn new(
        group_reader: AsyncArrowZarrGroupReader,
        projected_schema: arrow::datatypes::SchemaRef,
        array_slice_pushdowns: Option<HashMap<String, ArraySlicePushDown>>,
    ) -> Result<ArrowZarrStreamComposerRef, String> {
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

        let mut chunk_grid = None;
        let mut dimension_names = vec![];
        let mut dimension_sizes = vec![];
        let queue = crossbeam::queue::SegQueue::new();

        if let Some(var_name) = max_dims_var {
            let var = group_reader.arrays().get(var_name.as_str()).unwrap();
            let chunk_shape = var.chunk_grid();
            let var_dimension_names = var
                .dimension_names()
                .as_ref()
                .unwrap()
                .iter()
                .map(|d| d.clone().unwrap())
                .collect::<Vec<_>>();

            chunk_grid = Some(chunk_shape.clone());
            dimension_names = var_dimension_names;
            dimension_sizes = var.shape().to_vec();
        }

        if let Some(chunk_grid) = &chunk_grid {
            // Generate all chunk indices
            for chunk_indices in chunk_grid.iter_chunk_indices() {
                queue.push(chunk_indices);
            }
        } else {
            // We should at least have one chunk to read, we can add an empty as it should only contain scalar attributes and arrays
            queue.push(Vec::new());
        }

        Ok(Arc::new(Self {
            group_reader,
            projected_schema,
            chunk_grid,
            dimension_names,
            dimension_sizes,
            readable_chunks: queue,
            array_slice_pushdowns,
        }))
    }

    pub fn generate_array_subset(&self, chunk_indices: &ChunkIndices) -> ArraySubset {
        match self.chunk_grid.as_ref() {
            Some(chunk_grid) => chunk_grid.subset(chunk_indices).unwrap().unwrap(),
            None => ArraySubset::new_empty(0), // Scalar array
        }
    }

    pub fn pollable_shared_stream(self: &Arc<Self>) -> ArrowZarrStream {
        ArrowZarrStream {
            composer: self.clone(),
            ongoing: None,
        }
    }
}

pub type ChunkIndices = Vec<u64>;

type ArrowZarrStreamFuture =
    Pin<Box<dyn Future<Output = Option<Result<NdRecordBatch, String>>> + Send>>;

#[pin_project::pin_project]
pub struct ArrowZarrStream {
    composer: Arc<ArrowZarrStreamComposer>,
    // Pinned state
    #[pin]
    ongoing: Option<ArrowZarrStreamFuture>,
}

impl ArrowZarrStream {
    pub async fn next_chunk(
        composer: Arc<ArrowZarrStreamComposer>,
    ) -> Option<Result<NdRecordBatch, String>> {
        let chunk_indices = match composer.readable_chunks.pop() {
            Some(chunk_indices) => chunk_indices,
            None => return None, // Queue is empty
        };
        let array_subset = composer.generate_array_subset(&chunk_indices);

        // Blend dimension_names and array_subset ranges into a map (this will wrap ranges that exceed dimension sizes)
        let mut dimension_subset_ranges: IndexMap<String, std::ops::Range<u64>> = composer
            .dimension_names
            .iter()
            .zip(array_subset.to_ranges())
            .zip(composer.dimension_sizes.iter())
            .map(|((name, range), dim_size)| {
                if range.end > *dim_size {
                    (name.clone(), range.start..*dim_size)
                } else {
                    (name.clone(), range)
                }
            })
            .collect();
        if let Some(pushdowns) = &composer.array_slice_pushdowns {
            // Apply array slice pushdowns
            for (dim_name, pushdown) in pushdowns.iter() {
                if let Some(range) = dimension_subset_ranges.get_mut(dim_name) {
                    if let Some(overlap) =
                        pushdown.overlapping_range(range.start as usize..range.end as usize)
                    {
                        *range = overlap.start as u64..overlap.end as u64;
                        // println!("Applying pushdown on dimension {}: {:?}", dim_name, range);
                    } else {
                        // No overlap, return empty batch
                        // println!(
                        //     "Skipping chunk {:?} due to no overlap with pushdown on dimension {}",
                        //     chunk_indices, dim_name
                        // );
                        let mut arrays = vec![];
                        let mut fields = vec![];
                        for field in composer.projected_schema.fields() {
                            fields.push(field.as_ref().clone());
                            arrays.push(NdArrowArray::new_null_scalar(Some(
                                field.data_type().clone(),
                            )));
                        }

                        let empty_batch =
                            NdRecordBatch::new(fields, arrays).map_err(|e| e.to_string());
                        return Some(empty_batch);
                    }
                }
            }
        }
        tracing::debug!(
            "Reading chunk {:?} with subset ranges: {:?}",
            chunk_indices,
            dimension_subset_ranges
        );
        let mut arrays = vec![];
        let mut fields = vec![];
        for column in composer.projected_schema.fields().iter() {
            let array_name = column.name();
            fields.push(column.as_ref().clone());

            if array_name.contains('.') {
                // Reading attribute
                if let Some(attribute) = composer.group_reader.read_attribute(array_name) {
                    arrays.push(attribute);
                } else {
                    return Some(Err(format!(
                        "Attribute {} not found in Zarr group.",
                        array_name
                    )));
                }
            } else {
                // Reading array
                let subset = if let Some(dimension_names) =
                    composer.group_reader.arrays()[array_name.as_str()].dimension_names()
                {
                    let ranges = dimension_names
                        .iter()
                        .map(|dim_name| {
                            dimension_subset_ranges
                                .get(dim_name.as_ref().unwrap().as_str())
                                .unwrap()
                                .clone()
                        })
                        .collect::<Vec<_>>();

                    ArraySubset::new_with_ranges(&ranges)
                } else {
                    // Scalar array
                    ArraySubset::new_empty(0)
                };
                let array = composer
                    .group_reader
                    .read_array(array_name, &subset)
                    .await
                    .unwrap()
                    .unwrap();

                arrays.push(array);
            }
        }

        let record_batch = NdRecordBatch::new(fields, arrays).map_err(|e| e.to_string());

        Some(record_batch)
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
