//! Arrow record batch builders for grouped Nd arrays.
//!
//! An `ArrayGroup` represents a bundle of logically-related `NdArrowArray`
//! entries.  Writers rely on this module to flatten multidimensional tensors
//! into Arrow-friendly batches while preserving dimension metadata.  The
//! accompanying `ArrayGroupReader` reverses the process.

use std::sync::Arc;

use arrow::{
    array::{
        Array, ArrayRef, AsArray, ListArray, ListBuilder, NullBufferBuilder, RecordBatch,
        StringBuilder, UInt32Builder,
    },
    buffer::OffsetBuffer,
    datatypes::UInt32Type,
};
use arrow_schema::DataType;
use nd_arrow_array::{NdArrowArray, dimensions::Dimensions};
use serde::{Deserialize, Serialize};

use crate::{
    error::{BBFReadingError, BBFResult, BBFWritingError},
    util::super_type_arrow,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArrayGroupMetadata {
    pub uncompressed_array_byte_size: usize,
    pub num_chunks: usize,
    pub num_elements: usize,
}

#[derive(Debug)]
/// A built array group containing a `RecordBatch` and computed metadata.
///
/// The `RecordBatch` contains three columns: `values` (a list array of
/// concatenated element arrays), `dimension_names` and `dimension_sizes`.
pub struct ArrayGroup {
    /// Summary metadata about the group (sizes, number of chunks, elements).
    pub metadata: ArrayGroupMetadata,
    /// The Arrow `RecordBatch` representing the group contents.
    pub batch: RecordBatch,
}

/// Builder for creating an `ArrayGroup`.
///
/// The builder accepts `NdArrowArray` values and null entries. It will
/// maintain a `data_type` that represents the common super-type of all
/// appended arrays and will cast existing arrays when a new, larger
/// super-type is required.
pub struct ArrayGroupBuilder {
    array_name: String,
    data_type: DataType,
    arrays: Vec<Option<NdArrowArray>>,
}

impl ArrayGroupBuilder {
    pub fn new(array_name: String, data_type: Option<DataType>) -> Self {
        let data_type = data_type.unwrap_or(DataType::Null);
        Self {
            array_name,
            data_type,
            arrays: Vec::new(),
        }
    }

    pub fn array_data_type(&self) -> &DataType {
        &self.data_type
    }

    /// Return the total buffer memory size (in bytes) of arrays in the
    /// current group (ignores null entries).
    pub fn group_size(&self) -> usize {
        self.arrays
            .iter()
            .filter_map(|arr| {
                arr.as_ref()
                    .map(|a| a.as_arrow_array().get_buffer_memory_size())
            })
            .sum()
    }

    pub fn build(self) -> BBFResult<ArrayGroup> {
        let values_array = self.build_values_array();
        let dimension_names_array = self.build_dimension_names_array();
        let dimension_sizes_array = self.build_dimension_sizes_array();

        // Build RecordBatch
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("values", values_array.data_type().clone(), true),
            arrow::datatypes::Field::new(
                "dimension_names",
                dimension_names_array.data_type().clone(),
                true,
            ),
            arrow::datatypes::Field::new(
                "dimension_sizes",
                dimension_sizes_array.data_type().clone(),
                true,
            ),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![values_array, dimension_names_array, dimension_sizes_array],
        )
        .map_err(|e| BBFWritingError::ArrayGroupBuildFailure(Box::new(e)))?;

        let uncompressed_array_byte_size = batch
            .columns()
            .iter()
            .map(|col| col.get_buffer_memory_size())
            .sum();
        let num_chunks = self.arrays.len();
        let num_elements = self
            .arrays
            .iter()
            .filter_map(|arr| arr.as_ref().map(|a| a.as_arrow_array().len()))
            .sum();

        Ok(ArrayGroup {
            metadata: ArrayGroupMetadata {
                uncompressed_array_byte_size,
                num_chunks,
                num_elements,
            },
            batch,
        })
    }

    /// Build the flattened `ListArray` of values for this group.
    ///
    /// This concatenates all non-null underlying arrays into a single
    /// values array and creates a list offsets buffer describing where each
    /// entry starts and ends. Null entries are represented with a null
    /// bitmap.
    fn build_values_array(&self) -> ArrayRef {
        let all_arrays = self
            .arrays
            .iter()
            .filter_map(|a| a.as_ref().map(|a| a.as_arrow_array().as_ref()))
            .collect::<Vec<_>>();

        let arrow_array_flat = arrow::compute::concat(all_arrays.as_slice()).unwrap();
        // Build a vector of lengths (one length per entry). `from_lengths` expects
        // per-list lengths (not cumulative offsets), and produces an offsets buffer.
        let mut lengths = vec![];
        let mut nulls_builder = NullBufferBuilder::new(self.arrays.len());
        for arr in self.arrays.iter() {
            if let Some(a) = arr {
                lengths.push(a.as_arrow_array().len());
                nulls_builder.append_non_null();
            } else {
                lengths.push(0usize);
                nulls_builder.append_null();
            }
        }

        let offsets = OffsetBuffer::from_lengths(lengths);

        let list_field = Arc::new(arrow::datatypes::Field::new(
            "array",
            self.data_type.clone(),
            true,
        ));

        Arc::new(ListArray::new(
            list_field,
            offsets,
            arrow_array_flat,
            nulls_builder.finish(),
        ))
    }

    /// Build a list-of-string array with dimension names for each entry.
    fn build_dimension_names_array(&self) -> ArrayRef {
        let mut builder = ListBuilder::new(StringBuilder::new());
        for arr in self.arrays.iter() {
            if let Some(a) = arr {
                match a.dimensions() {
                    Dimensions::MultiDimensional(dims) => {
                        for dim in dims.iter() {
                            builder.values().append_value(&dim.name);
                        }
                    }
                    Dimensions::Scalar => {}
                }
                builder.append(true);
            } else {
                builder.append(false);
            }
        }
        Arc::new(builder.finish())
    }

    /// Build a list-of-uint32 array with dimension sizes for each entry.
    fn build_dimension_sizes_array(&self) -> ArrayRef {
        let mut builder = ListBuilder::new(UInt32Builder::new());
        for arr in self.arrays.iter() {
            if let Some(a) = arr {
                match a.dimensions() {
                    Dimensions::MultiDimensional(dims) => {
                        for dim in dims.iter() {
                            builder.values().append_value(dim.size as u32);
                        }
                    }
                    Dimensions::Scalar => {}
                }
                builder.append(true);
            } else {
                builder.append(false);
            }
        }
        Arc::new(builder.finish())
    }

    /// Append an explicit null entry to the group.
    pub fn append_null_array(&mut self) {
        self.arrays.push(None);
    }

    pub fn append_array(&mut self, mut array: NdArrowArray) -> BBFResult<()> {
        let array_data_type = array.as_arrow_array().data_type();
        if self.data_type != *array_data_type {
            // Super cast to the super type of both
            let super_type = super_type_arrow(&self.data_type, array_data_type).ok_or(
                BBFWritingError::SuperTypeNotFound(
                    crate::util::SuperTypeError::NoCommonSuperType {
                        left: self.data_type.clone(),
                        right: array_data_type.clone(),
                        column_name: self.array_name.clone(),
                    },
                ),
            )?;

            // Cast existing arrays
            if self.data_type != super_type {
                for arr in self.arrays.iter_mut().flatten() {
                    *arr = Self::cast_nd_array(arr, &super_type)?;
                }
            }

            self.data_type = super_type.clone();

            // Cast new array
            if *array_data_type != super_type {
                array = Self::cast_nd_array(&array, &super_type)?;
            }
        }

        self.arrays.push(Some(array));

        Ok(())
    }

    fn cast_nd_array(array: &NdArrowArray, data_type: &DataType) -> BBFResult<NdArrowArray> {
        let arrow_array = array.as_arrow_array();
        let casted_array = arrow::compute::cast(&arrow_array, data_type).map_err(|e| {
            BBFWritingError::ArrowCastFailure(arrow_array.data_type().clone(), data_type.clone(), e)
        })?;
        Ok(
            NdArrowArray::new(casted_array, array.dimensions().clone()).map_err(|e| {
                BBFWritingError::NdArrowArrayCreationFailure(
                    e,
                    format!("Failed to create NdArrowArray after casting to {data_type}"),
                )
            })?,
        )
    }
}

pub struct ArrayGroupReader {
    array_name: String,
    batch: RecordBatch,
}

impl ArrayGroupReader {
    pub fn new(array_name: String, batch: RecordBatch) -> Self {
        Self { array_name, batch }
    }

    pub fn try_get_array(&self, array_index: usize) -> BBFResult<Option<NdArrowArray>> {
        let batch_values_array = self.batch.column(0).as_list::<i32>();

        if batch_values_array.is_null(array_index) {
            return Ok(None);
        }

        let inner_values_array = batch_values_array.value(array_index);
        let dimension_names = self.batch.column(1).as_list::<i32>().value(array_index);
        let dimension_length = self.batch.column(2).as_list::<i32>().value(array_index);

        if dimension_names.len() != dimension_length.len() {
            return Err(BBFReadingError::ArrayGroupReadFailure(
                self.array_name.clone(),
                format!(
                    "Mismatched dimension names and sizes lengths for array at index {array_index}"
                )
                .into(),
            )
            .into());
        }

        let dimensions = if dimension_names.is_empty() {
            Dimensions::Scalar
        } else {
            Dimensions::MultiDimensional(
                dimension_names
                    .as_string::<i32>()
                    .iter()
                    .zip(dimension_length.as_primitive::<UInt32Type>().iter())
                    .filter_map(|(name, length)| match (name, length) {
                        (Some(n), Some(l)) => Some(nd_arrow_array::dimensions::Dimension {
                            name: n.to_string(),
                            size: l as usize,
                        }),
                        _ => None,
                    })
                    .collect(),
            )
        };

        let nd_array = NdArrowArray::new(inner_values_array.clone(), dimensions).map_err(|e| {
            BBFReadingError::ArrayGroupReadFailure(
                self.array_name.clone(),
                format!("Failed to create NdArrowArray for array at index {array_index}: {e}")
                    .into(),
            )
        })?;

        Ok(Some(nd_array))
    }
}
