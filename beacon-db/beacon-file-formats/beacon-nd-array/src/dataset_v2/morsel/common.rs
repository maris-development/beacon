//! Steps that the gridded and the ragged morsels share.

use std::sync::Arc;

use arrow::{
    array::{ArrayRef, RecordBatch, RecordBatchOptions, new_null_array},
    datatypes::{DataType, Field, Schema},
};
use beacon_datafusion_ext::nd::{Dimension, Dimensions, NdArrowArray};
use datafusion::error::{DataFusionError, Result};

use crate::{NdArrayD, array::subset::ArraySubset, arrow::array::ndarray_to_arrow_array};

/// Read `subset` of `array` as flat Arrow values in C order.
///
/// # Returns
///
/// The values, and the named dimensions of the subset in its axis order
pub(super) async fn read_subset(
    array: &dyn NdArrayD,
    subset: ArraySubset,
) -> Result<(ArrayRef, Vec<Dimension>)> {
    let sliced = array.subset(subset).await.map_err(external)?;
    let values = ndarray_to_arrow_array(sliced.as_ref())
        .await
        .map_err(external)?;
    let dimensions = sliced
        .dimensions()
        .iter()
        .zip(sliced.shape())
        .map(|(name, size)| Dimension::new(name.as_str(), size))
        .collect();
    Ok((values, dimensions))
}

/// The column of a field that the file does not hold: a rank-0 null, which broadcasts onto any grid.
pub(super) fn null_column(data_type: &DataType) -> Result<NdArrowArray> {
    NdArrowArray::try_new(new_null_array(data_type, 1), Dimensions::scalar())
}

/// The batch of a scan without columns (`COUNT(*)`): only a row count.
pub(super) fn row_count_batch(rows: usize) -> Result<RecordBatch> {
    let options = RecordBatchOptions::new().with_row_count(Some(rows));
    RecordBatch::try_new_with_options(Arc::new(Schema::empty()), vec![], &options)
        .map_err(arrow_error)
}

/// The batch field of a scan field: the scan name and nullability, with the file type.
pub(super) fn file_field(field: &Field, data_type: DataType) -> Field {
    Field::new(field.name(), data_type, field.is_nullable())
}

/// The number of cells in one step of the first axis, from the axes after it.
pub(super) fn block_size(trailing: &[Dimension]) -> usize {
    trailing.iter().map(Dimension::size).product()
}

pub(super) fn column_error(name: &str, error: DataFusionError) -> DataFusionError {
    DataFusionError::Execution(format!("nd column '{name}': {error}"))
}

pub(super) fn external(error: anyhow::Error) -> DataFusionError {
    DataFusionError::External(error.into())
}

pub(super) fn arrow_error(error: arrow::error::ArrowError) -> DataFusionError {
    DataFusionError::ArrowError(Box::new(error), None)
}
