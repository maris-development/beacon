//! The partition columns of an nd plugin scan, as `beacon.nd`-encoded values.

use std::sync::Arc;

use beacon_datafusion_ext::nd::encoding::{decode_nd_array, encode_nd_array, nd_encoded_field};
use beacon_datafusion_ext::nd::{Dimension, Dimensions, NdArrowArray};
use datafusion::arrow::array::{Array, ArrayRef, AsArray, RecordBatch};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, FieldRef};
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{FileOpenFuture, FileOpener};
use datafusion::error::{DataFusionError, Result};
use datafusion::scalar::ScalarValue;
use futures::StreamExt;

/// The axis of a scan that reads no file column, as for Beacon's built-in nd formats.
const ROW_AXIS: &str = "row";

/// The partition fields of a table, nd-encoded with the nullability that the table
/// declares.
pub(crate) fn encoded_partition_cols(cols: &[FieldRef]) -> Vec<FieldRef> {
    cols.iter()
        .map(|field| {
            Arc::new(
                nd_encoded_field(field.name(), field.data_type())
                    .with_nullable(field.is_nullable()),
            )
        })
        .collect()
}

/// Wraps a `ProjectionOpener` of an nd scan.
///
/// The partition values of each file become nd-encoded rank-0 structs, so the
/// literals of `ProjectionOpener` match the encoded partition fields. A rank-0 value
/// broadcasts onto the grid of the file columns.
pub(crate) struct NdPartitionOpener {
    pub(crate) inner: Arc<dyn FileOpener>,
    /// The logical type of each partition column, in table order.
    pub(crate) types: Vec<DataType>,
    /// True when the scan reads no file column.
    pub(crate) no_file_columns: bool,
}

impl FileOpener for NdPartitionOpener {
    fn open(&self, mut file: PartitionedFile) -> Result<FileOpenFuture> {
        file.partition_values = file
            .partition_values
            .iter()
            .zip(&self.types)
            .map(|(value, data_type)| encode_scalar(value, data_type))
            .collect::<Result<_>>()?;
        let future = self.inner.open(file)?;
        if !self.no_file_columns {
            return Ok(future);
        }
        Ok(Box::pin(async move {
            let stream = future.await?;
            Ok(stream
                .map(|batch| batch.and_then(|batch| spread_over_rows(&batch)))
                .boxed())
        }))
    }
}

/// `value` as a one-row `beacon.nd` struct that holds a rank-0 array of `data_type`.
fn encode_scalar(value: &ScalarValue, data_type: &DataType) -> Result<ScalarValue> {
    let mut values = value.to_array_of_size(1)?;
    if values.data_type() != data_type {
        values = cast(&values, data_type)?;
    }
    let encoded = encode_nd_array(&NdArrowArray::try_new(values, Dimensions::scalar())?);
    Ok(ScalarValue::Struct(Arc::new(encoded.as_struct().clone())))
}

/// A batch of a scan that reads no file column: one row per grid cell, each with
/// rank-0 values. Rank-0 columns alone define a grid of one row, so this makes one
/// nd batch that holds each value over a `row` axis of the cell count.
fn spread_over_rows(batch: &RecordBatch) -> Result<RecordBatch> {
    let rows = batch.num_rows();
    if rows == 0 || batch.num_columns() == 0 {
        return Ok(batch.clone());
    }
    let axis = Dimensions::try_new(vec![Dimension::new(ROW_AXIS, rows)])?;
    let columns = batch
        .columns()
        .iter()
        .map(|column| {
            let value = decode_nd_array(column, 0)?;
            let value = ScalarValue::try_from_array(value.values().as_ref(), 0)?;
            let array = NdArrowArray::try_new(value.to_array_of_size(rows)?, axis.clone())?;
            Ok(encode_nd_array(&array))
        })
        .collect::<Result<Vec<ArrayRef>>>()?;
    RecordBatch::try_new(batch.schema(), columns)
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
}
