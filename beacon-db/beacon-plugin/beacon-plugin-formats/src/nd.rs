//! The parts of a plugin scan that only an nd format needs.
//!
//! Each column of an nd scan is a `beacon.nd` struct, one row per nd batch.

use std::sync::Arc;

use beacon_datafusion_ext::nd::encoding::{
    decode_nd_array, decode_nd_record_batch_row, encode_nd_array, nd_batch_count, nd_encoded_field,
};
use beacon_datafusion_ext::nd::{Dimension, Dimensions, NdArrowArray};
use datafusion::arrow::array::{Array, ArrayRef, AsArray, RecordBatch, RecordBatchOptions};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, FieldRef, SchemaRef};
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{FileOpenFuture, FileOpener};
use datafusion::error::{DataFusionError, Result};
use datafusion::scalar::ScalarValue;
use futures::{StreamExt, TryStreamExt};

/// The axis of a scan that reads no file column, as for Beacon's built-in nd formats.
const ROW_AXIS: &str = "row";

/// Wraps the plugin opener of an nd scan that reads no file column (`COUNT(*)`, or
/// only partition columns). The plugin reads every column, and each nd batch becomes
/// a batch of no column with the grid size as its row count.
pub(crate) struct NdCountOpener {
    pub(crate) inner: Arc<dyn FileOpener>,
    /// The schema of no column that the scan reads.
    pub(crate) schema: SchemaRef,
}

impl FileOpener for NdCountOpener {
    fn open(&self, file: PartitionedFile) -> Result<FileOpenFuture> {
        let schema = Arc::clone(&self.schema);
        let opened = self.inner.open(file)?;
        Ok(Box::pin(async move {
            let stream = opened.await?;
            Ok(stream
                .map(move |batch| batch.and_then(|batch| count_batches(&batch, &schema)))
                .map_ok(|batches| futures::stream::iter(batches.into_iter().map(Ok)))
                .try_flatten()
                .boxed())
        }))
    }
}

/// One batch of no column per nd batch in `batch`, with the grid size as its row count.
fn count_batches(batch: &RecordBatch, schema: &SchemaRef) -> Result<Vec<RecordBatch>> {
    let mut out = Vec::new();
    if batch.num_rows() == 0 {
        return Ok(out);
    }
    for row in 0..nd_batch_count(batch) {
        let rows = decode_nd_record_batch_row(batch, row)?.num_rows();
        let options = RecordBatchOptions::new().with_row_count(Some(rows));
        out.push(RecordBatch::try_new_with_options(
            Arc::clone(schema),
            Vec::new(),
            &options,
        )?);
    }
    Ok(out)
}

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
