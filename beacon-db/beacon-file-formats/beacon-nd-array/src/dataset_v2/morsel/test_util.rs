//! Fixtures for the morsel tests.

use std::sync::Arc;

use arrow::{
    array::{Array, Int32Array, RecordBatch},
    compute::concat_batches,
    datatypes::{DataType, Field, Schema, SchemaRef},
};
use beacon_datafusion_ext::nd::encoding::{decode_nd_record_batch_row, nd_batch_count};

use crate::{NdArray, NdArrayD};

pub(super) fn array(values: Vec<i32>, shape: &[usize], dims: &[&str]) -> Arc<dyn NdArrayD> {
    Arc::new(
        NdArray::<i32>::try_new_from_vec_in_mem(
            values,
            shape.to_vec(),
            dims.iter().map(|d| d.to_string()).collect(),
            None,
        )
        .unwrap(),
    )
}

pub(super) fn text(value: &str) -> Arc<dyn NdArrayD> {
    Arc::new(
        NdArray::<String>::try_new_from_vec_in_mem(vec![value.to_string()], vec![], vec![], None)
            .unwrap(),
    )
}

pub(super) fn schema(fields: &[(&str, DataType)]) -> SchemaRef {
    Arc::new(Schema::new(
        fields
            .iter()
            .map(|(name, datatype)| Field::new(*name, datatype.clone(), true))
            .collect::<Vec<_>>(),
    ))
}

/// Decode each row of an nd-encoded batch, broadcast it to flat rows, and join the rows.
pub(super) fn decode_all(encoded: &RecordBatch) -> RecordBatch {
    let batches: Vec<RecordBatch> = (0..nd_batch_count(encoded))
        .map(|row| {
            decode_nd_record_batch_row(encoded, row)
                .unwrap()
                .materialize()
                .unwrap()
        })
        .collect();
    concat_batches(&batches[0].schema(), &batches).unwrap()
}

pub(super) fn i32_values(batch: &RecordBatch, name: &str) -> Vec<i32> {
    let column = batch.column_by_name(name).unwrap();
    column
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap()
        .values()
        .to_vec()
}

pub(super) fn nullable_i32_values(batch: &RecordBatch, name: &str) -> Vec<Option<i32>> {
    let column = batch.column_by_name(name).unwrap();
    column
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap()
        .iter()
        .collect()
}
