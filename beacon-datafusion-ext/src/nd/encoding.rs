//! A self-describing Arrow encoding of nd arrays.
//!
//! An [`NdArrowArray`] is encoded as an Arrow `Struct` whose single row carries
//! the flat values plus the dimensions:
//!
//! ```text
//! Struct{
//!   values:    List<T>,       // the flat, C-order values (one list element)
//!   dim_sizes: List<UInt32>,  // size per axis
//!   dim_names: List<Utf8>,    // name per axis
//! }
//! ```
//!
//! The struct field carries the `beacon.nd` Arrow extension type so the intent
//! survives IPC. Because it is an ordinary Arrow array, an nd batch encoded
//! this way rides through a `DataSourceExec` (and any other operator) as a
//! normal `RecordBatch`; [`NdSourceExec`](crate::nd::exec::NdSourceExec) decodes
//! it back into an [`NdRecordBatch`] on the way out.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, ListArray, StringArray, StructArray, UInt32Array,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::common::plan_err;
use datafusion::error::{DataFusionError, Result};

use super::array::NdArrowArray;
use super::batch::NdRecordBatch;
use super::dimensions::{Dimension, Dimensions};

/// Arrow extension type name tagged on an nd column's field.
pub const ND_EXTENSION_NAME: &str = "beacon.nd";

fn err(e: impl std::fmt::Display) -> DataFusionError {
    DataFusionError::Execution(e.to_string())
}

/// The three struct fields of the nd encoding for a given element type.
fn nd_struct_fields(value_type: &DataType) -> Fields {
    Fields::from(vec![
        Field::new(
            "values",
            DataType::List(Arc::new(Field::new("item", value_type.clone(), true))),
            false,
        ),
        Field::new(
            "dim_sizes",
            DataType::List(Arc::new(Field::new("item", DataType::UInt32, false))),
            false,
        ),
        Field::new(
            "dim_names",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, false))),
            false,
        ),
    ])
}

/// The struct `DataType` of the nd encoding for a given element type.
pub fn nd_encoded_type(value_type: &DataType) -> DataType {
    DataType::Struct(nd_struct_fields(value_type))
}

/// An nd column field named `name` carrying values of `value_type`, tagged with
/// the `beacon.nd` extension type.
pub fn nd_encoded_field(name: &str, value_type: &DataType) -> Field {
    let metadata = HashMap::from([
        ("ARROW:extension:name".to_string(), ND_EXTENSION_NAME.to_string()),
        ("ARROW:extension:metadata".to_string(), "{}".to_string()),
    ]);
    Field::new(name, nd_encoded_type(value_type), false).with_metadata(metadata)
}

/// True when `field` is an nd-encoded column.
pub fn is_nd_encoded(field: &Field) -> bool {
    field.metadata().get("ARROW:extension:name").map(String::as_str) == Some(ND_EXTENSION_NAME)
}

/// Element type carried by an nd-encoded struct type (the `values` list item).
pub fn nd_value_type(encoded: &DataType) -> Result<DataType> {
    let DataType::Struct(fields) = encoded else {
        return plan_err!("not an nd-encoded type: {encoded}");
    };
    let values = fields
        .iter()
        .find(|f| f.name() == "values")
        .ok_or_else(|| err("nd-encoded struct is missing a 'values' field"))?;
    match values.data_type() {
        DataType::List(item) => Ok(item.data_type().clone()),
        other => plan_err!("nd-encoded 'values' must be a List, got {other}"),
    }
}

/// Encode one [`NdArrowArray`] as a single-row `Struct` array.
pub fn encode_nd_array(array: &NdArrowArray) -> ArrayRef {
    let values = array.values();
    let dims = array.dims();

    let values_list = ListArray::new(
        Arc::new(Field::new("item", values.data_type().clone(), true)),
        OffsetBuffer::from_lengths([values.len()]),
        values.clone(),
        None,
    );

    let sizes: UInt32Array = dims.iter().map(|d| d.size() as u32).collect();
    let dim_sizes_list = ListArray::new(
        Arc::new(Field::new("item", DataType::UInt32, false)),
        OffsetBuffer::from_lengths([dims.rank()]),
        Arc::new(sizes),
        None,
    );

    let names: StringArray = dims.iter().map(|d| Some(d.name().to_string())).collect();
    let dim_names_list = ListArray::new(
        Arc::new(Field::new("item", DataType::Utf8, false)),
        OffsetBuffer::from_lengths([dims.rank()]),
        Arc::new(names),
        None,
    );

    let struct_array = StructArray::new(
        nd_struct_fields(values.data_type()),
        vec![
            Arc::new(values_list),
            Arc::new(dim_sizes_list),
            Arc::new(dim_names_list),
        ],
        None,
    );
    Arc::new(struct_array)
}

/// Decode row `row` of an nd-encoded `Struct` column back into an [`NdArrowArray`].
pub fn decode_nd_array(column: &ArrayRef, row: usize) -> Result<NdArrowArray> {
    let structs = column
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| err("nd column is not a Struct array"))?;

    let list_at = |name: &str| -> Result<ArrayRef> {
        let list = structs
            .column_by_name(name)
            .ok_or_else(|| err(format!("nd struct is missing field '{name}'")))?
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| err(format!("nd struct field '{name}' is not a List")))?;
        Ok(list.value(row))
    };

    let values = list_at("values")?;

    let sizes = list_at("dim_sizes")?;
    let sizes = sizes
        .as_any()
        .downcast_ref::<UInt32Array>()
        .ok_or_else(|| err("nd 'dim_sizes' is not UInt32"))?;

    let names = list_at("dim_names")?;
    let names = names
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| err("nd 'dim_names' is not Utf8"))?;

    if sizes.len() != names.len() {
        return plan_err!(
            "nd dim_sizes ({}) and dim_names ({}) disagree",
            sizes.len(),
            names.len()
        );
    }

    let dims = Dimensions::try_new(
        (0..sizes.len())
            .map(|i| Dimension::new(names.value(i), sizes.value(i) as usize))
            .collect(),
    )?;

    NdArrowArray::try_new(values, dims)
}

/// Encode an [`NdRecordBatch`] as a flat `RecordBatch` of nd-encoded (`beacon.nd`)
/// struct columns — one struct row per column.
pub fn encode_nd_record_batch(batch: &NdRecordBatch) -> Result<RecordBatch> {
    let fields: Vec<Field> = batch
        .schema()
        .fields()
        .iter()
        .zip(batch.columns())
        .map(|(field, column)| nd_encoded_field(field.name(), column.data_type()))
        .collect();

    let columns: Vec<ArrayRef> = batch.columns().iter().map(encode_nd_array).collect();

    let options = RecordBatchOptions::new().with_row_count(Some(1));
    RecordBatch::try_new_with_options(Arc::new(Schema::new(fields)), columns, &options)
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
}

/// The logical (decoded) schema of an nd-encoded schema: each `beacon.nd`
/// struct column becomes its element type.
pub fn logical_schema(encoded: &Schema) -> Result<SchemaRef> {
    let fields = encoded
        .fields()
        .iter()
        .map(|field| Ok(Field::new(field.name(), nd_value_type(field.data_type())?, true)))
        .collect::<Result<Vec<_>>>()?;
    Ok(Arc::new(Schema::new(fields)))
}

/// Decode an nd-encoded `RecordBatch` (row 0 of each struct column) back into an
/// [`NdRecordBatch`]. The target grid is inferred as the union of the columns'
/// dimensions, ordered by the highest-rank column.
pub fn decode_nd_record_batch(batch: &RecordBatch) -> Result<NdRecordBatch> {
    let columns = batch
        .columns()
        .iter()
        .map(|column| decode_nd_array(column, 0))
        .collect::<Result<Vec<_>>>()?;

    let target = infer_target(&columns)?;
    let schema = logical_schema(batch.schema_ref())?;
    NdRecordBatch::try_new(schema, columns, target)
}

/// Infer the target grid from decoded columns: the highest-rank column defines
/// the axis order (it spans the grid in C-order), and any axis only present on
/// lower-rank columns is appended.
fn infer_target(columns: &[NdArrowArray]) -> Result<Dimensions> {
    let mut order: Vec<Dimension> = Vec::new();
    if let Some(widest) = columns.iter().max_by_key(|c| c.dims().rank()) {
        order.extend(widest.dims().iter().cloned());
    }
    for column in columns {
        for dim in column.dims().iter() {
            if !order.iter().any(|d| d.name() == dim.name()) {
                order.push(dim.clone());
            }
        }
    }
    Dimensions::try_new(order)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{AsArray, Float64Array, Int32Array};
    use arrow::datatypes::{DataType, Field, Float64Type, Int32Type, Schema};

    use super::*;

    fn dims(spec: &[(&str, usize)]) -> Dimensions {
        Dimensions::try_new(
            spec.iter()
                .map(|(name, size)| Dimension::new(*name, *size))
                .collect(),
        )
        .unwrap()
    }

    #[test]
    fn nd_array_round_trip() {
        let a = NdArrowArray::try_new(
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6])),
            dims(&[("time", 2), ("lat", 3)]),
        )
        .unwrap();

        let encoded = encode_nd_array(&a);
        // The encoded column is a single struct row.
        assert_eq!(encoded.len(), 1);

        let decoded = decode_nd_array(&encoded, 0).unwrap();
        assert_eq!(decoded.dims(), a.dims());
        assert_eq!(
            decoded.values().as_primitive::<Int32Type>().values(),
            &[1, 2, 3, 4, 5, 6]
        );
    }

    #[test]
    fn encoded_field_is_tagged() {
        let field = nd_encoded_field("sst", &DataType::Float64);
        assert!(is_nd_encoded(&field));
        assert_eq!(nd_value_type(field.data_type()).unwrap(), DataType::Float64);
    }

    #[test]
    fn record_batch_round_trip_infers_target() {
        // time coord (1-D), lat coord (1-D), sst data (2-D) over (time=2, lat=3).
        let schema = Arc::new(Schema::new(vec![
            Field::new("time", DataType::Int32, true),
            Field::new("lat", DataType::Int32, true),
            Field::new("sst", DataType::Float64, true),
        ]));
        let time =
            NdArrowArray::try_new(Arc::new(Int32Array::from(vec![7, 8])), dims(&[("time", 2)]))
                .unwrap();
        let lat = NdArrowArray::try_new(
            Arc::new(Int32Array::from(vec![10, 20, 30])),
            dims(&[("lat", 3)]),
        )
        .unwrap();
        let sst = NdArrowArray::try_new(
            Arc::new(Float64Array::from(vec![0.0, 0.1, 0.2, 1.0, 1.1, 1.2])),
            dims(&[("time", 2), ("lat", 3)]),
        )
        .unwrap();
        let nd = NdRecordBatch::try_new(
            schema.clone(),
            vec![time, lat, sst],
            dims(&[("time", 2), ("lat", 3)]),
        )
        .unwrap();

        // Encode → flat struct RecordBatch, all columns tagged beacon.nd.
        let encoded = encode_nd_record_batch(&nd).unwrap();
        assert_eq!(encoded.num_rows(), 1);
        assert_eq!(encoded.num_columns(), 3);
        for field in encoded.schema().fields() {
            assert!(is_nd_encoded(field), "{} not tagged", field.name());
        }

        // Decode → NdRecordBatch, target inferred from the widest (sst) column.
        let decoded = decode_nd_record_batch(&encoded).unwrap();
        assert_eq!(decoded.target(), &dims(&[("time", 2), ("lat", 3)]));

        // Materializing the decoded batch matches the original.
        let expected = nd.materialize().unwrap();
        let actual = decoded.materialize().unwrap();
        assert_eq!(actual, expected);
        assert_eq!(
            actual.column(2).as_primitive::<Float64Type>().values(),
            &[0.0, 0.1, 0.2, 1.0, 1.1, 1.2]
        );
    }

    #[test]
    fn logical_schema_unwraps_structs() {
        let encoded = Schema::new(vec![
            nd_encoded_field("lat", &DataType::Int32),
            nd_encoded_field("sst", &DataType::Float64),
        ]);
        let logical = logical_schema(&encoded).unwrap();
        assert_eq!(logical.field(0).data_type(), &DataType::Int32);
        assert_eq!(logical.field(1).data_type(), &DataType::Float64);
    }
}
