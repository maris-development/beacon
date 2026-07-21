use std::any::Any;
use std::sync::Arc;

use arrow::array::{ArrayRef, StringBuilder, UInt8Builder};
use arrow::datatypes::DataType as ArrowDataType;
use datafusion::arrow::array::Array;
use datafusion::common::Result;
use datafusion::common::ScalarValue;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use datafusion::physical_plan::ColumnarValue as PhysicalColumnarValue;

pub fn coalesce_label() -> ScalarUDF {
    ScalarUDF::new_from_impl(CoalesceLabel::new())
}

/// A UDF that takes any number of (column, label) pairs,
/// and for each row returns the label corresponding to the first non-null column.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct CoalesceLabel {
    signature: Signature,
}

impl CoalesceLabel {
    pub fn new() -> Self {
        let signature = Signature::new(TypeSignature::VariadicAny, Volatility::Immutable);
        Self { signature }
    }
}

impl ScalarUDFImpl for CoalesceLabel {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "coalesce_label"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[ArrowDataType]) -> Result<ArrowDataType> {
        // Always returns Utf8
        Ok(ArrowDataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let mut labels = vec![];
        // Convert any scalars to arrays, then collect an ArrayRef per arg
        let mut arrays: Vec<ArrayRef> = vec![];
        for idx in 0..args.args.len() / 2 {
            let col = &args.args[idx * 2];
            let label = &args.args[idx * 2 + 1];

            // Convert the column to an ArrayRef
            arrays.push(col.to_array(args.number_rows)?);

            // Convert the label to a StringBuilder
            if let PhysicalColumnarValue::Scalar(ScalarValue::Utf8(Some(label_str))) = label {
                labels.push(label_str.clone());
            } else {
                return Err(datafusion::common::DataFusionError::Execution(
                    "Expected label to be a non-null Utf8 scalar".to_string(),
                ));
            }
        }

        let mut index: Vec<Option<u8>> = vec![None; args.number_rows];
        for x in 0..arrays.len() {
            // Each pair of args should be a column and a label
            let col_arr = &arrays[x];
            let nulls = col_arr.nulls();
            if let Some(nulls) = nulls {
                nulls.iter().enumerate().for_each(|(i, is_not_null)| {
                    if is_not_null {
                        // If the column is not null, set the index to the label index
                        index[i] = Some(x as u8);
                    }
                });
            } else {
                // If there are no nulls, set the index for all rows
                for i in 0..col_arr.len() {
                    index[i] = Some(x as u8);
                }
            }
        }

        let mut label_array_builder =
            StringBuilder::with_capacity(args.number_rows, args.number_rows * 4);
        for i in index {
            if let Some(idx) = i {
                // If we have a valid index, append the corresponding label
                label_array_builder.append_value(&labels[idx as usize]);
            } else {
                // If no valid index, append null
                label_array_builder.append_null();
            }
        }

        Ok(ColumnarValue::Array(Arc::new(label_array_builder.finish())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::Field;
    use datafusion::config::ConfigOptions;

    fn utf8_scalar(s: &str) -> ColumnarValue {
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(s.to_string())))
    }

    fn call(args: Vec<ColumnarValue>, number_rows: usize) -> Result<StringArray> {
        let arg_fields = args
            .iter()
            .enumerate()
            .map(|(i, c)| Arc::new(Field::new(format!("a{i}"), c.data_type(), true)))
            .collect();
        let out = CoalesceLabel::new().invoke_with_args(ScalarFunctionArgs {
            args,
            arg_fields,
            number_rows,
            return_field: Arc::new(Field::new("out", ArrowDataType::Utf8, true)),
            config_options: Arc::new(ConfigOptions::default()),
        })?;
        let array = out.to_array(number_rows)?;
        Ok(array
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .clone())
    }

    #[test]
    fn udf_metadata_is_stable() {
        let udf = coalesce_label();
        assert_eq!(udf.name(), "coalesce_label");
    }

    #[test]
    fn rows_with_no_non_null_column_yield_null() {
        let col0 = ColumnarValue::Array(Arc::new(Int32Array::from(vec![None, Some(1)])));
        let out = call(vec![col0, utf8_scalar("A")], 2).unwrap();
        assert!(out.is_null(0), "all-null row -> NULL label");
        assert_eq!(out.value(1), "A");
    }

    /// A non-Utf8 (or NULL) label argument is rejected.
    #[test]
    fn non_utf8_label_is_an_error() {
        let col0 = ColumnarValue::Array(Arc::new(Int32Array::from(vec![Some(1)])));
        let bad_label = ColumnarValue::Scalar(ScalarValue::Int32(Some(9)));
        assert!(call(vec![col0, bad_label], 1).is_err());
    }

    /// SUSPECTED BUG: the doc comment says "the label corresponding to the first
    /// non-null column", but the implementation overwrites the chosen index for
    /// every non-null column in ascending order, so the *last* non-null column's
    /// label wins. This test pins the ACTUAL (last-non-null) behaviour; if the
    /// intent is truly "first", `invoke_with_args` has an ordering bug.
    #[test]
    fn multiple_non_null_columns_currently_pick_the_last_not_the_first() {
        // Row 0: both columns non-null. Row 1: only the first is non-null.
        let col0 = ColumnarValue::Array(Arc::new(Int32Array::from(vec![Some(1), Some(1)])));
        let col1 = ColumnarValue::Array(Arc::new(Int32Array::from(vec![Some(2), None])));
        let out = call(
            vec![col0, utf8_scalar("first"), col1, utf8_scalar("second")],
            2,
        )
        .unwrap();
        assert_eq!(
            out.value(0),
            "second",
            "both non-null: the LAST non-null column's label wins (contradicts the doc's 'first')"
        );
        assert_eq!(out.value(1), "first", "only the first column is non-null here");
    }
}
