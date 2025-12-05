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
            arrays.push(col.to_array(args.number_rows).unwrap());

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
