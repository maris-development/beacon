use std::sync::Arc;

use arrow::array::StringArray;
use datafusion::{
    common::HashMap,
    logical_expr::{ColumnarValue, ScalarUDF},
    prelude::create_udf,
    scalar::ScalarValue,
};
use lazy_static::lazy_static;

lazy_static! {
    static ref WOD_FLAG_TO_SDN: HashMap<i64, &'static str> = {
        let mut map = HashMap::new();
        map.insert(0, "1");
        map.insert(1, "3");
        map.insert(2, "3");
        map.insert(3, "3");
        map.insert(4, "3");
        map.insert(5, "3");
        map.insert(6, "4");
        map.insert(7, "4");
        map.insert(8, "4");
        map.insert(9, "4");

        map
    };
}

pub fn map_wod_quality_flag() -> ScalarUDF {
    create_udf(
        "map_wod_quality_flag",
        vec![datafusion::arrow::datatypes::DataType::Int64],
        datafusion::arrow::datatypes::DataType::Utf8,
        datafusion::logical_expr::Volatility::Immutable,
        Arc::new(map_wod_quality_flag_impl),
    )
}

fn map_wod_quality_flag_impl(
    parameters: &[ColumnarValue],
) -> datafusion::error::Result<ColumnarValue> {
    match &parameters[0] {
        ColumnarValue::Array(flag) => {
            let flag_array =
                crate::util::downcast_arg::<arrow::array::Int64Array>(flag, "map_wod_quality_flag")?;

            let array = flag_array.iter().map(|flag| {
                flag.map(|wod_flag| WOD_FLAG_TO_SDN.get(&wod_flag).map(|s| s).cloned())
                    .flatten()
            });

            let array = StringArray::from_iter(array);

            Ok(ColumnarValue::Array(Arc::new(array)))
        }
        ColumnarValue::Scalar(ScalarValue::Int64(value)) => {
            let sdn_flag = value
                .map(|wod_flag| WOD_FLAG_TO_SDN.get(&wod_flag).map(|s| s.to_string()))
                .flatten();

            Ok(ColumnarValue::Scalar(
                datafusion::scalar::ScalarValue::Utf8(sdn_flag),
            ))
        }
        _ => {
            return Err(datafusion::error::DataFusionError::Execution(
                "Invalid input type".to_string(),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int64Array};

    #[test]
    fn flag_lookup_table() {
        assert_eq!(WOD_FLAG_TO_SDN.get(&0), Some(&"1"));
        assert_eq!(WOD_FLAG_TO_SDN.get(&3), Some(&"3"));
        assert_eq!(WOD_FLAG_TO_SDN.get(&9), Some(&"4"));
        assert_eq!(WOD_FLAG_TO_SDN.get(&99), None);
    }

    #[test]
    fn impl_array_path() {
        let input = ColumnarValue::Array(Arc::new(Int64Array::from(vec![Some(0), Some(6), Some(99)])));
        let ColumnarValue::Array(arr) = map_wod_quality_flag_impl(&[input]).unwrap() else {
            panic!("expected array");
        };
        let arr = arr.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(arr.value(0), "1");
        assert_eq!(arr.value(1), "4");
        assert!(arr.is_null(2));
    }

    #[test]
    fn impl_scalar_path() {
        let input = ColumnarValue::Scalar(ScalarValue::Int64(Some(1)));
        match map_wod_quality_flag_impl(&[input]).unwrap() {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => assert_eq!(s, "3"),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn impl_rejects_non_int64_scalar() {
        let input = ColumnarValue::Scalar(ScalarValue::Utf8(Some("x".into())));
        assert!(map_wod_quality_flag_impl(&[input]).is_err());
    }
}
