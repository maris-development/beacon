use std::sync::Arc;

use arrow::array::StringArray;
use datafusion::{
    logical_expr::{ColumnarValue, ScalarUDF},
    prelude::create_udf,
    scalar::ScalarValue,
};

pub fn map_measuring_area_type_feature_type() -> ScalarUDF {
    create_udf(
        "map_measuring_area_type_feature_type",
        vec![datafusion::arrow::datatypes::DataType::Utf8],
        datafusion::arrow::datatypes::DataType::Utf8,
        datafusion::logical_expr::Volatility::Immutable,
        Arc::new(map_measuring_area_type_feature_type_impl),
    )
}

fn map_measuring_area_type_feature_type_impl(
    parameters: &[ColumnarValue],
) -> datafusion::error::Result<ColumnarValue> {
    match &parameters[0] {
        ColumnarValue::Array(flag) => {
            let flag_array = crate::util::downcast_arg::<arrow::array::StringArray>(
                flag,
                "map_measuring_area_type_feature_type",
            )?;

            let array = flag_array
                .iter()
                .map(|flag| flag.and_then(map_str_feature_type));

            let array = StringArray::from_iter(array);

            Ok(ColumnarValue::Array(Arc::new(array)))
        }
        ColumnarValue::Scalar(ScalarValue::Utf8(value)) => {
            let sdn_flag = value.as_ref().and_then(map_str_feature_type);

            Ok(ColumnarValue::Scalar(
                datafusion::scalar::ScalarValue::Utf8(sdn_flag),
            ))
        }
        _ => Err(datafusion::error::DataFusionError::Execution(
            "Invalid input type".to_string(),
        )),
    }
}

fn map_str_feature_type<S: AsRef<str>>(input: S) -> Option<String> {
    let input = input.as_ref();
    if input.contains("curve") {
        Some("trajectory".to_string())
    } else if input.contains("point") {
        Some("profile".to_string())
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Array;

    #[test]
    fn feature_type_keywords() {
        assert_eq!(
            map_str_feature_type("a curve segment"),
            Some("trajectory".to_string())
        );
        assert_eq!(
            map_str_feature_type("a single point"),
            Some("profile".to_string())
        );
        assert_eq!(map_str_feature_type("neither keyword"), None);
    }

    #[test]
    fn impl_scalar_path() {
        let input = ColumnarValue::Scalar(ScalarValue::Utf8(Some("curve".into())));
        match map_measuring_area_type_feature_type_impl(&[input]).unwrap() {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => assert_eq!(s, "trajectory"),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn impl_array_path() {
        let input = ColumnarValue::Array(Arc::new(StringArray::from(vec![
            Some("point data"),
            Some("unmatched"),
        ])));
        let ColumnarValue::Array(arr) =
            map_measuring_area_type_feature_type_impl(&[input]).unwrap()
        else {
            panic!("expected array");
        };
        let arr = arr.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(arr.value(0), "profile");
        assert!(arr.is_null(1));
    }

    #[test]
    fn impl_rejects_non_utf8_scalar() {
        let input = ColumnarValue::Scalar(ScalarValue::Int64(Some(1)));
        assert!(map_measuring_area_type_feature_type_impl(&[input]).is_err());
    }
}
