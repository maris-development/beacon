use std::sync::Arc;

use arrow::array::StringArray;
use datafusion::{
    logical_expr::{ColumnarValue, ScalarUDF},
    prelude::create_udf,
    scalar::ScalarValue,
};

pub fn map_cmems_bigram_l05() -> ScalarUDF {
    create_udf(
        "map_cmems_bigram_l05",
        vec![datafusion::arrow::datatypes::DataType::Utf8],
        datafusion::arrow::datatypes::DataType::Utf8,
        datafusion::logical_expr::Volatility::Immutable,
        Arc::new(map_cmems_bigram_l05_impl),
    )
}

fn map_cmems_bigram_l05_impl(
    parameters: &[ColumnarValue],
) -> datafusion::error::Result<ColumnarValue> {
    if parameters.len() != 1 {
        return Err(datafusion::error::DataFusionError::Execution(
            "map_cmems_bigram_l05 function requires 1 parameter. The Bigram".to_string(),
        ));
    }

    match &parameters[0] {
        ColumnarValue::Array(array) => {
            let bigram_array =
                crate::util::downcast_arg::<StringArray>(array, "map_cmems_bigram_l05")?;

            Ok(ColumnarValue::Array(Arc::new(StringArray::from_iter(
                bigram_array.iter().map(|bigram| map_bigram_l05(bigram)),
            ))))
        }
        ColumnarValue::Scalar(ScalarValue::Utf8(bigram)) => Ok(ColumnarValue::Scalar(
            ScalarValue::Utf8(map_bigram_l05(bigram.as_deref()).map(|s| s.to_string())),
        )),
        _ => Err(datafusion::error::DataFusionError::Execution(
            "map_cora_platform_l06 function requires 1 parameter. The Bigram".to_string(),
        )),
    }
}

fn map_bigram_l05(platform_bigram: Option<&str>) -> Option<&'static str> {
    match platform_bigram {
        Some("BO") => Some("SDN:L05::30"),
        Some("CT") => Some("SDN:L05::130"),
        Some("XB") => Some("SDN:L05::132"),
        Some("TX") => Some("SDN:L05::135"),
        Some("TS") => Some("SDN:L05::133"),
        Some("ML") => Some("SDN:L05::134"),
        Some("SF") => Some("SDN:L05::131"),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Array;

    #[test]
    fn known_and_unknown_bigrams() {
        assert_eq!(map_bigram_l05(Some("BO")), Some("SDN:L05::30"));
        assert_eq!(map_bigram_l05(Some("SF")), Some("SDN:L05::131"));
        assert_eq!(map_bigram_l05(Some("ZZ")), None);
        assert_eq!(map_bigram_l05(None), None);
    }

    #[test]
    fn impl_rejects_wrong_argument_count() {
        assert!(map_cmems_bigram_l05_impl(&[]).is_err());
    }

    #[test]
    fn impl_array_path() {
        let input = ColumnarValue::Array(Arc::new(StringArray::from(vec![
            Some("CT"),
            Some("ZZ"),
        ])));
        let ColumnarValue::Array(arr) = map_cmems_bigram_l05_impl(&[input]).unwrap() else {
            panic!("expected array");
        };
        let arr = arr.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(arr.value(0), "SDN:L05::130");
        assert!(arr.is_null(1));
    }

    #[test]
    fn impl_scalar_path() {
        let input = ColumnarValue::Scalar(ScalarValue::Utf8(Some("XB".into())));
        match map_cmems_bigram_l05_impl(&[input]).unwrap() {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => assert_eq!(s, "SDN:L05::132"),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn impl_rejects_non_utf8_scalar() {
        let input = ColumnarValue::Scalar(ScalarValue::Int64(Some(1)));
        assert!(map_cmems_bigram_l05_impl(&[input]).is_err());
    }
}
