use std::sync::Arc;

use arrow::array::StringArray;
use datafusion::{
    logical_expr::{ColumnarValue, ScalarUDF},
    prelude::create_udf,
    scalar::ScalarValue,
};

pub fn map_cora_platform_l06() -> ScalarUDF {
    create_udf(
        "map_cora_platform_l06",
        vec![
            datafusion::arrow::datatypes::DataType::Utf8,
            datafusion::arrow::datatypes::DataType::Utf8,
        ],
        datafusion::arrow::datatypes::DataType::Utf8,
        datafusion::logical_expr::Volatility::Immutable,
        Arc::new(map_cora_platform_l06_impl),
    )
}

fn map_cora_platform_l06_impl(
    parameters: &[ColumnarValue],
) -> datafusion::error::Result<ColumnarValue> {
    if parameters.len() != 2 {
        return Err(datafusion::error::DataFusionError::Execution(
            "map_cora_platform_l06 function requires 2 parameters. The Bigram and WMO_INST_TYPE"
                .to_string(),
        ));
    }

    match (&parameters[0], &parameters[1]) {
        (ColumnarValue::Array(array), ColumnarValue::Array(wmo_inst_type)) => {
            let bigram_array =
                crate::util::downcast_arg::<StringArray>(array, "map_cora_platform_l06")?;
            let wmo_inst_type_array =
                crate::util::downcast_arg::<StringArray>(wmo_inst_type, "map_cora_platform_l06")?;

            Ok(ColumnarValue::Array(Arc::new(StringArray::from_iter(
                bigram_array.iter().zip(wmo_inst_type_array.iter()).map(
                    |(bigram, wmo_inst_type)| map_cora_platform_func_impl(bigram, wmo_inst_type),
                ),
            ))))
        }
        (
            ColumnarValue::Array(bigram_arr),
            ColumnarValue::Scalar(ScalarValue::Utf8(wmo_inst_type)),
        ) => {
            let bigram_array =
                crate::util::downcast_arg::<StringArray>(bigram_arr, "map_cora_platform_l06")?;

            Ok(ColumnarValue::Array(Arc::new(StringArray::from_iter(
                bigram_array
                    .iter()
                    .map(|bigram| map_cora_platform_func_impl(bigram, wmo_inst_type.as_deref())),
            ))))
        }
        (
            ColumnarValue::Scalar(ScalarValue::Utf8(bigram)),
            ColumnarValue::Array(wmo_inst_type_arr),
        ) => {
            let wmo_inst_type_array =
                crate::util::downcast_arg::<StringArray>(wmo_inst_type_arr, "map_cora_platform_l06")?;

            Ok(ColumnarValue::Array(Arc::new(StringArray::from_iter(
                wmo_inst_type_array.iter().map(|wmo_inst_type| {
                    map_cora_platform_func_impl(bigram.as_deref(), wmo_inst_type)
                }),
            ))))
        }
        (
            ColumnarValue::Scalar(ScalarValue::Utf8(bigram)),
            ColumnarValue::Scalar(ScalarValue::Utf8(wmo_inst_type)),
        ) => {
            let platform_code = map_cora_platform_func_impl(
                bigram.as_ref().map(|s| s.as_str()),
                wmo_inst_type.as_ref().map(|s| s.as_str()),
            );

            if let Some(code) = platform_code {
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                    code.to_string(),
                ))))
            } else {
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)))
            }
        }
        _ => Err(datafusion::error::DataFusionError::Execution(
            "map_cora_platform_l06 function requires 2 parameters. The Bigram and WMO_INST_TYPE"
                .to_string(),
        )),
    }
}

fn map_cora_platform_func_impl(
    platform_bigram: Option<&str>,
    wmo_inst_type: Option<&str>,
) -> Option<&'static str> {
    match (platform_bigram, wmo_inst_type) {
        (Some("BO"), _) => Some("SDN:L06::30"),
        (Some("CT"), Some("995")) => Some("SDN:L06::30"),
        (Some("CT"), _) => Some("SDN:L06::30"),
        (Some("DB"), _) => Some("SDN:L06::42"),
        (Some("FB"), _) => Some("SDN:L06::35"),
        (Some("GL"), _) => Some("SDN:L06::27"),
        (Some("ML"), _) => Some("SDN:L06::36"),
        (Some("MO"), _) => Some("SDN:L06::48"),
        (Some("PF"), _) => Some("SDN:L06::46"),
        (Some("SD"), _) => Some("SDN:L06::3B"),
        (Some("SF"), _) => Some("SDN:L06::23"),
        (Some("SM"), _) => Some("SDN:L06::70"),
        (Some("TS"), _) => Some("SDN:L06::30"),
        (Some("TX"), _) => Some("SDN:L06::48"),
        (Some("XB"), _) => Some("SDN:L06::30"),
        (Some("XX"), _) => Some("SDN:L06::0"),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Array;

    #[test]
    fn known_bigrams_map_to_l06_codes() {
        assert_eq!(map_cora_platform_func_impl(Some("BO"), None), Some("SDN:L06::30"));
        assert_eq!(map_cora_platform_func_impl(Some("DB"), None), Some("SDN:L06::42"));
        assert_eq!(map_cora_platform_func_impl(Some("GL"), None), Some("SDN:L06::27"));
        assert_eq!(map_cora_platform_func_impl(Some("XX"), None), Some("SDN:L06::0"));
    }

    #[test]
    fn wmo_inst_type_is_ignored_for_codes_that_do_not_depend_on_it() {
        assert_eq!(
            map_cora_platform_func_impl(Some("CT"), Some("995")),
            Some("SDN:L06::30")
        );
        assert_eq!(
            map_cora_platform_func_impl(Some("CT"), Some("anything")),
            Some("SDN:L06::30")
        );
    }

    #[test]
    fn unknown_or_missing_bigram_returns_none() {
        assert_eq!(map_cora_platform_func_impl(Some("ZZ"), None), None);
        assert_eq!(map_cora_platform_func_impl(None, Some("995")), None);
        assert_eq!(map_cora_platform_func_impl(None, None), None);
    }

    #[test]
    fn udf_rejects_wrong_argument_count() {
        let single = [ColumnarValue::Scalar(ScalarValue::Utf8(Some("BO".into())))];
        assert!(map_cora_platform_l06_impl(&single).is_err());
    }

    #[test]
    fn udf_handles_array_array() {
        let bigrams = ColumnarValue::Array(Arc::new(StringArray::from(vec![
            Some("BO"),
            Some("ZZ"),
        ])));
        let wmo = ColumnarValue::Array(Arc::new(StringArray::from(vec![
            None::<&str>,
            None::<&str>,
        ])));

        let out = map_cora_platform_l06_impl(&[bigrams, wmo]).unwrap();
        let ColumnarValue::Array(arr) = out else {
            panic!("expected array output");
        };
        let arr = arr.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(arr.value(0), "SDN:L06::30");
        assert!(arr.is_null(1));
    }

    #[test]
    fn udf_handles_scalar_scalar() {
        let bigram = ColumnarValue::Scalar(ScalarValue::Utf8(Some("DB".into())));
        let wmo = ColumnarValue::Scalar(ScalarValue::Utf8(None));
        let out = map_cora_platform_l06_impl(&[bigram, wmo]).unwrap();
        match out {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(code))) => {
                assert_eq!(code, "SDN:L06::42")
            }
            other => panic!("unexpected output: {other:?}"),
        }
    }

    #[test]
    fn udf_scalar_scalar_unknown_is_null() {
        let bigram = ColumnarValue::Scalar(ScalarValue::Utf8(Some("ZZ".into())));
        let wmo = ColumnarValue::Scalar(ScalarValue::Utf8(None));
        let out = map_cora_platform_l06_impl(&[bigram, wmo]).unwrap();
        assert!(matches!(out, ColumnarValue::Scalar(ScalarValue::Utf8(None))));
    }
}
