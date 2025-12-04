use std::sync::Arc;

use arrow::array::StringArray;
use datafusion::{
    logical_expr::{ColumnarValue, ScalarUDF},
    prelude::create_udf,
    scalar::ScalarValue,
};

pub fn map_cmems_bigram_l06() -> ScalarUDF {
    create_udf(
        "map_cmems_bigram_l06",
        vec![
            datafusion::arrow::datatypes::DataType::Utf8,
            datafusion::arrow::datatypes::DataType::Utf8,
        ],
        datafusion::arrow::datatypes::DataType::Utf8,
        datafusion::logical_expr::Volatility::Immutable,
        Arc::new(map_cmems_bigram_l06_impl),
    )
}

fn map_cmems_bigram_l06_impl(
    parameters: &[ColumnarValue],
) -> datafusion::error::Result<ColumnarValue> {
    if parameters.len() != 2 {
        return Err(datafusion::error::DataFusionError::Execution(
            "map_cmems_bigram_l06 function requires 2 parameters. The Bigram and wmo_instrument_type"
                .to_string(),
        ));
    }

    match (&parameters[0], &parameters[1]) {
        (ColumnarValue::Array(array), ColumnarValue::Array(wmo_inst_type)) => {
            let bigram_array = array.as_any().downcast_ref::<StringArray>().unwrap();
            let wmo_inst_type_array = wmo_inst_type
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();

            Ok(ColumnarValue::Array(Arc::new(StringArray::from_iter(
                bigram_array
                    .iter()
                    .zip(wmo_inst_type_array.iter())
                    .map(|(bigram, wmo_inst_type)| map_bigram_l06(bigram, wmo_inst_type)),
            ))))
        }
        (
            ColumnarValue::Array(bigram_arr),
            ColumnarValue::Scalar(ScalarValue::Utf8(wmo_inst_type)),
        ) => {
            let bigram_array = bigram_arr.as_any().downcast_ref::<StringArray>().unwrap();

            Ok(ColumnarValue::Array(Arc::new(StringArray::from_iter(
                bigram_array
                    .iter()
                    .map(|bigram| map_bigram_l06(bigram, wmo_inst_type.as_deref())),
            ))))
        }
        (
            ColumnarValue::Scalar(ScalarValue::Utf8(bigram)),
            ColumnarValue::Array(wmo_inst_type_arr),
        ) => {
            let wmo_inst_type_array = wmo_inst_type_arr
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();

            Ok(ColumnarValue::Array(Arc::new(StringArray::from_iter(
                wmo_inst_type_array
                    .iter()
                    .map(|wmo_inst_type| map_bigram_l06(bigram.as_deref(), wmo_inst_type)),
            ))))
        }
        (
            ColumnarValue::Scalar(ScalarValue::Utf8(bigram)),
            ColumnarValue::Scalar(ScalarValue::Utf8(wmo_inst_type)),
        ) => {
            let platform_code = map_bigram_l06(
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

fn map_bigram_l06(
    platform_bigram: Option<&str>,
    wmo_instrument_type: Option<&str>,
) -> Option<&'static str> {
    match (platform_bigram, wmo_instrument_type) {
        (Some("BO"), _) => Some("SDN:L06::30"),
        (Some("CT"), Some("995")) => Some("SDN:L06::70"),
        (Some("CT"), _) => Some("SDN:L06::30"),
        (Some("XB"), _) => Some("SDN:L06::30"),
        (Some("GL"), _) => Some("SDN:L06::27"),
        (Some("PF"), _) => Some("SDN:L06::46"),
        (Some("SD"), _) => Some("SDN:L06::3B"),
        (Some("TX"), _) => Some("SDN:L06::48"),
        (Some("DB"), _) => Some("SDN:L06::42"),
        (Some("FB"), _) => Some("SDN:L06::35"),
        (Some("TS"), _) => Some("SDN:L06::30"),
        (Some("MO"), _) => Some("SDN:L06::48"),
        (Some("SM"), _) => Some("SDN:L06::70"),
        (Some("XX"), _) => Some("SDN:L06::0"),
        (Some("ML"), _) => Some("SDN:L06::46"),
        (Some("SF"), _) => Some("SDN:L06::23"),
        _ => None,
    }
}
