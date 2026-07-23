use std::{collections::HashMap, sync::Arc};

use arrow::array::AsArray;
use datafusion::{
    logical_expr::{ColumnarValue, ScalarUDF},
    prelude::create_udf,
    scalar::ScalarValue,
};
use lazy_static::lazy_static;

use crate::blue_cloud::util::read_from_to_mappings_from_reader;

const EDMO_MAPPINGS_CSV: &[u8] = include_bytes!("approx_wod_edmo_mappings.csv");

lazy_static! {
    static ref EDMO_APPROX_MAP: HashMap<String, Option<i64>> = {
        let mappings =
            read_from_to_mappings_from_reader(EDMO_MAPPINGS_CSV, "WOD_INSTITUTE", "confirmed EDMO")
                .unwrap();
        mappings
            .into_iter()
            .map(|(k, v)| (k.to_lowercase(), v.parse::<i64>().ok()))
            .collect()
    };
}

pub fn map_wod_edmo_approx() -> ScalarUDF {
    create_udf(
        "map_wod_edmo_approx",
        vec![datafusion::arrow::datatypes::DataType::Utf8],
        datafusion::arrow::datatypes::DataType::Int64,
        datafusion::logical_expr::Volatility::Immutable,
        Arc::new(map_wod_edmo_approx_impl),
    )
}

fn map_wod_edmo_approx_impl(
    parameters: &[ColumnarValue],
) -> datafusion::error::Result<ColumnarValue> {
    match &parameters[0] {
        ColumnarValue::Array(ref array) => {
            let iter = array.as_string::<i32>().iter().map(|val| match val {
                Some(v) => map_wod_edmo_scalar(v),
                None => 1051, // Default to "Unknown (1051)" if not found
            });
            let result_array = arrow::array::Int64Array::from_iter(iter);
            Ok(ColumnarValue::Array(Arc::new(result_array)))
        }
        ColumnarValue::Scalar(ScalarValue::Utf8(val)) => match val {
            Some(ref v) => Ok(ColumnarValue::Scalar(ScalarValue::Int64(Some(
                map_wod_edmo_scalar(v),
            )))),
            None => Ok(ColumnarValue::Scalar(ScalarValue::Int64(Some(1051)))),
        },
        _ => Err(datafusion::error::DataFusionError::Internal(
            "Invalid argument type for map_wod_edmo".to_string(),
        )),
    }
}

fn map_wod_edmo_scalar(value: &str) -> i64 {
    EDMO_APPROX_MAP
        .get(&value.to_lowercase())
        .copied()
        .flatten()
        .unwrap_or(1051) // Default to "Unknown (1051)" if not found
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mapping() {
        assert_eq!(
            map_wod_edmo_scalar(
                "COMMONWEALTH SCIENTIFIC AND INDUSTRIAL RESEARCH ORGANIZATION (CSIRO)"
            ),
            3945
        );
        assert_eq!(
            map_wod_edmo_scalar("LOCEAN (LABORATOIRE D'OCEANOGRAPHIE ET DU CLIMAT)"),
            494
        );
        assert_eq!(
            map_wod_edmo_scalar(
                "MOSS LANDING MARINE LABORATORIES OF CALIFORNIA STATE UNIVERSITIES (MLML)A"
            ),
            1051
        );
    }
}
