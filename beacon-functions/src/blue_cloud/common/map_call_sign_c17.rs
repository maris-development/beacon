use arrow::datatypes::DataType;
use chrono::{DateTime, NaiveDateTime, Utc};
use datafusion::{
    logical_expr::{ColumnarValue, ScalarUDF},
    prelude::create_udf,
    scalar::ScalarValue,
};
use serde::Deserialize;
use std::{collections::HashMap, sync::Arc};

use lazy_static::lazy_static;

const CALL_SIGN_MAPPINGS_JSON: &[u8] = include_bytes!("callsign_map.json");

lazy_static! {
    static ref CALL_SIGN_MAP: HashMap<String, Vec<RecordNaive>> = {
        let mappings: HashMap<String, Vec<RecordUtc>> =
            serde_json::from_slice(CALL_SIGN_MAPPINGS_JSON)
                .expect("Failed to parse call sign mappings");

        to_naive(mappings)
    };
}

type DataUtc = HashMap<String, Vec<RecordUtc>>;
type DataNaive = HashMap<String, Vec<RecordNaive>>;

fn to_naive(data_utc: DataUtc) -> DataNaive {
    data_utc
        .into_iter()
        .map(|(code, records)| {
            let naive_records = records
                .into_iter()
                .map(|r| RecordNaive {
                    c17: r.c17,
                    commissioned: r.commissioned.map(|d| d.naive_utc()),
                    decommissioned: r.decommissioned.map(|d| d.naive_utc()),
                })
                .collect();
            (code, naive_records)
        })
        .collect()
}

#[derive(Debug, Clone, Deserialize)]
struct RecordUtc {
    c17: String,
    commissioned: Option<DateTime<Utc>>,
    decommissioned: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Deserialize)]
struct RecordNaive {
    c17: String,
    commissioned: Option<NaiveDateTime>,
    decommissioned: Option<NaiveDateTime>,
}

pub fn map_call_sign_c17() -> ScalarUDF {
    create_udf(
        "map_call_sign_c17",
        vec![
            DataType::Utf8,
            DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
        ],
        datafusion::arrow::datatypes::DataType::Utf8,
        datafusion::logical_expr::Volatility::Immutable,
        Arc::new(map_call_sign_c17_impl),
    )
}

fn map_call_sign_c17_impl(
    parameters: &[ColumnarValue],
) -> datafusion::error::Result<ColumnarValue> {
    // Expecting two parameters: call sign and timestamp
    if parameters.len() != 2 {
        return Err(datafusion::error::DataFusionError::Execution(
            "Expected two parameters: call sign and timestamp[ns]".to_string(),
        ));
    }

    match (&parameters[0], &parameters[1]) {
        (ColumnarValue::Array(string_array), ColumnarValue::Array(ts_array)) => {
            let call_sign_array = string_array
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .unwrap();

            let timestamp_array = ts_array
                .as_any()
                .downcast_ref::<arrow::array::TimestampNanosecondArray>()
                .unwrap();

            let c17 =
                call_sign_array
                    .iter()
                    .zip(timestamp_array.iter())
                    .map(|(call_sign, timestamp)| match (call_sign, timestamp) {
                        (Some(cs), Some(ts)) => {
                            let naive_dt = NaiveDateTime::from_timestamp_nanos(ts).unwrap();
                            find_c17(&CALL_SIGN_MAP, cs, naive_dt)
                        }
                        _ => None,
                    });

            let array = arrow::array::StringArray::from_iter(c17);

            Ok(ColumnarValue::Array(Arc::new(array)))
        }
        (ColumnarValue::Scalar(ScalarValue::Utf8(call_sign)), ColumnarValue::Array(time_array)) => {
            let timestamp_array = time_array
                .as_any()
                .downcast_ref::<arrow::array::TimestampNanosecondArray>()
                .unwrap();

            let c17 =
                timestamp_array
                    .iter()
                    .map(|timestamp| match (call_sign.as_ref(), timestamp) {
                        (Some(cs), Some(ts)) => {
                            let naive_dt = NaiveDateTime::from_timestamp_nanos(ts).unwrap();
                            find_c17(&CALL_SIGN_MAP, cs, naive_dt)
                        }
                        _ => None,
                    });

            let array = arrow::array::StringArray::from_iter(c17);

            Ok(ColumnarValue::Array(Arc::new(array)))
        }
        (
            ColumnarValue::Array(call_sign_array),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(timestamp, _)),
        ) => {
            let call_signs = call_sign_array
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .unwrap();

            let c17 = call_signs
                .iter()
                .map(|call_sign| match (call_sign, timestamp) {
                    (Some(cs), Some(ts)) => {
                        let naive_dt = NaiveDateTime::from_timestamp_nanos(*ts).unwrap();
                        find_c17(&CALL_SIGN_MAP, cs, naive_dt)
                    }
                    _ => None,
                });

            let array = arrow::array::StringArray::from_iter(c17);

            Ok(ColumnarValue::Array(Arc::new(array)))
        }
        (
            ColumnarValue::Scalar(ScalarValue::Utf8(call_sign)),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(timestamp, _)),
        ) => {
            let c17 = match (call_sign, timestamp) {
                (Some(cs), Some(ts)) => {
                    let naive_dt = NaiveDateTime::from_timestamp_nanos(*ts).unwrap();
                    find_c17(&CALL_SIGN_MAP, cs, naive_dt)
                }
                _ => None,
            };

            Ok(ColumnarValue::Scalar(
                datafusion::scalar::ScalarValue::Utf8(c17),
            ))
        }
        _ => Err(datafusion::error::DataFusionError::Execution(
            "Invalid input type".to_string(),
        )),
    }
}

/// Finds the c17 for the given code and timestamp.
fn find_c17(
    mappings: &HashMap<String, Vec<RecordNaive>>,
    code: &str,
    timestamp: NaiveDateTime,
) -> Option<String> {
    let records = mappings.get(code)?;
    for record in records {
        let commissioned_ok = record.commissioned.is_none_or(|c| timestamp >= c);
        let decommissioned_ok = record.decommissioned.is_none_or(|d| timestamp < d);

        if commissioned_ok && decommissioned_ok {
            return Some(record.c17.clone());
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_loading_call_sign_mappings() {
        let mappings = CALL_SIGN_MAP.clone();
        assert!(
            !mappings.is_empty(),
            "CALL_SIGN_MAP mappings should not be empty"
        );
        println!("Loaded {:?} CALL_SIGN_MAP mappings", mappings);
    }
}
