use std::{collections::HashMap, error::Error, sync::Arc};

use arrow::array::AsArray;
use datafusion::{
    logical_expr::{ColumnarValue, ScalarUDF},
    prelude::create_udf,
    scalar::ScalarValue,
};
use lazy_static::lazy_static;

const EDMO_MAPPINGS_CSV: &[u8] = include_bytes!("edmo.csv");

lazy_static! {
    static ref EDMO_MAP: HashMap<String, i64> =
        read_from_to_mappings_from_reader(EDMO_MAPPINGS_CSV).unwrap();
}

pub fn map_wod_edmo() -> ScalarUDF {
    create_udf(
        "map_wod_edmo",
        vec![datafusion::arrow::datatypes::DataType::Utf8],
        datafusion::arrow::datatypes::DataType::Int64,
        datafusion::logical_expr::Volatility::Immutable,
        Arc::new(map_wod_edmo_impl),
    )
}

fn map_wod_edmo_impl(parameters: &[ColumnarValue]) -> datafusion::error::Result<ColumnarValue> {
    match &parameters[0] {
        ColumnarValue::Array(ref array) => {
            let iter = array.as_string::<i32>().iter().map(|val| match val {
                Some(v) => map_wod_edmo_scalar(v),
                None => None,
            });
            let result_array = arrow::array::Int64Array::from_iter(iter);
            Ok(ColumnarValue::Array(Arc::new(result_array)))
        }
        ColumnarValue::Scalar(ScalarValue::Utf8(val)) => match val {
            Some(ref v) => Ok(ColumnarValue::Scalar(ScalarValue::Int64(
                map_wod_edmo_scalar(v),
            ))),
            None => Ok(ColumnarValue::Scalar(ScalarValue::Int64(None))),
        },
        _ => Err(datafusion::error::DataFusionError::Internal(
            "Invalid argument type for map_wod_edmo".to_string(),
        )),
    }
}

fn map_wod_edmo_scalar(value: &str) -> Option<i64> {
    EDMO_MAP.get(&value.to_lowercase()).copied()
}

fn read_from_to_mappings_from_reader<R: std::io::Read>(
    reader: R,
) -> Result<HashMap<String, i64>, Box<dyn Error + Send + Sync>> {
    let mut rdr = csv::Reader::from_reader(reader);

    let mut map: HashMap<String, i64> = HashMap::new();

    for record in rdr.records().flatten() {
        let key_name = record.get(1).unwrap_or("").trim();
        let value = record.get(0).unwrap_or("").trim();

        if !value.is_empty() {
            if let Ok(parsed_value) = value.parse::<i64>() {
                map.insert(key_name.to_lowercase().to_string(), parsed_value);
            }
        }
    }

    Ok(map)
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use strsim::jaro_winkler;

    use super::*;

    fn normalize(s: &str) -> String {
        let mut out = String::with_capacity(s.len());
        for ch in s.to_lowercase().chars() {
            if ch.is_alphanumeric() || ch.is_whitespace() {
                out.push(ch);
            } else {
                out.push(' ');
            }
        }
        out.split_whitespace().collect::<Vec<_>>().join(" ")
    }

    fn tokens(s: &str) -> HashSet<String> {
        normalize(s)
            .split_whitespace()
            .map(|t| t.to_string())
            .collect()
    }

    // Sørensen–Dice over token sets
    fn dice(a: &HashSet<String>, b: &HashSet<String>) -> f64 {
        if a.is_empty() && b.is_empty() {
            return 1.0;
        }
        if a.is_empty() || b.is_empty() {
            return 0.0;
        }
        let inter = a.intersection(b).count() as f64;
        (2.0 * inter) / ((a.len() + b.len()) as f64)
    }

    fn best_match<'a>(needle: &str, haystack: &'a [String]) -> Option<(&'a str, f64)> {
        let nt = tokens(needle);

        haystack
            .iter()
            .map(|cand| {
                let ct = tokens(cand);
                let token_score = dice(&nt, &ct);
                let jw = jaro_winkler(&normalize(needle), &normalize(cand));
                // Token score dominates; JW helps with typos inside tokens
                let score = 0.8 * token_score + 0.2 * jw;
                (cand.as_str(), score)
            })
            .max_by(|a, b| a.1.partial_cmp(&b.1).unwrap())
    }

    #[test]
    fn test_name() {
        println!("EDMO Mappings: {:?}", *EDMO_MAP);
    }

    #[test]
    fn test_mappings_applied() {
        let bytes = include_bytes!("country-institutes-wod.csv");
        let reader = std::io::Cursor::new(bytes);
        let mut rdr = csv::Reader::from_reader(reader);

        // Get all institute names from the CSV for testing
        let edmo_institute_names: Vec<String> = EDMO_MAP.keys().cloned().collect();
        println!("Institute names for matching: {:?}", edmo_institute_names);
        let wod_institutes: Vec<(String, String)> = rdr
            .records()
            .map(|r| {
                (
                    r.as_ref().unwrap().get(0).unwrap().to_string(),
                    r.as_ref().unwrap().get(1).unwrap().to_string(),
                )
            })
            .collect();

        // Read only the 2nd column (institute name) and apply the mapping
        let values = wod_institutes
            .iter()
            .map(
                |(country, institute)| match best_match(institute, &edmo_institute_names) {
                    Some((best, score)) if score > 0.75 => (
                        country.to_string(),
                        institute.to_string(),
                        best.to_string(),
                        score,
                    ),
                    _ => (
                        country.to_string(),
                        institute.to_string(),
                        "".to_string(),
                        0.0,
                    ),
                },
            )
            .collect::<Vec<_>>();
        println!("Mapped values: {:?}", values);

        // Write to csv file
        let mut wtr = csv::Writer::from_path("mapped_institutes.csv").unwrap();
        wtr.write_record([
            "WOD_COUNTRY",
            "WOD_INSTITUTE",
            "SEADATANET_EDMO_INSTITUTE",
            "EDMO_CODE",
        ])
        .unwrap();
        for (country, institute, mapped, _) in values {
            let edmo_code = EDMO_MAP.get(&mapped.to_lowercase()).copied();
            wtr.write_record(&[
                country,
                institute,
                mapped,
                edmo_code.map_or("".to_string(), |c| c.to_string()),
            ])
            .unwrap();
        }
        wtr.flush().unwrap();
    }
}
