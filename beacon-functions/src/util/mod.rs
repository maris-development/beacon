use datafusion::logical_expr::ScalarUDF;

pub mod beacon_version;
pub mod cast_int8_as_char;
pub mod coalesce_label;
pub mod try_arrow_cast;

pub fn util_udfs() -> Vec<ScalarUDF> {
    vec![
        cast_int8_as_char::cast_int8_as_char(),
        try_arrow_cast::try_arrow_cast(),
        coalesce_label::coalesce_label(),
        beacon_version::beacon_version(),
    ]
}
