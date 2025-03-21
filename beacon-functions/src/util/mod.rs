use datafusion::logical_expr::ScalarUDF;

pub mod cast_int8_as_char;

pub fn util_udfs() -> Vec<ScalarUDF> {
    vec![cast_int8_as_char::cast_int8_as_char()]
}
