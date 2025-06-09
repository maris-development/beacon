use datafusion::logical_expr::ScalarUDF;

pub mod cast_int8_as_char;
pub mod try_arrow_cast;

pub fn util_udfs() -> Vec<ScalarUDF> {
    vec![
        cast_int8_as_char::cast_int8_as_char(),
        try_arrow_cast::try_arrow_cast(),
    ]
}
