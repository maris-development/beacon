use datafusion::logical_expr::ScalarUDF;

pub mod cast_int8_as_char;
pub mod coalesce_label;
pub mod mask_if_not_null;
pub mod mask_if_null;
pub mod try_arrow_cast;

pub fn util_udfs() -> Vec<ScalarUDF> {
    vec![
        cast_int8_as_char::cast_int8_as_char(),
        try_arrow_cast::try_arrow_cast(),
        coalesce_label::coalesce_label(),
        mask_if_null::mask_if_null(),
        mask_if_not_null::mask_if_not_null(),
    ]
}
