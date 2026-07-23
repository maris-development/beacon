use datafusion::logical_expr::ScalarUDF;

pub mod beacon_version;
pub mod cast_int8_as_char;
pub mod coalesce_label;
pub mod try_arrow_cast;

/// Downcast a UDF argument array to a concrete Arrow array type, returning a
/// descriptive [`DataFusionError`](datafusion::error::DataFusionError) instead
/// of panicking when the runtime type does not match.
///
/// UDF signatures coerce arguments to their declared types, so a mismatch here
/// normally indicates an internal/coercion inconsistency rather than bad user
/// input — but surfacing it as an error keeps a malformed call from crashing the
/// whole query engine.
pub(crate) fn downcast_arg<'a, T: std::any::Any>(
    array: &'a dyn arrow::array::Array,
    func: &str,
) -> datafusion::error::Result<&'a T> {
    array.as_any().downcast_ref::<T>().ok_or_else(|| {
        datafusion::error::DataFusionError::Execution(format!(
            "{func}: unexpected argument array type {:?} (expected {})",
            array.data_type(),
            std::any::type_name::<T>(),
        ))
    })
}

pub fn util_udfs() -> Vec<ScalarUDF> {
    vec![
        cast_int8_as_char::cast_int8_as_char(),
        try_arrow_cast::try_arrow_cast(),
        coalesce_label::coalesce_label(),
        beacon_version::beacon_version(),
    ]
}

pub fn register_util_udfs(session_context: &datafusion::prelude::SessionContext) {
    for udf in util_udfs() {
        session_context.register_udf(udf);
    }
}
