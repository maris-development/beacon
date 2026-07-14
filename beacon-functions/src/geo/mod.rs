use datafusion::logical_expr::ScalarUDF;

pub mod st_geojson_as_wkt;
pub mod st_within_point;

/// Build the geo UDFs. `st_within_point_cache_size` sizes the per-invocation
/// LRU cache used by `st_within_point`.
pub fn geo_udfs(st_within_point_cache_size: usize) -> Vec<ScalarUDF> {
    vec![
        ScalarUDF::new_from_impl(st_geojson_as_wkt::GeoJsonAsWktUdf::new()),
        ScalarUDF::new_from_impl(st_within_point::WithinPointUdf::new(
            st_within_point_cache_size,
        )),
    ]
}

pub fn register_geo_udfs(
    session_context: &datafusion::prelude::SessionContext,
    st_within_point_cache_size: usize,
) {
    for udf in geo_udfs(st_within_point_cache_size) {
        session_context.register_udf(udf);
    }
}
