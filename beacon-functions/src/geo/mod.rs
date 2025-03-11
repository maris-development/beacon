use datafusion::logical_expr::ScalarUDF;

pub mod st_geojson_as_wkt;
pub mod st_within_point;

pub fn geo_udfs() -> Vec<ScalarUDF> {
    vec![
        ScalarUDF::new_from_impl(st_geojson_as_wkt::GeoJsonAsWktUdf::new()),
        ScalarUDF::new_from_impl(st_within_point::WithinPointUdf::new()),
    ]
}
