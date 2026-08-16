use datafusion::logical_expr::ScalarUDF;

pub mod st_geojson_as_wkt;
pub mod st_within_point;

/// Build the geo UDFs. `st_within_point_cache_size` sizes the per-invocation
/// LRU cache used by `st_within_point`.
///
/// These two are Beacon's own functions. The PostGIS-named set comes from `datafusion-spatial`
/// and registers first. `st_within_point` stays beside `ST_Within` for two reasons: it takes a
/// WKT string with two ordinate columns, which needs no geometry column, and it caches a
/// point-in-geometry answer per invocation.
///
/// `benches/within_point.rs` prices the two against each other. `ST_Within` wins on a column of
/// distinct coordinates. `st_within_point` wins by 4x to 12x on a column that repeats them, which
/// is what one station reporting at many depths produces.
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Every function name the spatial crate registers, in lower case.
    fn spatial_names() -> Vec<String> {
        let mut names: Vec<String> = datafusion_spatial::scalar_udfs()
            .iter()
            .map(|f| f.name().to_lowercase())
            .collect();
        names.extend(
            datafusion_spatial::aggregate_udfs()
                .iter()
                .map(|f| f.name().to_lowercase()),
        );
        names.extend(
            datafusion_spatial::window_udfs()
                .iter()
                .map(|f| f.name().to_lowercase()),
        );
        names
    }

    /// `register_functions` registers the spatial set first and the Beacon geo UDFs after it.
    /// A later registration replaces an earlier one of the same name. A shared name would
    /// therefore hide a spatial function without a build error.
    #[test]
    fn beacon_geo_udfs_take_no_spatial_name() {
        let taken = spatial_names();
        for udf in geo_udfs(16) {
            let name = udf.name().to_lowercase();
            assert!(
                !taken.contains(&name),
                "{name} now exists in datafusion-spatial. Rename the Beacon UDF or drop it."
            );
        }
    }

    /// One name from each of the three registries. DataFusion keeps a separate registry for a
    /// scalar function, an aggregate function and a window function.
    #[test]
    fn the_spatial_set_covers_the_three_registries() {
        let names = spatial_names();
        for expected in ["st_distance", "st_extent", "st_clusterkmeans"] {
            assert!(names.contains(&expected.to_string()), "{expected} is absent");
        }
        assert!(names.len() >= 122, "{} functions", names.len());
    }
}
