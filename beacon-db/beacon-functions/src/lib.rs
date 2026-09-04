use std::sync::Arc;

use beacon_datafusion_ext::format_ext::FileFormatFactoryExt;
use datafusion::catalog::TableFunctionImpl;
use tokio::runtime::Handle;

use crate::{
    blue_cloud::register_blue_cloud_udfs, file_formats::register_table_functions,
    metadata::register_metadata_functions, util::register_util_udfs,
};

pub mod blue_cloud;
pub mod file_formats;
pub mod listing;
pub mod metadata;
pub mod util;

/// Registers every Beacon UDF and table function on `session_context`.
///
/// DataFusion catalogs the scalar and aggregate functions itself (they surface in
/// `information_schema.routines`, which `SHOW FUNCTIONS` reads). Its UDTF registry cannot be
/// enumerated with metadata afterwards, so beacon's table functions are catalogued nowhere — a
/// consumer that needs to know them (the embedded Python client, for `con.read_*`) carries its
/// own list.
pub fn register_functions(
    session_context: Arc<datafusion::prelude::SessionContext>,
    runtime_handle: Handle,
    file_formats: Vec<Arc<dyn FileFormatFactoryExt>>,
) {
    // The PostGIS-named spatial set: 118 scalar, 3 aggregate and 2 window functions. It runs
    // first, so a Beacon UDF below it wins a name it shares. It is the whole geospatial surface
    // of Beacon: the two Beacon geo UDFs that used to sit beside it are gone, and `ST_Within`,
    // `ST_Point` and `ST_GeomFromGeoJSON` state their test instead. `tests` below guards that the
    // set registers.
    datafusion_spatial::register_all(session_context.as_ref());
    register_util_udfs(session_context.as_ref());
    register_blue_cloud_udfs(session_context.as_ref());

    // Both builders only *construct* their functions; registering them on the session
    // is this function's job.
    let mut table_functions =
        register_metadata_functions(session_context.clone(), runtime_handle.clone());
    table_functions.extend(register_table_functions(
        runtime_handle,
        session_context.clone(),
        file_formats,
    ));

    for table_function in table_functions.iter() {
        session_context.register_udtf(
            table_function.name().as_str(),
            Arc::clone(table_function) as Arc<dyn TableFunctionImpl>,
        );
    }
}

#[cfg(test)]
mod tests {
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

    /// One name from each of the three registries. DataFusion keeps a separate registry for a
    /// scalar function, an aggregate function and a window function.
    #[test]
    fn the_spatial_set_covers_the_three_registries() {
        let names = spatial_names();
        for expected in ["st_distance", "st_extent", "st_clusterkmeans"] {
            assert!(
                names.contains(&expected.to_string()),
                "{expected} is absent"
            );
        }
        assert!(names.len() >= 122, "{} functions", names.len());
    }

    /// The spatial set holds every function the GeoJSON filter of the JSON query plans. Beacon
    /// carries no geospatial UDF of its own any more, so a gap here breaks that filter.
    #[test]
    fn the_spatial_set_holds_what_the_geojson_filter_plans() {
        let names = spatial_names();
        for expected in ["st_point", "st_within", "st_geomfromgeojson"] {
            assert!(
                names.contains(&expected.to_string()),
                "{expected} is absent"
            );
        }
    }
}
