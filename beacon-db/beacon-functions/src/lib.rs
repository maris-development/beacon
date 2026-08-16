use std::sync::Arc;

use beacon_datafusion_ext::format_ext::FileFormatFactoryExt;
use datafusion::catalog::TableFunctionImpl;
use tokio::runtime::Handle;

use crate::{
    blue_cloud::register_blue_cloud_udfs, file_formats::register_table_functions,
    geo::register_geo_udfs, metadata::register_metadata_functions,
    util::register_util_udfs,
};

pub mod blue_cloud;
pub mod file_formats;
pub mod geo;
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
    // The PostGIS-named spatial set: 117 scalar, 3 aggregate and 2 window functions. It runs
    // first, so a Beacon UDF below it wins a name it shares. The two Beacon geo UDFs carry names
    // that this set does not hold (`st_within_point`, `st_geojson_as_wkt`), so neither replaces
    // a spatial function today. `geo::tests` guards that.
    datafusion_spatial::register_all(session_context.as_ref());
    register_util_udfs(session_context.as_ref());
    register_blue_cloud_udfs(session_context.as_ref());
    register_geo_udfs(
        session_context.as_ref(),
        128 * 1024, // 128K entries in the LRU cache for st_within_point
    );

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
