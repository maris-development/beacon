use std::sync::Arc;

use beacon_datafusion_ext::format_ext::FileFormatFactoryExt;
use datafusion::catalog::TableFunctionImpl;
use datafusion::execution::object_store::ObjectStoreUrl;
use tokio::runtime::Handle;

use crate::{
    blue_cloud::register_blue_cloud_udfs, file_formats::register_table_functions,
    function_doc::FunctionDoc, geo::register_geo_udfs, metadata::register_metadata_functions,
    util::register_util_udfs,
};

pub mod blue_cloud;
pub mod file_formats;
pub mod function_doc;
pub mod geo;
pub mod metadata;
pub mod util;

/// Registers every Beacon UDF and table function on `session_context`, returning the
/// table functions' documentation.
///
/// The docs are returned because DataFusion's UDTF registry cannot be enumerated with
/// metadata afterwards, so callers that expose a function catalog must snapshot them here.
pub fn register_functions(
    session_context: Arc<datafusion::prelude::SessionContext>,
    runtime_handle: Handle,
    file_formats: Vec<Arc<dyn FileFormatFactoryExt>>,
) -> Vec<FunctionDoc> {
    geodatafusion::register(session_context.as_ref());
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
    // table_functions.extend(register_table_functions(
    //     runtime_handle,
    //     session_context.clone(),
    //     default_store_url,
    //     file_formats,
    // ));

    for table_function in table_functions.iter() {
        session_context.register_udtf(
            table_function.name().as_str(),
            Arc::clone(table_function) as Arc<dyn TableFunctionImpl>,
        );
    }

    table_functions
        .iter()
        .map(|f| FunctionDoc::from_beacon_table_function(f.as_ref()))
        .collect()
}
