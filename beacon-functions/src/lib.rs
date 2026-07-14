use std::sync::Arc;

use beacon_datafusion_ext::format_ext::FileFormatFactoryExt;
use datafusion::execution::object_store::ObjectStoreUrl;
use tokio::runtime::Handle;

use crate::{
    blue_cloud::register_blue_cloud_udfs, file_formats::register_table_functions,
    geo::register_geo_udfs, metadata::register_metadata_functions, util::register_util_udfs,
};

pub mod blue_cloud;
pub mod file_formats;
pub mod function_doc;
pub mod geo;
pub mod metadata;
pub mod util;

pub fn register_functions(
    session_context: Arc<datafusion::prelude::SessionContext>,
    runtime_handle: Handle,
    file_formats: Vec<Arc<dyn FileFormatFactoryExt>>,
    default_store_url: ObjectStoreUrl,
) {
    geodatafusion::register(session_context.as_ref());
    register_util_udfs(session_context.as_ref());
    register_blue_cloud_udfs(session_context.as_ref());
    register_geo_udfs(
        session_context.as_ref(),
        128 * 1024, // 128K entries in the LRU cache for st_within_point
    );
    register_metadata_functions(session_context.clone(), runtime_handle.clone());
    register_table_functions(
        runtime_handle,
        session_context,
        default_store_url,
        file_formats,
    );
}
