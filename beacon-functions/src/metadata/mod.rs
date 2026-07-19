use std::sync::Arc;

use beacon_datafusion_ext::stats_cache::beacon_file_statistics_cache;
use datafusion::{execution::object_store::ObjectStoreUrl, prelude::SessionContext};

use crate::file_formats::BeaconTableFunctionImpl;

mod helpers;
pub mod view_dataset_statistics;
pub mod view_external_table_statistics;
pub mod view_statistics_cache;

pub fn register_metadata_functions(
    session_ctx: Arc<SessionContext>,
    runtime_handle: tokio::runtime::Handle,
) -> Vec<Arc<dyn BeaconTableFunctionImpl>> {
    // vec![
    //     Arc::new(view_dataset_statistics::ViewDatasetStatisticsFunc::new(
    //         runtime_handle.clone(),
    //         Arc::downgrade(&session_ctx),
    //         datasets_url.clone(),
    //     )),
    //     Arc::new(
    //         view_external_table_statistics::ViewExternalTableStatisticsFunc::new(
    //             Arc::downgrade(&session_ctx),
    //             beacon_file_statistics_cache(),
    //             runtime_handle.clone(),
    //             datasets_url.clone(),
    //         ),
    //     ),
    //     Arc::new(view_statistics_cache::ViewStatisticsCacheFunc::new(
    //         Arc::downgrade(&session_ctx),
    //         beacon_file_statistics_cache(),
    //         runtime_handle,
    //         datasets_url,
    //     )),
    // ]
    todo!()
}
