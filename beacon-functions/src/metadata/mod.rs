use std::sync::Arc;

use beacon_datafusion_ext::stats_cache::beacon_file_statistics_cache;
use datafusion::prelude::SessionContext;

use crate::file_formats::BeaconTableFunctionImpl;

mod helpers;
pub mod view_dataset_statistics;
pub mod view_external_table_statistics;
pub mod view_statistics_cache;

/// Build the metadata table functions (`view_*_statistics`). Each resolves the
/// store it needs through the session's `ListingFactory` (or, for the external
/// table view, from each listing URL's own store), so there is no assumption about
/// a fixed datasets store URL.
pub fn register_metadata_functions(
    session_ctx: Arc<SessionContext>,
    runtime_handle: tokio::runtime::Handle,
) -> Vec<Arc<dyn BeaconTableFunctionImpl>> {
    vec![
        Arc::new(view_dataset_statistics::ViewDatasetStatisticsFunc::new(
            runtime_handle.clone(),
            Arc::downgrade(&session_ctx),
        )),
        Arc::new(
            view_external_table_statistics::ViewExternalTableStatisticsFunc::new(
                Arc::downgrade(&session_ctx),
                beacon_file_statistics_cache(),
                runtime_handle.clone(),
            ),
        ),
        Arc::new(view_statistics_cache::ViewStatisticsCacheFunc::new(
            Arc::downgrade(&session_ctx),
            beacon_file_statistics_cache(),
            runtime_handle,
        )),
    ]
}
