use std::sync::Arc;

use beacon_datafusion_ext::format_ext::FileFormatFactoryExt;
use beacon_object_storage::DatasetsStore;
use datafusion::{execution::object_store::ObjectStoreUrl, prelude::SessionContext};

// The shared table-function trait and the glob-arg parser now live in
// `beacon-common` so each `beacon-arrow-*` format crate can host its own
// `read_*` table function. Re-exported here for the cross-format functions
// (`read_schema`, `list_datasets`) and existing consumers.
pub use beacon_common::table_function::{parse_glob_paths_arg, BeaconTableFunctionImpl};

// Cross-format table functions that don't belong to a single format crate.
pub mod list_datasets;
pub mod read_schema;

/// Build the `read_*` table functions. The per-format functions are constructed
/// from their respective `beacon-arrow-*` crate; the cross-format ones
/// (`read_schema`, `list_datasets`) live here.
pub fn register_table_functions(
    runtime_handle: tokio::runtime::Handle,
    session_ctx: Arc<SessionContext>,
    data_object_store_url: ObjectStoreUrl,
    datasets_object_store: Arc<DatasetsStore>,
    file_formats: Vec<Arc<dyn FileFormatFactoryExt>>,
) -> Vec<Arc<dyn BeaconTableFunctionImpl>> {
    vec![
        Arc::new(beacon_arrow_parquet::datafusion::ReadParquetFunc::new(
            runtime_handle.clone(),
            session_ctx.clone(),
            data_object_store_url.clone(),
        )),
        Arc::new(beacon_arrow_geoparquet::datafusion::ReadGeoParquetFunc::new(
            runtime_handle.clone(),
            session_ctx.clone(),
            data_object_store_url.clone(),
        )),
        Arc::new(beacon_arrow_ipc::datafusion::ReadArrowFunc::new(
            runtime_handle.clone(),
            session_ctx.clone(),
            data_object_store_url.clone(),
        )),
        Arc::new(beacon_arrow_csv::datafusion::ReadCsvFunc::new(
            runtime_handle.clone(),
            session_ctx.clone(),
            data_object_store_url.clone(),
        )),
        Arc::new(beacon_arrow_zarr::datafusion::ReadZarrFunc::new(
            runtime_handle.clone(),
            session_ctx.clone(),
            data_object_store_url.clone(),
        )),
        Arc::new(beacon_arrow_netcdf::datafusion::ReadNetCDFFunc::new(
            runtime_handle.clone(),
            session_ctx.clone(),
            data_object_store_url.clone(),
        )),
        Arc::new(beacon_arrow_tiff::datafusion::ReadTiffFunc::new(
            runtime_handle.clone(),
            session_ctx.clone(),
            data_object_store_url.clone(),
            datasets_object_store.clone(),
        )),
        Arc::new(beacon_arrow_bbf::datafusion::ReadBBFFunc::new(
            runtime_handle.clone(),
            session_ctx.clone(),
            data_object_store_url.clone(),
        )),
        Arc::new(read_schema::ReadSchemaFunc::new(
            runtime_handle.clone(),
            session_ctx.clone(),
            data_object_store_url.clone(),
            datasets_object_store.clone(),
        )),
        Arc::new(beacon_arrow_odv::datafusion::ReadOdvAsciiFunc::new(
            runtime_handle.clone(),
            session_ctx.clone(),
            data_object_store_url.clone(),
        )),
        Arc::new(beacon_arrow_atlas::datafusion::ReadAtlasFunc::new(
            runtime_handle.clone(),
            session_ctx.clone(),
            data_object_store_url.clone(),
        )),
        Arc::new(beacon_delta::ReadDeltaFunc::new(
            runtime_handle.clone(),
            session_ctx.clone(),
            data_object_store_url.clone(),
            datasets_object_store.clone(),
        )),
        Arc::new(list_datasets::ListDatasetsFunc::new(
            runtime_handle,
            session_ctx,
            data_object_store_url,
            file_formats,
        )),
    ]
}
