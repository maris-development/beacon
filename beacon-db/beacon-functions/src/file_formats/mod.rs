use std::sync::Arc;

use beacon_datafusion_ext::format_ext::FileFormatFactoryExt;
use datafusion::prelude::SessionContext;

// The shared table-function trait and the glob-arg parser now live in
// `beacon-common` so each `beacon-arrow-*` format crate can host its own
// `read_*` table function. Re-exported here for the cross-format functions
// (`<read_fn>_schema`, `list_datasets`) and existing consumers.
pub use beacon_common::table_function::{parse_glob_paths_arg, BeaconTableFunctionImpl};

// Cross-format table functions that don't belong to a single format crate.
pub mod list_datasets;
pub mod schema_function;

use schema_function::SchemaTableFunc;

/// Build the `read_*` table functions plus their `<read_fn>_schema` schema
/// counterparts. The per-format readers are constructed from their respective
/// `beacon-arrow-*` crate; the cross-format `list_datasets` lives here.
pub fn register_table_functions(
    runtime_handle: tokio::runtime::Handle,
    session_ctx: Arc<SessionContext>,
    file_formats: Vec<Arc<dyn FileFormatFactoryExt>>,
) -> Vec<Arc<dyn BeaconTableFunctionImpl>> {
    // The per-format file readers. Each gets a `<name>_schema` counterpart below.
    let readers: Vec<Arc<dyn BeaconTableFunctionImpl>> = vec![
        Arc::new(beacon_arrow_parquet::datafusion::ReadParquetFunc::new(
            runtime_handle.clone(),
            Arc::downgrade(&session_ctx),
        )),
        Arc::new(
            beacon_arrow_geoparquet::datafusion::ReadGeoParquetFunc::new(
                runtime_handle.clone(),
                Arc::downgrade(&session_ctx),
            ),
        ),
        Arc::new(beacon_arrow_ipc::datafusion::ReadArrowFunc::new(
            runtime_handle.clone(),
            Arc::downgrade(&session_ctx),
        )),
        Arc::new(beacon_arrow_csv::datafusion::ReadCsvFunc::new(
            runtime_handle.clone(),
            Arc::downgrade(&session_ctx),
        )),
        Arc::new(beacon_arrow_zarr::datafusion::ReadZarrFunc::new(
            runtime_handle.clone(),
            Arc::downgrade(&session_ctx),
        )),
        Arc::new(beacon_arrow_netcdf::datafusion::ReadNetCDFFunc::new(
            runtime_handle.clone(),
            Arc::downgrade(&session_ctx),
        )),
        Arc::new(beacon_arrow_tiff::datafusion::ReadTiffFunc::new(
            runtime_handle.clone(),
            Arc::downgrade(&session_ctx),
        )),
        Arc::new(beacon_arrow_bbf::datafusion::ReadBBFFunc::new(
            runtime_handle.clone(),
            Arc::downgrade(&session_ctx),
        )),
        Arc::new(beacon_arrow_odv::datafusion::ReadOdvAsciiFunc::new(
            runtime_handle.clone(),
            Arc::downgrade(&session_ctx),
        )),
        Arc::new(
            beacon_arrow_atlas::datafusion::table_function::ReadAtlasFunc::new(
                runtime_handle.clone(),
                Arc::downgrade(&session_ctx),
            ),
        ),
        Arc::new(beacon_delta::ReadDeltaFunc::new(
            runtime_handle.clone(),
            Arc::downgrade(&session_ctx),
        )),
    ];

    // For every reader, a `<name>_schema` function that returns the file(s)'
    // schema instead of their data (e.g. `read_parquet` -> `read_parquet_schema`).
    let schema_funcs: Vec<Arc<dyn BeaconTableFunctionImpl>> = readers
        .iter()
        .map(|reader| Arc::new(SchemaTableFunc::wrapping(reader.clone())) as Arc<dyn BeaconTableFunctionImpl>)
        .collect();

    let mut functions = readers;
    functions.extend(schema_funcs);
    functions.push(Arc::new(list_datasets::ListDatasetsFunc::new(
        runtime_handle,
        Arc::downgrade(&session_ctx),
        file_formats,
    )));
    functions
}
