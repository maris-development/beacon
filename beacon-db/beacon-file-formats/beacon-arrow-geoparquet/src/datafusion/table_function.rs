use std::sync::{Arc, Weak};

use crate::datafusion::{GeoParquetFormat, GeoParquetOptions};
use arrow::datatypes::{DataType, Field};
use beacon_common::super_table::SuperListingTable;
use beacon_datafusion_ext::listing_factory::ListingFactory;
use datafusion::{catalog::TableFunctionImpl, prelude::SessionContext};

use beacon_common::table_function::BeaconTableFunctionImpl;

pub struct ReadGeoParquetFunc {
    // Session Reference
    runtime_handle: tokio::runtime::Handle,
    session_ctx: Weak<SessionContext>,
}

impl ReadGeoParquetFunc {
    pub fn new(runtime_handle: tokio::runtime::Handle, session_ctx: Weak<SessionContext>) -> Self {
        Self {
            runtime_handle,
            session_ctx,
        }
    }
}

impl std::fmt::Debug for ReadGeoParquetFunc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ReadGeoParquetFunc")
    }
}

impl BeaconTableFunctionImpl for ReadGeoParquetFunc {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn description(&self) -> Option<String> {
        Some(
            "Reads GeoParquet files from specified glob paths, decoding geometry columns to native GeoArrow."
                .to_string(),
        )
    }

    fn name(&self) -> String {
        "read_geoparquet".to_string()
    }

    fn arguments(&self) -> Option<Vec<arrow::datatypes::Field>> {
        Some(vec![Field::new(
            "glob_paths",
            DataType::List(Arc::new(Field::new("glob_path", DataType::Utf8, false))),
            false,
        )])
    }
}

impl TableFunctionImpl for ReadGeoParquetFunc {
    fn call(
        &self,
        args: &[datafusion::prelude::Expr],
    ) -> datafusion::error::Result<std::sync::Arc<dyn datafusion::catalog::TableProvider>> {
        let session_ctx = self.session_ctx.upgrade().ok_or_else(|| {
            datafusion::common::plan_datafusion_err!("session context has been dropped")
        })?;
        let state = session_ctx.state();
        let listing_factory = state
            .config()
            .get_extension::<ListingFactory>()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Execution(
                    "read_geoparquet: the listing factory is not registered on the session"
                        .to_string(),
                )
            })?;
        let glob_paths =
            beacon_common::table_function::parse_glob_paths_arg(args, "read_geoparquet")?;

        tracing::debug!("read_geoparquet glob paths: {:?}", glob_paths);

        let mut listing_urls = vec![];
        for path in &glob_paths {
            tracing::debug!("read_geoparquet processing path: {}", path);
            listing_urls.push(listing_factory.parse_listing_table_url(&state, path)?);
        }

        // Reading does not use the lon/lat write options; defaults are fine.
        let file_format = GeoParquetFormat::new(GeoParquetOptions {
            longitude_column: None,
            latitude_column: None,
        });

        let super_listing_table = tokio::task::block_in_place(|| {
            self.runtime_handle.block_on(async move {
                SuperListingTable::new(&session_ctx.state(), Arc::new(file_format), listing_urls)
                    .await
            })
        })?;

        Ok(Arc::new(super_listing_table))
    }
}
