use std::collections::HashMap;
use std::sync::{Arc, Weak};

use arrow::datatypes::{DataType, Field};
use beacon_common::super_table::SuperListingTable;
use beacon_datafusion_ext::listing_factory::ListingFactory;
use datafusion::{
    catalog::TableFunctionImpl,
    common::plan_err,
    prelude::{Expr, SessionContext},
    scalar::ScalarValue,
};

use beacon_common::table_function::BeaconTableFunctionImpl;

use crate::datafusion::object_meta_resolver::create_object_resolver;
use crate::datafusion::NetcdfFormat;

/// Format identity the NetCDF factory is registered under (its `get_ext`).
const NETCDF_FORMAT: &str = "nc";

pub struct ReadNetCDFFunc {
    // Session Reference
    runtime_handle: tokio::runtime::Handle,
    session_ctx: Weak<SessionContext>,
}

impl ReadNetCDFFunc {
    pub fn new(runtime_handle: tokio::runtime::Handle, session_ctx: Weak<SessionContext>) -> Self {
        Self {
            runtime_handle,
            session_ctx,
        }
    }
}

impl std::fmt::Debug for ReadNetCDFFunc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ReadNetCDFFunc")
    }
}

impl BeaconTableFunctionImpl for ReadNetCDFFunc {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn description(&self) -> Option<String> {
        Some("Reads NetCDF files from specified glob paths.".to_string())
    }

    fn name(&self) -> String {
        "read_netcdf".to_string()
    }

    fn arguments(&self) -> Option<Vec<arrow::datatypes::Field>> {
        Some(vec![Field::new(
            "glob_paths",
            DataType::List(Arc::new(Field::new("glob_path", DataType::Utf8, false))),
            false,
        )])
    }
}

impl TableFunctionImpl for ReadNetCDFFunc {
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
                    "read_netcdf: the ListingFactory is not registered on the session".to_string(),
                )
            })?;
        let glob_paths = beacon_common::table_function::parse_glob_paths_arg(args, "read_netcdf")?;
        let mut dimensions: Vec<String> = vec![];
        if let Some(dimensions_arg) = args.get(1) {
            if let Expr::Literal(ScalarValue::List(values), _) = dimensions_arg {
                let string_array = values.as_ref().values();
                match string_array
                    .as_any()
                    .downcast_ref::<arrow::array::StringArray>()
                {
                    Some(str_arr) => {
                        dimensions = str_arr
                            .iter()
                            .filter_map(|opt_str| opt_str.map(|s| s.to_string()))
                            .collect();
                    }
                    None => {
                        return plan_err!(
                            "read_netcdf second argument must be a List<Utf8> of dimension names"
                        );
                    }
                }
            }
        }

        tracing::debug!("read_netcdf glob paths: {:?}", glob_paths);

        let mut root_store = None;
        let mut listing_urls = vec![];
        for path in &glob_paths {
            tracing::debug!("read_netcdf processing path: {}", path);
            let url = listing_factory.parse_listing_table_url(&state, path)?;
            let native_read_store = listing_factory.native_read_root(&url)?;
            // NetCDF is read natively (by path / http range-read), so reject a
            // remote object store (s3/gs/az) here with a clear error rather than
            // failing later inside the reader.
            listing_urls.push(url);
            match &mut root_store {
                Some(store) => {
                    // Compare if store is the same as the first one, if not, return an error
                    if store != &native_read_store {
                        return plan_err!(
                            "read_netcdf: all glob paths must resolve to the same root store (local or http/https)"
                        );
                    }
                }
                None => {
                    root_store = Some(native_read_store);
                }
            };
        }

        if listing_urls.is_empty() {
            return plan_err!("read_netcdf: no valid glob paths provided");
        }

        let object_resolver = match root_store {
            Some(store) => create_object_resolver(&store),
            None => {
                return plan_err!("read_netcdf: no root store could be determined from the provided glob paths. NetCDF files only work for local or http/https paths.");
            }
        };

        tracing::debug!("read_netcdf listing urls: {:?}", listing_urls);

        // Build the file format from the factory registered on the session, so the
        // table function shares the runtime's configured format + reader cache.
        // Per-call settings (read dimensions) are passed as table options.
        let mut format_options: HashMap<String, String> = HashMap::new();
        if !dimensions.is_empty() {
            format_options.insert("read_dimensions".to_string(), dimensions.join(","));
        }

        let factory = state
            .get_file_format_factory(NETCDF_FORMAT)
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Execution(
                    "read_netcdf: the NetCDF file format is not registered on the session"
                        .to_string(),
                )
            })?;
        let file_format = factory.create(&state, &format_options)?;
        let netcdf_file_format = file_format
            .as_any()
            .downcast_ref::<NetcdfFormat>()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Execution(
                    "read_netcdf: the file format registered under 'nc' is not a NetcdfFormat"
                        .to_string(),
                )
            })?
            .clone()
            .with_object_path_resolver(object_resolver);

        let super_listing_table = tokio::task::block_in_place(|| {
            self.runtime_handle.block_on(async {
                SuperListingTable::new(
                    &session_ctx.state(),
                    Arc::new(netcdf_file_format),
                    listing_urls,
                )
                .await
            })
        })?;

        Ok(Arc::new(super_listing_table))
    }
}
