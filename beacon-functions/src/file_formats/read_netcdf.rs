use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field};
use beacon_common::{listing_url::parse_listing_table_url, super_table::SuperListingTable};
use datafusion::{
    catalog::TableFunctionImpl,
    common::plan_err,
    datasource::file_format::FileFormatFactory,
    execution::object_store::ObjectStoreUrl,
    prelude::{Expr, SessionContext},
    scalar::ScalarValue,
};

use crate::file_formats::BeaconTableFunctionImpl;

/// Format identity the NetCDF factory is registered under (its `get_ext`).
const NETCDF_FORMAT: &str = "nc";

pub struct ReadNetCDFFunc {
    // Session Reference
    runtime_handle: tokio::runtime::Handle,
    session_ctx: Arc<SessionContext>,
    data_object_store_url: ObjectStoreUrl,
}

impl ReadNetCDFFunc {
    pub fn new(
        runtime_handle: tokio::runtime::Handle,
        session_ctx: Arc<SessionContext>,
        data_object_store_url: ObjectStoreUrl,
    ) -> Self {
        Self {
            runtime_handle,
            session_ctx,
            data_object_store_url,
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
        let glob_paths = crate::file_formats::parse_glob_paths_arg(args, "read_netcdf")?;
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

        let mut listing_urls = vec![];
        for path in &glob_paths {
            tracing::debug!("read_netcdf processing path: {}", path);
            listing_urls.push(parse_listing_table_url(&self.data_object_store_url, path)?);
        }

        tracing::debug!("read_netcdf listing urls: {:?}", listing_urls);

        // Build the file format from the factory registered on the session, so the
        // table function shares the runtime's configured format + reader cache.
        // Per-call settings (read dimensions) are passed as table options.
        let mut format_options: HashMap<String, String> = HashMap::new();
        if !dimensions.is_empty() {
            format_options.insert("read_dimensions".to_string(), dimensions.join(","));
        }

        let state = self.session_ctx.state();
        let factory = state.get_file_format_factory(NETCDF_FORMAT).ok_or_else(|| {
            datafusion::error::DataFusionError::Execution(
                "read_netcdf: the NetCDF file format is not registered on the session".to_string(),
            )
        })?;
        let file_format = factory.create(&state, &format_options)?;

        let super_listing_table = tokio::task::block_in_place(|| {
            self.runtime_handle.block_on(async {
                SuperListingTable::new(&self.session_ctx.state(), file_format, listing_urls).await
            })
        })?;

        Ok(Arc::new(super_listing_table))
    }
}
