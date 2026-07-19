use std::collections::HashMap;
use std::sync::{Arc, Weak};

use arrow::datatypes::{DataType, Field};
use beacon_common::super_table::SuperListingTable;
use datafusion::{
    catalog::TableFunctionImpl,
    common::plan_err,
    datasource::file_format::FileFormatFactory,
    execution::object_store::ObjectStoreUrl,
    prelude::{Expr, SessionContext},
    scalar::ScalarValue,
};

use beacon_common::table_function::BeaconTableFunctionImpl;

/// Format identity the atlas factory is registered under (its `get_ext`).
const ATLAS_FORMAT: &str = "atlas";

pub struct ReadAtlasFunc {
    // Session Reference
    runtime_handle: tokio::runtime::Handle,
    session_ctx: Weak<SessionContext>,
}

impl ReadAtlasFunc {
    pub fn new(runtime_handle: tokio::runtime::Handle, session_ctx: Weak<SessionContext>) -> Self {
        Self {
            runtime_handle,
            session_ctx,
        }
    }
}

impl std::fmt::Debug for ReadAtlasFunc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ReadAtlasFunc")
    }
}

impl BeaconTableFunctionImpl for ReadAtlasFunc {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn description(&self) -> Option<String> {
        Some(
            "Reads atlas stores. Each path must point to an atlas.json marker file \
             (exact path or glob like **/atlas.json). Optional second arg filters \
             arrays to those matching the listed dimensions."
                .to_string(),
        )
    }

    fn name(&self) -> String {
        "read_atlas".to_string()
    }

    fn arguments(&self) -> Option<Vec<arrow::datatypes::Field>> {
        Some(vec![
            Field::new(
                "glob_paths",
                DataType::List(Arc::new(Field::new("glob_path", DataType::Utf8, false))),
                false,
            ),
            Field::new(
                "dimensions",
                DataType::List(Arc::new(Field::new("dimension", DataType::Utf8, false))),
                false,
            ),
        ])
    }
}

impl TableFunctionImpl for ReadAtlasFunc {
    fn call(
        &self,
        args: &[datafusion::prelude::Expr],
    ) -> datafusion::error::Result<std::sync::Arc<dyn datafusion::catalog::TableProvider>> {
        let glob_paths = beacon_common::table_function::parse_glob_paths_arg(args, "read_atlas")?;

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
                            "read_atlas second argument must be a List<Utf8> of dimension names"
                        );
                    }
                }
            }
        }

        tracing::debug!("read_atlas glob paths: {:?}", glob_paths);

        let session_ctx = self.session_ctx.upgrade().ok_or_else(|| {
            datafusion::common::plan_datafusion_err!("session context has been dropped")
        })?;
        let state = session_ctx.state();
        let listing_factory = state
            .config()
            .get_extension::<beacon_datafusion_ext::listing_factory::ListingFactory>()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Execution(
                    "read_atlas: the listing factory is not registered on the session".to_string(),
                )
            })?;

        let mut listing_urls = vec![];
        for path in &glob_paths {
            tracing::debug!("read_atlas processing path: {}", path);
            listing_urls.push(listing_factory.parse_listing_table_url(&state, path)?);
        }

        // Build the file format from the factory registered on the session, so the
        // table function shares the runtime's configured format + reader cache.
        // Per-call settings (read dimensions) are passed as table options.
        let mut format_options: HashMap<String, String> = HashMap::new();
        if !dimensions.is_empty() {
            format_options.insert("read_dimensions".to_string(), dimensions.join(","));
        }

        let session_ctx = self.session_ctx.upgrade().ok_or_else(|| {
            datafusion::common::plan_datafusion_err!("session context has been dropped")
        })?;
        let state = session_ctx.state();
        let factory = state.get_file_format_factory(ATLAS_FORMAT).ok_or_else(|| {
            datafusion::error::DataFusionError::Execution(
                "read_atlas: the atlas file format is not registered on the session".to_string(),
            )
        })?;
        let file_format = factory.create(&state, &format_options)?;

        let super_listing_table = tokio::task::block_in_place(|| {
            self.runtime_handle.block_on(async {
                SuperListingTable::new(&session_ctx.state(), file_format, listing_urls).await
            })
        })?;

        Ok(Arc::new(super_listing_table))
    }
}
