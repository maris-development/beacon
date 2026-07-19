use std::{
    fmt::Debug,
    sync::{Arc, Weak},
};

use crate::datafusion::ZarrFormat;
use arrow::datatypes::{DataType, Field};
use beacon_common::super_table::SuperListingTable;
use beacon_datafusion_ext::listing_factory::ListingFactory;
use datafusion::{
    catalog::TableFunctionImpl,
    common::plan_err,
    execution::object_store::ObjectStoreUrl,
    prelude::{Expr, SessionContext},
    scalar::ScalarValue,
};

use beacon_common::table_function::BeaconTableFunctionImpl;

pub struct ReadZarrFunc {
    // Session Reference
    runtime_handle: tokio::runtime::Handle,
    session_ctx: Weak<SessionContext>,
}

impl ReadZarrFunc {
    pub fn new(runtime_handle: tokio::runtime::Handle, session_ctx: Weak<SessionContext>) -> Self {
        Self {
            runtime_handle,
            session_ctx,
        }
    }
}

impl Debug for ReadZarrFunc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ReadZarrFunc")
    }
}

impl BeaconTableFunctionImpl for ReadZarrFunc {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn description(&self) -> Option<String> {
        Some("Reads Zarr files from specified glob paths.".to_string())
    }

    fn name(&self) -> String {
        "read_zarr".to_string()
    }

    fn arguments(&self) -> Option<Vec<arrow::datatypes::Field>> {
        Some(vec![Field::new(
            "glob_paths",
            DataType::List(Arc::new(Field::new("glob_path", DataType::Utf8, false))),
            false,
        )])
    }
}

impl TableFunctionImpl for ReadZarrFunc {
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
                datafusion::common::plan_datafusion_err!(
                    "ListingFactory extension not found in session state"
                )
            })?;
        let glob_paths = beacon_common::table_function::parse_glob_paths_arg(args, "read_zarr")?;

        // Optional second argument: an explicit list of dimensions to read.
        let mut dimensions: Vec<String> = vec![];
        if let Some(dimensions_arg) = args.get(1)
            && let Expr::Literal(ScalarValue::List(values), _) = dimensions_arg
        {
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
                        "read_zarr second argument must be a List<Utf8> of dimension names"
                    );
                }
            }
        }

        tracing::debug!("read_zarr glob paths: {:?}", glob_paths);

        let mut listing_urls = vec![];
        for path in &glob_paths {
            tracing::debug!("read_zarr processing path: {}", path);
            listing_urls.push(listing_factory.parse_listing_table_url(&state, path)?);
        }

        // Predicate pushdown is handled automatically by the shared engine, so
        // no manual statistics/column selection is needed.
        let read_dimensions = (!dimensions.is_empty()).then_some(dimensions);
        let file_format = ZarrFormat::new(read_dimensions);

        let super_listing_table = tokio::task::block_in_place(|| {
            self.runtime_handle.block_on(async move {
                SuperListingTable::new(&session_ctx.state(), Arc::new(file_format), listing_urls)
                    .await
            })
        })?;

        Ok(Arc::new(super_listing_table))
    }
}
