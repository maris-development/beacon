use std::{
    fmt::Debug,
    sync::{Arc, Weak},
};

use crate::datafusion::{ZARR_EXTENSION, ZarrFormat, ZarrFormatFactory};
use arrow::datatypes::{DataType, Field};
use beacon_datafusion_ext::fast_object::FastObjectTable;
use beacon_datafusion_ext::listing_factory::ListingFactory;
use datafusion::{catalog::TableFunctionImpl, prelude::SessionContext};

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
        Some("Reads Zarr stores from specified glob paths.".to_string())
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
    fn call_with_args(
        &self,
        args: datafusion::catalog::TableFunctionArgs,
    ) -> datafusion::error::Result<std::sync::Arc<dyn datafusion::catalog::TableProvider>> {
        let args = args.exprs();
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
        // The dimension list and the skip flag are not supported.
        if args.len() > 1 {
            return Err(datafusion::common::not_impl_datafusion_err!(
                "read_zarr takes only the glob paths; dimensions and skip_unbroadcastable are not supported"
            ));
        }
        let glob_paths = beacon_common::table_function::parse_glob_paths_arg(args, "read_zarr")?;

        tracing::debug!("read_zarr glob paths: {:?}", glob_paths);

        let mut listing_urls = vec![];
        for path in &glob_paths {
            tracing::debug!("read_zarr processing path: {}", path);
            listing_urls.push(listing_factory.parse_listing_table_url(&state, path)?);
        }

        // The format shares the dataset cache of the session's Zarr factory, when there is one.
        let mut file_format = ZarrFormat::new();
        if let Some(factory) = state.get_file_format_factory(ZARR_EXTENSION)
            && let Some(factory) = factory.downcast_ref::<ZarrFormatFactory>()
        {
            file_format = file_format.with_dataset_cache(factory.dataset_cache.clone());
        }

        let fast_object_table = tokio::task::block_in_place(|| {
            self.runtime_handle.block_on(async move {
                FastObjectTable::try_new(&session_ctx.state(), Arc::new(file_format), listing_urls)
                    .await
            })
        })?;

        Ok(Arc::new(fast_object_table))
    }
}
