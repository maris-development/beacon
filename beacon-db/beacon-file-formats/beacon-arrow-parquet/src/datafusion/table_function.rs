use std::sync::{Arc, Weak};

use crate::datafusion::ParquetFormat;
use arrow::datatypes::{DataType, Field};
use beacon_common::super_table::SuperListingTable;
use datafusion::{
    catalog::TableFunctionImpl, execution::object_store::ObjectStoreUrl, prelude::SessionContext,
};

use beacon_common::table_function::BeaconTableFunctionImpl;

pub struct ReadParquetFunc {
    // Session Reference
    runtime_handle: tokio::runtime::Handle,
    session_ctx: Weak<SessionContext>,
}

impl ReadParquetFunc {
    pub fn new(runtime_handle: tokio::runtime::Handle, session_ctx: Weak<SessionContext>) -> Self {
        Self {
            runtime_handle,
            session_ctx,
        }
    }
}

impl std::fmt::Debug for ReadParquetFunc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ReadParquetFunc")
    }
}

impl BeaconTableFunctionImpl for ReadParquetFunc {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn description(&self) -> Option<String> {
        Some("Reads Parquet files from specified glob paths.".to_string())
    }

    fn name(&self) -> String {
        "read_parquet".to_string()
    }

    fn arguments(&self) -> Option<Vec<arrow::datatypes::Field>> {
        Some(vec![Field::new(
            "glob_paths",
            DataType::List(Arc::new(Field::new("glob_path", DataType::Utf8, false))),
            false,
        )])
    }
}

impl TableFunctionImpl for ReadParquetFunc {
    fn call(
        &self,
        args: &[datafusion::prelude::Expr],
    ) -> datafusion::error::Result<std::sync::Arc<dyn datafusion::catalog::TableProvider>> {
        let glob_paths = beacon_common::table_function::parse_glob_paths_arg(args, "read_parquet")?;
        let session_ctx = self.session_ctx.upgrade().ok_or_else(|| {
            datafusion::common::plan_datafusion_err!("session context has been dropped")
        })?;
        let state = session_ctx.state();
        let listing_factory = state
            .config()
            .get_extension::<beacon_datafusion_ext::listing_factory::ListingFactory>()
            .ok_or_else(|| {
                datafusion::common::plan_datafusion_err!(
                    "ListingFactory extension not found in session state"
                )
            })?;

        tracing::debug!("read_parquet glob paths: {:?}", glob_paths);

        let mut listing_urls = vec![];
        for path in &glob_paths {
            tracing::debug!("read_parquet processing path: {}", path);
            listing_urls.push(listing_factory.parse_listing_table_url(&state, path)?);
        }

        let file_format = ParquetFormat::default();
        let super_listing_table = tokio::task::block_in_place(|| {
            self.runtime_handle.block_on(async move {
                SuperListingTable::new(&session_ctx.state(), Arc::new(file_format), listing_urls)
                    .await
            })
        })?;

        Ok(Arc::new(super_listing_table))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::Expr;
    use datafusion::scalar::ScalarValue;
    use std::sync::Weak;

    fn glob(path: &str) -> Expr {
        Expr::Literal(ScalarValue::Utf8(Some(path.to_string())), None)
    }

    /// The function holds only a `Weak` session reference, so a call made after the
    /// session was dropped must return a plan error instead of panicking.
    #[tokio::test]
    async fn call_on_dropped_session_returns_an_error() {
        let func = ReadParquetFunc::new(tokio::runtime::Handle::current(), Weak::new());
        let err = func.call(&[glob("data/*.x")]).unwrap_err().to_string();
        assert!(err.contains("session context has been dropped"), "{err}");
    }

    /// Without Beacon's listing factory registered there is no way to resolve a
    /// glob path, so the call must fail with an explanatory error rather than
    /// falling back to raw DataFusion path handling.
    #[tokio::test]
    async fn call_without_listing_factory_reports_the_missing_extension() {
        let ctx = Arc::new(SessionContext::new());
        let func = ReadParquetFunc::new(tokio::runtime::Handle::current(), Arc::downgrade(&ctx));
        let err = func.call(&[glob("data/*.x")]).unwrap_err().to_string();
        assert!(err.to_lowercase().contains("listingfactory"), "{err}");
    }

    /// The advertised signature is what the admin UI and `information_schema` show;
    /// it must stay in sync with the name the function is registered under.
    #[tokio::test]
    async fn advertised_signature_matches_the_parsed_arguments() {
        let func = ReadParquetFunc::new(tokio::runtime::Handle::current(), Weak::new());
        assert_eq!(func.name(), "read_parquet");
        let args = func.arguments().expect("arguments should be declared");
        assert_eq!(args[0].name(), "glob_paths");
        assert!(matches!(args[0].data_type(), DataType::List(_)));
        assert!(func.description().is_some());
    }
}
