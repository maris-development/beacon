use std::collections::HashMap;
use std::sync::{Arc, Weak};

use arrow::datatypes::{DataType, Field};
use beacon_common::super_table::SuperListingTable;
use beacon_datafusion_ext::listing_factory::ListingFactory;
use datafusion::{
    catalog::TableFunctionImpl, datasource::file_format::FileFormatFactory,
    execution::object_store::ObjectStoreUrl, prelude::SessionContext,
};

use beacon_common::table_function::BeaconTableFunctionImpl;

/// Format identity the BBF factory is registered under (its `get_ext`).
const BBF_FORMAT: &str = "bbf";

pub struct ReadBBFFunc {
    // Session Reference
    runtime_handle: tokio::runtime::Handle,
    session_ctx: Weak<SessionContext>,
}

impl ReadBBFFunc {
    pub fn new(runtime_handle: tokio::runtime::Handle, session_ctx: Weak<SessionContext>) -> Self {
        Self {
            runtime_handle,
            session_ctx,
        }
    }
}

impl std::fmt::Debug for ReadBBFFunc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ReadBBFFunc")
    }
}

impl BeaconTableFunctionImpl for ReadBBFFunc {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> String {
        "read_bbf".to_string()
    }

    fn description(&self) -> Option<String> {
        Some("Reads BBF files from specified glob paths.".to_string())
    }

    fn arguments(&self) -> Option<Vec<arrow::datatypes::Field>> {
        Some(vec![Field::new(
            "glob_paths",
            DataType::List(Arc::new(Field::new("glob_path", DataType::Utf8, false))),
            false,
        )])
    }
}

impl TableFunctionImpl for ReadBBFFunc {
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
                    "read_bbf: the listing factory is not registered on the session".to_string(),
                )
            })?;
        let glob_paths = beacon_common::table_function::parse_glob_paths_arg(args, "read_bbf")?;

        tracing::debug!("read_bbf glob paths: {:?}", glob_paths);

        let mut listing_urls = vec![];
        for path in &glob_paths {
            tracing::debug!("read_bbf processing path: {}", path);
            listing_urls.push(listing_factory.parse_listing_table_url(&state, path)?);
        }

        // Build the file format from the factory registered on the session, so the
        // table function shares the runtime's configured format.
        let format_options: HashMap<String, String> = HashMap::new();

        let factory = state.get_file_format_factory(BBF_FORMAT).ok_or_else(|| {
            datafusion::error::DataFusionError::Execution(
                "read_bbf: the BBF file format is not registered on the session".to_string(),
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
        let func = ReadBBFFunc::new(tokio::runtime::Handle::current(), Weak::new());
        let err = func.call(&[glob("data/*.x")]).unwrap_err().to_string();
        assert!(err.contains("session context has been dropped"), "{err}");
    }

    /// Without Beacon's listing factory registered there is no way to resolve a
    /// glob path, so the call must fail with an explanatory error rather than
    /// falling back to raw DataFusion path handling.
    #[tokio::test]
    async fn call_without_listing_factory_reports_the_missing_extension() {
        let ctx = Arc::new(SessionContext::new());
        let func = ReadBBFFunc::new(tokio::runtime::Handle::current(), Arc::downgrade(&ctx));
        let err = func.call(&[glob("data/*.x")]).unwrap_err().to_string();
        assert!(err.to_lowercase().contains("listing factory"), "{err}");
    }

    /// The advertised signature is what the admin UI and `information_schema` show;
    /// it must stay in sync with the name the function is registered under.
    #[tokio::test]
    async fn advertised_signature_matches_the_parsed_arguments() {
        let func = ReadBBFFunc::new(tokio::runtime::Handle::current(), Weak::new());
        assert_eq!(func.name(), "read_bbf");
        let args = func.arguments().expect("arguments should be declared");
        assert_eq!(args[0].name(), "glob_paths");
        assert!(matches!(args[0].data_type(), DataType::List(_)));
        assert!(func.description().is_some());
    }
}
