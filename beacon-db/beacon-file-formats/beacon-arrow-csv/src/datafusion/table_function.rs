use std::sync::{Arc, Weak};

use crate::datafusion::CsvFormat;
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

pub struct ReadCsvFunc {
    // Session Reference
    runtime_handle: tokio::runtime::Handle,
    session_ctx: Weak<SessionContext>,
}

impl ReadCsvFunc {
    pub fn new(runtime_handle: tokio::runtime::Handle, session_ctx: Weak<SessionContext>) -> Self {
        Self {
            runtime_handle,
            session_ctx,
        }
    }
}

impl std::fmt::Debug for ReadCsvFunc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ReadCsvFunc")
    }
}

impl BeaconTableFunctionImpl for ReadCsvFunc {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> String {
        "read_csv".to_string()
    }

    fn description(&self) -> Option<String> {
        Some(
            "Reads CSV files from specified glob paths. Optional second argument is the \
             field delimiter — a single character (';', '|') or an escape ('\\t', '\\n', \
             '\\r'); defaults to ','. Optional third argument sets the schema-inference \
             sample size."
                .to_string(),
        )
    }

    fn arguments(&self) -> Option<Vec<arrow::datatypes::Field>> {
        Some(vec![
            Field::new(
                "glob_paths",
                DataType::List(Arc::new(Field::new("glob_path", DataType::Utf8, false))),
                false,
            ),
            Field::new("delimiter", DataType::Utf8, true),
            Field::new("infer_records", DataType::UInt64, true),
        ])
    }
}

impl TableFunctionImpl for ReadCsvFunc {
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
                    "read_csv: the listing factory is not registered on the session".to_string(),
                )
            })?;
        let glob_paths = beacon_common::table_function::parse_glob_paths_arg(args, "read_csv")?;

        let delimiter = match args.get(1) {
            None => b',',
            // A NULL delimiter (or an absent value) means "use the default".
            Some(Expr::Literal(
                ScalarValue::Utf8(None) | ScalarValue::LargeUtf8(None) | ScalarValue::Utf8View(None),
                _,
            )) => b',',
            // Accept every string-scalar flavour, and resolve escapes like `\t`.
            Some(Expr::Literal(
                ScalarValue::Utf8(Some(s))
                | ScalarValue::LargeUtf8(Some(s))
                | ScalarValue::Utf8View(Some(s)),
                _,
            )) => crate::parse_delimiter(s)
                .map_err(|e| datafusion::common::plan_datafusion_err!("read_csv: {e}"))?,
            Some(_) => {
                return plan_err!("read_csv second argument 'delimiter' must be a Utf8 string");
            }
        };

        let infer_records = args
            .get(2)
            .and_then(|arg| match arg {
                Expr::Literal(ScalarValue::UInt64(Some(v)), _) => Some(*v as usize),
                _ => None,
            })
            .unwrap_or(128_000);

        tracing::debug!("read_csv glob paths: {:?}", glob_paths);

        let mut listing_urls = vec![];
        for path in &glob_paths {
            tracing::debug!("read_csv processing path: {}", path);
            listing_urls.push(listing_factory.parse_listing_table_url(&state, path)?);
        }

        let file_format = CsvFormat::new(delimiter, infer_records);

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
        let func = ReadCsvFunc::new(tokio::runtime::Handle::current(), Weak::new());
        let err = func.call(&[glob("data/*.x")]).unwrap_err().to_string();
        assert!(err.contains("session context has been dropped"), "{err}");
    }

    /// Without Beacon's listing factory registered there is no way to resolve a
    /// glob path, so the call must fail with an explanatory error rather than
    /// falling back to raw DataFusion path handling.
    #[tokio::test]
    async fn call_without_listing_factory_reports_the_missing_extension() {
        let ctx = Arc::new(SessionContext::new());
        let func = ReadCsvFunc::new(tokio::runtime::Handle::current(), Arc::downgrade(&ctx));
        let err = func.call(&[glob("data/*.x")]).unwrap_err().to_string();
        assert!(err.to_lowercase().contains("listing factory"), "{err}");
    }

    /// The advertised signature is what the admin UI and `information_schema` show;
    /// it must stay in sync with the name the function is registered under.
    #[tokio::test]
    async fn advertised_signature_matches_the_parsed_arguments() {
        let func = ReadCsvFunc::new(tokio::runtime::Handle::current(), Weak::new());
        assert_eq!(func.name(), "read_csv");
        let args = func.arguments().expect("arguments should be declared");
        assert_eq!(args[0].name(), "glob_paths");
        assert!(matches!(args[0].data_type(), DataType::List(_)));
        assert!(func.description().is_some());
    }
}
