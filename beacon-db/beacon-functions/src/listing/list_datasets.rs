//! `list_datasets([pattern[, offset[, limit]]])`: the datasets under a glob.

use std::sync::Arc;

use beacon_datafusion_ext::format_ext::{DatasetMetadata, FileFormatFactoryExt};
use beacon_datafusion_ext::listing_factory::ListingFactory;
use datafusion::{
    catalog::{TableFunctionImpl, TableProvider},
    error::DataFusionError,
    prelude::{Expr, SessionContext},
    scalar::ScalarValue,
};
use futures::TryStreamExt;

use super::classify::classify;
use super::provider::DatasetsTable;
use crate::file_formats::BeaconTableFunctionImpl;

/// Every dataset matching `pattern` under the datasets store, as a `Vec`.
///
/// The same stream a query reads through [`ListDatasetsFunc`], collected.
pub async fn list_datasets(
    session_ctx: &SessionContext,
    file_formats: &[Arc<dyn FileFormatFactoryExt>],
    pattern: &str,
) -> datafusion::error::Result<Vec<DatasetMetadata>> {
    let state = session_ctx.state();
    let listing_factory = state
        .config()
        .get_extension::<ListingFactory>()
        .ok_or_else(|| {
            DataFusionError::Execution(
                "list_datasets: the listing factory is not registered on the session".to_string(),
            )
        })?;

    let listing = listing_factory.listing(&state, pattern)?;
    classify(file_formats.to_vec(), listing.stream())
        .try_collect()
        .await
}

pub struct ListDatasetsFunc {
    file_formats: Vec<Arc<dyn FileFormatFactoryExt>>,
}

impl ListDatasetsFunc {
    pub fn new(file_formats: Vec<Arc<dyn FileFormatFactoryExt>>) -> Self {
        Self { file_formats }
    }
}

impl std::fmt::Debug for ListDatasetsFunc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ListDatasetsFunc")
    }
}

impl BeaconTableFunctionImpl for ListDatasetsFunc {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> String {
        "list_datasets".to_string()
    }

    fn description(&self) -> Option<String> {
        Some(
            "Lists the datasets stored in beacon. Optional arguments: \
             list_datasets(pattern, offset, limit) — a glob (default '**/*'), \
             a row offset, and a row limit."
                .to_string(),
        )
    }
}

/// A `Utf8` literal argument, or `None` when absent.
fn string_arg(args: &[Expr], index: usize) -> Option<String> {
    match args.get(index) {
        Some(Expr::Literal(ScalarValue::Utf8(value), _)) => value.clone(),
        _ => None,
    }
}

/// A non-negative integer literal argument, or `None` when absent.
fn usize_arg(args: &[Expr], index: usize) -> Option<usize> {
    match args.get(index) {
        Some(Expr::Literal(scalar, _)) => match scalar {
            ScalarValue::Int64(Some(v)) if *v >= 0 => Some(*v as usize),
            ScalarValue::UInt64(Some(v)) => Some(*v as usize),
            ScalarValue::Int32(Some(v)) if *v >= 0 => Some(*v as usize),
            ScalarValue::UInt32(Some(v)) => Some(*v as usize),
            _ => None,
        },
        _ => None,
    }
}

impl TableFunctionImpl for ListDatasetsFunc {
    /// `list_datasets([pattern[, offset[, limit]]])`. No I/O happens here; the
    /// [`DatasetsTable`] lists when scanned.
    fn call(&self, args: &[Expr]) -> datafusion::error::Result<Arc<dyn TableProvider>> {
        let pattern = string_arg(args, 0).unwrap_or_else(|| "**/*".to_string());
        let offset = usize_arg(args, 1).unwrap_or(0);
        let limit = usize_arg(args, 2);

        Ok(Arc::new(DatasetsTable::new(
            pattern,
            offset,
            limit,
            self.file_formats.clone(),
        )))
    }
}
