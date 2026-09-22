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

use super::provider::DatasetsTable;
use crate::file_formats::BeaconTableFunctionImpl;

/// Discover the datasets matching `pattern` (default `**/*`) under the datasets
/// object store, asking each registered file format which objects it owns.
///
/// The whole listing, as a `Vec`. The crawler wants every dataset at once. A
/// query goes through [`ListDatasetsFunc`] instead, which streams.
pub async fn list_datasets(
    session_ctx: &SessionContext,
    file_formats: &[Arc<dyn FileFormatFactoryExt>],
    offset: Option<usize>,
    limit: Option<usize>,
    search_pattern: Option<String>,
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

    // Discovery + object-metadata enrichment lives on the listing factory; this
    // function only adds pagination on top.
    let datasets = listing_factory
        .list_datasets(
            &state,
            file_formats,
            &search_pattern.unwrap_or_else(|| "**/*".to_string()),
        )
        .await?;

    // `saturating_sub`: an offset past the end must yield an empty page, not an
    // underflow panic (`end` is clamped to `datasets.len()`, so it can be < start).
    let start = offset.unwrap_or(0);
    let end = limit.map(|l| start + l).unwrap_or(datasets.len());
    let datasets = datasets
        .into_iter()
        .skip(start)
        .take(end.saturating_sub(start))
        .collect();

    Ok(datasets)
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
    /// `list_datasets([pattern[, offset[, limit]]])`.
    ///
    /// All three are optional and positional. Omitting them lists everything,
    /// which is the historical behaviour. No I/O happens here: the arguments
    /// are read and handed to a [`DatasetsTable`], which lists when scanned.
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
