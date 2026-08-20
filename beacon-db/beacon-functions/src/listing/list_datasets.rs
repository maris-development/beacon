//! `list_datasets([pattern[, offset[, limit]]])`: the whole tree under a glob.

use std::sync::Arc;

use beacon_datafusion_ext::format_ext::{DatasetMetadata, FileFormatFactoryExt};
use beacon_datafusion_ext::listing_factory::ListingFactory;
use datafusion::{
    catalog::{TableFunctionImpl, TableProvider},
    error::DataFusionError,
    prelude::{Expr, SessionContext},
};

use super::provider::{DatasetsTable, Listing};
use crate::file_formats::BeaconTableFunctionImpl;
use crate::listing::args::{string_arg, usize_arg};

/// Discover the datasets matching `pattern` (default `**/*`) under the datasets
/// object store at `datasets_url`, asking each registered file format which
/// objects it owns.
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

    // Keep current pagination semantics to avoid behavior regressions.
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
            "Lists the datasets stored in beacon, descending the whole tree. \
             Optional arguments: list_datasets(pattern, offset, limit) — a glob \
             (default '**/*'), a row offset, and a row limit. Use browse_datasets \
             to read a single directory level instead."
                .to_string(),
        )
    }
}

impl TableFunctionImpl for ListDatasetsFunc {
    /// `list_datasets([pattern[, offset[, limit]]])`: every dataset the glob
    /// matches, anywhere below it.
    ///
    /// All three are optional and positional; omitting them lists everything,
    /// which is the historical behaviour. No I/O happens here: the arguments are
    /// read and handed to a [`DatasetsTable`], which lists when it is scanned.
    fn call(&self, args: &[Expr]) -> datafusion::error::Result<Arc<dyn TableProvider>> {
        let pattern = string_arg(args, 0).unwrap_or_else(|| "**/*".to_string());
        let offset = usize_arg(args, 1).unwrap_or(0);
        let limit = usize_arg(args, 2);

        Ok(Arc::new(DatasetsTable::new(
            Listing::Glob { pattern },
            offset,
            limit,
            self.file_formats.clone(),
        )))
    }
}

