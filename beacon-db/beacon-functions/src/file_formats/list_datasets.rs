use std::sync::{Arc, Weak};

use arrow::{
    array::{BooleanArray, StringArray, UInt64Array},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use beacon_datafusion_ext::format_ext::{DatasetMetadata, FileFormatFactoryExt};
use beacon_datafusion_ext::listing_factory::ListingFactory;
use datafusion::{
    catalog::{MemTable, TableFunctionImpl, TableProvider},
    error::DataFusionError,
    prelude::{Expr, SessionContext},
    scalar::ScalarValue,
};

use crate::file_formats::BeaconTableFunctionImpl;

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
    runtime_handle: tokio::runtime::Handle,
    session_ctx: Weak<SessionContext>,
    file_formats: Vec<Arc<dyn FileFormatFactoryExt>>,
}

impl ListDatasetsFunc {
    pub fn new(
        runtime_handle: tokio::runtime::Handle,
        session_ctx: Weak<SessionContext>,
        file_formats: Vec<Arc<dyn FileFormatFactoryExt>>,
    ) -> Self {
        Self {
            runtime_handle,
            session_ctx,
            file_formats,
        }
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

/// The full [`DatasetMetadata`] shape, so a caller gets everything discovery
/// computed rather than just the name and format.
pub fn list_datasets_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("file_name", DataType::Utf8, false),
        Field::new("file_format", DataType::Utf8, false),
        Field::new("can_inspect", DataType::Boolean, false),
        Field::new("can_partial_explore", DataType::Boolean, false),
        // Null when the size or timestamp could not be resolved.
        Field::new("size", DataType::UInt64, true),
        Field::new("last_modified", DataType::Utf8, true),
    ]))
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
    /// All three are optional and positional; omitting them recursively lists
    /// everything, which is the historical behaviour.
    fn call(&self, args: &[Expr]) -> datafusion::error::Result<Arc<dyn TableProvider>> {
        let pattern = string_arg(args, 0).unwrap_or_else(|| "**/*".to_string());
        let offset = usize_arg(args, 1);
        let limit = usize_arg(args, 2);

        let file_formats = self.file_formats.clone();
        let session_ctx = self.session_ctx.upgrade().ok_or_else(|| {
            datafusion::common::plan_datafusion_err!("session context has been dropped")
        })?;

        let datasets: Vec<DatasetMetadata> = tokio::task::block_in_place(|| {
            self.runtime_handle.block_on(async move {
                list_datasets(&session_ctx, &file_formats, offset, limit, Some(pattern)).await
            })
        })?;

        let schema = list_datasets_schema();
        let file_names: StringArray = datasets
            .iter()
            .map(|d| Some(d.file_path.as_str()))
            .collect();
        let formats: StringArray = datasets.iter().map(|d| Some(d.format.as_str())).collect();
        let can_inspect = BooleanArray::from(
            datasets.iter().map(|d| d.can_inspect).collect::<Vec<_>>(),
        );
        let can_partial_explore = BooleanArray::from(
            datasets
                .iter()
                .map(|d| d.can_partial_explore)
                .collect::<Vec<_>>(),
        );
        let sizes = UInt64Array::from(datasets.iter().map(|d| d.size).collect::<Vec<_>>());
        let last_modified: StringArray = datasets
            .iter()
            .map(|d| d.last_modified.map(|ts| ts.to_rfc3339()))
            .collect();

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(file_names),
                Arc::new(formats),
                Arc::new(can_inspect),
                Arc::new(can_partial_explore),
                Arc::new(sizes),
                Arc::new(last_modified),
            ],
        )?;

        Ok(Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))
    }
}
