use std::sync::Arc;

use arrow::{
    array::{BooleanArray, StringArray, UInt64Array},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use beacon_datafusion_ext::format_ext::{DatasetMetadata, FileFormatFactoryExt};
use beacon_datafusion_ext::listing_factory::ListingFactory;
use datafusion::{
    arrow::datatypes::SchemaRef,
    catalog::{MemTable, Session, TableFunctionImpl, TableProvider},
    datasource::TableType,
    error::DataFusionError,
    physical_plan::ExecutionPlan,
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

/// What a [`DatasetsTable`] enumerates.
#[derive(Debug, Clone)]
pub enum Listing {
    /// Every dataset the glob matches, anywhere below it.
    Glob { pattern: String },
    /// One directory level: the datasets directly inside `prefix`.
    Browse { prefix: String },
}

/// The datasets of the store, as a table.
///
/// Built by `list_datasets(...)` and `browse_datasets(...)`, and deliberately
/// inert until it is scanned. The listing used to run inside
/// `TableFunctionImpl::call`, which is a *synchronous* trait method, so it
/// reached the object store through `block_in_place` + `block_on` and held a
/// worker thread for the whole walk. It also ran during logical planning, so a
/// statement paid for the listing before anything decided to read it.
///
/// [`TableProvider::scan`] is async, so the walk belongs there. Nothing here
/// touches the store until DataFusion asks it to.
#[derive(Debug)]
pub struct DatasetsTable {
    listing: Listing,
    offset: usize,
    limit: Option<usize>,
    file_formats: Vec<Arc<dyn FileFormatFactoryExt>>,
    schema: SchemaRef,
}

impl DatasetsTable {
    pub fn new(
        listing: Listing,
        offset: usize,
        limit: Option<usize>,
        file_formats: Vec<Arc<dyn FileFormatFactoryExt>>,
    ) -> Self {
        Self {
            listing,
            offset,
            limit,
            file_formats,
            schema: list_datasets_schema(),
        }
    }

    /// What this table enumerates. Query-time authorization reads it, so the
    /// provider is recognised rather than treated as unintrospectable.
    pub fn listing(&self) -> &Listing {
        &self.listing
    }

    /// Run the listing and page it.
    async fn rows(&self, state: &dyn Session) -> datafusion::error::Result<Vec<DatasetMetadata>> {
        let factory = state
            .config()
            .get_extension::<ListingFactory>()
            .ok_or_else(|| {
                DataFusionError::Execution(
                    "the listing factory is not registered on the session".to_string(),
                )
            })?;

        let datasets = match &self.listing {
            Listing::Glob { pattern } => {
                factory
                    .list_datasets(state, &self.file_formats, pattern)
                    .await?
            }
            Listing::Browse { prefix } => {
                factory
                    .browse_datasets(state, &self.file_formats, prefix)
                    .await?
                    .datasets
            }
        };

        // `saturating_sub`: an offset past the end is an empty page, not a panic.
        let start = self.offset;
        let end = self.limit.map(|l| start + l).unwrap_or(datasets.len());
        Ok(datasets
            .into_iter()
            .skip(start)
            .take(end.saturating_sub(start))
            .collect())
    }
}

/// Pack discovered datasets into the one batch the table returns.
fn datasets_batch(
    schema: SchemaRef,
    datasets: &[DatasetMetadata],
) -> datafusion::error::Result<RecordBatch> {
    let file_names: StringArray = datasets.iter().map(|d| Some(d.file_path.as_str())).collect();
    let formats: StringArray = datasets.iter().map(|d| Some(d.format.as_str())).collect();
    let can_inspect =
        BooleanArray::from(datasets.iter().map(|d| d.can_inspect).collect::<Vec<_>>());
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

    Ok(RecordBatch::try_new(
        schema,
        vec![
            Arc::new(file_names),
            Arc::new(formats),
            Arc::new(can_inspect),
            Arc::new(can_partial_explore),
            Arc::new(sizes),
            Arc::new(last_modified),
        ],
    )?)
}

#[async_trait::async_trait]
impl TableProvider for DatasetsTable {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    /// Where the listing happens.
    ///
    /// `limit` is the planner push-down. It bounds the rows returned, but not
    /// yet the walk behind them: a glob listing classifies objects into datasets
    /// in one pass and the two do not correspond one to one, so stopping the
    /// walk early needs the classifier to work in chunks. The rows are correct
    /// either way; the walk is the part still to shorten.
    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let mut datasets = self.rows(state).await?;
        if let Some(limit) = limit {
            datasets.truncate(limit);
        }
        let batch = datasets_batch(self.schema(), &datasets)?;
        MemTable::try_new(self.schema(), vec![vec![batch]])?
            .scan(state, projection, filters, limit)
            .await
    }
}

impl TableFunctionImpl for ListDatasetsFunc {
    /// `list_datasets([pattern[, offset[, limit]]])`.
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

/// `browse_datasets([prefix])`: one directory level of the datasets store.
pub struct BrowseDatasetsFunc {
    file_formats: Vec<Arc<dyn FileFormatFactoryExt>>,
}

impl BrowseDatasetsFunc {
    pub fn new(file_formats: Vec<Arc<dyn FileFormatFactoryExt>>) -> Self {
        Self { file_formats }
    }
}

impl std::fmt::Debug for BrowseDatasetsFunc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "BrowseDatasetsFunc")
    }
}

impl BeaconTableFunctionImpl for BrowseDatasetsFunc {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> String {
        "browse_datasets".to_string()
    }

    fn description(&self) -> Option<String> {
        Some(
            "Lists the datasets directly inside one directory of the datasets store. \
             Optional argument: browse_datasets(prefix), a directory (default the root). \
             Unlike list_datasets it does not descend, so its cost does not grow with \
             the size of the store below the prefix."
                .to_string(),
        )
    }
}

impl TableFunctionImpl for BrowseDatasetsFunc {
    fn call(&self, args: &[Expr]) -> datafusion::error::Result<Arc<dyn TableProvider>> {
        let prefix = string_arg(args, 0).unwrap_or_default();
        Ok(Arc::new(DatasetsTable::new(
            Listing::Browse { prefix },
            0,
            None,
            self.file_formats.clone(),
        )))
    }
}
