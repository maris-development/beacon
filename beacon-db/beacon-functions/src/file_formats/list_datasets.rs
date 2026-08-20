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
            "Lists the datasets stored in beacon, descending the whole tree. \
             Optional arguments: list_datasets(pattern, offset, limit) — a glob \
             (default '**/*'), a row offset, and a row limit. Use browse_datasets \
             to read a single directory level instead."
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
        // True for a sub-directory, which only a depth-limited listing reports.
        // A recursive listing describes files, so this is always false there.
        Field::new("is_directory", DataType::Boolean, false),
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

/// One row of a listing: a dataset, or a sub-directory of one.
#[derive(Debug, Clone)]
struct Row {
    file_name: String,
    file_format: String,
    can_inspect: bool,
    can_partial_explore: bool,
    size: Option<u64>,
    last_modified: Option<String>,
    is_directory: bool,
}

impl From<DatasetMetadata> for Row {
    fn from(d: DatasetMetadata) -> Self {
        Self {
            file_name: d.file_path,
            file_format: d.format,
            can_inspect: d.can_inspect,
            can_partial_explore: d.can_partial_explore,
            size: d.size,
            last_modified: d.last_modified.map(|ts| ts.to_rfc3339()),
            is_directory: false,
        }
    }
}

impl Row {
    fn directory(path: String) -> Self {
        Self {
            file_name: path,
            file_format: String::new(),
            can_inspect: false,
            can_partial_explore: false,
            size: None,
            last_modified: None,
            is_directory: true,
        }
    }
}

/// The datasets of the store, as a table.
///
/// One provider, two table functions over it. `list_datasets` globs and
/// descends; `browse_datasets` reads one directory level and reports its
/// sub-directories as rows with `is_directory` set. They differ only in how deep
/// they go, so they share everything below this point.
///
/// Two names rather than one with a depth argument. A depth argument would
/// change what the *first* argument means — glob when absent, directory when set
/// — and an argument that retypes another argument is a bad seam. It cannot be
/// inferred from the glob either: DataFusion matches listing globs with the
/// default `MatchOptions`, where `require_literal_separator` is false, so `sub/*`
/// already matches `sub/deep/c.csv` and reading it as one level would silently
/// change what existing patterns return.
///
/// The difference is not a constant factor. One level is a single delimiter
/// request; a recursive walk of a 2 853 217-object bucket took 79.9 s where the
/// delimiter took 14 ms.
///
/// # Laziness
///
/// Deliberately inert until scanned. The listing used to run inside
/// `TableFunctionImpl::call`, a *synchronous* trait method, so it reached the
/// store through `block_in_place` + `block_on` and held a worker thread for the
/// whole walk — during logical planning, before anything decided to read it.
/// [`TableProvider::scan`] is async, so the walk belongs there.
#[derive(Debug, Clone)]
pub enum Listing {
    /// Every dataset the glob matches, anywhere below it.
    Glob { pattern: String },
    /// One directory level: what is directly inside `prefix`.
    Level { prefix: String },
}

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
    async fn rows(&self, state: &dyn Session) -> datafusion::error::Result<Vec<Row>> {
        let factory = state
            .config()
            .get_extension::<ListingFactory>()
            .ok_or_else(|| {
                DataFusionError::Execution(
                    "the listing factory is not registered on the session".to_string(),
                )
            })?;

        let rows: Vec<Row> = if let Listing::Level { prefix } = &self.listing {
            let level = factory
                .browse_datasets(state, &self.file_formats, prefix)
                .await?;
            let base = level.prefix.trim_end_matches('/').to_string();
            let join = |name: &str| {
                if base.is_empty() {
                    name.to_string()
                } else {
                    format!("{base}/{name}")
                }
            };
            // Directories first, so a browse reads like a directory listing.
            level
                .folders
                .into_iter()
                .map(|name| Row::directory(join(&name)))
                .chain(level.datasets.into_iter().map(Row::from))
                .collect()
        } else {
            let Listing::Glob { pattern } = &self.listing else {
                unreachable!("the level case is handled above")
            };
            factory
                .list_datasets(state, &self.file_formats, pattern)
                .await?
                .into_iter()
                .map(Row::from)
                .collect()
        };

        // `saturating_sub`: an offset past the end is an empty page, not a panic.
        let start = self.offset;
        let end = self.limit.map(|l| start + l).unwrap_or(rows.len());
        Ok(rows
            .into_iter()
            .skip(start)
            .take(end.saturating_sub(start))
            .collect())
    }
}

/// Pack listing rows into the one batch the table returns.
fn rows_batch(schema: SchemaRef, rows: &[Row]) -> datafusion::error::Result<RecordBatch> {
    let file_names: StringArray = rows.iter().map(|r| Some(r.file_name.as_str())).collect();
    let formats: StringArray = rows.iter().map(|r| Some(r.file_format.as_str())).collect();
    let can_inspect = BooleanArray::from(rows.iter().map(|r| r.can_inspect).collect::<Vec<_>>());
    let can_partial_explore =
        BooleanArray::from(rows.iter().map(|r| r.can_partial_explore).collect::<Vec<_>>());
    let sizes = UInt64Array::from(rows.iter().map(|r| r.size).collect::<Vec<_>>());
    let last_modified: StringArray = rows.iter().map(|r| r.last_modified.clone()).collect();
    let is_directory = BooleanArray::from(rows.iter().map(|r| r.is_directory).collect::<Vec<_>>());

    Ok(RecordBatch::try_new(
        schema,
        vec![
            Arc::new(file_names),
            Arc::new(formats),
            Arc::new(can_inspect),
            Arc::new(can_partial_explore),
            Arc::new(sizes),
            Arc::new(last_modified),
            Arc::new(is_directory),
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
    /// yet the walk behind them: a recursive listing classifies objects into
    /// datasets in one pass and the two do not correspond one to one, so
    /// stopping the walk early needs the classifier to work in chunks. The rows
    /// are correct either way; the walk is the part still to shorten.
    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let mut rows = self.rows(state).await?;
        if let Some(limit) = limit {
            rows.truncate(limit);
        }
        let batch = rows_batch(self.schema(), &rows)?;
        MemTable::try_new(self.schema(), vec![vec![batch]])?
            .scan(state, projection, filters, limit)
            .await
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

/// `browse_datasets([prefix[, offset[, limit]]])`: one directory level.
///
/// The same provider as `list_datasets`, stopped at one level. A folder view
/// wants this: its cost is one delimiter request, so it does not grow with the
/// size of the store below the prefix.
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
            "Lists one directory level of the datasets store. Optional arguments: \
             browse_datasets(prefix, offset, limit) — a directory (default the root), \
             a row offset, and a row limit. Sub-directories come back as rows with \
             `is_directory` set. Unlike list_datasets it does not descend, so its \
             cost does not grow with the size of the store below the prefix."
                .to_string(),
        )
    }
}

impl TableFunctionImpl for BrowseDatasetsFunc {
    fn call(&self, args: &[Expr]) -> datafusion::error::Result<Arc<dyn TableProvider>> {
        let prefix = string_arg(args, 0).unwrap_or_default();
        let offset = usize_arg(args, 1).unwrap_or(0);
        let limit = usize_arg(args, 2);

        Ok(Arc::new(DatasetsTable::new(
            Listing::Level { prefix },
            offset,
            limit,
            self.file_formats.clone(),
        )))
    }
}
