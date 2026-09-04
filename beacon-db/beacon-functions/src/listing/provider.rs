//! [`DatasetsTable`]: the table both listing functions return.
//!
//! # Nothing happens until it is scanned
//!
//! The listing used to run inside `TableFunctionImpl::call`, a *synchronous*
//! trait method, so it reached the store through `block_in_place` + `block_on`
//! and held a worker thread for the whole walk — during logical planning, before
//! anything decided to read it. [`TableProvider::scan`] is async, so the walk
//! belongs there, and even there it only builds the plan: the walk itself starts
//! when the plan is executed. See [`super::exec`].

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use beacon_datafusion_ext::format_ext::{DatasetMetadata, FileFormatFactoryExt};
use beacon_datafusion_ext::listing_factory::{ListingFactory, ObjectLevel};
use datafusion::{
    arrow::datatypes::SchemaRef,
    catalog::{Session, TableProvider},
    datasource::TableType,
    error::DataFusionError,
    physical_expr::PhysicalExpr,
    physical_plan::{ExecutionPlan, expressions::Column, projection::ProjectionExec},
    prelude::Expr,
};
use futures::stream::StreamExt;
use object_store::ObjectMeta;

use super::exec::{DatasetsExec, RowStreamFactory};

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
        // True for a sub-directory, which only a one-level listing reports. A
        // recursive listing describes files, so this is always false there.
        Field::new("is_directory", DataType::Boolean, false),
    ]))
}

/// One row of a listing: a dataset, or a sub-directory of one.
///
/// The Rust shape of [`list_datasets_schema`], field for field. Public because
/// [`DatasetsExec`] streams it, and a plan node that cannot name its own rows
/// cannot be built by a caller.
#[derive(Debug, Clone)]
pub struct Row {
    pub file_name: String,
    pub file_format: String,
    pub can_inspect: bool,
    pub can_partial_explore: bool,
    pub size: Option<u64>,
    pub last_modified: Option<String>,
    pub is_directory: bool,
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
    pub(super) fn directory(path: String) -> Self {
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

/// What a [`DatasetsTable`] enumerates.
#[derive(Debug, Clone)]
pub enum Listing {
    /// Every dataset the glob matches, anywhere below it.
    Glob { pattern: String },
    /// One directory level: what is directly inside `prefix`.
    Level { prefix: String },
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

    /// How this listing prints in `EXPLAIN`.
    fn label(&self) -> String {
        match &self.listing {
            Listing::Glob { pattern } => format!("glob={pattern}"),
            Listing::Level { prefix } if prefix.is_empty() => "level=<root>".to_string(),
            Listing::Level { prefix } => format!("level={prefix}"),
        }
    }
}

/// The first format that claims `object`, as a row.
///
/// Order is registration order, and the first claim wins: a format that reads an
/// extension another also reads would otherwise produce two rows for one file.
fn classify(formats: &[Arc<dyn FileFormatFactoryExt>], object: &ObjectMeta) -> Option<Row> {
    formats
        .iter()
        .find_map(|format| format.classify_object(object))
        .map(Row::from)
}

/// The tighter of the function's own limit and the planner push-down.
fn effective_limit(declared: Option<usize>, pushed: Option<usize>) -> Option<usize> {
    match (declared, pushed) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (a, b) => a.or(b),
    }
}

/// One directory level, as rows. Directories first, so it reads like one.
///
/// Folder names arrive relative to the level, and a row carries a full path, so
/// they are joined back on here.
fn level_rows(formats: &[Arc<dyn FileFormatFactoryExt>], level: ObjectLevel) -> Vec<Row> {
    let base = level.prefix.trim_end_matches('/').to_string();
    let join = |name: &str| {
        if base.is_empty() {
            name.to_string()
        } else {
            format!("{base}/{name}")
        }
    };
    level
        .folders
        .iter()
        .map(|name| Row::directory(join(name)))
        .chain(level.objects.iter().filter_map(|object| classify(formats, object)))
        .collect()
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

    /// Builds the plan. The walk itself runs when the plan is executed.
    ///
    /// `limit` is the planner push-down, and here it bounds the walk as well as
    /// the rows: a glob listing is a lazy stream, so the node stopping stops the
    /// pages behind it. `LIMIT 50` over a bucket of millions reads one page.
    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let schema = self.schema();
        let factory = state
            .config()
            .get_extension::<ListingFactory>()
            .ok_or_else(|| {
                DataFusionError::Execution(
                    "the listing factory is not registered on the session".to_string(),
                )
            })?;

        // The two listings differ only here.
        let rows: RowStreamFactory = match &self.listing {
            // A glob streams. The path is resolved here, against this session.
            // The handle holds no session, so each execute rebuilds the walk
            // from it and the plan can run more than once.
            Listing::Glob { pattern } => {
                let listing = factory.listing(state, pattern)?;
                let formats = self.file_formats.clone();
                Arc::new(move || {
                    let formats = formats.clone();
                    Ok(listing
                        .stream()
                        .filter_map(move |object| {
                            let formats = formats.clone();
                            async move {
                                match object {
                                    // An object no format claims is not a dataset,
                                    // and not an error either.
                                    Ok(object) => classify(&formats, &object).map(Ok),
                                    Err(e) => Some(Err(e)),
                                }
                            }
                        })
                        .boxed())
                })
            }
            // One level is a single request. `scan` is async, so it is answered
            // here and the rows are what the plan replays.
            Listing::Level { prefix } => {
                let level = factory.listing(state, prefix)?.level().await?;
                let rows = Arc::new(level_rows(&self.file_formats, level));
                Arc::new(move || {
                    let rows = Arc::clone(&rows);
                    Ok(futures::stream::iter((0..rows.len()).map(move |i| Ok(rows[i].clone())))
                        .boxed())
                })
            }
        };

        let plan = Arc::new(DatasetsExec::new(
            Arc::clone(&schema),
            rows,
            self.offset,
            effective_limit(self.limit, limit),
            self.label(),
        ));

        // The node produces the full row shape, so a projection sits above it.
        match projection {
            Some(projection) => {
                let exprs: Vec<(Arc<dyn PhysicalExpr>, String)> = projection
                    .iter()
                    .map(|index| {
                        let field = schema.field(*index);
                        (
                            Arc::new(Column::new(field.name(), *index)) as Arc<dyn PhysicalExpr>,
                            field.name().to_string(),
                        )
                    })
                    .collect();
                Ok(Arc::new(ProjectionExec::try_new(exprs, plan)?))
            }
            None => Ok(plan),
        }
    }
}
