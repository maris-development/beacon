use std::sync::Arc;

use datafusion::{
    common::Column,
    datasource::{
        listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl},
        physical_plan::{parquet::ParquetExecBuilder, FileScanConfig},
    },
    execution::{
        object_store::ObjectStoreUrl,
        options::{ArrowReadOptions, ReadOptions},
    },
    logical_expr::{LogicalPlanBuilder, SortExpr},
    prelude::{col, lit, CsvReadOptions, Expr, ParquetReadOptions, SessionContext},
};
use utoipa::ToSchema;

use crate::{
    output::OutputFormat, sources::parquet_format::SuperParquetFormat, super_typing::super_type_schema,
};

pub mod parser;

#[derive(Debug, serde::Serialize, serde::Deserialize, ToSchema)]
pub struct Query {
    #[serde(flatten)]
    pub inner: InnerQuery,
    #[schema(value_type = Object)]
    pub output: OutputFormat,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, ToSchema)]
pub enum InnerQuery {
    #[serde(rename = "sql")]
    Sql(String),
    #[serde(untagged)]
    Json(QueryBody),
}

#[derive(Debug, serde::Serialize, serde::Deserialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub struct QueryBody {
    select: Vec<Select>,
    filter: Option<Filter>,
    from: From,
    sort_by: Option<Vec<Sort>>,
    distinct: Option<Vec<String>>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub struct Select {
    pub column: String,
    pub alias: Option<String>,
}

impl Select {
    pub fn to_expr(&self) -> Expr {
        match &self.alias {
            Some(alias) => col(Column::from_qualified_name_ignore_case(&self.column)).alias(alias),
            None => col(Column::from_qualified_name_ignore_case(&self.column)),
        }
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum From {
    Parquet {
        path: PathType,
    },
    ArrowIpc {
        path: PathType,
    },
    Csv {
        path: PathType,
        delimiter: Option<char>,
        header: Option<bool>,
    },
}

#[derive(Debug, serde::Serialize, serde::Deserialize, ToSchema)]
#[serde(untagged)]
pub enum PathType {
    ManyPaths(Vec<String>),
    Path(String),
}

impl From {
    pub async fn init_builder(
        &self,
        session_ctx: &SessionContext,
    ) -> anyhow::Result<LogicalPlanBuilder> {
        match self {
            From::Parquet { path } => {
                let paths = match path {
                    PathType::ManyPaths(items) => items.clone(),
                    PathType::Path(path) => vec![path.clone()],
                };
                let session_state = session_ctx.state();

                let listing_options = ListingOptions::new(Arc::new(SuperParquetFormat::new()));

                let table_urls = paths
                    .iter()
                    .map(|path| ListingTableUrl::parse(path).unwrap())
                    .collect::<Vec<_>>();

                // LogicalPlanBuilder::copy_to(input, output_url, file_type, options, partition_by)

                let mut schemas = vec![];
                for table_url in &table_urls {
                    let schema = listing_options
                        .infer_schema(&session_state, table_url)
                        .await?;
                    schemas.push(schema);
                }

                let super_schema = Arc::new(
                    super_type_schema(&schemas)
                        .map_err(|e| anyhow::anyhow!("Failed to super type schema: {}", e))?,
                );

                let config = ListingTableConfig::new_with_multi_paths(table_urls)
                    .with_listing_options(listing_options)
                    .with_schema(super_schema);

                let table = ListingTable::try_new(config).unwrap();

                let plan = session_ctx
                    .read_table(Arc::new(table))?
                    .into_unoptimized_plan();

                Ok(LogicalPlanBuilder::new(plan))
            }
            From::Csv {
                path,
                delimiter,
                header,
            } => {
                let options = CsvReadOptions::new()
                    .delimiter_option(delimiter.map(|d| d as u8))
                    .has_header(header.unwrap_or(true));
                let paths = match path {
                    PathType::ManyPaths(items) => items.clone(),
                    PathType::Path(path) => vec![path.clone()],
                };
                Ok(LogicalPlanBuilder::new(
                    session_ctx
                        .read_csv(paths, options)
                        .await?
                        .into_unoptimized_plan(),
                ))
            }
            From::ArrowIpc { path } => {
                let path = match path {
                    PathType::ManyPaths(items) => items.clone(),
                    PathType::Path(path) => vec![path.clone()],
                };
                Ok(LogicalPlanBuilder::new(
                    session_ctx
                        .read_arrow(path, ArrowReadOptions::default())
                        .await?
                        .into_unoptimized_plan(),
                ))
            }
        }
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum Filter {
    IsNull {
        column: String,
    },
    IsNotNull {
        column: String,
    },
    And(Vec<Filter>),
    Or(Vec<Filter>),
    #[serde(untagged)]
    Range {
        column: String,
        #[serde(flatten)]
        filter: RangeFilter,
    },
    #[serde(untagged)]
    GreaterThan {
        column: String,
        #[serde(flatten)]
        filter: GreaterThanFilter,
    },
    #[serde(untagged)]
    GreaterThanOrEqual {
        column: String,
        #[serde(flatten)]
        filter: GreaterThanOrEqualFilter,
    },
    #[serde(untagged)]
    LessThan {
        column: String,
        #[serde(flatten)]
        filter: LessThanFilter,
    },
    #[serde(untagged)]
    LessThanOrEqual {
        column: String,
        #[serde(flatten)]
        filter: LessThanOrEqualFilter,
    },
    #[serde(untagged)]
    Equality {
        column: String,
        #[serde(flatten)]
        filter: EqualityFilter,
    },
    #[serde(untagged)]
    NotEqual {
        column: String,
        #[serde(flatten)]
        filter: NotEqualFilter,
    },
}

impl Filter {
    pub fn column_name(name: &str) -> Expr {
        col(Column::from_qualified_name_ignore_case(name))
    }

    pub fn to_expr(&self) -> anyhow::Result<Expr> {
        Ok(match self {
            Filter::Range { column, filter } => filter
                .to_expr(Self::column_name(column))
                .ok_or_else(|| anyhow::anyhow!("Invalid range filter expression."))?,
            Filter::GreaterThan { column, filter } => filter.to_expr(col(column)),
            Filter::GreaterThanOrEqual { column, filter } => filter.to_expr(col(column)),
            Filter::LessThan { column, filter } => filter.to_expr(col(column)),
            Filter::LessThanOrEqual { column, filter } => filter.to_expr(col(column)),
            Filter::Equality { column, filter } => filter.to_expr(col(column)),
            Filter::NotEqual { column, filter } => filter.to_expr(col(column)),
            Filter::IsNull { column } => col(column).is_null(),
            Filter::IsNotNull { column } => col(column).is_not_null(),
            Filter::And(filters) => filters
                .iter()
                .map(|f| f.to_expr())
                .fold(Ok(lit(true)), |acc, expr| {
                    acc.and_then(|acc| expr.map(|expr| acc.and(expr)))
                })?,
            Filter::Or(filters) => filters
                .iter()
                .map(|f| f.to_expr())
                .fold(Ok(lit(false)), |acc, expr| {
                    acc.and_then(|acc| expr.map(|expr| acc.or(expr)))
                })?,
        })
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize, ToSchema)]
#[serde(untagged)]
pub enum RangeFilter {
    Numeric {
        min: Option<f64>,
        max: Option<f64>,
    },
    String {
        min: Option<String>,
        max: Option<String>,
    },
}

impl RangeFilter {
    pub fn to_expr(&self, col: Expr) -> Option<Expr> {
        match self {
            RangeFilter::Numeric { min, max } => match (min, max) {
                (Some(min), Some(max)) => {
                    Some(col.clone().gt_eq(lit(*min)).and(col.lt_eq(lit(*max))))
                }
                (Some(min), None) => Some(col.gt_eq(lit(*min))),
                (None, Some(max)) => Some(col.lt_eq(lit(*max))),
                (None, None) => None,
            },
            RangeFilter::String { min, max } => match (min, max) {
                (Some(min), Some(max)) => {
                    Some(col.clone().gt_eq(lit(min)).and(col.lt_eq(lit(max))))
                }
                (Some(min), None) => Some(col.gt_eq(lit(min))),
                (None, Some(max)) => Some(col.lt_eq(lit(max))),
                (None, None) => None,
            },
        }
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize, ToSchema)]
#[serde(untagged)]
pub enum EqualityFilter {
    Numeric { eq: f64 },
    String { eq: String },
}

impl EqualityFilter {
    pub fn to_expr(&self, col: Expr) -> Expr {
        match self {
            EqualityFilter::Numeric { eq } => col.eq(lit(*eq)),
            EqualityFilter::String { eq } => col.eq(lit(eq)),
        }
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize, ToSchema)]
#[serde(untagged)]
pub enum NotEqualFilter {
    Numeric { neq: f64 },
    String { neq: String },
}

impl NotEqualFilter {
    pub fn to_expr(&self, col: Expr) -> Expr {
        match self {
            NotEqualFilter::Numeric { neq } => col.not_eq(lit(*neq)),
            NotEqualFilter::String { neq } => col.not_eq(lit(neq)),
        }
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize, ToSchema)]
#[serde(untagged)]
pub enum GreaterThanFilter {
    Numeric { gt: f64 },
    String { gt: String },
}

impl GreaterThanFilter {
    pub fn to_expr(&self, col: Expr) -> Expr {
        match self {
            GreaterThanFilter::Numeric { gt } => col.gt(lit(*gt)),
            GreaterThanFilter::String { gt } => col.gt(lit(gt)),
        }
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize, ToSchema)]
#[serde(untagged)]
pub enum GreaterThanOrEqualFilter {
    Numeric { gte: f64 },
    String { gte: String },
}

impl GreaterThanOrEqualFilter {
    pub fn to_expr(&self, col: Expr) -> Expr {
        match self {
            GreaterThanOrEqualFilter::Numeric { gte } => col.gt_eq(lit(*gte)),
            GreaterThanOrEqualFilter::String { gte } => col.gt_eq(lit(gte)),
        }
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize, ToSchema)]
#[serde(untagged)]
pub enum LessThanFilter {
    Numeric { lt: f64 },
    String { lt: String },
}

impl LessThanFilter {
    pub fn to_expr(&self, col: Expr) -> Expr {
        match self {
            LessThanFilter::Numeric { lt } => col.lt(lit(*lt)),
            LessThanFilter::String { lt } => col.lt(lit(lt)),
        }
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize, ToSchema)]
#[serde(untagged)]
pub enum LessThanOrEqualFilter {
    Numeric { lte: f64 },
    String { lte: String },
}

impl LessThanOrEqualFilter {
    pub fn to_expr(&self, col: Expr) -> Expr {
        match self {
            LessThanOrEqualFilter::Numeric { lte } => col.lt_eq(lit(*lte)),
            LessThanOrEqualFilter::String { lte } => col.lt_eq(lit(lte)),
        }
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize, ToSchema)]
pub enum Sort {
    Asc(String),
    Desc(String),
}

impl Sort {
    pub fn to_expr(&self) -> SortExpr {
        match self {
            Sort::Asc(column) => SortExpr::new(col(column), true, false),
            Sort::Desc(column) => SortExpr::new(col(column), false, false),
        }
    }
}
