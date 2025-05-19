use std::{path::Path, sync::Arc};

use beacon_output::{Output, OutputFormat};
use datafusion::{
    arrow::datatypes::DataType,
    common::Column,
    datasource::{listing::ListingTableUrl, provider_as_source},
    execution::SessionState,
    logical_expr::{expr, ExprSchemable, LogicalPlanBuilder, SortExpr},
    prelude::{col, lit, lit_timestamp_nano, try_cast, CsvReadOptions, Expr, SessionContext},
    scalar::ScalarValue,
};
use filter::Filter;
use utoipa::ToSchema;

use beacon_sources::{
    arrow_format::SuperArrowFormat, formats_factory::Formats, odv_format::OdvFormat,
    parquet_format::SuperParquetFormat, DataSource,
};

pub mod filter;
pub mod output;
pub mod parser;
pub mod plan;
pub mod util;

#[derive(Debug, serde::Serialize, serde::Deserialize, ToSchema)]
pub struct Query {
    #[serde(flatten)]
    pub inner: InnerQuery,
    #[schema(value_type = Object)]
    pub output: Output,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, ToSchema)]
pub enum InnerQuery {
    #[serde(rename = "sql")]
    Sql(String),
    #[serde(untagged)]
    Json(QueryBody),
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct QueryBody {
    #[serde(alias = "query_parameters")]
    select: Vec<Select>,
    filter: Option<Filter>,
    // To Support legacy queries
    #[schema(ignore)]
    filters: Option<Vec<Filter>>,
    #[serde(default)]
    from: Option<From>,
    sort_by: Option<Vec<Sort>>,
    distinct: Option<Vec<String>>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, ToSchema)]
#[serde(untagged)]
pub enum Select {
    TryCast {
        #[serde(skip_serializing)]
        #[serde(flatten)]
        select: Box<Select>,
        #[schema(value_type = String)]
        try_cast: DataType,
    },
    ColumnName(String),
    Column {
        #[serde(alias = "column_name")]
        column: String,
        alias: Option<String>,
    },
    Function {
        function: String,
        args: Vec<Select>,
        alias: Option<String>,
    },
    Literal {
        value: Literal,
        alias: Option<String>,
    },
}
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, ToSchema)]
#[serde(untagged)]
pub enum Literal {
    String(String),
    Number(f64),
    Boolean(bool),
    Null(Option<()>),
}

impl Literal {
    pub fn to_expr(&self) -> Expr {
        match self {
            Literal::Null(_) => lit(ScalarValue::Null),
            Literal::String(s) => lit(s),
            Literal::Number(n) => lit(*n),
            Literal::Boolean(b) => lit(*b),
        }
    }
}

impl Select {
    pub fn to_expr(&self, session_state: &SessionState) -> anyhow::Result<Expr> {
        match self {
            Select::ColumnName(name) => Ok(column_name(name)),
            Select::Column { column, alias } => match alias {
                Some(alias) => Ok(column_name(column).alias(alias)),
                None => Ok(column_name(column)),
            },
            Select::Literal { value, alias } => {
                let expr = value.to_expr();

                match alias {
                    Some(alias) => Ok(expr.alias(alias)),
                    None => Ok(expr),
                }
            }
            Select::Function {
                function,
                args,
                alias,
            } => {
                let function = session_state
                    .scalar_functions()
                    .get(function)
                    .ok_or_else(|| {
                        anyhow::anyhow!("Function {} not found in the registry.", function)
                    })?
                    .clone();

                let args = args
                    .iter()
                    .map(|arg| arg.to_expr(session_state))
                    .collect::<anyhow::Result<Vec<_>>>()?;

                let expr = function.call(args);

                match alias {
                    Some(alias) => Ok(expr.alias(alias)),
                    None => Ok(expr),
                }
            }
            Select::TryCast {
                select,
                try_cast: cast_as,
            } => {
                let expr = select.to_expr(session_state)?;

                Ok(try_cast(expr, cast_as.clone()))
            }
        }
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, ToSchema)]
#[serde(rename_all = "lowercase")]
#[serde(deny_unknown_fields)]
pub enum From {
    #[serde(untagged)]
    Table(String),
    #[serde(untagged)]
    Format {
        #[serde(flatten)]
        format: Formats,
    },
}

impl Default for From {
    fn default() -> Self {
        From::Table(beacon_config::CONFIG.default_table.clone())
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, ToSchema)]
#[serde(untagged)]
#[serde(deny_unknown_fields)]
pub enum FileSystemPath {
    ManyPaths(Vec<String>),
    Path(String),
}

impl FileSystemPath {
    pub fn parse_to_url<P: AsRef<Path>>(path: P) -> anyhow::Result<ListingTableUrl> {
        let table_url =
            ListingTableUrl::parse(&format!("/datasets/{}", path.as_ref().to_string_lossy()))?;
        if table_url
            .prefix()
            .prefix_matches(&beacon_config::DATASETS_DIR_PREFIX)
        {
            Ok(table_url)
        } else {
            Err(anyhow::anyhow!(
                "Path {} is not within the datasets directory.",
                table_url.as_str()
            ))
        }
    }
}

impl TryInto<Vec<ListingTableUrl>> for &FileSystemPath {
    type Error = anyhow::Error;

    fn try_into(self) -> Result<Vec<ListingTableUrl>, Self::Error> {
        match self {
            FileSystemPath::ManyPaths(items) => Ok(items
                .into_iter()
                .map(|path| FileSystemPath::parse_to_url(path))
                .collect::<anyhow::Result<_>>()?),
            FileSystemPath::Path(path) => Ok(vec![FileSystemPath::parse_to_url(path)
                .map_err(|e| anyhow::anyhow!("Failed to parse path: {}", e))?]),
        }
    }
}

impl From {
    pub async fn init_builder(
        &self,
        session_ctx: &SessionContext,
    ) -> anyhow::Result<LogicalPlanBuilder> {
        match self {
            From::Table(table) => session_ctx
                .table(table)
                .await
                .map(|table| LogicalPlanBuilder::new(table.into_unoptimized_plan()))
                .map_err(|e| {
                    anyhow::anyhow!(
                        "Failed to create logical plan builder for table {}: {}",
                        table,
                        e
                    )
                }),
            From::Format { format } => format.create_plan_builder(&session_ctx.state()).await,
        }
    }
}

fn column_name(name: &str) -> Expr {
    col(Column::from_name(name))
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, ToSchema)]
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

#[derive(Debug, serde::Serialize, serde::Deserialize, ToSchema)]
pub enum CopyTo {
    #[serde(alias = "csv")]
    Csv,
    #[serde(alias = "parquet")]
    Parquet,
    #[serde(alias = "arrow")]
    ArrowIpc,
    #[serde(alias = "json")]
    Json,
    #[serde(alias = "odv")]
    Odv,
    #[serde(alias = "netcdf")]
    NetCDF,
}
