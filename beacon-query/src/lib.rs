use std::{path::Path, sync::Arc};

use beacon_output::OutputFormat;
use datafusion::{
    common::Column,
    datasource::{listing::ListingTableUrl, provider_as_source},
    execution::SessionState,
    logical_expr::{LogicalPlanBuilder, SortExpr},
    prelude::{col, lit, lit_timestamp_nano, CsvReadOptions, Expr, SessionContext},
};
use utoipa::ToSchema;

use beacon_sources::{
    arrow_format::SuperArrowFormat, formats_factory::Formats, odv_format::OdvFormat,
    parquet_format::SuperParquetFormat, DataSource,
};

pub mod parser;
pub mod plan;

#[derive(Debug, serde::Serialize, serde::Deserialize, ToSchema)]
pub struct Query {
    #[serde(flatten)]
    pub inner: InnerQuery,
    #[schema(value_type = Object)]
    pub output: OutputFormat,
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
    Column {
        #[serde(alias = "column_name")]
        column: String,
        alias: Option<String>,
    },
    Function {
        function: String,
        args: Vec<Select>,
        alias: String,
    },
}

impl Select {
    pub fn to_expr(&self, session_state: &SessionState) -> anyhow::Result<Expr> {
        match self {
            Select::Column { column, alias } => match alias {
                Some(alias) => {
                    Ok(col(Column::from_qualified_name_ignore_case(column)).alias(alias))
                }
                None => Ok(col(Column::from_qualified_name_ignore_case(column))),
            },
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

                Ok(expr.alias(alias))
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

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum Filter {
    #[serde(alias = "is_null", alias = "is_missing")]
    IsNull {
        #[serde(alias = "for_query_parameter")]
        column: String,
    },
    #[serde(
        alias = "is_not_null",
        alias = "skip_fill_values",
        alias = "skip_missing"
    )]
    IsNotNull {
        #[serde(alias = "for_query_parameter")]
        column: String,
    },
    And(Vec<Filter>),
    Or(Vec<Filter>),
    #[serde(untagged)]
    GreaterThan {
        #[serde(alias = "for_query_parameter")]
        column: String,
        #[serde(flatten)]
        filter: GreaterThanFilter,
    },
    #[serde(untagged)]
    GreaterThanOrEqual {
        #[serde(alias = "for_query_parameter")]
        column: String,
        #[serde(flatten)]
        filter: GreaterThanOrEqualFilter,
    },
    #[serde(untagged)]
    LessThan {
        #[serde(alias = "for_query_parameter")]
        column: String,
        #[serde(flatten)]
        filter: LessThanFilter,
    },
    #[serde(untagged)]
    LessThanOrEqual {
        #[serde(alias = "for_query_parameter")]
        column: String,
        #[serde(flatten)]
        filter: LessThanOrEqualFilter,
    },
    #[serde(untagged)]
    Equality {
        #[serde(alias = "for_query_parameter")]
        column: String,
        #[serde(flatten)]
        filter: EqualityFilter,
    },
    #[serde(untagged)]
    NotEqual {
        #[serde(alias = "for_query_parameter")]
        column: String,
        #[serde(flatten)]
        filter: NotEqualFilter,
    },
    #[serde(untagged)]
    Range {
        #[serde(alias = "for_query_parameter")]
        column: String,
        #[serde(flatten)]
        filter: RangeFilter,
    },
    #[serde(untagged)]
    GeoJson(GeoJsonFilter),
}

fn column_name(name: &str) -> Expr {
    col(Column::from_qualified_name_ignore_case(name))
}

impl Filter {
    pub fn column_name(name: &str) -> Expr {
        col(Column::from_qualified_name_ignore_case(name))
    }

    pub fn to_expr(&self, session_state: &SessionState) -> anyhow::Result<Expr> {
        Ok(match self {
            Filter::Range { column, filter } => filter
                .to_expr(Self::column_name(column))
                .ok_or_else(|| anyhow::anyhow!("Invalid range filter expression."))?,
            Filter::GreaterThan { column, filter } => filter.to_expr(Self::column_name(column)),
            Filter::GreaterThanOrEqual { column, filter } => {
                filter.to_expr(Self::column_name(column))
            }
            Filter::LessThan { column, filter } => filter.to_expr(Self::column_name(column)),
            Filter::LessThanOrEqual { column, filter } => filter.to_expr(Self::column_name(column)),
            Filter::Equality { column, filter } => filter.to_expr(Self::column_name(column)),
            Filter::NotEqual { column, filter } => filter.to_expr(Self::column_name(column)),
            Filter::IsNull { column } => Self::column_name(column).is_null(),
            Filter::IsNotNull { column } => Self::column_name(column).is_not_null(),
            Filter::And(filters) => filters
                .iter()
                .map(|f| f.to_expr(session_state))
                .fold(Ok(lit(true)), |acc, expr| {
                    acc.and_then(|acc| expr.map(|expr| acc.and(expr)))
                })?,
            Filter::Or(filters) => filters
                .iter()
                .map(|f| f.to_expr(session_state))
                .fold(Ok(lit(false)), |acc, expr| {
                    acc.and_then(|acc| expr.map(|expr| acc.or(expr)))
                })?,
            Filter::GeoJson(filter) => filter.to_expr(session_state)?,
        })
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, ToSchema)]
pub struct GeoJsonFilter {
    #[serde(alias = "longitude_query_parameter")]
    longitude_column: String,
    #[serde(alias = "latitude_query_parameter")]
    latitude_column: String,
    #[schema(value_type = Object)]
    geometry: geojson::Geometry,
}

impl GeoJsonFilter {
    pub fn to_expr(&self, session_state: &SessionState) -> anyhow::Result<Expr> {
        let lon = column_name(&self.longitude_column);
        let lat = column_name(&self.latitude_column);

        let st_geojson_as_wkt = session_state
            .scalar_functions()
            .get("st_geojson_as_wkt")
            .ok_or_else(|| {
                anyhow::anyhow!("Function st_geojson_as_wkt not found in the registry.")
            })?
            .clone();

        let wkt_str = st_geojson_as_wkt.call(vec![lit(self.geometry.to_string())]);

        let st_within_point = session_state
            .scalar_functions()
            .get("st_within_point")
            .ok_or_else(|| anyhow::anyhow!("Function st_within_point not found in the registry."))?
            .clone();

        let filter_expr = st_within_point.call(vec![wkt_str, lon, lat]);

        Ok(filter_expr)
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, ToSchema)]
#[serde(untagged)]
pub enum RangeFilter {
    Numeric {
        min: Option<f64>,
        max: Option<f64>,
    },
    Timestamp {
        min: Option<chrono::NaiveDateTime>,
        max: Option<chrono::NaiveDateTime>,
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
            RangeFilter::Timestamp { min, max } => match (min, max) {
                (Some(min), Some(max)) => Some(
                    col.clone()
                        .gt_eq(lit_timestamp_nano(min.timestamp_nanos()))
                        .and(col.lt_eq(lit_timestamp_nano(max.timestamp_nanos()))),
                ),
                (Some(min), None) => Some(col.gt_eq(lit_timestamp_nano(min.timestamp_nanos()))),
                (None, Some(max)) => Some(col.lt_eq(lit_timestamp_nano(max.timestamp_nanos()))),
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

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, ToSchema)]
#[serde(untagged)]
pub enum EqualityFilter {
    Numeric { eq: f64 },
    Timestamp { eq: chrono::NaiveDateTime },
    String { eq: String },
}

impl EqualityFilter {
    pub fn to_expr(&self, col: Expr) -> Expr {
        match self {
            EqualityFilter::Numeric { eq } => col.eq(lit(*eq)),
            EqualityFilter::Timestamp { eq } => col.eq(lit_timestamp_nano(eq.timestamp_nanos())),
            EqualityFilter::String { eq } => col.eq(lit(eq)),
        }
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, ToSchema)]
#[serde(untagged)]
pub enum NotEqualFilter {
    Numeric { neq: f64 },
    Timestamp { neq: chrono::NaiveDateTime },
    String { neq: String },
}

impl NotEqualFilter {
    pub fn to_expr(&self, col: Expr) -> Expr {
        match self {
            NotEqualFilter::Numeric { neq } => col.not_eq(lit(*neq)),
            NotEqualFilter::Timestamp { neq } => {
                col.not_eq(lit_timestamp_nano(neq.timestamp_nanos()))
            }
            NotEqualFilter::String { neq } => col.not_eq(lit(neq)),
        }
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, ToSchema)]
#[serde(untagged)]
pub enum GreaterThanFilter {
    Numeric { gt: f64 },
    Timestamp { gt: chrono::NaiveDateTime },
    String { gt: String },
}

impl GreaterThanFilter {
    pub fn to_expr(&self, col: Expr) -> Expr {
        match self {
            GreaterThanFilter::Numeric { gt } => col.gt(lit(*gt)),
            GreaterThanFilter::Timestamp { gt } => col.gt(lit_timestamp_nano(gt.timestamp_nanos())),
            GreaterThanFilter::String { gt } => col.gt(lit(gt)),
        }
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, ToSchema)]
#[serde(untagged)]
pub enum GreaterThanOrEqualFilter {
    Numeric { gte: f64 },
    Timestamp { gte: chrono::NaiveDateTime },
    String { gte: String },
}

impl GreaterThanOrEqualFilter {
    pub fn to_expr(&self, col: Expr) -> Expr {
        match self {
            GreaterThanOrEqualFilter::Numeric { gte } => col.gt_eq(lit(*gte)),
            GreaterThanOrEqualFilter::Timestamp { gte } => {
                col.gt_eq(lit_timestamp_nano(gte.timestamp_nanos()))
            }
            GreaterThanOrEqualFilter::String { gte } => col.gt_eq(lit(gte)),
        }
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, ToSchema)]
#[serde(untagged)]
pub enum LessThanFilter {
    Numeric { lt: f64 },
    Timestamp { lt: chrono::NaiveDateTime },
    String { lt: String },
}

impl LessThanFilter {
    pub fn to_expr(&self, col: Expr) -> Expr {
        match self {
            LessThanFilter::Numeric { lt } => col.lt(lit(*lt)),
            LessThanFilter::Timestamp { lt } => col.lt(lit_timestamp_nano(lt.timestamp_nanos())),
            LessThanFilter::String { lt } => col.lt(lit(lt)),
        }
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, ToSchema)]
#[serde(untagged)]
pub enum LessThanOrEqualFilter {
    Numeric { lte: f64 },
    Timestamp { lte: chrono::NaiveDateTime },
    String { lte: String },
}

impl LessThanOrEqualFilter {
    pub fn to_expr(&self, col: Expr) -> Expr {
        match self {
            LessThanOrEqualFilter::Numeric { lte } => col.lt_eq(lit(*lte)),
            LessThanOrEqualFilter::Timestamp { lte } => {
                col.lt_eq(lit_timestamp_nano(lte.timestamp_nanos()))
            }
            LessThanOrEqualFilter::String { lte } => col.lt_eq(lit(lte)),
        }
    }
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
