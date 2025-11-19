use std::{path::Path, sync::Arc};

use datafusion::{
    arrow::datatypes::DataType,
    common::Column,
    datasource::{listing::ListingTableUrl, provider_as_source, DefaultTableSource},
    execution::SessionState,
    logical_expr::{expr, ExprSchemable, LogicalPlan, LogicalPlanBuilder, SortExpr, TableSource},
    prelude::{col, lit, lit_timestamp_nano, try_cast, CsvReadOptions, Expr, SessionContext},
    scalar::ScalarValue,
};
use filter::Filter;
use utoipa::ToSchema;

use crate::output::Output;

pub mod filter;
pub mod from;
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
    from: Option<crate::from::From>,
    sort_by: Option<Vec<Sort>>,
    distinct: Option<Distinct>,
    offset: Option<usize>,
    limit: Option<usize>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, ToSchema)]
pub struct Distinct {
    pub on: Vec<Select>,
    pub select: Vec<Select>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, ToSchema)]
#[serde(untagged)]
pub enum Select {
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
    pub fn collect_columns(&self, columns: &mut Vec<String>) {
        match self {
            Select::ColumnName(name) => {
                columns.push(name.clone());
            }
            Select::Column { column, .. } => {
                columns.push(column.clone());
            }
            Select::Literal { .. } => {}
            Select::Function { args, .. } => {
                for arg in args {
                    arg.collect_columns(columns);
                }
            }
        }
    }

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
