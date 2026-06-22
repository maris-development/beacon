use datafusion::{
    common::Column,
    execution::SessionState,
    logical_expr::SortExpr,
    prelude::{col, lit, Expr},
    scalar::ScalarValue,
};
use filter::Filter;
use utoipa::ToSchema;

use crate::query::output::Output;

pub mod compiler;
pub mod filter;
pub mod from;
pub mod output;

pub use compiler::compile_json_query;

/// A Beacon query request body.
///
/// The query is either a raw SQL string (`{"sql": "SELECT ..."}`) or a structured
/// JSON query (the fields of [`QueryBody`]: `select`, `filter`, `from`, ...). The
/// `inner` query is flattened, so its fields appear at the top level of the body.
/// An optional [`Output`] selects the result format (the default is an Arrow IPC
/// stream).
#[derive(Debug, serde::Serialize, serde::Deserialize, ToSchema)]
#[schema(example = json!({
    "select": [{ "column": "temperature" }, { "column": "depth" }],
    "filter": { "column": "depth", "gt_eq": 0, "lt_eq": 100 },
    "output": { "format": "csv" }
}))]
pub struct Query {
    /// The query itself: either SQL or a structured JSON query.
    #[serde(flatten)]
    pub inner: InnerQuery,
    /// Result output format. Omit for the default zstd-compressed Arrow IPC stream.
    pub output: Option<Output>,
}

impl Query {
    /// A SQL query with no output formatting (the form used by the SQL transports).
    pub fn sql(sql: String) -> Self {
        Self {
            inner: InnerQuery::Sql(sql),
            output: None,
        }
    }
}

/// The query itself: a SQL string or a structured JSON query.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, ToSchema)]
pub enum InnerQuery {
    /// A raw SQL query: `{ "sql": "SELECT ..." }`.
    #[serde(rename = "sql")]
    Sql(String),
    /// A structured JSON query (the fields of [`QueryBody`] at the top level).
    #[serde(untagged)]
    Json(QueryBody),
}

/// A structured (non-SQL) query.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct QueryBody {
    /// Columns, functions, or literals to project.
    #[serde(alias = "query_parameters")]
    select: Vec<Select>,
    /// Row filter to apply (a single, possibly nested, filter expression).
    filter: Option<Filter>,
    // To Support legacy queries
    #[schema(ignore)]
    filters: Option<Vec<Filter>>,
    /// Data source to read from. Defaults to the runtime's default table.
    #[serde(default)]
    from: Option<crate::query::from::From>,
    /// Ordering to apply to the result.
    sort_by: Option<Vec<Sort>>,
    /// Distinct-on specification.
    distinct: Option<Distinct>,
    /// Number of rows to skip.
    offset: Option<usize>,
    /// Maximum number of rows to return.
    limit: Option<usize>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, ToSchema)]
pub struct Distinct {
    pub on: Vec<Select>,
    pub select: Vec<Select>,
}

/// A single projected item in a query's `select`.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, ToSchema)]
#[serde(untagged)]
pub enum Select {
    /// A bare column name (`"temperature"`).
    ColumnName(String),
    /// A column with an optional alias (`{ "column": "temp", "alias": "t" }`).
    Column {
        #[serde(alias = "column_name")]
        column: String,
        alias: Option<String>,
    },
    /// A function call over other select items
    /// (`{ "function": "avg", "args": ["temp"] }`).
    Function {
        function: String,
        // `Select` -> `Function` -> `Select` is recursive; break it so utoipa's
        // schema generation terminates instead of overflowing the stack.
        #[schema(no_recursion)]
        args: Vec<Select>,
        alias: Option<String>,
    },
    /// A literal value (`{ "value": 0, "alias": "zero" }`).
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
