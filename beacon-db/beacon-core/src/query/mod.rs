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
pub mod temp_object;

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
    /// Positional parameter values bound to `$1..$n` placeholders in a SQL query, applied to
    /// the lowered plan via [`LogicalPlan::with_param_values`](datafusion::logical_expr::LogicalPlan::with_param_values).
    ///
    /// Not part of the request wire format: parameters are an *embedded*-API concern (the
    /// Python bindings bind them rather than interpolate, for injection safety), while the
    /// HTTP/Flight transports send fully-formed SQL. `#[serde(skip)]` keeps them out of the
    /// JSON contract and defaults them to empty for every deserialized request.
    #[serde(skip)]
    #[schema(ignore)]
    pub params: Vec<ScalarValue>,
}

impl Query {
    /// A SQL query with no output formatting (the form used by the SQL transports).
    pub fn sql(sql: String) -> Self {
        Self {
            inner: InnerQuery::Sql(sql),
            output: None,
            params: Vec::new(),
        }
    }

    /// A SQL query whose `$1..$n` placeholders are bound to `params` after planning.
    pub fn sql_with_params(sql: String, params: Vec<ScalarValue>) -> Self {
        Self {
            inner: InnerQuery::Sql(sql),
            output: None,
            params,
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

#[cfg(test)]
mod tests {
    use super::*;

    use datafusion::prelude::SessionContext;

    fn select(json: &str) -> Select {
        serde_json::from_str(json).expect("select item should deserialize")
    }

    /// `select` items are untagged, so the payload's shape alone decides the
    /// variant. A bare string, a `{column}` object, a `{function, args}` call and a
    /// `{value}` literal must each land on their own variant — a mis-ordered
    /// variant list would silently reinterpret a client's projection.
    #[test]
    fn untagged_select_items_match_by_shape() {
        assert!(matches!(select(r#""depth""#), Select::ColumnName(name) if name == "depth"));
        assert!(matches!(select(r#"{"column": "depth"}"#), Select::Column { .. }));
        // The legacy `column_name` spelling is still accepted.
        assert!(matches!(select(r#"{"column_name": "depth"}"#), Select::Column { .. }));
        assert!(matches!(
            select(r#"{"function": "abs", "args": ["depth"]}"#),
            Select::Function { .. }
        ));
        assert!(matches!(select(r#"{"value": 3}"#), Select::Literal { .. }));
    }

    /// Column collection drives the pushdown projection: every column reachable
    /// from a select item — including those nested inside function arguments —
    /// must be collected, or the scan drops a column the projection needs.
    #[test]
    fn collect_columns_reaches_nested_function_arguments() {
        let item = select(
            r#"{"function": "abs", "args": [{"function": "nullif", "args": ["depth", {"value": 0}]}, "temperature"]}"#,
        );
        let mut columns = vec![];
        item.collect_columns(&mut columns);
        assert_eq!(columns, vec!["depth".to_string(), "temperature".to_string()]);
    }

    /// A literal contributes no column to the pushdown projection — it must not
    /// leak a phantom column name into the scan.
    #[test]
    fn collect_columns_ignores_literals() {
        let mut columns = vec![];
        select(r#"{"value": "constant", "alias": "c"}"#).collect_columns(&mut columns);
        assert!(columns.is_empty());
    }

    /// Aliases are applied uniformly across the item kinds that accept one, so the
    /// output column name is the client's, not DataFusion's derived expression name.
    #[test]
    fn aliases_are_applied_to_every_aliasable_item() {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let expr = select(r#"{"column": "depth", "alias": "d"}"#)
            .to_expr(&state)
            .unwrap();
        assert_eq!(expr.schema_name().to_string(), "d");
        let expr = select(r#"{"value": 3, "alias": "three"}"#)
            .to_expr(&state)
            .unwrap();
        assert_eq!(expr.schema_name().to_string(), "three");
    }

    /// An unknown function is reported as a registry error naming the function,
    /// instead of being planned into something that fails later with a cryptic
    /// message (or, worse, resolving to a same-named column).
    #[test]
    fn unknown_function_is_rejected_with_its_name() {
        let session_ctx = SessionContext::new();
        let error = select(r#"{"function": "not_a_function", "args": []}"#)
            .to_expr(&session_ctx.state())
            .expect_err("an unregistered function must not plan");
        assert!(
            error.to_string().contains("not_a_function"),
            "unexpected error: {error}"
        );
    }

    /// Literal rendering must preserve the JSON type: a bare `null` becomes a SQL
    /// NULL rather than the string "null", and booleans do not degrade to numbers.
    #[test]
    fn literals_keep_their_json_type() {
        assert_eq!(Literal::Null(None).to_expr(), lit(ScalarValue::Null));
        assert_eq!(Literal::Boolean(true).to_expr(), lit(true));
        assert_eq!(Literal::Number(3.0).to_expr(), lit(3.0f64));
        assert_eq!(Literal::String("a".to_string()).to_expr(), lit("a"));
    }

    /// Sorting is ascending-nulls-last / descending-nulls-last: the `nulls_first`
    /// flag is `false` for both directions, so a descending sort does not surface a
    /// block of NULLs first.
    #[test]
    fn sort_directions_never_put_nulls_first() {
        let asc = Sort::Asc("depth".to_string()).to_expr();
        assert!(asc.asc);
        assert!(!asc.nulls_first);
        let desc = Sort::Desc("depth".to_string()).to_expr();
        assert!(!desc.asc);
        assert!(!desc.nulls_first);
    }

    /// A query body is either SQL or the structured form; the SQL key wins and the
    /// structured form rejects unknown keys, so a typo surfaces as an error rather
    /// than being silently dropped from the query.
    #[test]
    fn query_body_is_sql_or_structured_and_rejects_typos() {
        let query: Query = serde_json::from_str(r#"{"sql": "SELECT 1"}"#).unwrap();
        assert!(matches!(query.inner, InnerQuery::Sql(sql) if sql == "SELECT 1"));

        let query: Query = serde_json::from_str(r#"{"select": ["depth"], "limit": 10}"#).unwrap();
        match query.inner {
            InnerQuery::Json(body) => {
                assert_eq!(body.limit, Some(10));
                assert!(body.from.is_none());
            }
            other => panic!("expected a structured query, got {other:?}"),
        }

        assert!(serde_json::from_str::<Query>(r#"{"select": ["depth"], "limmit": 10}"#).is_err());
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
