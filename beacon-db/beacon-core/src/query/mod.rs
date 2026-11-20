use datafusion::scalar::ScalarValue;
use utoipa::ToSchema;

use crate::query::output::Output;

pub mod output;
pub mod temp_object;

pub use beacon_json_query::{
    compile_json_query, Distinct, JsonQueryOptions, Literal, QueryBody, Select, Sort,
};

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

    /// Parses a request body.
    ///
    /// Serde tries the structured form as an untagged variant, so its own error only says that
    /// the body matched no variant. On failure this parses the parts on their own, and the error
    /// names the field or the value that is wrong.
    pub fn from_json(value: serde_json::Value) -> anyhow::Result<Self> {
        match serde_json::from_value::<Self>(value.clone()) {
            Ok(query) => Ok(query),
            Err(error) => Err(match Self::diagnose(value) {
                Some(reason) => anyhow::anyhow!("invalid query: {reason}"),
                None => error.into(),
            }),
        }
    }

    /// The error a part of a malformed structured body gives on its own, if one does.
    fn diagnose(value: serde_json::Value) -> Option<String> {
        let serde_json::Value::Object(mut body) = value else {
            return None;
        };
        // The SQL form is tagged, so serde already names what is wrong with it.
        if body.contains_key("sql") {
            return None;
        }
        if let Some(output) = body.remove("output") {
            if let Err(error) = serde_json::from_value::<Output>(output) {
                return Some(format!("output: {error}"));
            }
        }
        serde_json::from_value::<QueryBody>(serde_json::Value::Object(body))
            .err()
            .map(|error| error.to_string())
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

#[cfg(test)]
mod tests {
    use super::*;

    /// A query body is either SQL or the structured form; the SQL key wins and the
    /// structured form rejects unknown keys, so a typo surfaces as an error rather
    /// than being silently dropped from the query.
    #[test]
    fn query_body_is_sql_or_structured_and_rejects_typos() {
        let query: Query = serde_json::from_str(r#"{"sql": "SELECT 1"}"#).unwrap();
        assert!(matches!(query.inner, InnerQuery::Sql(sql) if sql == "SELECT 1"));

        let query: Query = serde_json::from_str(r#"{"select": ["depth"], "limit": 10}"#).unwrap();
        assert!(matches!(query.inner, InnerQuery::Json(_)), "{:?}", query.inner);

        assert!(serde_json::from_str::<Query>(r#"{"select": ["depth"], "limmit": 10}"#).is_err());
    }

    /// The error of a malformed structured query names what is wrong, not only that the body
    /// matched no variant.
    fn from_json_error(json: &str) -> String {
        Query::from_json(serde_json::from_str(json).unwrap())
            .expect_err("the body is malformed")
            .to_string()
    }

    #[test]
    fn a_malformed_query_error_names_the_problem() {
        let typo = from_json_error(r#"{"select": ["depth"], "limmit": 10}"#);
        assert!(typo.contains("limmit"), "{typo}");

        let sort = from_json_error(r#"{"select": ["depth"], "sort_by": [{"Descending": "depth"}]}"#);
        assert!(sort.contains("Descending") && sort.contains("Asc"), "{sort}");

        let output = from_json_error(r#"{"select": ["depth"], "output": {"format": "xlsx"}}"#);
        assert!(output.contains("xlsx"), "{output}");
    }

    #[test]
    fn from_json_takes_what_serde_takes() {
        let query = Query::from_json(serde_json::json!({"sql": "SELECT 1"})).unwrap();
        assert!(matches!(query.inner, InnerQuery::Sql(sql) if sql == "SELECT 1"));
        let query =
            Query::from_json(serde_json::json!({"select": ["depth"], "output": {"format": "csv"}}))
                .unwrap();
        assert!(matches!(query.inner, InnerQuery::Json(_)) && query.output.is_some());
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
