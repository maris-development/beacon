use crate::output::OutputFormat;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct Query {
    #[serde(flatten)]
    pub inner: InnerQuery,
    pub output: Box<dyn OutputFormat>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub enum InnerQuery {
    #[serde(rename = "sql")]
    Sql(String),
    #[serde(untagged)]
    Json(QueryBody),
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct QueryBody {
    query_parameters: Vec<QueryParameter>,
    filters: Option<Vec<Filter>>,
    from: Option<String>,
    distinct: Option<Vec<String>>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct QueryParameter {}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct Filter {}
