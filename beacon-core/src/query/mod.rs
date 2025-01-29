use utoipa::ToSchema;

use crate::output::OutputFormat;

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
pub struct QueryBody {
    query_parameters: Vec<QueryParameter>,
    filters: Option<Vec<Filter>>,
    from: Option<String>,
    distinct: Option<Vec<String>>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, ToSchema)]
pub struct QueryParameter {}

#[derive(Debug, serde::Serialize, serde::Deserialize, ToSchema)]
pub struct Filter {}
