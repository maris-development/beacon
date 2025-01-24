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
pub struct QueryBody {}
