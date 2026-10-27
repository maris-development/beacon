//! The `/api/croissant` routes.

use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Extension, Router};
use beacon_core::query::output::{Output, OutputFormat};
use beacon_core::query::Query;
use beacon_core::query_result::QueryOutput;
use beacon_core::runtime::Runtime;
use beacon_core::{AuthIdentity, TableReference};
use futures::StreamExt;
use serde_json::{json, Value};

use crate::document::{catalog, dataset, CatalogEntry};
use crate::tables::readable_tables;
use crate::types::ServedSchema;
use crate::url::{base_url, CroissantSettings};

/// The media type of the Croissant documents.
const JSON_LD: &str = "application/ld+json";

/// A failure that the client gets as `{"error": "<text>"}`.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum CroissantError {
    /// The table does not exist, the caller cannot read it, or it has no served column.
    #[error("{0}")]
    NotFound(String),
    /// The query failed before the first row. `/api/query` also uses 400 for this.
    #[error("{0}")]
    Query(String),
    /// A server fault.
    #[error("{0}")]
    Internal(String),
}

impl CroissantError {
    /// The HTTP status of the error.
    pub fn status(&self) -> StatusCode {
        match self {
            CroissantError::NotFound(_) => StatusCode::NOT_FOUND,
            CroissantError::Query(_) => StatusCode::BAD_REQUEST,
            CroissantError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

impl IntoResponse for CroissantError {
    fn into_response(self) -> Response {
        let body = axum::Json(json!({ "error": self.to_string() }));
        (self.status(), body).into_response()
    }
}

#[derive(Clone)]
struct CroissantState {
    runtime: Arc<Runtime>,
    settings: Arc<CroissantSettings>,
}

/// The Croissant routes, ready to merge into the server router.
pub fn router<S: Clone + Send + Sync + 'static>(
    runtime: Arc<Runtime>,
    settings: CroissantSettings,
) -> Router<S> {
    Router::new()
        .route("/api/croissant", get(catalog_route))
        .route("/api/croissant/{table}", get(dataset_route))
        .route("/api/croissant/{table}/data.parquet", get(data_route))
        .with_state(CroissantState {
            runtime,
            settings: Arc::new(settings),
        })
}

/// The served schema of `table`, and the reference that found it. An unknown table and a table
/// without a read grant both give 404, so a caller cannot find the names of tables that they
/// cannot read.
pub(crate) async fn load_schema(
    runtime: &Runtime,
    identity: &AuthIdentity,
    table: &str,
) -> Result<(TableReference, ServedSchema), CroissantError> {
    // The bare name keeps its case. A name with a `.` or a `"` may also be a qualified name.
    let mut references = vec![TableReference::bare(table)];
    if table.contains(['.', '"']) {
        references.push(TableReference::from(table));
    }
    let mut failure = None;
    for reference in references {
        match runtime
            .table_arrow_schema(reference.clone(), identity)
            .await
        {
            Ok(schema) => return Ok((reference, ServedSchema::from_arrow(&schema))),
            Err(error) => failure = Some(error.to_string()),
        }
    }
    let error = failure.unwrap_or_default();
    let lower = error.to_lowercase();
    if lower.contains("permission denied") || lower.contains("could not be resolved") {
        tracing::debug!(%table, %error, "Croissant dataset not served");
    } else {
        // Other errors can come from a provider that fails for a short time.
        tracing::warn!(%table, %error, "Croissant dataset not served");
    }
    Err(CroissantError::NotFound(format!(
        "table {table:?} not found"
    )))
}

fn json_ld(document: &Value) -> Response {
    ([(header::CONTENT_TYPE, JSON_LD)], document.to_string()).into_response()
}

async fn catalog_route(
    State(state): State<CroissantState>,
    Extension(identity): Extension<AuthIdentity>,
    headers: HeaderMap,
) -> Response {
    let names = match readable_tables(&state.runtime, &identity).await {
        Ok(names) => names,
        Err(error) => return CroissantError::Internal(error.to_string()).into_response(),
    };
    let mut entries = Vec::with_capacity(names.len());
    for name in names {
        let description = match state
            .runtime
            .table_arrow_schema(TableReference::bare(name.as_str()), &identity)
            .await
        {
            Ok(schema) => ServedSchema::from_arrow(&schema).table_comment,
            Err(error) => {
                // The table stays in the catalog, without a description.
                tracing::debug!(table = %name, %error, "Croissant catalog entry without schema");
                None
            }
        };
        entries.push(CatalogEntry { name, description });
    }
    let base = base_url(&state.settings, &headers);
    json_ld(&catalog(&state.settings.catalog_title, &entries, &base))
}

async fn dataset_route(
    State(state): State<CroissantState>,
    Extension(identity): Extension<AuthIdentity>,
    Path(table): Path<String>,
    headers: HeaderMap,
) -> Response {
    match load_schema(&state.runtime, &identity, &table).await {
        Ok((_, schema)) => {
            let base = base_url(&state.settings, &headers);
            json_ld(&dataset(&table, &schema, &base))
        }
        Err(error) => error.into_response(),
    }
}

async fn data_route(
    State(state): State<CroissantState>,
    Extension(identity): Extension<AuthIdentity>,
    Path(table): Path<String>,
) -> Response {
    data_response(&state.runtime, identity, &table)
        .await
        .unwrap_or_else(IntoResponse::into_response)
}

/// All served columns of `table`, as a Parquet file.
async fn data_response(
    runtime: &Runtime,
    identity: AuthIdentity,
    table: &str,
) -> Result<Response, CroissantError> {
    let (reference, schema) = load_schema(runtime, &identity, table).await?;
    if schema.columns.is_empty() {
        return Err(CroissantError::NotFound(format!(
            "table {table:?} has no column that Croissant can describe"
        )));
    }
    let columns: Vec<String> = schema
        .columns
        .iter()
        .map(|column| quote_ident(&column.name))
        .collect();
    let sql = format!(
        "SELECT {} FROM {}",
        columns.join(", "),
        quoted_table(&reference)
    );
    let mut query = Query::sql(sql);
    query.output = Some(Output {
        format: OutputFormat::Parquet,
    });

    let result = runtime
        .run_query(query, identity)
        .await
        .map_err(|error| CroissantError::Query(error.to_string()))?;
    let QueryOutput::File(file) = result.query_output else {
        return Err(CroissantError::Internal(
            "a query with an output format gave no file".to_string(),
        ));
    };
    let handle = tokio::fs::File::open(file.path()).await.map_err(|error| {
        CroissantError::Internal(format!("cannot open the result file: {error}"))
    })?;
    let content_type = file.kind().content_type();
    // The stream owns the result file, so the file stays on disk until the body ends.
    let body = tokio_util::io::ReaderStream::new(handle).map(move |chunk| {
        let _owner = &file;
        chunk
    });

    let mut response = Response::new(Body::from_stream(body));
    let headers = response.headers_mut();
    headers.insert(header::CONTENT_TYPE, HeaderValue::from_static(content_type));
    let disposition = format!("attachment; filename=\"{}.parquet\"", file_name(table));
    let disposition = HeaderValue::from_str(&disposition)
        .map_err(|error| CroissantError::Internal(format!("invalid file name: {error}")))?;
    headers.insert(header::CONTENT_DISPOSITION, disposition);
    Ok(response)
}

/// The table reference with every part in double quotes, so a keyword or a mixed-case name works.
fn quoted_table(table: &TableReference) -> String {
    [table.catalog(), table.schema(), Some(table.table())]
        .into_iter()
        .flatten()
        .map(quote_ident)
        .collect::<Vec<_>>()
        .join(".")
}

/// A SQL identifier in double quotes. A `"` inside the name is doubled.
fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// The table name with each character that a quoted header value cannot hold as `_`.
fn file_name(table: &str) -> String {
    table
        .chars()
        .map(|c| match c {
            '"' | '\\' => '_',
            ' ' => ' ',
            c if c.is_ascii_graphic() => c,
            _ => '_',
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identifiers_are_quoted() {
        assert_eq!(quote_ident("a#b"), "\"a#b\"");
        assert_eq!(quote_ident("say \"hi\""), "\"say \"\"hi\"\"\"");
        assert_eq!(
            quoted_table(&TableReference::partial("s", "odd name")),
            "\"s\".\"odd name\""
        );
    }

    #[test]
    fn file_name_keeps_a_valid_header() {
        assert_eq!(file_name("odd name"), "odd name");
        assert_eq!(file_name("a\"b\\c\u{e9}\n"), "a_b_c__");
    }

    #[test]
    fn errors_have_their_status() {
        assert_eq!(
            CroissantError::NotFound("x".into()).status(),
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            CroissantError::Query("x".into()).status(),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            CroissantError::Internal("x".into()).status(),
            StatusCode::INTERNAL_SERVER_ERROR
        );
    }
}
