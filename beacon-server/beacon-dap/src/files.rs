//! File downloads of a DAP2 subset (`.csv`, `.nc`) for clients that cannot read Sequences,
//! such as xarray and netCDF tools.

use axum::body::Body;
use axum::http::{header, HeaderValue};
use axum::response::Response;
use beacon_core::query::output::{Output, OutputFormat};
use beacon_core::query::Query;
use beacon_core::query_result::QueryOutput;
use beacon_core::runtime::Runtime;
use beacon_core::AuthIdentity;
use futures::StreamExt;

use crate::compile;
use crate::constraint;
use crate::error::DapError;
use crate::http::load_schema;
use crate::names::encode;

/// The subset of `table` for the raw constraint `query`, as a file in `format`.
pub(crate) async fn file_response(
    runtime: &Runtime,
    identity: AuthIdentity,
    table: &str,
    query: &str,
    format: OutputFormat,
) -> Result<Response, DapError> {
    let (reference, schema) = load_schema(runtime, &identity, table).await?;
    let constraint = constraint::parse(query)?;
    let compiled = compile::compile(&reference, &schema, &constraint)?;
    let mut query = Query::sql_with_params(compiled.sql, compiled.params);
    query.output = Some(Output { format });

    let result = runtime
        .run_query(query, identity)
        .await
        .map_err(|error| DapError::Query(error.to_string()))?;
    let QueryOutput::File(file) = result.query_output else {
        return Err(DapError::Internal(
            "a query with an output format gave no file".to_string(),
        ));
    };
    let handle = tokio::fs::File::open(file.path())
        .await
        .map_err(|error| DapError::Internal(format!("cannot open the result file: {error}")))?;
    let content_type = HeaderValue::from_str(file.kind().content_type())
        .map_err(|error| DapError::Internal(format!("invalid content type: {error}")))?;
    let filename = format!("{}{}", encode(table), file.kind().suggested_extension());
    // The stream owns the result file, so the file stays on disk until the body ends.
    let body = tokio_util::io::ReaderStream::new(handle).map(move |chunk| {
        let _owner = &file;
        chunk
    });

    let mut response = Response::new(Body::from_stream(body));
    let headers = response.headers_mut();
    headers.insert(header::CONTENT_TYPE, content_type);
    let disposition = HeaderValue::from_str(&format!("attachment; filename=\"{filename}\""))
        .map_err(|error| DapError::Internal(format!("invalid file name: {error}")))?;
    headers.insert(header::CONTENT_DISPOSITION, disposition);
    Ok(response)
}
