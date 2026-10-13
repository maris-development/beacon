//! The `/dap/{dataset}` routes.

use std::sync::Arc;

use axum::extract::{Path, RawQuery, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Extension, Router};
use beacon_core::query::output::OutputFormat;
use beacon_core::runtime::Runtime;
use beacon_core::AuthIdentity;
use datafusion::sql::TableReference;

use crate::compile;
use crate::constraint;
use crate::das::das;
use crate::dds::dds;
use crate::error::DapError;
use crate::headers::{dap2_response, insert_server_headers};
use crate::types::ServedSchema;

/// The response that a URL suffix asks for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Suffix {
    /// No known suffix: a page with links to the other responses.
    Index,
    Dds,
    Das,
    Dods,
    Csv,
    Nc,
}

/// The suffixes, in the order that the index page lists them.
pub(crate) const SUFFIXES: [(&str, Suffix); 5] = [
    (".dds", Suffix::Dds),
    (".das", Suffix::Das),
    (".dods", Suffix::Dods),
    (".csv", Suffix::Csv),
    (".nc", Suffix::Nc),
];

/// Splits `argo.dds` into the table name and the suffix. Only a known suffix is removed.
pub(crate) fn split_suffix(dataset: &str) -> (&str, Suffix) {
    for (text, suffix) in SUFFIXES {
        if let Some(table) = dataset.strip_suffix(text) {
            return (table, suffix);
        }
    }
    (dataset, Suffix::Index)
}

/// The DAP2 routes, ready to merge into the server router.
pub fn router<S: Clone + Send + Sync + 'static>(runtime: Arc<Runtime>) -> Router<S> {
    Router::new()
        .route("/dap/{dataset}", get(dataset))
        .with_state(runtime)
}

/// Turns the bare 401 of the identity middleware into a DAP2 error that asks for Basic
/// credentials.
pub async fn basic_challenge(response: Response) -> Response {
    if response.status() == StatusCode::UNAUTHORIZED {
        DapError::Unauthorized.into_response()
    } else {
        response
    }
}

async fn dataset(
    State(runtime): State<Arc<Runtime>>,
    Extension(identity): Extension<AuthIdentity>,
    Path(dataset): Path<String>,
    RawQuery(query): RawQuery,
) -> Response {
    let (table, suffix) = split_suffix(&dataset);
    let query = query.unwrap_or_default();
    let result = match suffix {
        Suffix::Index => index_response(&runtime, &identity, table).await,
        Suffix::Dds => dds_response(&runtime, &identity, table, &query).await,
        Suffix::Das => das_response(&runtime, &identity, table).await,
        Suffix::Dods => crate::data::dods_response(&runtime, identity, table, &query).await,
        Suffix::Csv => {
            crate::files::file_response(&runtime, identity, table, &query, OutputFormat::Csv).await
        }
        Suffix::Nc => {
            crate::files::file_response(
                &runtime,
                identity,
                table,
                &query,
                OutputFormat::NetCDF(Default::default()),
            )
            .await
        }
    };
    result.unwrap_or_else(IntoResponse::into_response)
}

/// The served schema of `table`, and the reference that found it. An unknown table and a table
/// without a read grant both give 404, so a caller cannot find the names of tables that they
/// cannot read.
pub(crate) async fn load_schema(
    runtime: &Runtime,
    identity: &AuthIdentity,
    table: &str,
) -> Result<(TableReference, ServedSchema), DapError> {
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
        tracing::debug!(%table, %error, "DAP2 dataset not served");
    } else {
        // Other errors can come from a provider that fails for a short time.
        tracing::warn!(%table, %error, "DAP2 dataset not served");
    }
    Err(DapError::NotFound(format!("dataset {table:?} not found")))
}

async fn index_response(
    runtime: &Runtime,
    identity: &AuthIdentity,
    table: &str,
) -> Result<Response, DapError> {
    load_schema(runtime, identity, table).await?;
    let links: Vec<String> = SUFFIXES
        .iter()
        .map(|(text, _)| format!("{table}{text}"))
        .collect();
    let body = format!("DAP2 dataset {table}\n\n{}\n", links.join("\n"));
    let mut response =
        ([(header::CONTENT_TYPE, "text/plain; charset=utf-8")], body).into_response();
    insert_server_headers(response.headers_mut());
    Ok(response)
}

async fn dds_response(
    runtime: &Runtime,
    identity: &AuthIdentity,
    table: &str,
    query: &str,
) -> Result<Response, DapError> {
    let (_, schema) = load_schema(runtime, identity, table).await?;
    let constraint = constraint::parse(query)?;
    let columns = compile::projected(&schema, &constraint.projection)?;
    Ok(dap2_response(
        "dods-dds",
        "text/plain; charset=utf-8",
        dds(table, &columns).into(),
    ))
}

async fn das_response(
    runtime: &Runtime,
    identity: &AuthIdentity,
    table: &str,
) -> Result<Response, DapError> {
    let (_, schema) = load_schema(runtime, identity, table).await?;
    Ok(dap2_response(
        "dods-das",
        "text/plain; charset=utf-8",
        das(table, &schema).into(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_suffix_reads_known_suffixes_from_the_end() {
        assert_eq!(split_suffix("argo.dds"), ("argo", Suffix::Dds));
        assert_eq!(split_suffix("argo.das"), ("argo", Suffix::Das));
        assert_eq!(split_suffix("argo.dods"), ("argo", Suffix::Dods));
        assert_eq!(split_suffix("argo.csv"), ("argo", Suffix::Csv));
        assert_eq!(split_suffix("argo.nc"), ("argo", Suffix::Nc));
        assert_eq!(split_suffix("my.table.dds"), ("my.table", Suffix::Dds));
        assert_eq!(split_suffix("argo"), ("argo", Suffix::Index));
        assert_eq!(
            split_suffix("argo.parquet"),
            ("argo.parquet", Suffix::Index)
        );
    }
}
