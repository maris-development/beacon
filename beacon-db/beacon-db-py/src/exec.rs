//! Running SQL against a [`Database`], shared by [`crate::connection`] and
//! [`crate::relation`].
//!
//! Both a `Connection.execute(...)` and a `Relation` terminal method reduce to the same two
//! moves — run a statement to a [`ResultSet`], or plan one just far enough to read its schema —
//! so they live here rather than being duplicated (and drifting) between the two.

use std::path::PathBuf;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use beacon_core::embedded::Database;
use beacon_core::query::output::{Output, OutputFormat};
use beacon_core::query::{InnerQuery, Query};
use beacon_core::query_result::QueryOutput;
use beacon_core::AuthIdentity;
use futures::TryStreamExt;
use pyo3::prelude::*;

use crate::errors::{map_engine_error, not_supported, programming_error};
use crate::result::ResultSet;
use crate::runtime::block_on;

/// Runs `sql` as `identity` and collects the whole result, with the GIL released while the
/// engine works.
pub fn run_sql(
    py: Python<'_>,
    database: &Arc<Database>,
    identity: &AuthIdentity,
    sql: String,
) -> PyResult<ResultSet> {
    let database = database.clone();
    let identity = identity.clone();

    let (schema, batches) = block_on(py, async move {
        let stream = database.sql(sql, identity).await?.into_record_stream()?;
        let schema = stream.schema();
        let batches: Vec<RecordBatch> = stream.try_collect().await?;
        Ok::<_, anyhow::Error>((schema, batches))
    })?
    .map_err(map_engine_error)?;

    Ok(ResultSet::new(schema, batches))
}

/// Runs `sql` with its `$1..$n` placeholders bound to `params`, collecting the whole result.
///
/// The values are bound to the plan, never spliced into the SQL text — the only injection-safe
/// way to run a parameterized statement.
pub fn run_sql_with_params(
    py: Python<'_>,
    database: &Arc<Database>,
    identity: &AuthIdentity,
    sql: String,
    params: Vec<datafusion::scalar::ScalarValue>,
) -> PyResult<ResultSet> {
    let database = database.clone();
    let identity = identity.clone();

    let (schema, batches) = block_on(py, async move {
        let stream = database
            .sql_with_params(sql, params, identity)
            .await?
            .into_record_stream()?;
        let schema = stream.schema();
        let batches: Vec<RecordBatch> = stream.try_collect().await?;
        Ok::<_, anyhow::Error>((schema, batches))
    })?
    .map_err(map_engine_error)?;

    Ok(ResultSet::new(schema, batches))
}

/// Plans `sql` far enough to read its output schema, **without** materializing rows.
///
/// The stream carries its schema before the first poll, so this sets up the physical plan but
/// runs no row work. It is not free — a `ListingTable` may list files at plan time — which is
/// why `Relation.columns`/`types` document a cost.
pub fn schema_of(
    py: Python<'_>,
    database: &Arc<Database>,
    identity: &AuthIdentity,
    sql: String,
) -> PyResult<SchemaRef> {
    let database = database.clone();
    let identity = identity.clone();

    block_on(py, async move {
        let stream = database.sql(sql, identity).await?.into_record_stream()?;
        Ok::<_, anyhow::Error>(stream.schema())
    })?
    .map_err(map_engine_error)
}

/// Runs `sql` and writes its result to `destination` in `format`.
///
/// The engine writes to a temporary file (a [`beacon_core::query_result::QueryOutputFile`],
/// which deletes itself on drop); this copies that file to `destination` **before** the
/// `QueryResult` is dropped, so the temp is never removed out from under the copy.
pub fn run_to_file(
    py: Python<'_>,
    database: &Arc<Database>,
    identity: &AuthIdentity,
    sql: String,
    format: OutputFormat,
    destination: PathBuf,
) -> PyResult<()> {
    // Only local filesystem destinations are supported today. A scheme (`s3://…`) would need
    // routing through the object-store registry, which the sinks don't do yet — so refuse
    // clearly rather than fail deep inside a filesystem copy.
    if let Some(scheme) = url_scheme(&destination) {
        return Err(not_supported(format!(
            "writing to `{scheme}://` destinations is not supported yet; write to a local path \
             and upload it, for now"
        )));
    }

    let database = database.clone();
    let identity = identity.clone();

    block_on(py, async move {
        let query = Query {
            inner: InnerQuery::Sql(sql),
            output: Some(Output { format }),
            params: Vec::new(),
        };
        let result = database.run_query(query, identity).await?;
        match result.query_output {
            QueryOutput::File(file) => {
                tokio::fs::copy(file.path(), &destination).await.map_err(|e| {
                    anyhow::anyhow!("failed to write output to {}: {e}", destination.display())
                })?;
                Ok(())
            }
            // An output format always produces a file; a stream here is an engine invariant break.
            QueryOutput::Stream(_) => {
                anyhow::bail!("the engine returned a stream for a file-output query")
            }
        }
    })?
    .map_err(map_engine_error)
}

/// The URL scheme of a destination, if it has one (`s3://bucket/x` → `Some("s3")`).
///
/// Deliberately keyed on `://` so a Windows drive path (`C:\data\out.parquet`) is not mistaken
/// for a scheme.
fn url_scheme(path: &std::path::Path) -> Option<String> {
    let text = path.to_str()?;
    let (scheme, _rest) = text.split_once("://")?;
    if !scheme.is_empty() && scheme.chars().all(|c| c.is_ascii_alphanumeric() || c == '+' || c == '-' || c == '.') {
        Some(scheme.to_string())
    } else {
        None
    }
}

/// Builds a table-function call expression from a function name and Python arguments, encoding
/// each argument as a SQL literal. Used by the `read_*` readers.
pub fn table_function_call(function: &str, args: &[Bound<'_, PyAny>]) -> PyResult<String> {
    let mut rendered = Vec::with_capacity(args.len());
    for arg in args {
        rendered.push(py_to_sql_literal(arg)?);
    }
    Ok(format!("{function}({})", rendered.join(", ")))
}

/// Encodes a Python value as a SQL literal.
///
/// Deliberately narrow — strings, numbers, booleans, null — because these are the argument
/// kinds beacon's table functions actually take (a path, an option flag). Anything else is
/// refused rather than guessed, so a mistake surfaces here instead of as malformed SQL.
fn py_to_sql_literal(value: &Bound<'_, PyAny>) -> PyResult<String> {
    if value.is_none() {
        return Ok("NULL".to_string());
    }
    // bool before int: in Python `bool` is a subclass of `int`, so the int arm would swallow it.
    if let Ok(flag) = value.extract::<bool>() {
        return Ok(if flag { "TRUE" } else { "FALSE" }.to_string());
    }
    if let Ok(text) = value.extract::<String>() {
        return Ok(format!("'{}'", text.replace('\'', "''")));
    }
    if let Ok(int) = value.extract::<i64>() {
        return Ok(int.to_string());
    }
    if let Ok(float) = value.extract::<f64>() {
        return Ok(float.to_string());
    }
    Err(programming_error(format!(
        "cannot pass a value of type `{}` to a table function; use a string path, number, \
         boolean, or None",
        value.get_type().name()?
    )))
}
