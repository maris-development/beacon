//! Catalog reads, expressed as SQL and mapped back to the API's view types.
//!
//! Each function here replaces a typed accessor that used to live on `Runtime`.
//! They all run as the calling identity, so catalog reads are authorized exactly
//! like any other query rather than bypassing authorization entirely.

use std::sync::Arc;

use beacon_core::api::{DatasetInfo, SchemaView};
use beacon_core::extensions::TableExtensions;
use beacon_core::AuthIdentity;
use serde_json::Value;

use super::sql::{query_rows, quote_ident, quote_literal, str_field};
use super::DataLake;

/// The tables registered in beacon's own schema, sorted.
pub(crate) async fn list_table_names(
    lake: &Arc<DataLake>,
    identity: AuthIdentity,
) -> anyhow::Result<Vec<String>> {
    let rows = query_rows(
        lake,
        "SELECT table_name FROM information_schema.tables \
         WHERE table_catalog = 'beacon' AND table_schema = 'public' \
         ORDER BY table_name",
        identity,
    )
    .await?;

    Ok(rows
        .iter()
        .map(|row| str_field(row, "table_name").to_string())
        .filter(|name| !name.is_empty())
        .collect())
}

/// A table's schema as a [`SchemaView`], or `None` when it is not registered.
///
/// Uses a zero-row scan (`SELECT * FROM t LIMIT 0`) rather than
/// `information_schema.columns`: tables in the persistent schema provider load
/// their schema lazily, so `information_schema.columns` can be empty for a table
/// that is nonetheless listed in `information_schema.tables`. The scan is the
/// same reliable source [`table_arrow_schema`] (Flight SQL) and
/// [`dataset_schema_view`] use.
pub(crate) async fn table_schema_view(
    lake: &Arc<DataLake>,
    table: &str,
    identity: AuthIdentity,
) -> anyhow::Result<Option<SchemaView>> {
    match table_arrow_schema(lake, &quote_ident(table), identity).await {
        Ok(schema) => Ok(Some(SchemaView::from(schema.as_ref()))),
        // A table that does not resolve surfaces as a planning error; the API
        // contract for that is `None` (→ 404), not a 500.
        Err(_) => Ok(None),
    }
}

/// A table's extensions. `SHOW EXTENSIONS` emits the same JSON document the typed
/// accessor used to deserialize, so the mapping back is exact. Errors when the
/// table is not registered.
pub(crate) async fn table_extensions(
    lake: &Arc<DataLake>,
    table: &str,
    identity: AuthIdentity,
) -> anyhow::Result<TableExtensions> {
    let rows = query_rows(
        lake,
        format!("SHOW EXTENSIONS FOR {}", quote_ident(table)),
        identity,
    )
    .await?;

    let document = rows
        .first()
        .and_then(|row| row.as_object()?.values().next())
        .and_then(Value::as_str)
        .unwrap_or("{}");
    Ok(serde_json::from_str(document).unwrap_or_default())
}

/// The catalogs, schemas, and tables visible to `identity`, as
/// `(catalog, schema, table)` triples — what Flight SQL's metadata endpoints
/// enumerate.
pub(crate) async fn list_qualified_tables(
    lake: &Arc<DataLake>,
    identity: AuthIdentity,
) -> anyhow::Result<Vec<(String, String, String)>> {
    let rows = query_rows(
        lake,
        "SELECT table_catalog, table_schema, table_name FROM information_schema.tables \
         ORDER BY table_catalog, table_schema, table_name",
        identity,
    )
    .await?;

    Ok(rows
        .iter()
        .map(|row| {
            (
                str_field(row, "table_catalog").to_string(),
                str_field(row, "table_schema").to_string(),
                str_field(row, "table_name").to_string(),
            )
        })
        .collect())
}

/// A table's true Arrow schema, via a zero-row scan.
///
/// `information_schema` renders types as strings, which cannot be turned back
/// into Arrow types faithfully; planning `SELECT * FROM t LIMIT 0` yields the
/// real schema, which is what Flight SQL has to return.
pub(crate) async fn table_arrow_schema(
    lake: &Arc<DataLake>,
    qualified_name: &str,
    identity: AuthIdentity,
) -> anyhow::Result<arrow::datatypes::SchemaRef> {
    let result = lake
        .runtime()
        .run_query(
            beacon_core::query::Query::sql(format!("SELECT * FROM {qualified_name} LIMIT 0")),
            identity,
        )
        .await?;
    Ok(result.into_record_stream()?.schema())
}

/// Discover datasets via the `list_datasets` table function.
///
/// `pattern` defaults to a recursive listing; `offset`/`limit` paginate. The
/// UDTF returns the full metadata, so this maps straight onto [`DatasetInfo`].
pub(crate) async fn list_datasets(
    lake: &Arc<DataLake>,
    pattern: Option<String>,
    offset: Option<usize>,
    limit: Option<usize>,
    identity: AuthIdentity,
) -> anyhow::Result<Vec<DatasetInfo>> {
    let sql = format!(
        "SELECT * FROM list_datasets({}, {}, {})",
        quote_literal(&pattern.unwrap_or_else(|| "**/*".to_string())),
        offset.unwrap_or(0),
        // 0 would mean "no rows"; the UDTF treats a missing limit as unbounded,
        // so pass a limit only when the caller asked for one.
        limit.map(|l| l.to_string()).unwrap_or_else(|| "NULL".to_string()),
    );
    let rows = query_rows(lake, sql, identity).await?;

    Ok(rows
        .iter()
        .map(|row| DatasetInfo {
            file_path: str_field(row, "file_name").to_string(),
            format: str_field(row, "file_format").to_string(),
            can_inspect: row.get("can_inspect").and_then(Value::as_bool).unwrap_or(false),
            can_partial_explore: row
                .get("can_partial_explore")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            size: row.get("size").and_then(Value::as_u64),
            last_modified: row
                .get("last_modified")
                .and_then(Value::as_str)
                .map(str::to_string),
        })
        .collect())
}

/// The `read_*` table function that reads a dataset file with the given
/// extension, or `None` when the extension is not one beacon reads by path.
fn read_function_for_extension(ext: &str) -> Option<&'static str> {
    Some(match ext.to_ascii_lowercase().as_str() {
        "parquet" => "read_parquet",
        "csv" => "read_csv",
        "nc" | "cdf" | "netcdf" => "read_netcdf",
        "arrow" | "arrows" | "ipc" => "read_arrow",
        "zarr" => "read_zarr",
        "tif" | "tiff" => "read_tiff",
        "bbf" => "read_bbf",
        _ => return None,
    })
}

/// The Arrow schema produced when reading a dataset file.
///
/// A zero-row scan (`SELECT * FROM read_<fmt>(file) LIMIT 0`) yields the real
/// Arrow schema, the same trick [`table_arrow_schema`] uses for tables. The
/// reader is chosen from the file extension (there is no format-agnostic
/// `read_*` function).
pub(crate) async fn dataset_schema_view(
    lake: &Arc<DataLake>,
    file: &str,
    identity: AuthIdentity,
) -> anyhow::Result<SchemaView> {
    let ext = std::path::Path::new(file)
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("");
    let read_fn = read_function_for_extension(ext).ok_or_else(|| {
        anyhow::anyhow!("cannot infer a reader for '{file}': unsupported extension '{ext}'")
    })?;

    let result = lake
        .runtime()
        .run_query(
            beacon_core::query::Query::sql(format!(
                "SELECT * FROM {read_fn}({}) LIMIT 0",
                quote_literal(file)
            )),
            identity,
        )
        .await?;
    let schema = result.into_record_stream()?.schema();
    Ok(SchemaView::from(schema.as_ref()))
}

/// The table a JSON query without a `from` resolves against. Configuration, not
/// catalog state, so it needs no query.
pub(crate) fn default_table(lake: &Arc<DataLake>) -> String {
    lake.config().sql.default_table.clone()
}
