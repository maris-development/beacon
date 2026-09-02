//! Catalog reads, expressed as SQL and mapped back to the API's view types.
//!
//! Each function here replaces a typed accessor that used to live on `Runtime`.
//! They run as the calling identity, so catalog reads are authorized exactly like
//! any other query rather than bypassing authorization entirely.
//!
//! The one thing a caller cannot ask for themselves is the catalog listing:
//! `information_schema` is the super-user's alone, so the enumeration goes
//! through [`Runtime::visible_tables`](beacon_core::runtime::Runtime::visible_tables),
//! which reads it as the engine and returns the rows that identity is entitled to
//! see — as Arrow, which this module translates like any other result.

use std::sync::Arc;

use crate::api::DatasetInfo;
use beacon_core::extensions::TableExtensions;
use beacon_core::AuthIdentity;
use serde_json::Value;

use super::sql::{query_rows, quote_ident, quote_literal, rows_from_batches, str_field};
use super::Server;

/// The tables `identity` may see in beacon's own schema, sorted.
pub(crate) async fn list_table_names(
    server: &Arc<Server>,
    identity: AuthIdentity,
) -> anyhow::Result<Vec<String>> {
    let (catalog, schema) = default_catalog_and_schema(server);

    Ok(list_catalog_tables(server, identity)
        .await?
        .into_iter()
        .filter(|entry| entry.catalog == catalog && entry.schema == schema)
        .map(|entry| entry.table)
        .filter(|name| !name.is_empty())
        .collect())
}

/// A [`TableReference`](datafusion::sql::TableReference) for `table`, qualified
/// by `schema` and `catalog` when the caller named them.
///
/// The parts are passed to the catalog as-is rather than being interpolated into
/// a SQL string, so they need no quoting and cannot be re-parsed. A catalog
/// without a schema cannot be expressed as a reference, so it is ignored.
pub(crate) fn table_reference(
    catalog: Option<&str>,
    schema: Option<&str>,
    table: &str,
) -> datafusion::sql::TableReference {
    use datafusion::sql::TableReference;
    match (catalog, schema) {
        (Some(catalog), Some(schema)) => {
            TableReference::full(catalog.to_string(), schema.to_string(), table.to_string())
        }
        (None, Some(schema)) => TableReference::partial(schema.to_string(), table.to_string()),
        _ => TableReference::bare(table.to_string()),
    }
}

/// A table's Arrow schema, or `None` when it is not registered.
///
/// Uses a zero-row scan (`SELECT * FROM t LIMIT 0`) rather than
/// `information_schema.columns`: tables in the persistent schema provider load
/// their schema lazily, so `information_schema.columns` can be empty for a table
/// that is nonetheless listed in `information_schema.tables`. The scan is the
/// same reliable source [`table_arrow_schema`] (Flight SQL) and
/// [`dataset_schema`] use.
pub(crate) async fn table_schema(
    server: &Arc<Server>,
    table: impl Into<datafusion::sql::TableReference>,
    identity: AuthIdentity,
) -> anyhow::Result<Option<arrow::datatypes::SchemaRef>> {
    match table_arrow_schema(server, table, identity).await {
        Ok(schema) => Ok(Some(schema)),
        // A table that does not resolve surfaces as a planning error; the API
        // contract for that is `None` (→ 404), not a 500.
        Err(_) => Ok(None),
    }
}

/// A table's extensions. `SHOW EXTENSIONS` emits the same JSON document the typed
/// accessor used to deserialize, so the mapping back is exact. Errors when the
/// table is not registered.
pub(crate) async fn table_extensions(
    server: &Arc<Server>,
    table: &str,
    identity: AuthIdentity,
) -> anyhow::Result<TableExtensions> {
    let rows = query_rows(
        server,
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

/// One entry of the catalog listing: a table and where it lives.
pub(crate) struct QualifiedTable {
    pub catalog: String,
    pub schema: String,
    pub table: String,
    /// `BASE TABLE`, `VIEW`, … as `information_schema` reports it.
    pub table_type: String,
}

/// Every table `identity` is entitled to see, across all catalogs and schemas,
/// sorted by catalog, then schema, then table.
///
/// For the super-user that is the whole namespace — beacon's `public` and
/// `system` schemas, `information_schema`, and any attached remote catalog. For
/// anyone else it is the tables their roles grant `Select` on, with beacon's
/// metadata schemas and internal tables omitted entirely.
pub(crate) async fn list_catalog_tables(
    server: &Arc<Server>,
    identity: AuthIdentity,
) -> anyhow::Result<Vec<QualifiedTable>> {
    let batches = server.runtime().visible_tables(&identity).await?;

    Ok(rows_from_batches(&batches)?
        .iter()
        .map(|row| QualifiedTable {
            catalog: str_field(row, "table_catalog").to_string(),
            schema: str_field(row, "table_schema").to_string(),
            table: str_field(row, "table_name").to_string(),
            table_type: str_field(row, "table_type").to_string(),
        })
        .collect())
}

/// The catalogs, schemas, and tables visible to `identity`, as
/// `(catalog, schema, table)` triples — what Flight SQL's metadata endpoints
/// enumerate.
pub(crate) async fn list_qualified_tables(
    server: &Arc<Server>,
    identity: AuthIdentity,
) -> anyhow::Result<Vec<(String, String, String)>> {
    Ok(list_catalog_tables(server, identity)
        .await?
        .into_iter()
        .map(|entry| (entry.catalog, entry.schema, entry.table))
        .collect())
}

/// The catalog and schema an unqualified table name resolves against, read from
/// the session's own settings rather than assumed.
pub(crate) fn default_catalog_and_schema(server: &Arc<Server>) -> (String, String) {
    server.runtime().default_catalog_and_schema()
}

/// A table's true Arrow schema, taken from its table provider.
///
/// `information_schema` renders types as strings, which cannot be turned back
/// into Arrow types faithfully, so it is not a usable source for the schema
/// Flight SQL must return.
///
/// This asks the provider rather than planning `SELECT * FROM t LIMIT 0`. A
/// zero-row scan still forces the whole read path to resolve, and an
/// N-dimensional table whose variables cannot be broadcast onto a common shape
/// fails there — reporting no schema for a table whose schema is perfectly well
/// defined. The provider answers without planning or I/O, and the runtime
/// applies the same read authorization the scan would have.
pub(crate) async fn table_arrow_schema(
    server: &Arc<Server>,
    table: impl Into<datafusion::sql::TableReference>,
    identity: AuthIdentity,
) -> anyhow::Result<arrow::datatypes::SchemaRef> {
    server.runtime().table_arrow_schema(table, &identity).await
}

/// Discover datasets via the `list_datasets` table function.
///
/// `pattern` defaults to a recursive listing; `offset`/`limit` paginate. The
/// UDTF returns the full metadata, so this maps straight onto [`DatasetInfo`].
pub(crate) async fn list_datasets(
    server: &Arc<Server>,
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
    let rows = query_rows(server, sql, identity).await?;

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
        "atlas" => "read_atlas",
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
pub(crate) async fn dataset_schema(
    server: &Arc<Server>,
    file: &str,
    identity: AuthIdentity,
) -> anyhow::Result<arrow::datatypes::SchemaRef> {
    let ext = std::path::Path::new(file)
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("");
    let read_fn = read_function_for_extension(ext).ok_or_else(|| {
        anyhow::anyhow!("cannot infer a reader for '{file}': unsupported extension '{ext}'")
    })?;

    let result = server
        .runtime()
        .run_query(
            beacon_core::query::Query::sql(format!(
                "SELECT * FROM {read_fn}({}) LIMIT 0",
                quote_literal(file)
            )),
            identity,
        )
        .await?;
    Ok(result.into_record_stream()?.schema())
}

/// The table a JSON query without a `from` resolves against. Configuration, not
/// catalog state, so it needs no query.
pub(crate) fn default_table(server: &Arc<Server>) -> String {
    server.config().sql.default_table.clone()
}
