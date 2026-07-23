//! Catalog reads, expressed as SQL and mapped back to the API's view types.
//!
//! Each function here replaces a typed accessor that used to live on `Runtime`.
//! They all run as the calling identity, so catalog reads are authorized exactly
//! like any other query rather than bypassing authorization entirely.

use std::sync::Arc;

use beacon_core::api::{DatasetInfo, SchemaFieldView, SchemaView};
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

/// A table's columns as a [`SchemaView`], or `None` when it is not registered.
///
/// `information_schema` carries no Arrow field metadata, so the `metadata` maps
/// are empty — the name, type and nullability are what the API contract exposes.
pub(crate) async fn table_schema_view(
    lake: &Arc<DataLake>,
    table: &str,
    identity: AuthIdentity,
) -> anyhow::Result<Option<SchemaView>> {
    let rows = query_rows(
        lake,
        format!(
            "SELECT column_name, data_type, is_nullable FROM information_schema.columns \
             WHERE table_catalog = 'beacon' AND table_schema = 'public' AND table_name = {} \
             ORDER BY ordinal_position",
            quote_literal(table)
        ),
        identity,
    )
    .await?;

    if rows.is_empty() {
        return Ok(None);
    }

    Ok(Some(SchemaView {
        fields: rows.iter().map(schema_field).collect(),
        metadata: Default::default(),
    }))
}

fn schema_field(row: &Value) -> SchemaFieldView {
    SchemaFieldView {
        name: str_field(row, "column_name").to_string(),
        data_type: str_field(row, "data_type").to_string(),
        nullable: str_field(row, "is_nullable").eq_ignore_ascii_case("YES"),
        metadata: Default::default(),
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

/// The Arrow schema produced when reading a dataset file, via `read_schema`.
pub(crate) async fn dataset_schema_view(
    lake: &Arc<DataLake>,
    file: &str,
    identity: AuthIdentity,
) -> anyhow::Result<SchemaView> {
    // A zero-row scan of the file yields its real Arrow schema, the same trick
    // the Flight SQL metadata path uses for tables.
    let result = lake
        .runtime()
        .run_query(
            beacon_core::query::Query::sql(format!(
                "SELECT * FROM read_schema({}) LIMIT 0",
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
