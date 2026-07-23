//! Running SQL against the lake's runtime and translating the result to JSON.
//!
//! The runtime exposes one entry point — `run_query` — so every endpoint that
//! used to call a typed accessor now asks a question in SQL and maps the
//! resulting `RecordBatch`es into the JSON shape its contract already promised.
//! This module is that translation layer.

use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use beacon_core::query::Query;
use beacon_core::AuthIdentity;
use futures::TryStreamExt;
use serde_json::Value;

use super::DataLake;

/// Run `sql` as `identity` and return its rows as JSON objects, keyed by column.
pub(crate) async fn query_rows(
    lake: &Arc<DataLake>,
    sql: impl Into<String>,
    identity: AuthIdentity,
) -> anyhow::Result<Vec<Value>> {
    let batches = collect(lake, sql, identity).await?;
    Ok(serde_json::from_str(&batches_to_json(&batches)?)?)
}

/// Run `sql` as `identity`, discarding any rows. For statements executed for
/// their effect (DDL, `SET EXTENSION`, `RUN CRAWLER`).
pub(crate) async fn execute(
    lake: &Arc<DataLake>,
    sql: impl Into<String>,
    identity: AuthIdentity,
) -> anyhow::Result<()> {
    collect(lake, sql, identity).await.map(|_| ())
}

/// Run `sql` and collect every batch. Catalog queries are bounded by the size of
/// the catalog, so there is no row cap here.
async fn collect(
    lake: &Arc<DataLake>,
    sql: impl Into<String>,
    identity: AuthIdentity,
) -> anyhow::Result<Vec<RecordBatch>> {
    let result = lake
        .runtime()
        .run_query(Query::sql(sql.into()), identity)
        .await?;
    Ok(result.into_record_stream()?.try_collect().await?)
}

/// Serialize record batches to a JSON array of row objects.
fn batches_to_json(batches: &[RecordBatch]) -> anyhow::Result<String> {
    if batches.iter().all(|b| b.num_rows() == 0) {
        return Ok("[]".to_string());
    }
    let mut buf = Vec::new();
    let mut writer = arrow_json::ArrayWriter::new(&mut buf);
    for batch in batches {
        writer.write(batch)?;
    }
    writer.finish()?;
    Ok(String::from_utf8(buf)?)
}

/// A row's string column, or `""` when absent or null.
pub(crate) fn str_field<'a>(row: &'a Value, key: &str) -> &'a str {
    row.get(key).and_then(Value::as_str).unwrap_or_default()
}

/// Quote a SQL identifier, escaping embedded double quotes.
pub(crate) fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

/// Quote a SQL string literal, escaping embedded single quotes.
pub(crate) fn quote_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identifiers_and_literals_escape_their_quote_character() {
        assert_eq!(quote_ident(r#"we"ird"#), r#""we""ird""#);
        assert_eq!(quote_literal("O'Brien"), "'O''Brien'");
    }

    #[test]
    fn empty_batches_serialize_to_an_empty_array() {
        assert_eq!(batches_to_json(&[]).unwrap(), "[]");
    }
}
