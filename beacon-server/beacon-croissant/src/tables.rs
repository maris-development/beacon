//! The tables that a caller can read.

use arrow::array::AsArray;
use beacon_core::runtime::Runtime;
use beacon_core::{AuthIdentity, TableReference};

use crate::document::CatalogEntry;
use crate::types::ServedSchema;

/// The tables in the default catalog and schema that `identity` can read, sorted by name.
///
/// `information_schema` is for the super-user only, so the list comes from
/// `Runtime::visible_tables`. It reads the catalog as the engine and keeps only the tables
/// that the roles of `identity` can select from.
pub async fn readable_tables(
    runtime: &Runtime,
    identity: &AuthIdentity,
) -> anyhow::Result<Vec<String>> {
    let (default_catalog, default_schema) = runtime.default_catalog_and_schema();
    let batches = runtime.visible_tables(identity).await?;

    let mut names = Vec::new();
    for batch in &batches {
        let column = |name: &str| {
            batch
                .column_by_name(name)
                .and_then(|array| array.as_string_opt::<i32>())
        };
        let (Some(catalogs), Some(schemas), Some(tables)) = (
            column("table_catalog"),
            column("table_schema"),
            column("table_name"),
        ) else {
            anyhow::bail!("the catalog listing is missing an expected column");
        };
        for row in 0..batch.num_rows() {
            if catalogs.value(row) == default_catalog && schemas.value(row) == default_schema {
                names.push(tables.value(row).to_string());
            }
        }
    }
    names.sort();
    Ok(names)
}

/// The catalog entry of each table that `identity` can read, sorted by name.
///
/// The description is the table comment. A table whose schema does not load stays in the
/// list without a description.
pub async fn catalog_entries(
    runtime: &Runtime,
    identity: &AuthIdentity,
) -> anyhow::Result<Vec<CatalogEntry>> {
    let names = readable_tables(runtime, identity).await?;
    let mut entries = Vec::with_capacity(names.len());
    for name in names {
        let description = match runtime
            .table_arrow_schema(TableReference::bare(name.as_str()), identity)
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
    Ok(entries)
}
