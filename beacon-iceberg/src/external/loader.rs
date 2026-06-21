//! Loading an external Iceberg table via a per-location [`FileCatalog`].
//!
//! Unlike beacon's *managed* tables (which all live in one fixed warehouse behind
//! the process-wide catalog), an external table lives at a user-chosen
//! `datasets://` location. We build a fresh [`FileCatalog`] rooted so that table
//! resolves, then load it. Because the loaded [`iceberg_rust::table::Table`]
//! carries that catalog, the resulting [`DataFusionTable`] is *writable* (commits
//! `INSERT`s through the same file catalog) — not just readable.

use std::sync::Arc;

use anyhow::Context;
use datafusion_iceberg::DataFusionTable;
use iceberg_file_catalog::FileCatalog;
use iceberg_rust::catalog::identifier::Identifier;
use iceberg_rust::catalog::tabular::Tabular;
use iceberg_rust::catalog::Catalog;

use crate::catalog::builder_from_storage;

/// Parse an external table location into the `(catalog_path, namespace, table)` a
/// [`FileCatalog`] needs.
///
/// The location is relative to beacon's datasets store, optionally with a
/// `datasets://` (or any `scheme://`) prefix, and must name at least a
/// `<namespace>/<table>` since [`FileCatalog`] roots tables at
/// `<catalog_path>/<namespace>/<table>/` and supports only single-level
/// namespaces. The trailing segment is the table, the one before it the
/// namespace, and anything earlier becomes the catalog sub-prefix:
///
/// - `datasets://db/orders`      -> (`""`,        `db`, `orders`)
/// - `datasets://wh/db/orders`   -> (`"wh"`,      `db`, `orders`)
///
/// For an S3 backend the returned `catalog_path` is the absolute
/// `s3://<bucket>/<sub_prefix>` URL; for local it is the bare sub-prefix (the
/// object store builder is already rooted at `datasets_dir`).
fn parse_location(
    storage: &beacon_config::StorageConfig,
    location: &str,
) -> anyhow::Result<(String, String, String)> {
    let without_scheme = match location.split_once("://") {
        Some((_scheme, rest)) => rest,
        None => location,
    };
    let trimmed = without_scheme.trim_matches('/');
    anyhow::ensure!(
        !trimmed.is_empty(),
        "Iceberg table location must not be empty"
    );

    let segments: Vec<&str> = trimmed.split('/').filter(|s| !s.is_empty()).collect();
    anyhow::ensure!(
        segments.len() >= 2,
        "Iceberg table location {location:?} must be '<namespace>/<table>' \
         (optionally nested, e.g. 'datasets://warehouse/db/table')"
    );

    let table = segments[segments.len() - 1].to_string();
    let namespace = segments[segments.len() - 2].to_string();
    let sub_prefix = segments[..segments.len() - 2].join("/");

    let catalog_path = if let Some(s3) = &storage.s3 {
        if sub_prefix.is_empty() {
            format!("s3://{}", s3.bucket)
        } else {
            format!("s3://{}/{sub_prefix}", s3.bucket)
        }
    } else {
        sub_prefix
    };

    Ok((catalog_path, namespace, table))
}

/// Load the external Iceberg table at `location` (relative to the datasets store)
/// and return a writable [`DataFusionTable`].
///
/// The table must already exist in the file-catalog (Hadoop-style)
/// `<location>/metadata/vN.metadata.json` layout — [`FileCatalog::load_tabular`]
/// discovers the current metadata version itself, so no version handling is
/// needed here.
pub async fn load_external_iceberg_table(
    storage: &beacon_config::StorageConfig,
    location: &str,
) -> anyhow::Result<DataFusionTable> {
    let (catalog_path, namespace, table) = parse_location(storage, location)?;

    let builder = builder_from_storage(storage)?;
    let catalog: Arc<dyn Catalog> = Arc::new(
        FileCatalog::new(&catalog_path, builder)
            .await
            .map_err(|error| anyhow::anyhow!("Failed to open Iceberg catalog for {location:?}: {error}"))?,
    );

    let identifier = Identifier::new(std::slice::from_ref(&namespace), &table);
    let tabular = catalog
        .clone()
        .load_tabular(&identifier)
        .await
        .with_context(|| format!("Failed to load external Iceberg table at {location:?}"))?;

    match tabular {
        Tabular::Table(table) => Ok(DataFusionTable::from(table)),
        _ => anyhow::bail!("Iceberg location {location:?} does not refer to a table"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn local_storage() -> beacon_config::StorageConfig {
        beacon_config::StorageConfig {
            datasets_dir: PathBuf::from("/data/datasets"),
            ..Default::default()
        }
    }

    fn s3_storage() -> beacon_config::StorageConfig {
        beacon_config::StorageConfig {
            s3: Some(beacon_config::S3Config {
                bucket: "my-bucket".to_string(),
                endpoint: None,
                region: None,
                enable_virtual_hosting: false,
                allow_http: false,
            }),
            ..Default::default()
        }
    }

    #[test]
    fn parses_local_namespace_table() {
        let (catalog_path, ns, table) =
            parse_location(&local_storage(), "datasets://db/orders").unwrap();
        assert_eq!(catalog_path, "");
        assert_eq!(ns, "db");
        assert_eq!(table, "orders");
    }

    #[test]
    fn parses_nested_local_prefix() {
        let (catalog_path, ns, table) =
            parse_location(&local_storage(), "datasets://wh/db/orders").unwrap();
        assert_eq!(catalog_path, "wh");
        assert_eq!(ns, "db");
        assert_eq!(table, "orders");
    }

    #[test]
    fn parses_s3_into_bucket_url() {
        let (catalog_path, ns, table) =
            parse_location(&s3_storage(), "datasets://db/orders").unwrap();
        assert_eq!(catalog_path, "s3://my-bucket");
        assert_eq!(ns, "db");
        assert_eq!(table, "orders");

        let (catalog_path, _, _) =
            parse_location(&s3_storage(), "datasets://wh/db/orders").unwrap();
        assert_eq!(catalog_path, "s3://my-bucket/wh");
    }

    #[test]
    fn requires_namespace_and_table() {
        assert!(parse_location(&local_storage(), "datasets://orders").is_err());
        assert!(parse_location(&local_storage(), "datasets://").is_err());
    }

    #[test]
    fn scheme_is_optional() {
        let (_, ns, table) = parse_location(&local_storage(), "db/orders").unwrap();
        assert_eq!(ns, "db");
        assert_eq!(table, "orders");
    }
}
