//! Opening an Iceberg table by location and turning it into a DataFusion
//! [`TableProvider`].
//!
//! ## No catalog
//!
//! A table is named by a location only — the directory that holds `metadata/`
//! and the data files. There is no catalog, so the current metadata file is
//! found the way a Hadoop-style table records it: `metadata/version-hint.text`
//! if present, otherwise the highest-versioned `*.metadata.json` in the metadata
//! directory. A REST or Glue catalog would replace exactly this step —
//! [`resolve_metadata_location`] is the seam.
//!
//! ## Reads go through Beacon's object store
//!
//! [`crate::storage::BeaconStorage`] serves every byte, so a table on S3 reads
//! with no local copy and needs no separate credential configuration.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Context;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::TableProvider;
use futures::StreamExt as _;
use iceberg::io::FileIOBuilder;
use iceberg::spec::TableMetadata;
use iceberg::table::StaticTable;
use iceberg::TableIdent;
use iceberg_datafusion::IcebergStaticTableProvider;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectMeta, ObjectStore, ObjectStoreExt as _};

use crate::storage::{register_store, BeaconStorage, BeaconStorageFactory, BEACON_ICEBERG_SCHEME};

/// The namespace reported for a location-named table. Nothing resolves through
/// it — Iceberg only wants an identifier — but it keeps error messages readable.
const BEACON_NAMESPACE: &str = "beacon";

/// The metadata directory inside an Iceberg table.
const METADATA_DIR: &str = "metadata";

/// The optional file that names the current metadata version.
const VERSION_HINT: &str = "version-hint.text";

/// An Iceberg table opened at one metadata file.
pub struct OpenedTable {
    /// The DataFusion provider for the table.
    pub provider: Arc<dyn TableProvider>,
    /// Path of the metadata file this provider reads, relative to the table
    /// directory (e.g. `metadata/00003-….metadata.json`).
    pub metadata_location: String,
    /// Schema of the table at that metadata file.
    pub schema: SchemaRef,
}

/// Read the `snapshot_id` selector out of table OPTIONS.
///
/// Accepts both the raw key and the `format.`-prefixed form DataFusion produces
/// for an `OPTIONS` key without a dot.
pub fn snapshot_id_from_options(options: &HashMap<String, String>) -> anyhow::Result<Option<i64>> {
    let value = options
        .get("snapshot_id")
        .or_else(|| options.get("format.snapshot_id"));
    match value {
        None => Ok(None),
        Some(raw) => {
            let id: i64 = raw
                .parse()
                .with_context(|| format!("invalid Iceberg `snapshot_id` option: {raw:?}"))?;
            Ok(Some(id))
        }
    }
}

/// Strip an optional `scheme://` and the surrounding slashes from a Beacon
/// location, yielding the path of the table directory relative to the datasets
/// store root (e.g. `datasets://argo/obs` -> `argo/obs`).
pub(crate) fn location_to_prefix(location: &str) -> anyhow::Result<String> {
    let without_scheme = match location.split_once("://") {
        Some((_scheme, rest)) => rest,
        None => location,
    };
    let trimmed = without_scheme.trim_matches('/');
    anyhow::ensure!(
        !trimmed.is_empty(),
        "Iceberg table location must not be empty"
    );
    Ok(trimmed.to_string())
}

/// The version number a metadata file name carries, if any.
///
/// Both naming conventions in the wild put it first: `v3.metadata.json` (a
/// table written without a catalog) and `00003-<uuid>.metadata.json` (a table
/// written through one).
fn metadata_file_version(file_name: &str) -> Option<u64> {
    let stem = file_name.strip_suffix(".metadata.json")?;
    let stem = stem.strip_suffix(".gz").unwrap_or(stem);
    let digits = stem
        .strip_prefix('v')
        .unwrap_or(stem)
        .split('-')
        .next()
        .unwrap_or_default();
    digits.parse().ok()
}

/// Is this a metadata file rather than, say, a manifest?
fn is_metadata_file(file_name: &str) -> bool {
    file_name.ends_with(".metadata.json")
}

async fn object_exists(store: &Arc<dyn ObjectStore>, path: &ObjectPath) -> anyhow::Result<bool> {
    match store.head(path).await {
        Ok(_) => Ok(true),
        Err(object_store::Error::NotFound { .. }) => Ok(false),
        Err(error) => Err(anyhow::Error::new(error).context(format!("failed to stat {path}"))),
    }
}

/// Find the current metadata file of the table at `prefix`, returned as a path
/// relative to the table directory.
///
/// `version-hint.text` is authoritative when it exists and names a file that
/// exists. Otherwise the metadata directory is listed and the highest version
/// wins, with the most recently written file breaking a tie.
pub async fn resolve_metadata_location(
    store: &Arc<dyn ObjectStore>,
    prefix: &str,
) -> anyhow::Result<String> {
    let metadata_dir = format!("{prefix}/{METADATA_DIR}");

    if let Some(hinted) = read_version_hint(store, &metadata_dir).await? {
        return Ok(hinted);
    }

    let mut newest: Option<(Option<u64>, ObjectMeta)> = None;
    let mut listing = store.list(Some(&ObjectPath::from(metadata_dir.as_str())));
    while let Some(object) = listing.next().await {
        let object = object.with_context(|| format!("failed to list {metadata_dir:?}"))?;
        let Some(file_name) = object.location.filename() else {
            continue;
        };
        if !is_metadata_file(file_name) {
            continue;
        }
        let version = metadata_file_version(file_name);
        let wins = newest.as_ref().is_none_or(|(best_version, best)| {
            (version, object.last_modified) > (*best_version, best.last_modified)
        });
        if wins {
            newest = Some((version, object));
        }
    }

    let file_name = newest
        .as_ref()
        .and_then(|(_, object)| object.location.filename())
        .ok_or_else(|| {
            anyhow::anyhow!(
                "no Iceberg metadata found under {metadata_dir:?}; \
                 a location must name the table directory, not the metadata file"
            )
        })?;
    Ok(format!("{METADATA_DIR}/{file_name}"))
}

/// Read `metadata/version-hint.text` and turn it into a metadata file path.
///
/// The file holds either a bare version number or a file name. A hint that
/// points at a file that is not there is ignored rather than fatal: the listing
/// fallback then finds the real current metadata.
async fn read_version_hint(
    store: &Arc<dyn ObjectStore>,
    metadata_dir: &str,
) -> anyhow::Result<Option<String>> {
    let hint_path = ObjectPath::from(format!("{metadata_dir}/{VERSION_HINT}"));
    let hint = match store.get(&hint_path).await {
        Ok(result) => result
            .bytes()
            .await
            .with_context(|| format!("failed to read {hint_path}"))?,
        Err(object_store::Error::NotFound { .. }) => return Ok(None),
        Err(error) => {
            return Err(anyhow::Error::new(error).context(format!("failed to read {hint_path}")))
        }
    };
    let hint = String::from_utf8_lossy(&hint).trim().to_string();
    if hint.is_empty() {
        return Ok(None);
    }

    let candidates: Vec<String> = if is_metadata_file(&hint) {
        vec![hint]
    } else if let Ok(version) = hint.parse::<u64>() {
        vec![
            format!("v{version}.metadata.json"),
            format!("{version}.metadata.json"),
        ]
    } else {
        return Ok(None);
    };

    for candidate in candidates {
        let path = ObjectPath::from(format!("{metadata_dir}/{candidate}"));
        if object_exists(store, &path).await? {
            return Ok(Some(format!("{METADATA_DIR}/{candidate}")));
        }
    }
    Ok(None)
}

/// Open the Iceberg table at `location` and build a DataFusion provider for it.
///
/// `metadata_location` pins a metadata file (relative to the table directory) so
/// a plan and its scan read the same table version; pass `None` to resolve the
/// current one. `snapshot_id` selects an older snapshot inside that metadata for
/// time travel.
pub async fn open_iceberg_table(
    store: Arc<dyn ObjectStore>,
    location: &str,
    metadata_location: Option<&str>,
    snapshot_id: Option<i64>,
) -> anyhow::Result<OpenedTable> {
    let prefix = location_to_prefix(location)?;

    // The storage looks its object store up by prefix, because `iceberg::io::Storage`
    // must be serializable and an object store is not.
    register_store(&prefix, store.clone());

    let metadata_location = match metadata_location {
        Some(pinned) => pinned.to_string(),
        None => resolve_metadata_location(&store, &prefix).await?,
    };

    // Phase 1: read the metadata file, whose path we already know relative to the
    // table directory. Going through Iceberg's own reader (rather than the object
    // store) keeps gzip-compressed metadata working.
    let bootstrap = FileIOBuilder::new(Arc::new(BeaconStorageFactory::new(
        BeaconStorage::bootstrap(&prefix),
    )))
    .build();
    let metadata = TableMetadata::read_from(
        &bootstrap,
        format!("{BEACON_ICEBERG_SCHEME}:///{metadata_location}"),
    )
    .await
    .with_context(|| {
        format!("failed to read Iceberg metadata {metadata_location:?} of {location:?}")
    })?;

    // Phase 2: the metadata declares the table root, which is what every manifest
    // and data-file path is written against. Rebase those onto Beacon's store.
    let file_io = FileIOBuilder::new(Arc::new(BeaconStorageFactory::new(
        BeaconStorage::rooted_at(&prefix, metadata.location()),
    )))
    .build();

    let table_name = prefix.rsplit('/').next().unwrap_or(prefix.as_str());
    let identifier = TableIdent::from_strs([BEACON_NAMESPACE, table_name])
        .with_context(|| format!("invalid Iceberg table name {table_name:?}"))?;

    let table = StaticTable::from_metadata(metadata, identifier, file_io)
        .await
        .with_context(|| format!("failed to open Iceberg table at {location:?}"))?
        .into_table();

    let provider = match snapshot_id {
        Some(snapshot_id) => {
            IcebergStaticTableProvider::try_new_from_table_snapshot(table, snapshot_id)
                .await
                .with_context(|| {
                    format!("failed to open snapshot {snapshot_id} of Iceberg table {location:?}")
                })?
        }
        None => IcebergStaticTableProvider::try_new_from_table(table)
            .await
            .with_context(|| {
                format!("failed to build a provider for Iceberg table {location:?}")
            })?,
    };

    let provider: Arc<dyn TableProvider> = Arc::new(provider);
    let schema = provider.schema();
    Ok(OpenedTable {
        provider,
        metadata_location,
        schema,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use object_store::PutPayload;

    fn store() -> Arc<dyn ObjectStore> {
        Arc::new(InMemory::new())
    }

    async fn put(store: &Arc<dyn ObjectStore>, path: &str, body: &str) {
        store
            .put(&ObjectPath::from(path), PutPayload::from(body.to_string()))
            .await
            .unwrap();
    }

    #[test]
    fn location_to_prefix_strips_scheme_and_slashes() {
        assert_eq!(
            location_to_prefix("datasets://argo/obs").unwrap(),
            "argo/obs"
        );
        assert_eq!(location_to_prefix("/argo/obs/").unwrap(), "argo/obs");
        assert_eq!(location_to_prefix("argo/obs").unwrap(), "argo/obs");
        assert!(location_to_prefix("datasets://").is_err());
        assert!(location_to_prefix("///").is_err());
    }

    #[test]
    fn metadata_file_version_reads_both_naming_conventions() {
        assert_eq!(metadata_file_version("v3.metadata.json"), Some(3));
        assert_eq!(metadata_file_version("3.metadata.json"), Some(3));
        assert_eq!(
            metadata_file_version("00042-2f3c9a1e-0000-0000-0000-000000000000.metadata.json"),
            Some(42)
        );
        assert_eq!(metadata_file_version("v7.gz.metadata.json"), Some(7));
        // Not a metadata file, or no version to read.
        assert_eq!(metadata_file_version("snap-123.avro"), None);
        assert_eq!(metadata_file_version("current.metadata.json"), None);
    }

    #[tokio::test]
    async fn the_highest_version_wins_when_no_hint_exists() {
        let store = store();
        for name in [
            "v1.metadata.json",
            "v10.metadata.json",
            "v2.metadata.json",
            // Neither of these is a metadata file.
            "snap-1.avro",
            "version-hint.text.bak",
        ] {
            put(&store, &format!("argo/obs/metadata/{name}"), "{}").await;
        }
        assert_eq!(
            resolve_metadata_location(&store, "argo/obs").await.unwrap(),
            "metadata/v10.metadata.json"
        );
    }

    #[tokio::test]
    async fn a_version_hint_wins_over_the_listing() {
        let store = store();
        put(&store, "argo/obs/metadata/v1.metadata.json", "{}").await;
        put(&store, "argo/obs/metadata/v2.metadata.json", "{}").await;
        put(&store, "argo/obs/metadata/version-hint.text", "1\n").await;
        assert_eq!(
            resolve_metadata_location(&store, "argo/obs").await.unwrap(),
            "metadata/v1.metadata.json"
        );
    }

    #[tokio::test]
    async fn a_version_hint_may_name_the_file_itself() {
        let store = store();
        put(&store, "argo/obs/metadata/00007-abc.metadata.json", "{}").await;
        put(
            &store,
            "argo/obs/metadata/version-hint.text",
            "00007-abc.metadata.json",
        )
        .await;
        assert_eq!(
            resolve_metadata_location(&store, "argo/obs").await.unwrap(),
            "metadata/00007-abc.metadata.json"
        );
    }

    #[tokio::test]
    async fn a_stale_version_hint_falls_back_to_the_listing() {
        let store = store();
        put(&store, "argo/obs/metadata/v4.metadata.json", "{}").await;
        // The hinted version was expired away; the listing still knows the truth.
        put(&store, "argo/obs/metadata/version-hint.text", "2").await;
        assert_eq!(
            resolve_metadata_location(&store, "argo/obs").await.unwrap(),
            "metadata/v4.metadata.json"
        );
    }

    #[tokio::test]
    async fn a_directory_without_metadata_is_a_clear_error() {
        let store = store();
        put(&store, "argo/obs/data/a.parquet", "not metadata").await;
        let error = resolve_metadata_location(&store, "argo/obs")
            .await
            .unwrap_err();
        assert!(error.to_string().contains("no Iceberg metadata"), "{error}");
    }

    #[test]
    fn snapshot_id_is_read_from_either_option_spelling() {
        let mut options = HashMap::new();
        options.insert("snapshot_id".to_string(), "12".to_string());
        assert_eq!(snapshot_id_from_options(&options).unwrap(), Some(12));

        let mut options = HashMap::new();
        options.insert("format.snapshot_id".to_string(), "-3".to_string());
        assert_eq!(snapshot_id_from_options(&options).unwrap(), Some(-3));

        assert_eq!(snapshot_id_from_options(&HashMap::new()).unwrap(), None);

        let mut options = HashMap::new();
        options.insert("snapshot_id".to_string(), "latest".to_string());
        let error = snapshot_id_from_options(&options).unwrap_err();
        assert!(error.to_string().contains("snapshot_id"), "{error}");
    }
}
