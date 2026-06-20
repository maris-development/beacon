//! Shared Iceberg catalog for beacon managed tables.
//!
//! Beacon holds a single process-wide Iceberg [`Catalog`] backed by an object
//! store (the same local-or-S3 storage beacon already uses). The catalog is the
//! source of truth for Iceberg table metadata; beacon's own `table.json` files
//! are thin pointers (see [`crate::definition::IcebergTableDefinition`]) that let
//! the existing table-discovery path rebuild providers on startup.

use std::path::Path;
use std::sync::Arc;

use beacon_object_storage::DATASETS_WRITEABLE_PREFIX;
use iceberg_file_catalog::FileCatalog;
use iceberg_rust::catalog::Catalog;
use iceberg_rust::object_store::ObjectStoreBuilder;
use object_store::local::LocalFileSystem;
use object_store::prefix::PrefixStore;
use object_store::ObjectStore;
use tokio::sync::OnceCell;

/// The single Iceberg namespace beacon creates managed tables under.
pub const BEACON_NAMESPACE: &str = "beacon";

/// Sub-directory under the datasets internal prefix (`__beacon__`) that holds the
/// Iceberg warehouse. Tables live at `__beacon__/iceberg/<namespace>/<table>/`.
const WAREHOUSE_SUBDIR: &str = "iceberg";

/// Process-wide Iceberg catalog. Initialized once during runtime startup via
/// [`init_catalog`], read by [`get_catalog`] (notably from
/// `IcebergTableDefinition::build_provider`, which has no catalog argument).
static ICEBERG_CATALOG: OnceCell<Arc<dyn Catalog>> = OnceCell::const_new();

/// Process-wide handle to the raw warehouse object store, rooted at the warehouse
/// directory. Used to reclaim a table's files on `DROP TABLE`: the file catalog's
/// own `drop_table` is unimplemented, so beacon deletes the table's
/// `<namespace>/<name>/` directory directly.
static WAREHOUSE_STORE: OnceCell<Arc<dyn ObjectStore>> = OnceCell::const_new();

/// The namespace beacon uses, as the `Vec<String>` form the Iceberg API expects.
pub fn beacon_namespace() -> Vec<String> {
    vec![BEACON_NAMESPACE.to_string()]
}

/// Initialize the shared Iceberg catalog backed by the **datasets store**.
///
/// The warehouse lives under the datasets store's internal prefix
/// (`__beacon__/iceberg`), so Iceberg metadata and data sit alongside the
/// datasets on the same backend (local filesystem or S3, per
/// [`beacon_config`]) and stay hidden from user-facing dataset listings.
///
/// Sets both the shared catalog ([`get_catalog`]) and the warehouse store used
/// for `DROP TABLE` cleanup ([`get_warehouse_store`]).
pub async fn init_datasets_warehouse(
    datasets: std::sync::Arc<beacon_object_storage::DatasetsStore>,
    storage: &beacon_config::StorageConfig,
    datasets_dir: &std::path::Path,
) -> anyhow::Result<()> {
    tracing::info!(
        backend = if storage.s3.data_lake { "s3" } else { "local" },
        "initializing Iceberg datasets warehouse"
    );
    // Full warehouse prefix within the backing store, e.g. `__beacon__/iceberg`.
    let warehouse_prefix = format!("{DATASETS_WRITEABLE_PREFIX}/{WAREHOUSE_SUBDIR}");

    // The file catalog needs an `ObjectStoreBuilder` (it cannot accept an
    // arbitrary `ObjectStore`), so mirror the datasets store's backend choice.
    // `catalog_path` is the warehouse root the catalog roots every table under.
    let (object_store_builder, catalog_path) = if storage.s3.data_lake {
        let bucket = storage.s3.bucket.as_deref().ok_or_else(|| {
            anyhow::anyhow!("Iceberg warehouse on S3 requires a bucket name (set BEACON_S3_BUCKET)")
        })?;
        // `AmazonS3Builder::from_env()` picks up the AWS_* env; mirror the two
        // settings beacon configures explicitly for the datasets store.
        let builder = ObjectStoreBuilder::s3()
            .with_config("aws_allow_http", "true")
            .and_then(|builder| {
                builder.with_config(
                    "aws_virtual_hosted_style_request",
                    if storage.s3.enable_virtual_hosting {
                        "true"
                    } else {
                        "false"
                    },
                )
            })
            .map_err(|error| anyhow::anyhow!("Failed to configure Iceberg S3 store: {error}"))?;
        (builder, format!("s3://{bucket}/{warehouse_prefix}"))
    } else {
        // Local: root the builder at the datasets directory so warehouse paths
        // resolve to `<datasets_dir>/__beacon__/iceberg/...`.
        let builder = ObjectStoreBuilder::filesystem(datasets_dir.to_path_buf());
        (builder, warehouse_prefix.clone())
    };

    let catalog: Arc<dyn Catalog> = Arc::new(
        FileCatalog::new(&catalog_path, object_store_builder)
            .await
            .map_err(|error| {
                tracing::error!(catalog_path = %catalog_path, error = %error, "failed to create Iceberg file catalog");
                anyhow::anyhow!("Failed to create Iceberg file catalog: {error}")
            })?,
    );

    // DROP cleanup deletes files via the datasets internal store (which already
    // maps paths under `__beacon__`), nested one level into the `iceberg`
    // sub-directory so a table's `<namespace>/<table>` prefix resolves to
    // `__beacon__/iceberg/<namespace>/<table>`.
    let drop_store: Arc<dyn ObjectStore> =
        Arc::new(PrefixStore::new(datasets.internal_store(), WAREHOUSE_SUBDIR));

    init_catalog(catalog);
    let _ = WAREHOUSE_STORE.set(drop_store);

    tracing::info!(catalog_path = %catalog_path, "Iceberg datasets warehouse initialized");
    Ok(())
}

/// Build an Iceberg catalog whose warehouse lives on the local filesystem under
/// `warehouse_dir`. Used by tests; production uses [`init_datasets_warehouse`].
/// Table metadata and data files are written below that root as
/// `<namespace>/<table>/{metadata,data}/...`.
pub async fn build_local_file_catalog(
    warehouse_dir: impl AsRef<Path>,
) -> anyhow::Result<Arc<dyn Catalog>> {
    let warehouse_dir = warehouse_dir.as_ref();
    let object_store = ObjectStoreBuilder::filesystem(warehouse_dir);
    // An empty catalog path roots every table location at the object store's
    // own prefix, so paths resolve to `<warehouse_dir>/<namespace>/<table>/...`.
    let catalog = FileCatalog::new("", object_store)
        .await
        .map_err(|error| anyhow::anyhow!("Failed to create Iceberg file catalog: {error}"))?;

    // A plain object store over the same root, used for DROP cleanup.
    let store: Arc<dyn ObjectStore> = Arc::new(
        LocalFileSystem::new_with_prefix(warehouse_dir)
            .map_err(|error| anyhow::anyhow!("Failed to open Iceberg warehouse store: {error}"))?,
    );
    let _ = WAREHOUSE_STORE.set(store);

    Ok(Arc::new(catalog))
}

/// Fetch the warehouse object store used for `DROP TABLE` cleanup.
pub fn get_warehouse_store() -> anyhow::Result<Arc<dyn ObjectStore>> {
    WAREHOUSE_STORE
        .get()
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("Iceberg warehouse store has not been initialized"))
}

/// Install the shared catalog. Idempotent: a second call is ignored so repeated
/// runtime initialization (e.g. in tests) does not panic.
pub fn init_catalog(catalog: Arc<dyn Catalog>) {
    let _ = ICEBERG_CATALOG.set(catalog);
}

/// Fetch the shared catalog, erroring if the runtime never initialized it.
pub fn get_catalog() -> anyhow::Result<Arc<dyn Catalog>> {
    ICEBERG_CATALOG
        .get()
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("Iceberg catalog has not been initialized"))
}
