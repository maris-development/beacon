//! Runtime-scoped Lance warehouse, backed by beacon's tables object store.
//!
//! Rather than raw filesystem paths, managed Lance datasets are read/written
//! through the same `object_store` instance beacon uses for the tables store. We
//! do this with Lance's [`ObjectStoreProvider`]/[`ObjectStoreRegistry`]: a
//! provider that hands Lance our store for a custom `db://` scheme is
//! registered in a registry, which travels to Lance through a [`Session`]. Reads
//! (`DatasetBuilder::with_session`) and writes (`WriteParams.session`) both pull
//! the registry from that session.
//!
//! The warehouse is **runtime-scoped** (built per `Runtime`, threaded through the
//! session as an extension), so multiple runtimes stay isolated. Beacon always
//! backs the tables store with a local filesystem, so this stays local in
//! practice — the abstraction just keeps Lance consistent with the rest of beacon.

use std::collections::HashMap;
use std::sync::Arc;

use lance::dataset::{DEFAULT_INDEX_CACHE_SIZE, DEFAULT_METADATA_CACHE_SIZE};
use lance::session::Session;
use lance_core::Result as LanceResult;
use lance_core::cache::{CacheBackend, MokaCacheBackend};
use lance_io::object_store::{
    ObjectStore, ObjectStoreParams, ObjectStoreProvider, ObjectStoreRegistry,
    DEFAULT_CLOUD_IO_PARALLELISM, DEFAULT_LOCAL_IO_PARALLELISM,
};
use object_store::DynObjectStore;
use parking_lot::{Mutex, RwLock};
use tokio::sync::Mutex as AsyncMutex;
use url::Url;

/// The single namespace beacon creates managed tables under (kept parallel to
/// the Iceberg integration so table layouts are predictable).
pub const BEACON_NAMESPACE: &str = "beacon";

/// Custom URI scheme used to route managed Lance datasets through beacon's redb
/// (`db://`) object store via the registered [`BeaconTablesProvider`]. Lance
/// resolves this through its own session registry, separate from DataFusion's
/// `db://` registration for the catalog — both name the same physical store.
const SCHEME: &str = "db";

/// Sub-prefix within the tables store under which managed Lance datasets live.
const PREFIX: &str = "lance";

/// The namespace beacon uses, in the `Vec<String>` form the rest of the crate
/// expects.
pub fn beacon_namespace() -> Vec<String> {
    vec![BEACON_NAMESPACE.to_string()]
}

/// A Lance object-store provider that hands Lance beacon's redb store for the
/// `db://` scheme.
#[derive(Debug)]
struct BeaconTablesProvider {
    inner: Arc<DynObjectStore>,
}

#[async_trait::async_trait]
impl ObjectStoreProvider for BeaconTablesProvider {
    async fn new_store(
        &self,
        base_path: Url,
        _params: &ObjectStoreParams,
    ) -> LanceResult<ObjectStore> {
        // Lance sizes its I/O queue from this value; a capacity of 1 deadlocks it.
        // The store is local, so the Lance local default applies, not the CPU count.
        Ok(ObjectStore::new(
            self.inner.clone(),
            base_path,
            None,  // block_size
            None,  // wrapper
            false, // use_constant_size_upload_parts
            false, // list_is_lexically_ordered (local FS isn't; let Lance sort)
            DEFAULT_LOCAL_IO_PARALLELISM,
            3, // download_retry_count
            None,
        ))
    }

    /// Give each dataset its own registry/cache entry (and thus an `ObjectStore`
    /// with the correct base path), rather than sharing one keyed only by scheme.
    fn calculate_object_store_prefix(
        &self,
        url: &Url,
        _storage_options: Option<&HashMap<String, String>>,
    ) -> LanceResult<String> {
        Ok(format!("{}${}{}", url.scheme(), url.authority(), url.path()))
    }
}

/// A Lance object-store provider for one URL scheme of external datasets.
///
/// It hands Lance the DataFusion object store registered for the URL authority,
/// so external datasets use the same credentials as other external tables.
#[derive(Debug, Default)]
struct ExternalStoresProvider {
    /// The stores of this scheme, keyed by URL authority.
    stores: RwLock<HashMap<String, Arc<DynObjectStore>>>,
}

impl ExternalStoresProvider {
    /// Set the store for `authority`. This replaces an earlier store.
    fn insert(&self, authority: &str, store: Arc<DynObjectStore>) {
        self.stores.write().insert(authority.to_string(), store);
    }

    fn store(&self, url: &Url) -> LanceResult<Arc<DynObjectStore>> {
        self.stores
            .read()
            .get(url.authority())
            .cloned()
            .ok_or_else(|| {
                lance_core::Error::invalid_input(format!(
                    "No object store is registered for '{}://{}'",
                    url.scheme(),
                    url.authority()
                ))
            })
    }
}

#[async_trait::async_trait]
impl ObjectStoreProvider for ExternalStoresProvider {
    async fn new_store(
        &self,
        base_path: Url,
        _params: &ObjectStoreParams,
    ) -> LanceResult<ObjectStore> {
        let inner = self.store(&base_path)?;
        // A capacity of 1 deadlocks Lance (see `BeaconTablesProvider::new_store`).
        let io_parallelism = if base_path.scheme() == "file" {
            DEFAULT_LOCAL_IO_PARALLELISM
        } else {
            DEFAULT_CLOUD_IO_PARALLELISM
        };
        Ok(ObjectStore::new(
            inner,
            base_path,
            None,  // block_size
            None,  // wrapper
            false, // use_constant_size_upload_parts
            false, // list_is_lexically_ordered
            io_parallelism,
            3, // download_retry_count
            None,
        ))
    }

    fn calculate_object_store_prefix(
        &self,
        url: &Url,
        _storage_options: Option<&HashMap<String, String>>,
    ) -> LanceResult<String> {
        // The store address makes Lance drop its cached store after a replacement.
        let store = self.store(url)?;
        Ok(format!(
            "{}${}{}#{:p}",
            url.scheme(),
            url.authority(),
            url.path(),
            Arc::as_ptr(&store)
        ))
    }
}

/// A runtime-scoped Lance warehouse over beacon's tables object store.
#[derive(Debug)]
pub struct LanceWarehouse {
    /// The tables object store (used directly for `DROP` cleanup).
    store: Arc<DynObjectStore>,
    /// Lance session carrying the registry that resolves `db://`.
    session: Arc<Session>,
    /// The registry of `session`, kept to add providers for external stores.
    registry: Arc<ObjectStoreRegistry>,
    /// The external store providers, keyed by URL scheme.
    external: Mutex<HashMap<String, Arc<ExternalStoresProvider>>>,
    /// The index cache of `session`. Lance keeps it private, so we hold the
    /// backend to clear a dropped table's entries (see [`Self::forget`]).
    index_cache: Arc<MokaCacheBackend>,
    /// Per-dataset write locks, keyed by dataset URI.
    locks: Mutex<HashMap<String, Arc<AsyncMutex<()>>>>,
}

/// Memory pool for Lance's index builds, in bytes. Lance's own default is
/// 100MB, which is not enough to sort a column of a large table: `CREATE INDEX`
/// on a 100M-row table failed with "Resources exhausted ... 32.0 KB remain
/// available for the total pool". With 1GB the same index builds in ~7s.
const INDEX_BUILD_MEM_POOL: &str = "1073741824";

/// Raise the memory limit Lance uses when building an index.
///
/// Lance reads this from the environment and offers no way to pass it in:
/// `lance::index::scalar` hardcodes `LanceExecutionOptions::default()`, whose
/// `mem_pool_size()` falls back to `$LANCE_MEM_POOL_SIZE`. So the environment is
/// the only lever available to us.
///
/// Called from `LanceWarehouse::new` (runtime startup) rather than from
/// `create_index`, because `set_var` is only sound while no other thread is
/// reading the environment; startup is the safest point we control. An explicit
/// `LANCE_MEM_POOL_SIZE` from the operator always wins.
fn raise_index_build_memory_limit() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        if std::env::var_os("LANCE_MEM_POOL_SIZE").is_none() {
            // SAFETY: runs once, at runtime construction, before any beacon
            // worker threads that might concurrently read the environment.
            unsafe { std::env::set_var("LANCE_MEM_POOL_SIZE", INDEX_BUILD_MEM_POOL) };
        }
    });
}

impl LanceWarehouse {
    /// Build a warehouse over `store` (beacon's tables object store).
    pub fn new(store: Arc<DynObjectStore>) -> Self {
        raise_index_build_memory_limit();
        let registry = Arc::new(ObjectStoreRegistry::default());
        registry.insert(
            SCHEME,
            Arc::new(BeaconTablesProvider {
                inner: store.clone(),
            }),
        );
        // Lance's cache capacities are in BYTES, weighed by each entry's size.
        // They used to be entry counts, and the old `128, 128` here survived that
        // API change as 128 *bytes* per cache: effectively no caching, so every
        // scan re-read the dataset and column metadata. That is invisible on
        // narrow tables and very expensive on wide ones (Lance notes column
        // metadata alone can be ~40MB for a few hundred columns).
        let index_cache = Arc::new(MokaCacheBackend::with_capacity(DEFAULT_INDEX_CACHE_SIZE));
        let session = Arc::new(Session::with_index_cache_backend(
            index_cache.clone(),
            DEFAULT_METADATA_CACHE_SIZE,
            registry.clone(),
        ));
        Self {
            store,
            session,
            registry,
            external: Mutex::new(HashMap::new()),
            index_cache,
            locks: Mutex::new(HashMap::new()),
        }
    }

    /// Let Lance open datasets in `store`, under the scheme and authority of
    /// `store_url`. A second call for the same scheme and authority replaces
    /// the store.
    pub fn register_store(&self, store_url: &Url, store: Arc<DynObjectStore>) {
        let mut external = self.external.lock();
        let provider = external
            .entry(store_url.scheme().to_string())
            .or_insert_with(|| {
                let provider = Arc::new(ExternalStoresProvider::default());
                self.registry.insert(store_url.scheme(), provider.clone());
                provider
            });
        provider.insert(store_url.authority(), store);
    }

    /// The Lance URI of `path` in the store at `store_url`:
    /// `<scheme>://<authority>/<path>`. It is also the write lock key.
    pub fn external_uri(store_url: &Url, path: &object_store::path::Path) -> String {
        format!(
            "{}://{}/{}",
            store_url.scheme(),
            store_url.authority(),
            path.as_ref()
        )
    }

    /// The Lance session (carries the object-store registry) for open/write.
    pub fn session(&self) -> Arc<Session> {
        self.session.clone()
    }

    /// Remove every cache entry of the dataset at `uri` from the session.
    ///
    /// Lance keys its caches by URI and version number. A new table with the
    /// name of a dropped table has the same URI and starts again at version 1,
    /// so without this it reads the manifests and index lists of the dropped
    /// table, and then fails on the index files that `DROP` deleted.
    ///
    /// Lance hashes its cache keys, so it cannot remove the keys of one URI.
    /// This clears both caches for all tables. `DROP` is rare, so the cost is
    /// some cold reads after it.
    pub async fn forget(&self, _uri: &str) {
        self.session.file_metadata_cache().clear().await;
        self.index_cache.clear().await;
    }

    /// The tables object store, for `DROP` cleanup.
    pub fn store(&self) -> &Arc<DynObjectStore> {
        &self.store
    }

    /// The `db://` URI of a managed table:
    /// `db:///lance/<namespace>/<name>.lance`.
    pub fn table_uri(&self, namespace: &[String], name: &str) -> String {
        let ns = namespace.join("/");
        format!("{SCHEME}:///{PREFIX}/{ns}/{name}.lance")
    }

    /// The object-store key prefix for a table URI (everything after the scheme
    /// authority), used to enumerate a dataset's objects on `DROP`.
    pub fn object_path(uri: &str) -> object_store::path::Path {
        let parsed = Url::parse(uri).ok();
        let path = parsed
            .as_ref()
            .map(|u| u.path().trim_start_matches('/').to_string())
            .unwrap_or_default();
        // `Url::path` is percent-encoded; the store keys hold the decoded name.
        object_store::path::Path::from_url_path(&path)
            .unwrap_or_else(|_| object_store::path::Path::from(path))
    }

    /// The write lock guarding a given dataset URI (created on first use).
    pub fn lock(&self, uri: &str) -> Arc<AsyncMutex<()>> {
        let mut locks = self.locks.lock();
        locks
            .entry(uri.to_string())
            .or_insert_with(|| Arc::new(AsyncMutex::new(())))
            .clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    fn warehouse() -> LanceWarehouse {
        LanceWarehouse::new(Arc::new(InMemory::new()))
    }

    #[test]
    fn beacon_namespace_is_the_single_default() {
        assert_eq!(beacon_namespace(), vec![BEACON_NAMESPACE.to_string()]);
        assert_eq!(BEACON_NAMESPACE, "beacon");
    }

    /// The table URI is the `db://` address the redb object store resolves, and
    /// it is persisted in `table.json`, so its exact shape must stay stable.
    #[test]
    fn table_uri_has_the_expected_scheme_prefix_and_extension() {
        let wh = warehouse();
        assert_eq!(
            wh.table_uri(&beacon_namespace(), "orders"),
            "db:///lance/beacon/orders.lance"
        );
        // Nested namespaces are joined with '/'.
        assert_eq!(
            wh.table_uri(&["a".to_string(), "b".to_string()], "t"),
            "db:///lance/a/b/t.lance"
        );
    }

    /// `object_path` is the key prefix used to enumerate a dataset's files on
    /// DROP: it must be everything after the scheme+authority, with no leading
    /// slash (a leading slash would miss every object).
    #[test]
    fn object_path_strips_scheme_and_leading_slash() {
        let uri = "db:///lance/beacon/orders.lance";
        assert_eq!(
            LanceWarehouse::object_path(uri).as_ref(),
            "lance/beacon/orders.lance"
        );
    }

    /// The prefix must match the store keys, which hold the decoded name.
    #[test]
    fn object_path_decodes_the_uri_path() {
        let uri = warehouse().table_uri(&["beacon".to_string()], "odd name é");
        assert_eq!(
            LanceWarehouse::object_path(&uri).as_ref(),
            "lance/beacon/odd name é.lance"
        );
    }

    /// Lance sizes its I/O queue from this value. A capacity of 1 blocks the
    /// queue on the first table write, so a host with one CPU never started.
    /// The value is the Lance local default and never the CPU count.
    #[tokio::test]
    async fn store_io_parallelism_is_the_lance_local_default() {
        let provider = BeaconTablesProvider {
            inner: Arc::new(InMemory::new()),
        };
        let params = ObjectStoreParams::default();
        let store = provider
            .new_store(Url::parse("db:///lance").expect("valid url"), &params)
            .await
            .expect("store should build");
        assert_eq!(store.io_parallelism(), DEFAULT_LOCAL_IO_PARALLELISM);
        assert!(
            store.io_parallelism() >= 2,
            "an I/O capacity of 1 deadlocks the Lance scheduler"
        );
    }

    /// The same URI must hand back the *same* lock instance (so writers actually
    /// serialize), while different URIs get independent locks.
    #[test]
    fn locks_are_per_uri_and_stable() {
        let wh = warehouse();
        let a1 = wh.lock("db:///lance/beacon/a.lance");
        let a2 = wh.lock("db:///lance/beacon/a.lance");
        let b = wh.lock("db:///lance/beacon/b.lance");
        assert!(Arc::ptr_eq(&a1, &a2), "same URI must share one lock");
        assert!(!Arc::ptr_eq(&a1, &b), "different URIs get distinct locks");
    }

    mod external {
        use super::*;
        use crate::definition::LanceTableDefinition;
        use crate::io::{self, WriteKind};
        use crate::open_external_lance_table;
        use arrow::array::{Int64Array, RecordBatch, StringArray};
        use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
        use datafusion::catalog::TableProvider;
        use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
        use datafusion::prelude::SessionContext;
        use futures::StreamExt;
        use object_store::ObjectStore as _;
        use object_store::path::Path;

        fn bucket_url() -> Url {
            Url::parse("memory://bucket").unwrap()
        }

        fn external_warehouse() -> (Arc<LanceWarehouse>, Arc<InMemory>) {
            let wh = warehouse();
            let bucket = Arc::new(InMemory::new());
            wh.register_store(&bucket_url(), bucket.clone());
            (Arc::new(wh), bucket)
        }

        fn schema() -> ArrowSchema {
            ArrowSchema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("name", DataType::Utf8View, true),
            ])
        }

        fn definition(location: &str, options: &[(&str, &str)]) -> LanceTableDefinition {
            let mut definition = LanceTableDefinition::new("ext", vec![], location);
            definition.external = true;
            definition.options = options
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect();
            definition
        }

        fn uri(location: &str) -> String {
            LanceWarehouse::external_uri(&bucket_url(), &Path::from(location))
        }

        async fn append_two_rows(wh: &LanceWarehouse, uri: &str) {
            let schema = Arc::new(ArrowSchema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("name", DataType::Utf8, true),
            ]));
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(vec![1, 2])),
                    Arc::new(StringArray::from(vec!["a", "b"])),
                ],
            )
            .unwrap();
            let rows = Box::pin(RecordBatchStreamAdapter::new(
                schema,
                futures::stream::iter(vec![Ok(batch)]),
            ));
            io::write_stream(uri, wh.session(), rows, WriteKind::Append)
                .await
                .unwrap();
        }

        async fn count_rows(table: crate::LanceTable) -> i64 {
            let ctx = SessionContext::new();
            ctx.register_table("t", Arc::new(table)).unwrap();
            let batches = ctx
                .sql("SELECT count(*) FROM t")
                .await
                .unwrap()
                .collect()
                .await
                .unwrap();
            batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0)
        }

        #[test]
        fn external_uri_is_stable() {
            let path = Path::from("data/argo.lance");
            let first = LanceWarehouse::external_uri(&bucket_url(), &path);
            let second = LanceWarehouse::external_uri(&bucket_url(), &path);
            assert_eq!(first, second);
            assert!(first.starts_with("memory://bucket/"), "{first}");
            assert_eq!(first, "memory://bucket/data/argo.lance");
        }

        #[tokio::test]
        async fn create_then_open_external() {
            let (wh, bucket) = external_warehouse();
            let location = "data/new.lance";
            let created = open_external_lance_table(
                wh.clone(),
                definition(location, &[]),
                uri(location),
                Some(&schema()),
            )
            .await
            .expect("create should succeed");
            assert_eq!(created.uri(), uri(location));
            assert!(created.is_external());

            let files = bucket.list(Some(&Path::from(location))).count().await;
            assert!(files > 0, "the dataset files are in the registered store");

            let opened =
                open_external_lance_table(wh, definition(location, &[]), uri(location), None)
                    .await
                    .expect("open should succeed");
            let names = |s: arrow::datatypes::SchemaRef| -> Vec<String> {
                s.fields().iter().map(|f| f.name().clone()).collect()
            };
            assert_eq!(names(opened.schema()), names(created.schema()));
            assert_eq!(names(opened.schema()), vec!["id", "name"]);
        }

        #[tokio::test]
        async fn column_list_on_existing_dataset_fails() {
            let (wh, _) = external_warehouse();
            let location = "data/exists.lance";
            open_external_lance_table(
                wh.clone(),
                definition(location, &[]),
                uri(location),
                Some(&schema()),
            )
            .await
            .unwrap();
            let err = open_external_lance_table(
                wh,
                definition(location, &[]),
                uri(location),
                Some(&schema()),
            )
            .await
            .unwrap_err();
            assert_eq!(
                err.to_string(),
                "Lance external tables take their columns from the dataset; remove the column list"
            );
        }

        #[tokio::test]
        async fn missing_dataset_without_columns_fails() {
            let (wh, _) = external_warehouse();
            let location = "data/missing.lance";
            let err = open_external_lance_table(wh, definition(location, &[]), uri(location), None)
                .await
                .unwrap_err();
            assert_eq!(
                err.to_string(),
                "No Lance dataset at 'data/missing.lance'; add a column list to create one"
            );
        }

        #[tokio::test]
        async fn pinned_version_cannot_create() {
            let (wh, _) = external_warehouse();
            let location = "data/pinned-new.lance";
            let err = open_external_lance_table(
                wh,
                definition(location, &[("version", "1")]),
                uri(location),
                Some(&schema()),
            )
            .await
            .unwrap_err();
            assert_eq!(
                err.to_string(),
                "A pinned Lance table cannot create a dataset"
            );
        }

        #[tokio::test]
        async fn pinned_version_reads_old_rows() {
            let (wh, _) = external_warehouse();
            let location = "data/pinned.lance";
            open_external_lance_table(
                wh.clone(),
                definition(location, &[]),
                uri(location),
                Some(&schema()),
            )
            .await
            .unwrap();
            append_two_rows(&wh, &uri(location)).await;
            append_two_rows(&wh, &uri(location)).await;

            let pinned = open_external_lance_table(
                wh.clone(),
                definition(location, &[("version", "2")]),
                uri(location),
                None,
            )
            .await
            .expect("open at version 2");
            assert_eq!(
                pinned.ensure_writable().unwrap_err().to_string(),
                "Lance table 'ext' is pinned to version 2 and is read-only"
            );
            assert_eq!(count_rows(pinned).await, 2);

            let latest =
                open_external_lance_table(wh, definition(location, &[]), uri(location), None)
                    .await
                    .unwrap();
            assert_eq!(count_rows(latest).await, 4);
        }

        /// A second store for the same scheme and authority replaces the first.
        #[tokio::test]
        async fn register_store_replaces_the_store() {
            let (wh, old) = external_warehouse();
            let new = Arc::new(InMemory::new());
            wh.register_store(&bucket_url(), new.clone());
            let location = "data/replaced.lance";
            open_external_lance_table(
                wh,
                definition(location, &[]),
                uri(location),
                Some(&schema()),
            )
            .await
            .unwrap();
            let prefix = Path::from(location);
            assert!(new.list(Some(&prefix)).count().await > 0);
            assert_eq!(old.list(Some(&prefix)).count().await, 0);
        }

        /// Two authorities of one scheme resolve to their own stores.
        #[tokio::test]
        async fn stores_are_per_authority() {
            let (wh, first) = external_warehouse();
            let other_url = Url::parse("memory://other").unwrap();
            let second = Arc::new(InMemory::new());
            wh.register_store(&other_url, second.clone());
            let location = "data/two.lance";
            let other_uri = LanceWarehouse::external_uri(&other_url, &Path::from(location));
            open_external_lance_table(wh, definition(location, &[]), other_uri, Some(&schema()))
                .await
                .unwrap();
            let prefix = Path::from(location);
            assert!(second.list(Some(&prefix)).count().await > 0);
            assert_eq!(first.list(Some(&prefix)).count().await, 0);
        }

        #[tokio::test]
        async fn external_io_parallelism_is_never_one() {
            let provider = ExternalStoresProvider::default();
            provider.insert("bucket", Arc::new(InMemory::new()));
            provider.insert("", Arc::new(InMemory::new()));
            let params = ObjectStoreParams::default();
            let remote = provider
                .new_store(Url::parse("s3://bucket/a.lance").unwrap(), &params)
                .await
                .unwrap();
            assert_eq!(remote.io_parallelism(), DEFAULT_CLOUD_IO_PARALLELISM);
            let local = provider
                .new_store(Url::parse("file:///a.lance").unwrap(), &params)
                .await
                .unwrap();
            assert_eq!(local.io_parallelism(), DEFAULT_LOCAL_IO_PARALLELISM);
        }
    }
}
