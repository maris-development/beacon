//! An [`iceberg::io::Storage`] backed by Beacon's `object_store`.
//!
//! ## Why Beacon supplies its own storage
//!
//! `iceberg` 0.10 ships only a local-filesystem and an in-memory backend; every
//! other backend (S3, GCS, Azure) comes from a separate crate that talks to its
//! own credential configuration. Beacon already resolves one datasets store —
//! local FS or S3 — from the session's object-store registry, and every other
//! format reads through it. Implementing [`Storage`] over that store keeps
//! Iceberg on the same path: one credential source, one root, and S3 tables read
//! with no local copy.
//!
//! ## Path rebasing
//!
//! An Iceberg table records **absolute** paths — the table `location` in the
//! metadata, and one path per manifest and data file. Those paths are written by
//! whatever system created the table, so they name that writer's filesystem or
//! bucket, which is rarely how Beacon reaches the same bytes (a table written to
//! `/data/warehouse/obs` and mounted at `datasets://obs` is the normal case).
//!
//! So every path is rebased: strip the table root the metadata declares, then
//! join the remainder onto the table's prefix inside Beacon's store. A path that
//! sits outside the declared root is an error rather than a guess — it would
//! otherwise read some unrelated object.
//!
//! ## Serialization
//!
//! [`Storage`] is a `typetag` trait, so an implementation must be `Serialize` +
//! `DeserializeOwned`; an `Arc<dyn ObjectStore>` is neither. The store therefore
//! lives in a process-global registry keyed by the table prefix, and only the
//! key is serialized — the same shape `beacon-delta` uses for its log-store
//! factory.

use std::collections::HashMap;
use std::ops::Range;
use std::sync::{Arc, LazyLock, RwLock};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use iceberg::io::{
    FileMetadata, FileRead, FileWrite, InputFile, OutputFile, Storage, StorageConfig,
    StorageFactory,
};
use iceberg::{Error, ErrorKind, Result as IcebergResult};
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt as _};
use serde::{Deserialize, Serialize};

/// Scheme of the synthetic URL used to name a file by its path *inside* the
/// table directory, before the metadata (and with it the declared table root) is
/// known. See [`BeaconStorage::relative_path`].
pub(crate) const BEACON_ICEBERG_SCHEME: &str = "beacon-iceberg";

/// Table prefix -> the Beacon object store that serves it.
///
/// Populated by [`register_store`] each time a table is opened, and read back
/// when `typetag` deserializes a [`BeaconStorage`].
static STORES: LazyLock<RwLock<HashMap<String, Arc<dyn ObjectStore>>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

/// Record the object store that serves the table at `prefix`.
pub(crate) fn register_store(prefix: &str, store: Arc<dyn ObjectStore>) {
    STORES
        .write()
        .expect("iceberg store registry is not poisoned")
        .insert(prefix.to_string(), store);
}

fn registered_store(prefix: &str) -> IcebergResult<Arc<dyn ObjectStore>> {
    STORES
        .read()
        .expect("iceberg store registry is not poisoned")
        .get(prefix)
        .cloned()
        .ok_or_else(|| {
            Error::new(
                ErrorKind::Unexpected,
                format!("no Beacon object store is registered for Iceberg table {prefix:?}"),
            )
        })
}

/// Reduce a location to a comparable form: drop a `file://` / `file:` prefix,
/// keep any other scheme and authority, and trim trailing slashes.
///
/// `file:///warehouse/obs/` and `/warehouse/obs` are the same directory written
/// two ways; both normalize to `/warehouse/obs`.
fn normalize_location(location: &str) -> &str {
    let without_scheme = location
        .strip_prefix("file://")
        .or_else(|| location.strip_prefix("file:"))
        .unwrap_or(location);
    without_scheme.trim_end_matches('/')
}

/// The error every mutating [`Storage`] method returns. The Iceberg integration
/// is read only, so a write here means a caller reached a path that should have
/// been rejected earlier — say it plainly instead of half-applying a change.
fn read_only(operation: &str, path: &str) -> Error {
    Error::new(
        ErrorKind::FeatureUnsupported,
        format!("Beacon reads Iceberg tables read-only; refusing to {operation} {path:?}"),
    )
}

/// An [`iceberg::io::Storage`] that reads one Iceberg table through Beacon's
/// object store.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BeaconStorage {
    /// Path of the table directory inside Beacon's store, e.g. `argo/obs`. Also
    /// the key into the store registry.
    prefix: String,
    /// The table root as the metadata declares it, e.g.
    /// `file:///warehouse/argo/obs`. `None` while the metadata file itself is
    /// still being read, when only the synthetic scheme resolves.
    declared_root: Option<String>,
}

impl BeaconStorage {
    /// A storage that resolves only synthetic `beacon-iceberg:///<path>` URLs,
    /// used to read the metadata file that declares the table root.
    pub(crate) fn bootstrap(prefix: impl Into<String>) -> Self {
        Self {
            prefix: prefix.into(),
            declared_root: None,
        }
    }

    /// A storage that also rebases the absolute paths recorded in the table
    /// metadata (manifest lists, manifests, data and delete files).
    pub(crate) fn rooted_at(prefix: impl Into<String>, declared_root: impl Into<String>) -> Self {
        Self {
            prefix: prefix.into(),
            declared_root: Some(declared_root.into()),
        }
    }

    /// Turn a path Iceberg hands us into a path relative to the table directory.
    fn relative_path(&self, path: &str) -> IcebergResult<String> {
        // A path we minted ourselves: already relative to the table directory.
        if let Some(rest) = path.strip_prefix(&format!("{BEACON_ICEBERG_SCHEME}://")) {
            return Ok(rest.trim_start_matches('/').to_string());
        }

        if let Some(root) = &self.declared_root {
            let root = normalize_location(root);
            let normalized = normalize_location(path);
            // The remainder must start at a path separator, or `…/obs` would also
            // claim the files of a sibling `…/obs-2`.
            if let Some(rest) = normalized
                .strip_prefix(root)
                .and_then(|rest| rest.strip_prefix('/'))
            {
                if !rest.is_empty() {
                    return Ok(rest.to_string());
                }
            }
        }

        // A relative path (no scheme, no leading slash) is already what we need.
        // Some writers record data files this way.
        if !path.contains("://") && !path.starts_with('/') && !path.is_empty() {
            return Ok(path.to_string());
        }

        Err(Error::new(
            ErrorKind::DataInvalid,
            match &self.declared_root {
                Some(root) => format!(
                    "Iceberg file {path:?} lies outside the table root {root:?}; \
                     Beacon reads a table as one directory and cannot resolve it"
                ),
                None => format!(
                    "cannot resolve Iceberg file {path:?} before the table metadata is read"
                ),
            },
        ))
    }

    /// The full object path of `path` inside Beacon's store.
    fn object_path(&self, path: &str) -> IcebergResult<ObjectPath> {
        let relative = self.relative_path(path)?;
        Ok(ObjectPath::from(format!("{}/{}", self.prefix, relative)))
    }

    fn store(&self) -> IcebergResult<Arc<dyn ObjectStore>> {
        registered_store(&self.prefix)
    }
}

/// Map an `object_store` failure onto an Iceberg error, keeping the path.
fn io_error(operation: &str, path: &ObjectPath, error: object_store::Error) -> Error {
    Error::new(
        ErrorKind::Unexpected,
        format!("failed to {operation} Iceberg file {path}: {error}"),
    )
}

#[async_trait]
#[typetag::serde(name = "beacon_object_store")]
impl Storage for BeaconStorage {
    async fn exists(&self, path: &str) -> IcebergResult<bool> {
        let object_path = self.object_path(path)?;
        match self.store()?.head(&object_path).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(error) => Err(io_error("stat", &object_path, error)),
        }
    }

    async fn metadata(&self, path: &str) -> IcebergResult<FileMetadata> {
        let object_path = self.object_path(path)?;
        let meta = self
            .store()?
            .head(&object_path)
            .await
            .map_err(|error| io_error("stat", &object_path, error))?;
        Ok(FileMetadata { size: meta.size })
    }

    async fn read(&self, path: &str) -> IcebergResult<Bytes> {
        let object_path = self.object_path(path)?;
        let store = self.store()?;
        let result = store
            .get(&object_path)
            .await
            .map_err(|error| io_error("read", &object_path, error))?;
        result
            .bytes()
            .await
            .map_err(|error| io_error("read", &object_path, error))
    }

    async fn reader(&self, path: &str) -> IcebergResult<Box<dyn FileRead>> {
        Ok(Box::new(BeaconFileRead {
            store: self.store()?,
            path: self.object_path(path)?,
        }))
    }

    async fn write(&self, path: &str, _bs: Bytes) -> IcebergResult<()> {
        Err(read_only("write", path))
    }

    async fn writer(&self, path: &str) -> IcebergResult<Box<dyn FileWrite>> {
        Err(read_only("write", path))
    }

    async fn delete(&self, path: &str) -> IcebergResult<()> {
        Err(read_only("delete", path))
    }

    async fn delete_prefix(&self, path: &str) -> IcebergResult<()> {
        Err(read_only("delete", path))
    }

    async fn delete_stream(&self, _paths: BoxStream<'static, String>) -> IcebergResult<()> {
        Err(read_only("delete", "a stream of files"))
    }

    fn new_input(&self, path: &str) -> IcebergResult<InputFile> {
        Ok(InputFile::new(Arc::new(self.clone()), path.to_string()))
    }

    fn new_output(&self, path: &str) -> IcebergResult<OutputFile> {
        Ok(OutputFile::new(Arc::new(self.clone()), path.to_string()))
    }
}

/// Ranged reads of one object, used by the Parquet and Avro readers.
#[derive(Debug)]
struct BeaconFileRead {
    store: Arc<dyn ObjectStore>,
    path: ObjectPath,
}

#[async_trait]
impl FileRead for BeaconFileRead {
    async fn read(&self, range: Range<u64>) -> IcebergResult<Bytes> {
        self.store
            .get_range(&self.path, range)
            .await
            .map_err(|error| io_error("read", &self.path, error))
    }
}

/// Builds the [`BeaconStorage`] an Iceberg `FileIO` hands out.
///
/// The configuration Iceberg passes to [`StorageFactory::build`] is unused: a
/// Beacon table's prefix and root are fixed when the table is opened, so they
/// are carried on the factory itself.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BeaconStorageFactory {
    storage: BeaconStorage,
}

impl BeaconStorageFactory {
    pub(crate) fn new(storage: BeaconStorage) -> Self {
        Self { storage }
    }
}

#[typetag::serde(name = "beacon_object_store")]
impl StorageFactory for BeaconStorageFactory {
    fn build(&self, _config: &StorageConfig) -> IcebergResult<Arc<dyn Storage>> {
        Ok(Arc::new(self.storage.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt as _;
    use object_store::memory::InMemory;
    use object_store::PutPayload;

    #[test]
    fn normalize_location_drops_the_file_scheme_and_trailing_slash() {
        assert_eq!(
            normalize_location("file:///warehouse/obs/"),
            "/warehouse/obs"
        );
        assert_eq!(normalize_location("file:/warehouse/obs"), "/warehouse/obs");
        assert_eq!(normalize_location("/warehouse/obs"), "/warehouse/obs");
        // Any other scheme keeps its authority: the bucket is part of the root.
        assert_eq!(normalize_location("s3://bucket/obs/"), "s3://bucket/obs");
    }

    #[test]
    fn a_declared_root_is_stripped_from_absolute_paths() {
        let storage = BeaconStorage::rooted_at("argo/obs", "file:///warehouse/obs");
        assert_eq!(
            storage
                .relative_path("file:///warehouse/obs/data/00000.parquet")
                .unwrap(),
            "data/00000.parquet"
        );
        assert_eq!(
            storage
                .object_path("file:///warehouse/obs/metadata/v1.metadata.json")
                .unwrap(),
            ObjectPath::from("argo/obs/metadata/v1.metadata.json")
        );
    }

    #[test]
    fn an_s3_root_is_stripped_with_its_bucket() {
        let storage = BeaconStorage::rooted_at("obs", "s3://warehouse/argo/obs");
        assert_eq!(
            storage
                .relative_path("s3://warehouse/argo/obs/data/a.parquet")
                .unwrap(),
            "data/a.parquet"
        );
    }

    #[test]
    fn the_synthetic_scheme_resolves_before_the_root_is_known() {
        let storage = BeaconStorage::bootstrap("argo/obs");
        assert_eq!(
            storage
                .relative_path("beacon-iceberg:///metadata/v3.metadata.json")
                .unwrap(),
            "metadata/v3.metadata.json"
        );
        // Anything absolute is unresolvable until the metadata declares a root.
        assert!(storage
            .relative_path("file:///warehouse/obs/data/a.parquet")
            .is_err());
    }

    #[test]
    fn a_path_outside_the_declared_root_is_an_error() {
        let storage = BeaconStorage::rooted_at("argo/obs", "file:///warehouse/obs");
        let error = storage
            .relative_path("file:///warehouse/other/data/a.parquet")
            .unwrap_err();
        assert!(
            error.to_string().contains("outside the table root"),
            "{error}"
        );
        // The root itself names no file.
        assert!(storage.relative_path("file:///warehouse/obs").is_err());
        // A sibling whose name merely starts with the root's is not inside it.
        assert!(storage
            .relative_path("file:///warehouse/obs-2/data/a.parquet")
            .is_err());
    }

    #[test]
    fn a_relative_path_is_kept_as_is() {
        let storage = BeaconStorage::rooted_at("argo/obs", "file:///warehouse/obs");
        assert_eq!(
            storage.relative_path("data/a.parquet").unwrap(),
            "data/a.parquet"
        );
    }

    #[tokio::test]
    async fn reads_go_through_the_registered_object_store() {
        let store = Arc::new(InMemory::new());
        store
            .put(
                &ObjectPath::from("argo/obs/data/a.parquet"),
                PutPayload::from_static(b"0123456789"),
            )
            .await
            .unwrap();
        register_store("argo/obs", store);

        let storage = BeaconStorage::rooted_at("argo/obs", "file:///warehouse/obs");
        let path = "file:///warehouse/obs/data/a.parquet";

        assert!(storage.exists(path).await.unwrap());
        assert_eq!(storage.metadata(path).await.unwrap().size, 10);
        assert_eq!(storage.read(path).await.unwrap(), Bytes::from("0123456789"));

        // A ranged read is what the Parquet reader actually uses.
        let reader = storage.reader(path).await.unwrap();
        assert_eq!(reader.read(2..5).await.unwrap(), Bytes::from("234"));

        // A missing file is reported as absent, not as a failure.
        assert!(!storage
            .exists("file:///warehouse/obs/data/missing.parquet")
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn every_mutating_operation_is_refused() {
        let storage = BeaconStorage::rooted_at("argo/obs", "file:///warehouse/obs");
        let path = "file:///warehouse/obs/data/a.parquet";
        assert!(storage.write(path, Bytes::new()).await.is_err());
        assert!(storage.writer(path).await.is_err());
        assert!(storage.delete(path).await.is_err());
        assert!(storage.delete_prefix(path).await.is_err());
        assert!(storage
            .delete_stream(futures::stream::empty().boxed())
            .await
            .is_err());
    }

    #[test]
    fn an_unregistered_prefix_names_itself_in_the_error() {
        let error = registered_store("never/registered").unwrap_err();
        assert!(error.to_string().contains("never/registered"), "{error}");
    }
}
