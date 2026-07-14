//! A lazy [`ObjectStoreRegistry`] that materializes remote object stores on
//! demand, DataFusion-CLI / DuckDB style.
//!
//! [`LazyObjectStoreRegistry`] wraps a [`DefaultObjectStoreRegistry`] (which
//! holds beacon's eagerly-registered stores — `datasets://`, `db://`, `tmp://`,
//! `internal://`, and the built-in `file://`). On a lookup **miss** for a
//! supported remote scheme (`s3`/`gs`/`az`/`http`…), it builds the backing
//! store from the environment, layering in credentials from a matching
//! [`Secret`] (see [`crate::secrets`]), caches it in the inner registry, and
//! returns it. Subsequent lookups hit the cache.

use std::fmt;
use std::sync::Arc;

use datafusion::common::exec_datafusion_err;
use datafusion::execution::object_store::{DefaultObjectStoreRegistry, ObjectStoreRegistry};
use object_store::aws::{AmazonS3Builder, AmazonS3ConfigKey};
use object_store::azure::{AzureConfigKey, MicrosoftAzureBuilder};
use object_store::gcp::{GoogleCloudStorageBuilder, GoogleConfigKey};
use object_store::http::HttpBuilder;
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use url::Url;

use crate::secrets::{Secret, SecretStore};

/// An [`ObjectStoreRegistry`] that lazily builds remote stores on a miss, using
/// credentials from an in-memory [`SecretStore`].
pub struct LazyObjectStoreRegistry {
    inner: DefaultObjectStoreRegistry,
    secrets: Arc<SecretStore>,
}

impl LazyObjectStoreRegistry {
    /// A registry backed by the given secret store. The inner
    /// [`DefaultObjectStoreRegistry`] starts with the built-in `file://` store;
    /// beacon's other stores are registered on top via
    /// [`ObjectStoreRegistry::register_store`].
    pub fn new(secrets: Arc<SecretStore>) -> Self {
        Self {
            inner: DefaultObjectStoreRegistry::new(),
            secrets,
        }
    }

    /// The secret store this registry consults; mutate it to add/remove
    /// credentials at runtime.
    pub fn secrets(&self) -> &Arc<SecretStore> {
        &self.secrets
    }
}

impl fmt::Debug for LazyObjectStoreRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LazyObjectStoreRegistry")
            .field("secrets", &self.secrets)
            .finish()
    }
}

impl ObjectStoreRegistry for LazyObjectStoreRegistry {
    fn register_store(
        &self,
        url: &Url,
        store: Arc<dyn ObjectStore>,
    ) -> Option<Arc<dyn ObjectStore>> {
        self.inner.register_store(url, store)
    }

    fn get_store(&self, url: &Url) -> datafusion::error::Result<Arc<dyn ObjectStore>> {
        // Fast path: already registered (eager beacon stores, file://, or a
        // previously-materialized remote store).
        if let Ok(store) = self.inner.get_store(url) {
            return Ok(store);
        }

        // Miss: lazily build for a supported remote scheme, using the best
        // matching secret (or the environment chain if none applies).
        let key = store_key_url(url);
        let secret = self.secrets.resolve(url);
        let store = build_object_store(&key, secret.as_ref())?;
        self.inner.register_store(&key, store.clone());
        tracing::debug!(
            store = %key,
            secret = ?secret.as_ref().map(|s| &s.name),
            "lazily materialized object store",
        );
        Ok(store)
    }
}

/// The `scheme://authority` key an [`ObjectStoreRegistry`] uses to identify a
/// store, derived from a full object URL (the object path is irrelevant).
pub(crate) fn store_key_url(url: &Url) -> Url {
    let mut key = url.clone();
    key.set_path("");
    key.set_query(None);
    key.set_fragment(None);
    key
}

/// Build an [`ObjectStore`] for a `scheme://authority` URL. Credentials come
/// from `secret` (its `options` are `object_store` config keys applied to the
/// backend builder) layered over the standard environment chain
/// (`*Builder::from_env`); mirrors `beacon_object_storage::S3Config`.
pub(crate) fn build_object_store(
    url: &Url,
    secret: Option<&Secret>,
) -> datafusion::error::Result<Arc<dyn ObjectStore>> {
    let opts = secret.map(|s| &s.options);
    let store: Arc<dyn ObjectStore> = match url.scheme() {
        "s3" | "s3a" => {
            let mut builder = AmazonS3Builder::from_env().with_url(url.as_str());
            if let Some(opts) = opts {
                for (k, v) in opts {
                    match k.parse::<AmazonS3ConfigKey>() {
                        Ok(key) => builder = builder.with_config(key, v.as_str()),
                        Err(e) => {
                            tracing::warn!(key = %k, error = %e, "ignoring unknown S3 secret option")
                        }
                    }
                }
            }
            Arc::new(
                builder
                    .build()
                    .map_err(|e| exec_datafusion_err!("failed to build S3 store for {url}: {e}"))?,
            )
        }
        "gs" => {
            let mut builder = GoogleCloudStorageBuilder::from_env().with_url(url.as_str());
            if let Some(opts) = opts {
                for (k, v) in opts {
                    match k.parse::<GoogleConfigKey>() {
                        Ok(key) => builder = builder.with_config(key, v.as_str()),
                        Err(e) => {
                            tracing::warn!(key = %k, error = %e, "ignoring unknown GCS secret option")
                        }
                    }
                }
            }
            Arc::new(
                builder
                    .build()
                    .map_err(|e| exec_datafusion_err!("failed to build GCS store for {url}: {e}"))?,
            )
        }
        "az" | "azure" | "abfs" | "abfss" | "wasb" | "wasbs" | "adl" => {
            let mut builder = MicrosoftAzureBuilder::from_env().with_url(url.as_str());
            if let Some(opts) = opts {
                for (k, v) in opts {
                    match k.parse::<AzureConfigKey>() {
                        Ok(key) => builder = builder.with_config(key, v.as_str()),
                        Err(e) => {
                            tracing::warn!(key = %k, error = %e, "ignoring unknown Azure secret option")
                        }
                    }
                }
            }
            Arc::new(
                builder.build().map_err(|e| {
                    exec_datafusion_err!("failed to build Azure store for {url}: {e}")
                })?,
            )
        }
        "http" | "https" => Arc::new(
            HttpBuilder::new()
                .with_url(url.as_str())
                .build()
                .map_err(|e| exec_datafusion_err!("failed to build HTTP store for {url}: {e}"))?,
        ),
        "file" => Arc::new(LocalFileSystem::new()),
        other => {
            return Err(exec_datafusion_err!("unsupported object store scheme `{other}`"));
        }
    };
    Ok(store)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use futures::StreamExt;
    use object_store::memory::InMemory;
    use object_store::ObjectStoreExt;

    use super::*;
    use crate::listing_url_resolver::parse_listing_table_url;
    use crate::secrets::SecretType;

    #[test]
    fn store_key_strips_path() {
        let key = store_key_url(&Url::parse("s3://bucket/a/b.parquet").unwrap());
        assert_eq!(key.as_str(), "s3://bucket");
    }

    #[test]
    fn lazy_builds_and_caches_s3_on_miss() {
        let registry = LazyObjectStoreRegistry::new(Arc::new(SecretStore::new()));
        let url = Url::parse("s3://bucket/a/x.parquet").unwrap();

        // First lookup materializes the store...
        let first = registry.get_store(&url).unwrap();
        // ...and a second returns the cached instance (same Arc).
        let second = registry.get_store(&url).unwrap();
        assert!(Arc::ptr_eq(&first, &second), "store was not cached");
    }

    #[test]
    fn lazy_build_applies_secret_and_ignores_unknown_keys() {
        let secrets = Arc::new(SecretStore::new());
        let mut options = HashMap::new();
        options.insert("region".to_string(), "eu-west-1".to_string());
        options.insert("access_key_id".to_string(), "AKIA".to_string());
        options.insert("secret_access_key".to_string(), "shhh".to_string());
        // An unknown option must be ignored, not fail the build.
        options.insert("not_a_real_key".to_string(), "whatever".to_string());
        secrets.add(Secret {
            name: "s3".into(),
            secret_type: SecretType::S3,
            scope: "s3://bucket".into(),
            options,
        });

        let registry = LazyObjectStoreRegistry::new(secrets);
        // Build succeeds (credentials are resolved lazily at request time).
        assert!(registry
            .get_store(&Url::parse("s3://bucket/a/x.parquet").unwrap())
            .is_ok());
    }

    #[test]
    fn file_scheme_hits_inner_default_store() {
        let registry = LazyObjectStoreRegistry::new(Arc::new(SecretStore::new()));
        assert!(registry
            .get_store(&Url::parse("file:///tmp/x").unwrap())
            .is_ok());
    }

    #[test]
    fn register_store_passthrough() {
        let registry = LazyObjectStoreRegistry::new(Arc::new(SecretStore::new()));
        let url = Url::parse("datasets://").unwrap();
        registry.register_store(&url, Arc::new(InMemory::new()));
        assert!(registry.get_store(&url).is_ok());
    }

    #[test]
    fn unsupported_scheme_errors() {
        let registry = LazyObjectStoreRegistry::new(Arc::new(SecretStore::new()));
        assert!(registry
            .get_store(&Url::parse("ftp://host/x").unwrap())
            .is_err());
    }

    // The resolver and the lazy registry compose: resolving a schemed path
    // through `parse_listing_table_url` materializes the store in the registry.
    #[test]
    fn resolver_materializes_store_via_lazy_registry() {
        let registry = LazyObjectStoreRegistry::new(Arc::new(SecretStore::new()));
        // No default store URL + a schemed path → the resolver drives the
        // registry to build the s3://bucket store on demand.
        let table_url =
            parse_listing_table_url(None, "s3://bucket/data/*.parquet", &registry).unwrap();
        assert!(
            table_url.as_str().starts_with("s3://bucket"),
            "url={}",
            table_url.as_str()
        );
        // It is now cached in the registry (fast-path hit, no rebuild).
        assert!(registry
            .get_store(&Url::parse("s3://bucket").unwrap())
            .is_ok());
    }

    // End-to-end over a real local store: resolve a local glob, then use the
    // store the URL points at to actually enumerate and read the files. This
    // proves the resolved `ListingTableUrl` and the registered store agree.
    #[tokio::test]
    async fn end_to_end_local_listing_and_read() {
        let dir = tempfile::tempdir().unwrap();
        // Canonicalize so the path matches what LocalFileSystem reports (macOS
        // routes tmp through the /private symlink).
        let root = std::fs::canonicalize(dir.path()).unwrap();
        std::fs::write(root.join("a.txt"), b"hello").unwrap();
        std::fs::write(root.join("b.txt"), b"world").unwrap();
        std::fs::write(root.join("skip.bin"), b"nope").unwrap();

        let registry = LazyObjectStoreRegistry::new(Arc::new(SecretStore::new()));
        let pattern = format!("{}/*.txt", root.display());
        let table_url = parse_listing_table_url(None, &pattern, &registry).unwrap();

        // file:// scheme + the glob is preserved on the resolved URL.
        assert!(table_url.as_str().starts_with("file://"), "url={}", table_url.as_str());
        assert_eq!(
            table_url.get_glob().as_ref().map(glob::Pattern::as_str),
            Some("*.txt")
        );

        // Fetch the backing store the URL resolves against and list the prefix.
        let store = registry
            .get_store(&Url::parse(table_url.object_store().as_str()).unwrap())
            .unwrap();
        let prefix = table_url.prefix().clone();
        let metas: Vec<_> = store
            .list(Some(&prefix))
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .map(Result::unwrap)
            .collect();

        let txt: Vec<_> = metas
            .iter()
            .filter(|m| m.location.as_ref().ends_with(".txt"))
            .collect();
        assert_eq!(txt.len(), 2, "expected 2 .txt files, got {metas:?}");

        // And the bytes are actually readable through that store.
        let a = txt
            .iter()
            .find(|m| m.location.as_ref().ends_with("a.txt"))
            .expect("a.txt listed");
        let bytes = store.get(&a.location).await.unwrap().bytes().await.unwrap();
        assert_eq!(bytes.as_ref(), b"hello");
    }
}
