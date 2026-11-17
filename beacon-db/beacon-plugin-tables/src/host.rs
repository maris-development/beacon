//! Beacon as the host of plugin table code: its object stores and its `LOCATION` rule.

use std::sync::Arc;

use beacon_plugin_sdk::host::ffi::FFI_Host;
use beacon_plugin_sdk::host::{HostApi, ResolvedLocation};
use beacon_plugin_sdk::object_store::store::ObjectStore;
use datafusion::error::Result;
use datafusion::execution::object_store::{ObjectStoreRegistry, ObjectStoreUrl};
use tokio::runtime::Handle;
use url::Url;

/// The settings of the plugin host. Register one as a session config extension.
#[derive(Debug, Clone)]
pub struct PluginHostInfo {
    /// The runtime that runs the store I/O of a plugin: the query runtime.
    pub runtime: Handle,
    /// The URL of the datasets store. `None` when Beacon has no datasets store.
    pub datasets_url: Option<ObjectStoreUrl>,
    /// The location of the datasets store, for a reader that opens a path or URL itself.
    pub datasets_base_url: Option<Url>,
}

impl PluginHostInfo {
    /// The host of one plugin as FFI data, with the stores of `registry`.
    pub fn ffi_host(&self, registry: Arc<dyn ObjectStoreRegistry>) -> FFI_Host {
        let host = BeaconHost {
            stores: registry,
            datasets_url: self.datasets_url.clone(),
            datasets_base_url: self.datasets_base_url.clone(),
        };
        FFI_Host::new(Arc::new(host), self.runtime.clone())
    }
}

/// Beacon as a [`HostApi`].
///
/// The `LOCATION` rule:
/// - A relative location is a path in the datasets store.
/// - `datasets://a/b` and `datasets:///a/b` are the path `a/b` in the datasets store.
/// - A location with another scheme gives the store `scheme://authority` and its path.
///   The path keeps a query string and percent codes as text.
/// - An absolute local path stays, with no store.
/// - Without a datasets store, a relative location stays, with no store.
#[derive(Debug, Clone)]
pub struct BeaconHost {
    /// The object stores of the session.
    pub stores: Arc<dyn ObjectStoreRegistry>,
    /// The URL of the datasets store. `None` when Beacon has no datasets store.
    pub datasets_url: Option<ObjectStoreUrl>,
    /// The location of the datasets store.
    pub datasets_base_url: Option<Url>,
}

/// True for `/x`, `\x`, `C:\x` and `C:/x`.
fn is_absolute_path(location: &str) -> bool {
    let bytes = location.as_bytes();
    location.starts_with(['/', '\\'])
        || (bytes.len() >= 3
            && bytes[0].is_ascii_alphabetic()
            && bytes[1] == b':'
            && matches!(bytes[2], b'/' | b'\\'))
}

/// Splits `scheme://rest`. A text with no valid scheme gives `None`.
fn split_scheme(location: &str) -> Option<(&str, &str)> {
    let (scheme, rest) = location.split_once("://")?;
    // One letter is a Windows drive.
    let valid = scheme.len() > 1
        && scheme.starts_with(|c: char| c.is_ascii_alphabetic())
        && scheme
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '-' | '.'));
    valid.then_some((scheme, rest))
}

impl HostApi for BeaconHost {
    fn object_store(&self, url: &Url) -> Result<Arc<dyn ObjectStore>> {
        self.stores.get_store(url)
    }

    fn datasets_url(&self) -> Option<ObjectStoreUrl> {
        self.datasets_url.clone()
    }

    fn datasets_base_url(&self) -> Option<Url> {
        self.datasets_base_url.clone()
    }

    fn resolve(&self, location: &str) -> Result<ResolvedLocation> {
        let datasets = self.datasets_url.clone();
        if let Some((scheme, rest)) = split_scheme(location) {
            let (authority, path) = rest.split_once('/').unwrap_or((rest, ""));
            let datasets_authority = datasets
                .as_ref()
                .and_then(|url| split_scheme(url.as_str()))
                .filter(|(found, _)| found.eq_ignore_ascii_case(scheme))
                .map(|(_, rest)| rest.trim_end_matches('/'));
            let in_datasets = match datasets_authority {
                // The registry keys a store by scheme and authority, so the first segment is path.
                Some("") => Some(rest.trim_start_matches('/')),
                Some(found) if found.eq_ignore_ascii_case(authority) => Some(path),
                _ => None,
            };
            if let Some(path) = in_datasets {
                return Ok(ResolvedLocation {
                    store_url: datasets,
                    path: path.to_string(),
                });
            }
            return Ok(ResolvedLocation {
                store_url: Some(ObjectStoreUrl::parse(format!("{scheme}://{authority}"))?),
                path: path.to_string(),
            });
        }
        let store_url = match is_absolute_path(location) {
            true => None,
            false => datasets,
        };
        Ok(ResolvedLocation {
            store_url,
            path: location.to_string(),
        })
    }
}

#[cfg(test)]
mod tests {
    use beacon_plugin_sdk::object_store::store::memory::InMemory;
    use datafusion::execution::object_store::DefaultObjectStoreRegistry;

    use super::*;

    #[test]
    fn the_host_gives_the_stores_and_urls_of_the_session() {
        let stores = Arc::new(DefaultObjectStoreRegistry::new());
        let datasets = ObjectStoreUrl::parse("datasets://").unwrap();
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        stores.register_store(datasets.as_ref(), Arc::clone(&store));
        let base = Url::parse("s3://bucket/").unwrap();
        let host = BeaconHost {
            stores,
            datasets_url: Some(datasets.clone()),
            datasets_base_url: Some(base.clone()),
        };
        let found = host.object_store(datasets.as_ref()).unwrap();
        assert!(Arc::ptr_eq(&store, &found));
        assert!(host.object_store(&Url::parse("nope://x").unwrap()).is_err());
        assert_eq!(host.datasets_url(), Some(datasets));
        assert_eq!(host.datasets_base_url(), Some(base));
    }

    fn resolved(datasets: Option<&str>, location: &str) -> (Option<String>, String) {
        let host = BeaconHost {
            stores: Arc::new(DefaultObjectStoreRegistry::new()),
            datasets_url: datasets.map(|url| ObjectStoreUrl::parse(url).unwrap()),
            datasets_base_url: None,
        };
        let found = host.resolve(location).unwrap();
        (
            found.store_url.map(|url| url.as_str().to_string()),
            found.path,
        )
    }

    #[test]
    fn locations_resolve_against_the_datasets_store() {
        let datasets = Some("datasets:///".to_string());
        for (location, store, path) in [
            (
                "example-shards/part_*.lance",
                datasets.clone(),
                "example-shards/part_*.lance",
            ),
            (
                "datasets://example-shards/part_*.lance",
                datasets.clone(),
                "example-shards/part_*.lance",
            ),
            ("datasets:///a/b", datasets.clone(), "a/b"),
            ("DATASETS://obs/a", datasets.clone(), "obs/a"),
            (
                "s3://bucket/a/b.parquet",
                Some("s3://bucket/".to_string()),
                "a/b.parquet",
            ),
            ("s3://bucket", Some("s3://bucket/".to_string()), ""),
            (
                "https://example.com/api",
                Some("https://example.com/".to_string()),
                "api",
            ),
            ("/beacon/data/x.lance", None, "/beacon/data/x.lance"),
            (r"C:\data\x.lance", None, r"C:\data\x.lance"),
            ("C:/data/x.lance", None, "C:/data/x.lance"),
            ("C://data/x", None, "C://data/x"),
            (
                "file:///beacon/data/x",
                Some("file:///".to_string()),
                "beacon/data/x",
            ),
        ] {
            assert_eq!(
                resolved(Some("datasets://"), location),
                (store, path.to_string()),
                "{location}"
            );
        }
    }

    #[test]
    fn a_datasets_store_with_an_authority_matches_only_that_authority() {
        let datasets = Some("s3://bucket/".to_string());
        assert_eq!(
            resolved(Some("s3://bucket"), "obs/a"),
            (datasets.clone(), "obs/a".to_string())
        );
        assert_eq!(
            resolved(Some("s3://bucket"), "s3://bucket/obs/a"),
            (datasets, "obs/a".to_string())
        );
        assert_eq!(
            resolved(Some("s3://bucket"), "s3://other/obs/a"),
            (Some("s3://other/".to_string()), "obs/a".to_string())
        );
    }

    #[test]
    fn a_relative_location_stays_without_a_datasets_store() {
        assert_eq!(resolved(None, "a/b"), (None, "a/b".to_string()));
        assert_eq!(
            resolved(None, "s3://bucket/a"),
            (Some("s3://bucket/".to_string()), "a".to_string())
        );
    }
}
