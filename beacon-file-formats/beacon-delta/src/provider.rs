//! Building a DataFusion [`TableProvider`] for a Delta table backed by Beacon's
//! object store.
//!
//! ## Why a custom log-store scheme
//!
//! delta-rs resolves a table's data files at scan time through the DataFusion
//! `RuntimeEnv` object-store registry, which keys stores by **scheme + authority
//! only** (the path is ignored) and registers idempotently. If every Beacon
//! Delta table used `memory://`/`file://`, the first table scanned would win the
//! `memory://` slot and every other table would read against the wrong store.
//!
//! To give each table a unique registry key we mint a synthetic
//! `beacon-delta://<hash>/` URL per table location and register a matching
//! [`LogStoreFactory`] for the `beacon-delta` scheme. The object store we hand to
//! delta-rs is Beacon's datasets store (resolved from the session's object-store
//! registry) wrapped in a [`PrefixStore`] scoped to the table directory, so
//! local-FS and S3 both work transparently and commit safety derives from the
//! underlying store (conditional-put), not the scheme.

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Once};

use anyhow::Context;
use beacon_datafusion_ext::listing_factory::ListingFactory;
use datafusion::catalog::{Session, TableProvider};
use datafusion::execution::context::SessionState;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::prelude::SessionContext;
use deltalake::logstore::{
    default_logstore, logstore_factories, LogStore, LogStoreFactory, LogStoreRef, ObjectStoreRef,
    StorageConfig,
};
use deltalake::{DeltaResult, DeltaTableBuilder};
use futures::stream::{BoxStream, StreamExt};
use object_store::path::Path as ObjectPath;
use object_store::prefix::PrefixStore;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use url::Url;

/// URL scheme used for Beacon-backed Delta tables. Registered with delta-rs so
/// `DeltaTableBuilder` can resolve a log store for it; the authority is a
/// per-table hash so each table gets a distinct object-store registry key.
const BEACON_DELTA_SCHEME: &str = "beacon-delta";

/// A point-in-time selector for Delta's time travel.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TimeTravel {
    /// A specific table version (snapshot number).
    Version(i64),
    /// An ISO-8601 / RFC-3339 timestamp; the latest version at or before it.
    Timestamp(String),
}

impl TimeTravel {
    /// Parse a `version` / `timestamp` selector out of table OPTIONS.
    ///
    /// Accepts both the raw keys and the `format.`-prefixed forms DataFusion
    /// produces for `OPTIONS` without a dot. `version` wins if both are present.
    pub fn from_options(options: &HashMap<String, String>) -> anyhow::Result<Option<Self>> {
        let get = |key: &str| {
            options
                .get(key)
                .or_else(|| options.get(&format!("format.{key}")))
        };

        if let Some(version) = get("version") {
            let version: i64 = version
                .parse()
                .with_context(|| format!("invalid Delta `version` option: {version:?}"))?;
            return Ok(Some(TimeTravel::Version(version)));
        }
        if let Some(timestamp) = get("timestamp") {
            return Ok(Some(TimeTravel::Timestamp(timestamp.clone())));
        }
        Ok(None)
    }
}

/// A [`LogStoreFactory`] for the `beacon-delta` scheme that just builds the
/// default log store over whatever (already prefixed) store delta-rs decorates.
#[derive(Debug, Default)]
struct BeaconDeltaLogStoreFactory;

impl LogStoreFactory for BeaconDeltaLogStoreFactory {
    fn with_options(
        &self,
        prefixed_store: ObjectStoreRef,
        root_store: ObjectStoreRef,
        location: &Url,
        options: &StorageConfig,
    ) -> DeltaResult<Arc<dyn LogStore>> {
        Ok(default_logstore(
            prefixed_store,
            root_store,
            location,
            options,
        ))
    }
}

/// Sort a collected listing by object path, keeping errors at the front.
fn sort_listing(
    mut items: Vec<object_store::Result<ObjectMeta>>,
) -> Vec<object_store::Result<ObjectMeta>> {
    items.sort_by(|a, b| match (a, b) {
        (Ok(x), Ok(y)) => x.location.cmp(&y.location),
        (Err(_), Ok(_)) => std::cmp::Ordering::Less,
        (Ok(_), Err(_)) => std::cmp::Ordering::Greater,
        (Err(_), Err(_)) => std::cmp::Ordering::Equal,
    });
    items
}

/// An [`ObjectStore`] decorator that returns `list`/`list_with_offset` results in
/// lexicographic path order.
///
/// delta-rs's log replay assumes listings come back sorted (as S3 returns them),
/// but `LocalFileSystem` lists in arbitrary `readdir` order, which makes the
/// kernel report phantom "gaps" between commit files. Sorting here keeps Delta
/// correct on local-FS-backed datasets stores. Listings are buffered, which is
/// fine for the small `_delta_log` directory delta-rs lists.
#[derive(Debug)]
struct SortedListStore {
    inner: Arc<dyn ObjectStore>,
}

impl std::fmt::Display for SortedListStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SortedListStore({})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for SortedListStore {
    async fn put_opts(
        &self,
        location: &ObjectPath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjectPath,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &ObjectPath,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<ObjectPath>>,
    ) -> BoxStream<'static, object_store::Result<ObjectPath>> {
        self.inner.delete_stream(locations)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> object_store::Result<ListResult> {
        let mut result = self.inner.list_with_delimiter(prefix).await?;
        result.objects.sort_by(|a, b| a.location.cmp(&b.location));
        result.common_prefixes.sort();
        Ok(result)
    }

    async fn copy_opts(
        &self,
        from: &ObjectPath,
        to: &ObjectPath,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }

    fn list(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let inner = self.inner.clone();
        let prefix = prefix.cloned();
        futures::stream::once(async move {
            let items = inner.list(prefix.as_ref()).collect::<Vec<_>>().await;
            futures::stream::iter(sort_listing(items))
        })
        .flatten()
        .boxed()
    }

    fn list_with_offset(
        &self,
        prefix: Option<&ObjectPath>,
        offset: &ObjectPath,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let inner = self.inner.clone();
        let prefix = prefix.cloned();
        let offset = offset.clone();
        futures::stream::once(async move {
            let items = inner
                .list_with_offset(prefix.as_ref(), &offset)
                .collect::<Vec<_>>()
                .await;
            futures::stream::iter(sort_listing(items))
        })
        .flatten()
        .boxed()
    }
}

/// Register the `beacon-delta` log-store factory exactly once per process.
fn ensure_logstore_factory_registered() {
    static REGISTER: Once = Once::new();
    REGISTER.call_once(|| {
        let scheme = Url::parse(&format!("{BEACON_DELTA_SCHEME}://"))
            .expect("beacon-delta scheme should parse");
        logstore_factories().insert(scheme, Arc::new(BeaconDeltaLogStoreFactory));
    });
}

/// Strip an optional `scheme://` and leading slashes from a Beacon location,
/// yielding the object path of the Delta table relative to the datasets store
/// root (e.g. `datasets://argo/tbl` -> `argo/tbl`).
fn location_to_prefix(location: &str) -> anyhow::Result<String> {
    let without_scheme = match location.split_once("://") {
        Some((_scheme, rest)) => rest,
        None => location,
    };
    let trimmed = without_scheme.trim_matches('/');
    anyhow::ensure!(
        !trimmed.is_empty(),
        "Delta table location must not be empty"
    );
    Ok(trimmed.to_string())
}

/// Mint the per-table synthetic URL whose authority uniquely identifies the
/// table prefix, so its object store gets a distinct registry key.
fn table_url(prefix: &str) -> anyhow::Result<Url> {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    prefix.hash(&mut hasher);
    let host = format!("t{:016x}", hasher.finish());
    Url::parse(&format!("{BEACON_DELTA_SCHEME}://{host}/"))
        .with_context(|| format!("failed to build Delta table URL for {prefix:?}"))
}

/// Build (but do not yet expose) a loaded [`DeltaTable`] for `location`, backed
/// by `datasets_store` scoped to the table's prefix.
///
/// The returned table's `log_store` already carries the prefixed store, so the
/// caller registers it with the session and turns it into a provider.
async fn load_delta_table(
    store: Arc<dyn ObjectStore>,
    location: &str,
    time_travel: Option<TimeTravel>,
) -> anyhow::Result<deltalake::DeltaTable> {
    ensure_logstore_factory_registered();

    let prefix = location_to_prefix(location)?;
    let url = table_url(&prefix)?;

    // Scope Beacon's datasets store to the table directory. The synthetic URL has
    // an empty path, so delta-rs adds no further prefix and resolves data files
    // relative to this store's root (the table directory).
    let prefixed: Arc<dyn ObjectStore> =
        Arc::new(PrefixStore::new(store, ObjectPath::from(prefix.as_str())));
    // Ensure log listings come back sorted so delta-rs's log replay is correct on
    // local-FS-backed stores (S3 already returns sorted listings).
    let sorted: Arc<dyn ObjectStore> = Arc::new(SortedListStore { inner: prefixed });

    let mut builder = DeltaTableBuilder::from_url(url.clone())
        .context("invalid Delta table URL")?
        .with_storage_backend(sorted, url);

    match time_travel {
        Some(TimeTravel::Version(version)) => {
            let version: u64 = version
                .try_into()
                .with_context(|| format!("Delta `version` must be non-negative, got {version}"))?;
            builder = builder.with_version(version);
        }
        Some(TimeTravel::Timestamp(ts)) => {
            builder = builder
                .with_datestring(&ts)
                .with_context(|| format!("invalid Delta `timestamp` option: {ts:?}"))?;
        }
        None => {}
    }

    builder
        .load()
        .await
        .with_context(|| format!("failed to open Delta table at {location:?}"))
}

/// Open a Delta table at `location` and return a DataFusion [`TableProvider`].
///
/// The provider supports reads, time travel (via `time_travel`), and
/// `INSERT INTO` (append/overwrite) natively through delta-rs. `location` is
/// resolved against Beacon's datasets store, so both local-FS and S3 backends
/// work without extra configuration.
pub async fn open_delta_provider(
    ctx: Arc<SessionContext>,
    store: Arc<dyn ObjectStore>,
    location: &str,
    time_travel: Option<TimeTravel>,
) -> anyhow::Result<Arc<dyn TableProvider>> {
    reopen_delta_provider(&ctx.state(), store, location, time_travel).await
}

/// Re-open a Delta table from an active query [`Session`] and return a fresh
/// [`TableProvider`].
///
/// Used on every scan/insert so reads observe the latest committed version (a
/// Delta provider is pinned to the snapshot it was built from, so the registered
/// one would otherwise go stale after an `INSERT`). With `time_travel` set, the
/// pinned version/timestamp is re-resolved instead.
pub async fn reopen_delta_provider(
    session: &dyn Session,
    store: Arc<dyn ObjectStore>,
    location: &str,
    time_travel: Option<TimeTravel>,
) -> anyhow::Result<Arc<dyn TableProvider>> {
    let session_state = session
        .as_any()
        .downcast_ref::<SessionState>()
        .context("Delta table requires a DataFusion SessionState")?;

    let table = load_delta_table(store, location, time_travel).await?;

    // Register the table's (prefixed) object store under its unique synthetic URL
    // so the scan plan can resolve data-file URLs at execution time. Keyed by
    // scheme+authority, which is unique per table, so tables never clash.
    let log_store: LogStoreRef = table.log_store();
    session_state
        .runtime_env()
        .register_object_store(log_store.root_url(), log_store.object_store(None));

    let provider = table
        .table_provider()
        .with_session(Arc::new(session_state.clone()))
        .await
        .with_context(|| format!("failed to build Delta provider for {location:?}"))?;

    Ok(provider)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn location_to_prefix_strips_scheme_and_slashes() {
        assert_eq!(
            location_to_prefix("datasets://argo/tbl").unwrap(),
            "argo/tbl"
        );
        assert_eq!(location_to_prefix("/argo/tbl/").unwrap(), "argo/tbl");
        assert_eq!(location_to_prefix("argo/tbl").unwrap(), "argo/tbl");
        assert!(location_to_prefix("datasets://").is_err());
    }

    #[test]
    fn distinct_prefixes_get_distinct_authorities() {
        let a = table_url("argo/a").unwrap();
        let b = table_url("argo/b").unwrap();
        assert_ne!(a.authority(), b.authority());
        // Same prefix is stable (idempotent registration relies on this).
        assert_eq!(table_url("argo/a").unwrap(), a);
    }

    #[test]
    fn time_travel_parses_version_and_timestamp() {
        let mut opts = HashMap::new();
        opts.insert("version".to_string(), "3".to_string());
        assert_eq!(
            TimeTravel::from_options(&opts).unwrap(),
            Some(TimeTravel::Version(3))
        );

        let mut opts = HashMap::new();
        opts.insert(
            "format.timestamp".to_string(),
            "2026-01-01T00:00:00Z".to_string(),
        );
        assert_eq!(
            TimeTravel::from_options(&opts).unwrap(),
            Some(TimeTravel::Timestamp("2026-01-01T00:00:00Z".to_string()))
        );

        assert_eq!(TimeTravel::from_options(&HashMap::new()).unwrap(), None);
    }
}
