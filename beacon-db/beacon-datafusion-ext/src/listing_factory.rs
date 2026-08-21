use std::{path::PathBuf, sync::Arc};

use futures::stream::StreamExt;
use object_store::ObjectMeta;

use datafusion::{
    catalog::Session, datasource::listing::ListingTableUrl, execution::object_store::ObjectStoreUrl,
};
use url::Url;

use crate::format_ext::{DatasetMetadata, FileFormatFactoryExt};
use crate::listing_url_resolver::scheme_of;
use crate::object_store_registry::store_key_url;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RootStore {
    FileSystem(PathBuf), // File System full path, e.g. /path/to/root
    HttpsStore(String), // Object Store full path, e.g. https://s3.amazonaws.com/bucket-name/path/to/root
}

impl RootStore {
    /// Translate an object path (relative to the store this root describes) into the
    /// path / URL a native reader (netCDF-c) opens directly.
    ///
    /// - [`RootStore::FileSystem`] → the object path joined onto the local root.
    ///   (For a dynamic local store the root is the filesystem root `/`, so this
    ///   restores the absolute path; `PathBuf::join` leaves an already-absolute
    ///   Windows drive path unchanged.)
    /// - [`RootStore::HttpsStore`] → the object path appended to the base URL, with
    ///   the `#mode=bytes` suffix netCDF-c needs to range-read over HTTP.
    pub fn to_native_path(&self, object_path: &object_store::path::Path) -> String {
        match self {
            RootStore::FileSystem(root) => root
                .join(object_path.to_string())
                .to_string_lossy()
                .into_owned(),
            RootStore::HttpsStore(base) => {
                format!("{}/{}#mode=bytes", base.trim_end_matches('/'), object_path)
            }
        }
    }
}

/// A configured default store: the DataFusion object-store URL a bare (schemeless)
/// path resolves against, paired with the physical [`RootStore`] the same store
/// maps to for readers (netCDF-c) that open by path/URL instead of going through
/// the object store.
///
/// The two are always defined together — that is the whole point of this type:
/// `ListingFactory` holds an `Option<DefaultStore>`, so "a default store URL
/// without a root store" (or vice versa) is unrepresentable.
#[derive(Debug, Clone)]
pub struct DefaultStore {
    pub url: ObjectStoreUrl,
    pub root: RootStore,
}

impl DefaultStore {
    pub fn new(url: ObjectStoreUrl, root: RootStore) -> Self {
        Self { url, root }
    }
}

pub fn try_listing_factory_from_session(session: &dyn Session) -> Option<Arc<ListingFactory>> {
    session.config().get_extension::<ListingFactory>().clone()
}

/// Resolves user-supplied dataset paths against the configured store.
///
/// A factory is in one of two modes, enforced by construction:
/// - **Configured** (`Some(DefaultStore)`): every path is *relative to the store*
///   — an object-store prefix or a local filesystem directory. The in-path scheme
///   (if any) is irrelevant; the path is always joined onto the root.
/// - **Dynamic** (`None`): paths are resolved on the fly — a schemeless path is a
///   local filesystem path (relative paths are made absolute against the cwd), and
///   a schemed path (`s3://…`, `https://…`) is used with its scheme as given.
#[derive(Debug, Clone)]
pub struct ListingFactory {
    default_store: Option<DefaultStore>,
}

impl ListingFactory {
    pub fn new(default_store: Option<DefaultStore>) -> Self {
        Self { default_store }
    }

    /// A configured factory: bare paths resolve against `url` / `root`.
    pub fn configured(url: ObjectStoreUrl, root: RootStore) -> Self {
        Self {
            default_store: Some(DefaultStore::new(url, root)),
        }
    }

    /// A dynamic factory: paths are resolved by their own scheme, defaulting to
    /// the local filesystem when no scheme is present.
    pub fn dynamic() -> Self {
        Self {
            default_store: None,
        }
    }

    /// The configured default store URL, or `None` in dynamic mode.
    pub fn default_store_url(&self) -> Option<&ObjectStoreUrl> {
        self.default_store.as_ref().map(|d| &d.url)
    }

    pub fn parse_listing_table_url(
        &self,
        session: &dyn Session,
        glob_path: &str,
    ) -> datafusion::error::Result<ListingTableUrl> {
        crate::listing_url_resolver::parse_listing_table_url(
            self.default_store_url().cloned(),
            glob_path,
            session.runtime_env().object_store_registry.as_ref(),
        )
    }
    pub fn native_read_root(&self, url: &ListingTableUrl) -> datafusion::error::Result<RootStore> {
        if let Some(default) = &self.default_store {
            return Ok(default.root.clone());
        }
        match url.scheme() {
            // Local files: object paths are absolute w.r.t. the filesystem root.
            "file" => Ok(RootStore::FileSystem(PathBuf::from("/"))),
            // Range-reads over HTTP: the base is the store's `scheme://authority`.
            "http" | "https" => Ok(RootStore::HttpsStore(
                url.object_store().as_str().trim_end_matches('/').to_string(),
            )),
            other => Err(datafusion::error::DataFusionError::Execution(format!(
                "cannot read this format over `{other}://`: only local files and \
                 http/https are read natively by path. Configure a datasets store to \
                 read remote data; object stores like s3/gs/az are not natively readable."
            ))),
        }
    }

    pub fn parse_listing_table_url_with_store(
        &self,
        session: &dyn Session,
        store_url: &ObjectStoreUrl,
        glob_path: &str,
    ) -> datafusion::error::Result<ListingTableUrl> {
        crate::listing_url_resolver::parse_listing_table_url(
            Some(store_url.clone()),
            glob_path,
            session.runtime_env().object_store_registry.as_ref(),
        )
    }

    pub fn parse_to_store(&self, _session: &dyn Session, path: &str) -> Option<ObjectStoreUrl> {
        match &self.default_store {
            // Configured: everything lives in the default store.
            Some(default) => Some(default.url.clone()),
            // Dynamic: derive the store from the path's own scheme, or the local
            // filesystem store when the path carries no scheme.
            None => match scheme_of(path) {
                Some(_) => {
                    let url = Url::parse(path).ok()?;
                    ObjectStoreUrl::parse(store_key_url(&url).as_str()).ok()
                }
                None => ObjectStoreUrl::parse("file://").ok(),
            },
        }
    }

    pub fn rewrite_path(&self, path: &str) -> String {
        match self.default_store.as_ref().map(|d| &d.root) {
            Some(RootStore::FileSystem(root_path)) => {
                let full_path = root_path.join(path);
                full_path.to_string_lossy().to_string()
            }
            Some(RootStore::HttpsStore(root_url)) => {
                format!("{}/{}", root_url.trim_end_matches('/'), path)
            }
            None => path.to_string(),
        }
    }

    /// Discover the datasets matching `glob_path` under the resolved store,
    /// asking each registered file format which of the listed objects it owns.
    ///
    /// The path is resolved through [`Self::parse_listing_table_url`] (so it
    /// honors both configured and dynamic modes), every matching object is
    /// listed once, and each format's [`FileFormatFactoryExt::discover_datasets`]
    /// classifies them. The returned datasets are enriched with size and
    /// last-modified time from the object listing.
    pub async fn list_datasets(
        &self,
        session: &dyn Session,
        file_formats: &[Arc<dyn FileFormatFactoryExt>],
        glob_path: &str,
    ) -> datafusion::error::Result<Vec<DatasetMetadata>> {
        use datafusion::error::DataFusionError;
        use futures::StreamExt;

        let listing_url = self.parse_listing_table_url(session, glob_path)?;
        let store_url = listing_url.object_store();
        let store = session
            .runtime_env()
            .object_store(store_url.clone())
            .map_err(|e| {
                DataFusionError::Execution(format!(
                    "list_datasets: failed to get object store for {store_url}: {e}"
                ))
            })?;

        // Enumerate every object the glob matches once, up front, so each format
        // classifies against the same listing.
        let mut objects = Vec::new();
        let mut entry_stream = listing_url.list_all_files(session, &store, "").await?;
        while let Some(entry) = entry_stream.next().await {
            // Propagate. Discarding the error here turned a transient object-store
            // failure part-way through a walk into a short dataset list that looked
            // complete: a timeout on object 2 000 000 of 2 850 000 reported success
            // and lost the rest without a word.
            let entry = entry.map_err(|e| {
                DataFusionError::Execution(format!(
                    "list_datasets: listing `{glob_path}` failed after {} objects: {e}",
                    objects.len()
                ))
            })?;
            objects.push(entry);
        }

        // Ask each file format which objects it owns and how to interpret them.
        let mut datasets = vec![];
        for file_format in file_formats.iter() {
            datasets.extend(file_format.discover_datasets(&objects)?);
        }

        enrich_with_object_metadata(&mut datasets, &objects);

        Ok(datasets)
    }

    /// Resolve `glob_path` into the listing it names.
    ///
    /// The only half of a listing that needs a session. [`ObjectListing`] holds
    /// what the walk needs and can be read as many times as a caller wants, so a
    /// plan resolves once here and reads at execute time.
    pub fn listing(
        &self,
        session: &dyn Session,
        glob_path: &str,
    ) -> datafusion::error::Result<ObjectListing> {
        use datafusion::error::DataFusionError;

        let url = self.parse_listing_table_url(session, glob_path)?;
        let store_url = url.object_store();
        let store = session
            .runtime_env()
            .object_store(store_url.clone())
            .map_err(|e| {
                DataFusionError::Execution(format!(
                    "listing: failed to get object store for {store_url}: {e}"
                ))
            })?;
        Ok(ObjectListing { store, url })
    }
}

/// A resolved listing: a store, and the URL selecting objects within it.
///
/// Holds no session, so it is `'static` and readable more than once. Path
/// resolution happened when [`ListingFactory::listing`] built it, which is what
/// lets every reader below inherit the same rules — the configured default
/// store, a schemed path, a local directory, and the glob.
#[derive(Debug, Clone)]
pub struct ObjectListing {
    store: Arc<dyn object_store::ObjectStore>,
    url: ListingTableUrl,
}

impl ObjectListing {
    /// The store the objects live in.
    pub fn store(&self) -> &Arc<dyn object_store::ObjectStore> {
        &self.store
    }

    /// The directory this listing addresses, relative to the store root. For a
    /// glob that is the literal head, the part before the first wildcard.
    pub fn prefix(&self) -> &object_store::path::Path {
        self.url.prefix()
    }

    /// Every object the URL matches, as pages arrive.
    ///
    /// Yields each object as its page arrives and holds none of them, where
    /// [`ListingFactory::list_datasets`] drains the whole walk into a `Vec`
    /// first — about a gigabyte for a listing of 2 853 217 objects.
    ///
    /// Objects, not datasets: walking a store and deciding what a file *is* are
    /// different jobs, and only the second needs to know about formats. A caller
    /// that wants datasets classifies the stream itself.
    ///
    /// Stopping the stream stops the walk.
    pub fn stream(&self) -> futures::stream::BoxStream<'static, datafusion::error::Result<ObjectMeta>> {
        use datafusion::error::DataFusionError;
        use object_store::ObjectStoreExt;

        let store = Arc::clone(&self.store);
        let url = self.url.clone();
        let prefix = url.prefix().clone();
        async_stream::try_stream! {
            // A URL with no glob and no trailing slash names one object, not a
            // directory, and a store lists a prefix at segment boundaries — so
            // listing `obs/a.parquet` looks for a directory of that name and
            // finds nothing. Ask for the object itself, and fall back to listing
            // when it turns out to be a directory after all.
            if !url.is_collection() {
                match store.head(&prefix).await {
                    Ok(meta) => {
                        yield meta;
                        return;
                    }
                    Err(object_store::Error::NotFound { .. }) => {}
                    Err(e) => Err(DataFusionError::Execution(format!(
                        "listing `{prefix}` failed: {e}"
                    )))?,
                }
            }
            let mut objects = store.list(Some(&prefix));
            while let Some(object) = objects.next().await {
                let object = object.map_err(|e| {
                    DataFusionError::Execution(format!(
                        "listing `{prefix}` failed part-way: {e}"
                    ))
                })?;
                // The prefix is only the literal head of the glob, so the rest of
                // the pattern is applied here, as a listing table does.
                if url.contains(&object.location, false) {
                    yield object;
                }
            }
        }
        .boxed()
    }

    /// One directory level: the sub-folder names, and the objects directly in it.
    ///
    /// A single delimiter request rather than a walk. The difference is not a
    /// constant factor: against a SeaweedFS bucket of 2 853 217 objects the
    /// recursive walk took 79.9 s and this took 14 ms.
    ///
    /// Folder names are relative to [`Self::prefix`], so a caller descends by
    /// joining rather than by parsing. A directory-shaped dataset (Zarr) keeps
    /// its marker *inside* its directory, so at this level it is a folder;
    /// descending shows the marker. Naming it here would cost one request per
    /// sub-folder, which is the recursive cost this avoids.
    pub async fn level(&self) -> datafusion::error::Result<ObjectLevel> {
        use datafusion::error::DataFusionError;

        let base = self.prefix().clone();
        let listed = self
            .store
            .list_with_delimiter(Some(&base))
            .await
            .map_err(|e| {
                DataFusionError::Execution(format!("listing level `{base}` failed: {e}"))
            })?;

        let base_str = base.as_ref().to_string();
        let mut folders: Vec<String> = listed
            .common_prefixes
            .iter()
            .map(|p| {
                let full = p.as_ref();
                full.strip_prefix(&base_str)
                    .map(|r| r.trim_start_matches('/').to_string())
                    .unwrap_or_else(|| full.to_string())
            })
            .filter(|name| !name.is_empty())
            .collect();
        folders.sort();

        let mut objects = listed.objects;
        objects.sort_by(|a, b| a.location.cmp(&b.location));

        Ok(ObjectLevel {
            prefix: base_str,
            folders,
            objects,
        })
    }
}

/// One directory level of a store.
#[derive(Debug, Clone)]
pub struct ObjectLevel {
    /// The directory this describes, relative to the store root.
    pub prefix: String,
    /// Immediate sub-folder names, relative to `prefix`, sorted.
    pub folders: Vec<String>,
    /// Objects sitting directly in `prefix`, sorted by path.
    pub objects: Vec<ObjectMeta>,
}

/// Fill each dataset's `size` + `last_modified` from the object listing.
///
/// A single-file dataset matches an object exactly; a directory-shaped dataset
/// (e.g. Zarr) aggregates every object under its prefix (sum of sizes, newest
/// mtime). Datasets with no matching object keep `None`.
fn enrich_with_object_metadata(
    datasets: &mut [DatasetMetadata],
    objects: &[object_store::ObjectMeta],
) {
    use std::collections::HashMap;

    let by_path: HashMap<&str, &object_store::ObjectMeta> =
        objects.iter().map(|o| (o.location.as_ref(), o)).collect();
    for ds in datasets.iter_mut() {
        if let Some(obj) = by_path.get(ds.file_path.as_str()) {
            ds.size = Some(obj.size);
            ds.last_modified = Some(obj.last_modified);
        } else {
            let prefix = format!("{}/", ds.file_path);
            let mut total = 0u64;
            let mut latest = None;
            for o in objects {
                if o.location.as_ref().starts_with(&prefix) {
                    total += o.size;
                    latest = Some(match latest {
                        Some(l) if l >= o.last_modified => l,
                        _ => o.last_modified,
                    });
                }
            }
            if latest.is_some() {
                ds.size = Some(total);
                ds.last_modified = latest;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use datafusion::execution::context::SessionContext;
    use object_store::path::Path as ObjectPath;

    use super::*;

    // ---- ObjectListing ----------------------------------------------------

    /// A session over a local directory holding `files`, and the dynamic factory
    /// that resolves paths against it.
    fn local_listing(files: &[(&str, &str)]) -> (tempfile::TempDir, SessionContext, ListingFactory) {
        let dir = tempfile::tempdir().expect("temp dir");
        for (path, body) in files {
            let full = dir.path().join(path);
            std::fs::create_dir_all(full.parent().unwrap()).unwrap();
            std::fs::write(full, body).unwrap();
        }
        (dir, SessionContext::new(), ListingFactory::dynamic())
    }

    /// Paths the listing yields, relative to `root`, sorted.
    async fn streamed(listing: &ObjectListing, root: &std::path::Path) -> Vec<String> {
        use futures::stream::TryStreamExt;
        // Anchor on the temp directory name rather than the whole root:
        // `canonicalize` yields a verbatim prefix on Windows that the object
        // path does not carry.
        let anchor = format!("{}/", root.file_name().unwrap().to_string_lossy());
        let mut paths: Vec<String> = listing
            .stream()
            .map_ok(|meta| {
                let full = meta.location.as_ref().to_string();
                match full.split_once(&anchor) {
                    Some((_, rest)) => rest.to_string(),
                    None => full,
                }
            })
            .try_collect::<Vec<_>>()
            .await
            .expect("the walk succeeds");
        paths.sort();
        paths
    }

    /// A directory streams everything under it.
    #[tokio::test]
    async fn a_directory_streams_its_subtree() {
        let (dir, ctx, factory) =
            local_listing(&[("a.csv", "x"), ("sub/b.csv", "y"), ("sub/deep/c.csv", "z")]);
        let root = std::fs::canonicalize(dir.path()).unwrap();
        let listing = factory
            .listing(&ctx.state(), &format!("{}/", root.display()))
            .expect("the directory resolves");

        assert_eq!(
            streamed(&listing, &root).await,
            vec!["a.csv", "sub/b.csv", "sub/deep/c.csv"]
        );
    }
    /// A glob narrows the stream by extension, and crosses directories while it
    /// does.
    ///
    /// `*` does not stop at a separator here: DataFusion matches listing globs
    /// with the default `MatchOptions`, where `require_literal_separator` is
    /// false. That is why one directory level needs its own function rather than
    /// a cleverer pattern — see `browse_datasets`.
    #[tokio::test]
    async fn a_glob_narrows_the_stream_across_directories() {
        let (dir, ctx, factory) =
            local_listing(&[("a.csv", "x"), ("a.txt", "y"), ("sub/b.csv", "z")]);
        let root = std::fs::canonicalize(dir.path()).unwrap();
        let listing = factory
            .listing(&ctx.state(), &format!("{}/*.csv", root.display()))
            .expect("the glob resolves");

        // The `.txt` is excluded, and the nested `.csv` is not.
        assert_eq!(streamed(&listing, &root).await, vec!["a.csv", "sub/b.csv"]);
    }

    /// A path naming one file yields that file. A store lists a prefix at
    /// segment boundaries, so listing `a.csv` would look for a directory of that
    /// name and find nothing.
    #[tokio::test]
    async fn a_single_file_yields_itself() {
        let (dir, ctx, factory) = local_listing(&[("a.csv", "x"), ("b.csv", "y")]);
        let root = std::fs::canonicalize(dir.path()).unwrap();
        let listing = factory
            .listing(&ctx.state(), &format!("{}/a.csv", root.display()))
            .expect("the file resolves");

        assert_eq!(streamed(&listing, &root).await, vec!["a.csv"]);
    }

    /// A path that matches nothing is empty, not an error.
    #[tokio::test]
    async fn a_missing_path_streams_nothing() {
        let (dir, ctx, factory) = local_listing(&[("a.csv", "x")]);
        let root = std::fs::canonicalize(dir.path()).unwrap();
        let listing = factory
            .listing(&ctx.state(), &format!("{}/nope.csv", root.display()))
            .expect("the path resolves");

        assert!(streamed(&listing, &root).await.is_empty());
    }

    /// One level reports its sub-folders by name and its own objects, and stops.
    #[tokio::test]
    async fn a_level_reads_one_directory() {
        let (dir, ctx, factory) =
            local_listing(&[("a.csv", "x"), ("sub/b.csv", "y"), ("sub/deep/c.csv", "z")]);
        let root = std::fs::canonicalize(dir.path()).unwrap();
        let listing = factory
            .listing(&ctx.state(), &format!("{}/", root.display()))
            .expect("the directory resolves");

        let level = listing.level().await.expect("one level");
        assert_eq!(level.folders, vec!["sub"], "folders are named, not pathed");
        let files: Vec<String> = level
            .objects
            .iter()
            .map(|o| o.location.filename().unwrap_or_default().to_string())
            .collect();
        assert_eq!(files, vec!["a.csv"], "nothing from below this level");
    }

    /// The same listing reads more than once: a plan resolves it during `scan`
    /// and reads it again for every execution.
    #[tokio::test]
    async fn a_listing_reads_more_than_once() {
        let (dir, ctx, factory) = local_listing(&[("a.csv", "x"), ("b.csv", "y")]);
        let root = std::fs::canonicalize(dir.path()).unwrap();
        let listing = factory
            .listing(&ctx.state(), &format!("{}/", root.display()))
            .expect("the directory resolves");

        let first = streamed(&listing, &root).await;
        let second = streamed(&listing, &root).await;
        assert_eq!(first, second);
        assert_eq!(first.len(), 2);
    }

    /// A configured factory whose default store maps to `root`. The store URL is
    /// irrelevant to the path-resolution these tests exercise (that reads only the
    /// root), so any valid URL will do.
    fn configured(root: RootStore) -> ListingFactory {
        ListingFactory::configured(ObjectStoreUrl::parse("datasets://").unwrap(), root)
    }

    // ---- native_read_root -------------------------------------------------

    fn listing_url(s: &str) -> datafusion::datasource::listing::ListingTableUrl {
        datafusion::datasource::listing::ListingTableUrl::parse(s).unwrap()
    }

    #[test]
    fn dynamic_native_read_root_derives_from_the_resolved_scheme() {
        let factory = ListingFactory::dynamic();
        // A local resolved URL → the filesystem root (object paths are absolute).
        assert_eq!(
            factory.native_read_root(&listing_url("file:///data/a.nc")).unwrap(),
            RootStore::FileSystem(PathBuf::from("/"))
        );
        // An http/https URL → the `scheme://authority` base.
        assert_eq!(
            factory.native_read_root(&listing_url("https://host/a.nc")).unwrap(),
            RootStore::HttpsStore("https://host".to_string())
        );
        // Object stores are not natively readable.
        for url in ["s3://bucket/a.nc", "gs://bucket/a.nc"] {
            let err = factory
                .native_read_root(&listing_url(url))
                .unwrap_err()
                .to_string();
            assert!(err.contains("not natively readable"), "url={url}, err={err}");
        }
    }

    #[test]
    fn configured_native_read_root_is_the_configured_root() {
        // A configured root store (FileSystem or HttpsStore) is used regardless of
        // the resolved URL's scheme.
        let factory = configured(RootStore::FileSystem(PathBuf::from("/data/root")));
        assert_eq!(
            factory.native_read_root(&listing_url("datasets:///a.nc")).unwrap(),
            RootStore::FileSystem(PathBuf::from("/data/root"))
        );
    }

    #[test]
    fn root_store_translates_object_paths_to_native_paths() {
        // FileSystem: join onto the root.
        assert_eq!(
            RootStore::FileSystem(PathBuf::from("/data/root"))
                .to_native_path(&ObjectPath::from("argo/a.nc")),
            PathBuf::from("/data/root").join("argo/a.nc").to_string_lossy()
        );
        // HttpsStore: append + byte-range suffix, trimming a trailing slash.
        assert_eq!(
            RootStore::HttpsStore("https://example.org/bucket/".to_string())
                .to_native_path(&ObjectPath::from("argo/a.nc")),
            "https://example.org/bucket/argo/a.nc#mode=bytes"
        );
    }

    // ---- parse_to_store --------------------------------------------------

    #[test]
    fn configured_parse_to_store_always_returns_the_default_store() {
        let ctx = SessionContext::new();
        let state = ctx.state();
        let factory = configured(RootStore::FileSystem(PathBuf::from("/data/root")));
        let datasets = ObjectStoreUrl::parse("datasets://").unwrap();
        // The path is irrelevant when a default store is configured.
        for path in ["argo/a.nc", "s3://bucket/a.nc", "/abs/a.nc"] {
            assert_eq!(
                factory.parse_to_store(&state, path),
                Some(datasets.clone()),
                "path={path}"
            );
        }
    }

    #[test]
    fn dynamic_parse_to_store_derives_the_store_from_the_scheme() {
        let ctx = SessionContext::new();
        let state = ctx.state();
        let factory = ListingFactory::dynamic();
        // A schemed path resolves to its own scheme://authority store key.
        assert_eq!(
            factory.parse_to_store(&state, "s3://bucket/prefix/a.parquet"),
            Some(ObjectStoreUrl::parse("s3://bucket").unwrap())
        );
        // A schemeless path — relative or absolute — is a local filesystem path.
        let file_store = ObjectStoreUrl::parse("file://").unwrap();
        assert_eq!(
            factory.parse_to_store(&state, "argo/a.nc"),
            Some(file_store.clone())
        );
        assert_eq!(
            factory.parse_to_store(&state, "/abs/argo/a.nc"),
            Some(file_store)
        );
    }

    // ---- rewrite_path ----------------------------------------------------

    #[test]
    fn rewrite_path_prefixes_the_configured_root() {
        assert_eq!(
            configured(RootStore::FileSystem(PathBuf::from("/data/root"))).rewrite_path("a/b.nc"),
            PathBuf::from("/data/root").join("a/b.nc").to_string_lossy()
        );
        assert_eq!(
            configured(RootStore::HttpsStore("https://example.org/".to_string()))
                .rewrite_path("a/b.nc"),
            "https://example.org/a/b.nc"
        );
        // Dynamic: the path passes through untouched.
        assert_eq!(ListingFactory::dynamic().rewrite_path("a/b.nc"), "a/b.nc");
    }

    // ---- enrich_with_object_metadata -------------------------------------

    fn meta(location: &str, size: u64, ts: i64) -> object_store::ObjectMeta {
        object_store::ObjectMeta {
            location: ObjectPath::from(location),
            last_modified: chrono::DateTime::from_timestamp(ts, 0).unwrap(),
            size,
            e_tag: None,
            version: None,
        }
    }

    #[test]
    fn enrich_matches_a_single_file_dataset_to_its_object() {
        let objects = vec![meta("argo/a.nc", 100, 10), meta("argo/b.nc", 200, 20)];
        let mut datasets = vec![DatasetMetadata::new("argo/a.nc".into(), "nc".into())];
        enrich_with_object_metadata(&mut datasets, &objects);
        assert_eq!(datasets[0].size, Some(100));
        assert_eq!(datasets[0].last_modified, Some(objects[0].last_modified));
    }

    #[test]
    fn enrich_aggregates_a_directory_dataset_over_its_prefix() {
        // A directory-shaped dataset (e.g. Zarr): its `file_path` is a prefix, not
        // an object, so size sums and last_modified is the newest across the prefix.
        let objects = vec![
            meta("cube.zarr/.zmetadata", 10, 5),
            meta("cube.zarr/temp/0.0", 300, 30),
            meta("cube.zarr/temp/0.1", 400, 25),
            meta("other.nc", 999, 99), // outside the prefix, must be ignored
        ];
        let mut datasets = vec![DatasetMetadata::new("cube.zarr".into(), "zarr".into())];
        enrich_with_object_metadata(&mut datasets, &objects);
        assert_eq!(datasets[0].size, Some(10 + 300 + 400));
        // Newest mtime among the three prefixed objects (ts=30).
        assert_eq!(
            datasets[0].last_modified,
            Some(chrono::DateTime::from_timestamp(30, 0).unwrap())
        );
    }

    #[test]
    fn enrich_leaves_a_dataset_with_no_matching_object_untouched() {
        let objects = vec![meta("argo/a.nc", 100, 10)];
        let mut datasets = vec![DatasetMetadata::new("ghost/missing.nc".into(), "nc".into())];
        enrich_with_object_metadata(&mut datasets, &objects);
        assert_eq!(datasets[0].size, None);
        assert_eq!(datasets[0].last_modified, None);
    }
}

/// The objects under `listing_url`, as the store returns them.
///
/// The streaming counterpart to what [`ListingFactory::list_datasets`] does
/// internally. That one drains the walk into a `Vec` before anything looks at
/// it, so nothing is produced until it finishes and a listing of 2 853 217
/// objects holds about a gigabyte while it works. This yields each object as its
/// page arrives and holds none of them.
///
/// Objects, not datasets: walking a store and deciding what a file *is* are
/// different jobs, and only the second one needs to know about formats. A caller
/// that wants datasets classifies the stream itself.
///
/// Takes a resolved store and URL rather than a session, so the stream is
/// `'static` and can be built as many times as a caller needs. Stopping the
/// stream stops the walk.
pub fn stream_objects(
    store: Arc<dyn object_store::ObjectStore>,
    listing_url: ListingTableUrl,
) -> futures::stream::BoxStream<'static, datafusion::error::Result<ObjectMeta>> {
    use datafusion::error::DataFusionError;

    let prefix = listing_url.prefix().clone();
    async_stream::try_stream! {
        let mut objects = store.list(Some(&prefix));
        while let Some(object) = objects.next().await {
            let object = object.map_err(|e| {
                DataFusionError::Execution(format!(
                    "stream_objects: listing `{prefix}` failed part-way: {e}"
                ))
            })?;
            // The prefix is only the literal head of the glob, so the rest of the
            // pattern is applied here, exactly as a listing table does.
            if listing_url.contains(&object.location, false) {
                yield object;
            }
        }
    }
    .boxed()
}
