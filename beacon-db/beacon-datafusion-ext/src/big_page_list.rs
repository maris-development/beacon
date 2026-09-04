//! A store wrapper that lists a prefix in parallel shards, with sized pages.
//!
//! A recursive listing is a chain of pages, and each page needs the
//! continuation token of the one before it. Nothing overlaps, so the walk costs
//! one round trip per page however fast the server is. On a bucket of 2 853 217
//! objects that is 2854 strictly sequential requests.
//!
//! Two changes, both measured against a SeaweedFS bucket of that size. Every
//! run below returned the full 2 853 217 objects.
//!
//! | Strategy                          | Time       | Requests |
//! |-----------------------------------|------------|----------|
//! | sequential, no `max-keys` (today) | 79.9 s     | 2854     |
//! | sequential, `max-keys=5000`       | 64.3 s     | 571      |
//! | 2-level shards, 16 ways           | 33.8 s     | 571      |
//! | **3-level shards, 16 ways**       | **19.4 s** | 571      |
//! | 3-level shards, 32 ways           | 17.7 s     | 571      |
//!
//! # Page size
//!
//! [`ObjectStore::list`] sends no `max-keys`, so a server applies its own
//! default of 1000. [`DEFAULT_MAX_KEYS`] is 5000, well under the 65535 SeaweedFS
//! will parse.
//!
//! 65535 is not a server limit, and asking for it is not an error. It is a
//! timeout risk. `ClientOptions` gives a request 30 seconds by default, and one
//! page of 65535 keys from a filer whose metadata is not yet in page cache took
//! 6.5 s on its own; with 16 shards in flight, some requests crossed 30 s and
//! failed. Warm, the same walk completed with no errors at all. A moderate page
//! stays inside the budget whatever the cache is doing, and it already removes
//! 80% of the round trips. Raise `AWS_TIMEOUT` before raising this.
//!
//! # Shards
//!
//! [`list`](ObjectStore::list) first walks `list_with_delimiter` down
//! [`DEFAULT_FANOUT_DEPTH`] levels, which costs one request per directory seen
//! and finished in under a second on the bucket above. Each leaf prefix is then
//! walked as its own page chain, [`DEFAULT_CONCURRENCY`] at a time. Objects
//! sitting at the intermediate levels are emitted too, so nothing is missed.
//!
//! Expansion stops at [`MAX_SHARDS`]. A bucket that is wide rather than deep
//! would otherwise spend more requests finding shards than the walk saves, and
//! past a few thousand shards the throughput curve is flat anyway.
//!
//! # Order
//!
//! Shards interleave, so objects do not arrive sorted. `ObjectStore::list`
//! documents that the order of returned `ObjectMeta` is not guaranteed, and the
//! sequential path was only incidentally ordered. A caller that needs order
//! must sort.

use std::fmt;
use std::sync::Arc;

use futures::stream::{BoxStream, StreamExt, TryStreamExt};
use object_store::list::{PaginatedListOptions, PaginatedListStore};
use object_store::path::{Path, DELIMITER};
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as OsResult,
};

/// Keys requested per page. See the module docs for why this is not larger.
pub const DEFAULT_MAX_KEYS: usize = 5000;

/// Directory levels descended to find shards before walking them.
pub const DEFAULT_FANOUT_DEPTH: usize = 3;

/// Shard walks in flight. Throughput was flat from 16 upward.
pub const DEFAULT_CONCURRENCY: usize = 16;

/// Stop expanding once a level holds this many prefixes.
pub const MAX_SHARDS: usize = 4096;

/// A store that lists a prefix as parallel shards of sized pages.
#[derive(Debug, Clone)]
pub struct BigPageList<T> {
    inner: Arc<T>,
    max_keys: usize,
    fanout_depth: usize,
    concurrency: usize,
}

impl<T> BigPageList<T> {
    /// Wrap `inner` with the measured defaults.
    pub fn new(inner: T) -> Self {
        Self {
            inner: Arc::new(inner),
            max_keys: DEFAULT_MAX_KEYS,
            fanout_depth: DEFAULT_FANOUT_DEPTH,
            concurrency: DEFAULT_CONCURRENCY,
        }
    }

    /// Page size per request. Clamped to at least 1.
    pub fn with_max_keys(mut self, max_keys: usize) -> Self {
        self.max_keys = max_keys.max(1);
        self
    }

    /// Directory levels descended before walking. `0` disables sharding, which
    /// leaves a sequential walk with sized pages.
    pub fn with_fanout_depth(mut self, depth: usize) -> Self {
        self.fanout_depth = depth;
        self
    }

    /// Shard walks in flight. Clamped to at least 1.
    pub fn with_concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = concurrency.max(1);
        self
    }
}

impl<T: fmt::Debug> fmt::Display for BigPageList<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "BigPageList(max_keys={}, depth={}, concurrency={}, {:?})",
            self.max_keys, self.fanout_depth, self.concurrency, self.inner
        )
    }
}

/// A directory prefix as the raw string a paginated listing wants.
///
/// [`PaginatedListStore::list_paginated`] states that it adds no trailing
/// delimiter, where [`ObjectStore::list`] does. Without one the prefix matches
/// by byte, not by directory: a shard of `a/one` also returns `a/one.txt` from
/// the level above it and everything under a sibling `a/one2/`. Both are
/// duplicates, because the level above is emitted separately and the sibling is
/// a shard of its own.
///
/// An empty path selects the whole store, and `/` selects nothing, so it maps to
/// `None` as it does in `ObjectStore::list`.
fn shard_prefix(prefix: Option<&Path>) -> Option<String> {
    let prefix = prefix?.as_ref();
    (!prefix.is_empty()).then(|| format!("{prefix}{DELIMITER}"))
}

/// One prefix, walked to exhaustion as a lazy chain of sized pages.
fn walk_shard<T>(
    inner: Arc<T>,
    prefix: Option<Path>,
    max_keys: usize,
) -> BoxStream<'static, OsResult<ObjectMeta>>
where
    T: PaginatedListStore + 'static,
{
    let prefix = shard_prefix(prefix.as_ref());
    // `None` state means the previous page carried no continuation token.
    futures::stream::try_unfold(Some(None::<String>), move |state| {
        let inner = Arc::clone(&inner);
        let prefix = prefix.clone();
        async move {
            let Some(token) = state else {
                return Ok::<_, object_store::Error>(None);
            };
            let opts = PaginatedListOptions {
                max_keys: Some(max_keys),
                page_token: token,
                ..Default::default()
            };
            let page = inner.list_paginated(prefix.as_deref(), opts).await?;
            Ok(Some((
                futures::stream::iter(page.result.objects.into_iter().map(Ok)),
                page.page_token.map(Some),
            )))
        }
    })
    .try_flatten()
    .boxed()
}

/// Descend `depth` directory levels from `root`.
///
/// Returns the leaf prefixes to walk and every object found at the levels
/// above them, which belong to the listing just as much as the leaves do.
async fn discover_shards<T>(
    inner: &Arc<T>,
    root: Option<Path>,
    depth: usize,
    concurrency: usize,
) -> OsResult<(Vec<Option<Path>>, Vec<ObjectMeta>)>
where
    T: ObjectStore + 'static,
{
    let mut level = vec![root];
    let mut above = Vec::new();

    for _ in 0..depth {
        if level.len() >= MAX_SHARDS {
            break;
        }
        // Clone: a level with no children is itself the leaf level, and `level`
        // must still hold it for the walk.
        let expanded = futures::stream::iter(level.clone().into_iter().map(|p| {
            let inner = Arc::clone(inner);
            async move { inner.list_with_delimiter(p.as_ref()).await }
        }))
        .buffer_unordered(concurrency)
        .try_collect::<Vec<ListResult>>()
        .await?;

        let mut kids = Vec::new();
        let mut here = Vec::new();
        for r in expanded {
            kids.extend(r.common_prefixes.into_iter().map(Some));
            here.extend(r.objects);
        }
        // No children means this level is the leaf level, and its own walk will
        // return these objects. Keeping them would emit each one twice.
        if kids.is_empty() {
            break;
        }
        // Descending past this level, so nothing below will cover what sits
        // directly in it.
        above.extend(here);
        level = kids;
    }

    Ok((level, above))
}

#[async_trait::async_trait]
impl<T> ObjectStore for BigPageList<T>
where
    T: ObjectStore + PaginatedListStore + 'static,
{
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> OsResult<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> OsResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> OsResult<GetResult> {
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, OsResult<Path>>,
    ) -> BoxStream<'static, OsResult<Path>> {
        self.inner.delete_stream(locations)
    }

    /// Discover shards, then walk them concurrently.
    ///
    /// Discovery happens inside the stream, so building it costs nothing and a
    /// caller that never polls never talks to the store. A single shard means
    /// the prefix has no sub-directories worth splitting, and the walk is the
    /// plain sequential one.
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, OsResult<ObjectMeta>> {
        let inner = Arc::clone(&self.inner);
        let prefix = prefix.cloned();
        let (max_keys, depth, concurrency) = (self.max_keys, self.fanout_depth, self.concurrency);

        futures::stream::once(async move {
            if depth == 0 {
                return walk_shard(inner, prefix, max_keys);
            }
            let (shards, above) =
                match discover_shards(&inner, prefix.clone(), depth, concurrency).await {
                    Ok(found) => found,
                    // Discovery is an optimization. A store that cannot answer a
                    // delimiter listing still lists correctly the sequential way.
                    Err(_) => return walk_shard(inner, prefix, max_keys),
                };

            if shards.len() <= 1 {
                return walk_shard(inner, prefix, max_keys);
            }

            let walks = futures::stream::iter(
                shards
                    .into_iter()
                    .map(move |p| walk_shard(Arc::clone(&inner), p, max_keys)),
            )
            .flatten_unordered(Some(concurrency));

            futures::stream::iter(above.into_iter().map(Ok))
                .chain(walks)
                .boxed()
        })
        .flatten()
        .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> OsResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> OsResult<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::Mutex;

    use futures::stream::TryStreamExt;
    use object_store::{PutMultipartOptions, PutPayload};

    use super::*;

    /// A store built from a list of paths, with just enough behaviour to drive a
    /// sharded listing: paginated listing that honours `max_keys`, and a
    /// delimiter listing that reports one directory level.
    ///
    /// [`PaginatedListStore::list_paginated`] matches its prefix by byte and
    /// adds no trailing delimiter, so this one does too. A fake that split on
    /// directories instead would accept a prefix S3 rejects, and hide what
    /// [`shard_prefix`] is for.
    ///
    /// It records the page sizes it was asked for, which is how the tests below
    /// tell a sized page from a default one.
    #[derive(Debug)]
    struct FakeStore {
        paths: Vec<Path>,
        page_sizes: Mutex<Vec<Option<usize>>>,
    }

    impl FakeStore {
        fn new(paths: &[&str]) -> Self {
            let mut paths: Vec<Path> = paths.iter().map(|p| Path::from(*p)).collect();
            paths.sort();
            Self {
                paths,
                page_sizes: Mutex::new(Vec::new()),
            }
        }

        fn meta(location: &Path) -> ObjectMeta {
            ObjectMeta {
                location: location.clone(),
                last_modified: Default::default(),
                size: 1,
                e_tag: None,
                version: None,
            }
        }

        fn page_sizes(&self) -> Vec<Option<usize>> {
            self.page_sizes.lock().unwrap().clone()
        }
    }

    impl std::fmt::Display for FakeStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "FakeStore")
        }
    }

    #[async_trait::async_trait]
    impl PaginatedListStore for FakeStore {
        async fn list_paginated(
            &self,
            prefix: Option<&str>,
            opts: PaginatedListOptions,
        ) -> object_store::Result<object_store::list::PaginatedListResult> {
            self.page_sizes.lock().unwrap().push(opts.max_keys);

            let under: Vec<&Path> = self
                .paths
                .iter()
                .filter(|p| match prefix {
                    None => true,
                    Some(prefix) => p.as_ref().starts_with(prefix),
                })
                .collect();

            // The page token is the index of the next path to return.
            let start: usize = opts
                .page_token
                .as_deref()
                .map(|t| t.parse().unwrap())
                .unwrap_or(0);
            let size = opts.max_keys.unwrap_or(1000);
            let end = (start + size).min(under.len());
            let objects: Vec<ObjectMeta> = under[start..end].iter().map(|p| Self::meta(p)).collect();

            Ok(object_store::list::PaginatedListResult {
                result: ListResult {
                    common_prefixes: Vec::new(),
                    objects,
                },
                page_token: (end < under.len()).then(|| end.to_string()),
            })
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for FakeStore {
        async fn put_opts(
            &self,
            _l: &Path,
            _p: PutPayload,
            _o: PutOptions,
        ) -> OsResult<PutResult> {
            unimplemented!("the tests only list")
        }
        async fn put_multipart_opts(
            &self,
            _l: &Path,
            _o: PutMultipartOptions,
        ) -> OsResult<Box<dyn MultipartUpload>> {
            unimplemented!("the tests only list")
        }
        async fn get_opts(&self, _l: &Path, _o: GetOptions) -> OsResult<GetResult> {
            unimplemented!("the tests only list")
        }
        fn delete_stream(
            &self,
            _l: BoxStream<'static, OsResult<Path>>,
        ) -> BoxStream<'static, OsResult<Path>> {
            unimplemented!("the tests only list")
        }
        fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, OsResult<ObjectMeta>> {
            let prefix = prefix.map(|p| p.as_ref().to_string()).unwrap_or_default();
            let objects: Vec<OsResult<ObjectMeta>> = self
                .paths
                .iter()
                .filter(|p| prefix.is_empty() || p.as_ref().starts_with(&prefix))
                .map(|p| Ok(Self::meta(p)))
                .collect();
            futures::stream::iter(objects).boxed()
        }
        async fn list_with_delimiter(&self, prefix: Option<&Path>) -> OsResult<ListResult> {
            let base = prefix.map(|p| p.as_ref().to_string()).unwrap_or_default();
            let mut common = BTreeSet::new();
            let mut objects = Vec::new();
            for path in &self.paths {
                let full = path.as_ref();
                let rest = if base.is_empty() {
                    Some(full)
                } else if full.starts_with(&base) && full.as_bytes().get(base.len()) == Some(&b'/') {
                    Some(&full[base.len() + 1..])
                } else {
                    None
                };
                let Some(rest) = rest else { continue };
                match rest.split_once('/') {
                    Some((dir, _)) => {
                        let joined = if base.is_empty() {
                            dir.to_string()
                        } else {
                            format!("{base}/{dir}")
                        };
                        common.insert(Path::from(joined));
                    }
                    None => objects.push(Self::meta(path)),
                }
            }
            Ok(ListResult {
                common_prefixes: common.into_iter().collect(),
                objects,
            })
        }
        async fn copy_opts(&self, _f: &Path, _t: &Path, _o: CopyOptions) -> OsResult<()> {
            unimplemented!("the tests only list")
        }
    }

    /// A tree two levels deep, with an object at each level above the leaves so
    /// the walk has something to miss if it only reads the leaves.
    fn tree() -> Vec<&'static str> {
        vec![
            "top.txt",
            "a/mid.txt",
            "a/one/x1.txt",
            "a/one/x2.txt",
            "a/two/y1.txt",
            "b/one/z1.txt",
            "b/two/z2.txt",
        ]
    }

    async fn listed(store: BigPageList<FakeStore>) -> Vec<String> {
        let mut paths: Vec<String> = store
            .list(None)
            .map_ok(|meta| meta.location.to_string())
            .try_collect::<Vec<_>>()
            .await
            .expect("the walk succeeds");
        paths.sort();
        paths
    }

    /// Every object comes back exactly once, including the ones above the leaf
    /// directories the shards are taken from.
    #[tokio::test]
    async fn a_sharded_walk_returns_the_whole_tree() {
        let expected: Vec<String> = {
            let mut e: Vec<String> = tree().iter().map(|p| p.to_string()).collect();
            e.sort();
            e
        };
        let got = listed(BigPageList::new(FakeStore::new(&tree()))).await;
        assert_eq!(got, expected);
    }

    /// Depth 0 turns sharding off. The result must not change.
    #[tokio::test]
    async fn a_sequential_walk_returns_the_same_tree() {
        let sharded = listed(BigPageList::new(FakeStore::new(&tree()))).await;
        let sequential =
            listed(BigPageList::new(FakeStore::new(&tree())).with_fanout_depth(0)).await;
        assert_eq!(sharded, sequential);
    }

    /// The page size reaches the store. Without it a server applies its own
    /// default, which is the round-trip cost this type exists to remove.
    #[tokio::test]
    async fn the_page_size_reaches_the_store() {
        let store = Arc::new(FakeStore::new(&tree()));
        let listing = BigPageList {
            inner: Arc::clone(&store),
            max_keys: 3,
            fanout_depth: 0,
            concurrency: 1,
        };
        let mut walked = listing.list(None);
        while walked.try_next().await.expect("the walk succeeds").is_some() {}

        let sizes = store.page_sizes();
        assert!(!sizes.is_empty(), "the store was asked for at least one page");
        assert!(
            sizes.iter().all(|s| *s == Some(3)),
            "every page asked for 3 keys, got {sizes:?}"
        );
    }

    /// A page size of zero would ask for nothing forever, so it is clamped.
    #[tokio::test]
    async fn a_page_size_below_one_is_clamped() {
        let store = BigPageList::new(FakeStore::new(&tree())).with_max_keys(0);
        assert_eq!(store.max_keys, 1);
        // And it still terminates, one key at a time.
        assert_eq!(listed(store).await.len(), tree().len());
    }

    /// Concurrency is clamped for the same reason: zero shards in flight would
    /// never finish.
    #[test]
    fn concurrency_below_one_is_clamped() {
        let store = BigPageList::new(FakeStore::new(&tree())).with_concurrency(0);
        assert_eq!(store.concurrency, 1);
    }

    /// A prefix with no sub-directories has one shard, which is the plain
    /// sequential walk. It must still return that directory's objects.
    #[tokio::test]
    async fn a_flat_prefix_walks_sequentially() {
        let store = BigPageList::new(FakeStore::new(&["only/a.txt", "only/b.txt"]));
        let mut paths: Vec<String> = store
            .list(Some(&Path::from("only")))
            .map_ok(|meta| meta.location.to_string())
            .try_collect::<Vec<_>>()
            .await
            .expect("the walk succeeds");
        paths.sort();
        assert_eq!(paths, vec!["only/a.txt", "only/b.txt"]);
    }

    /// The delimiter listing is handed through untouched: it is one request, and
    /// sharding it would mean sharding the answer it already gives.
    #[tokio::test]
    async fn a_delimiter_listing_passes_through() {
        let store = BigPageList::new(FakeStore::new(&tree()));
        let level = store.list_with_delimiter(None).await.expect("one level");
        let dirs: Vec<String> = level
            .common_prefixes
            .iter()
            .map(|p| p.to_string())
            .collect();
        assert_eq!(dirs, vec!["a", "b"]);
        let files: Vec<String> = level.objects.iter().map(|o| o.location.to_string()).collect();
        assert_eq!(files, vec!["top.txt"]);
    }

    /// No object appears twice. The leaf level's own walk covers what sits in
    /// it, so a level that finds no children must not also contribute its
    /// objects to the ones gathered above.
    #[tokio::test]
    async fn a_sharded_walk_repeats_nothing() {
        let got = listed(BigPageList::new(FakeStore::new(&tree()))).await;
        let unique: BTreeSet<&String> = got.iter().collect();
        assert_eq!(got.len(), unique.len(), "duplicates in {got:?}");
    }

    /// An empty store yields nothing rather than hanging or erroring.
    #[tokio::test]
    async fn an_empty_store_yields_nothing() {
        let store = BigPageList::new(FakeStore::new(&[]));
        assert!(listed(store).await.is_empty());
    }

    /// A tree where names share a prefix: a directory `one`, a sibling
    /// directory `one2`, and a file `one.txt` beside them.
    fn shared_prefix_tree() -> Vec<&'static str> {
        vec![
            "a/one.txt",
            "a/one/x.txt",
            "a/one2/y.txt",
            "a/onemore/z.txt",
        ]
    }

    /// A shard is a directory, not a byte prefix.
    ///
    /// The shard of `a/one` must not also return `a/one.txt`, which the level
    /// above already emitted, nor `a/one2/y.txt`, which the shard of `a/one2`
    /// returns. Both come back twice when the trailing delimiter is missing.
    #[tokio::test]
    async fn a_shard_does_not_reach_a_sibling_that_shares_its_name() {
        let got = listed(BigPageList::new(FakeStore::new(&shared_prefix_tree()))).await;
        let mut expected: Vec<String> = shared_prefix_tree()
            .iter()
            .map(|p| p.to_string())
            .collect();
        expected.sort();
        assert_eq!(got, expected, "every object exactly once");
    }

    /// The same rule for the prefix a caller asks for. Listing `a/one` names
    /// that directory, so a sibling `a/one2` is not part of the answer.
    #[tokio::test]
    async fn a_listed_prefix_is_a_directory_not_a_byte_prefix() {
        let store = BigPageList::new(FakeStore::new(&shared_prefix_tree()));
        let mut paths: Vec<String> = store
            .list(Some(&Path::from("a/one")))
            .map_ok(|meta| meta.location.to_string())
            .try_collect::<Vec<_>>()
            .await
            .expect("the walk succeeds");
        paths.sort();
        assert_eq!(paths, vec!["a/one/x.txt"]);
    }

    /// An empty prefix selects the whole store, so it reaches the store as
    /// `None` rather than as a bare `/` that matches nothing.
    #[test]
    fn an_empty_prefix_selects_the_whole_store() {
        assert_eq!(shard_prefix(None), None);
        assert_eq!(shard_prefix(Some(&Path::from(""))), None);
        assert_eq!(
            shard_prefix(Some(&Path::from("a/one"))),
            Some("a/one/".to_string())
        );
    }
}
