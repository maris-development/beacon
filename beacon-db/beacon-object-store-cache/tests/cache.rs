//! Listing, metadata and write-through behavior of the cache.

mod common;

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use beacon_object_store_cache::{CachedObjectStore, StoreCacheConfig, head_uncached};
use common::{CountingStore, cached, config, paths};
use futures::StreamExt;
use futures::future::join_all;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{Error, ObjectStore, ObjectStoreExt, WriteMultipart, integration};

async fn seeded() -> Arc<CountingStore> {
    let source = CountingStore::new();
    for path in ["p/a", "p/sub/b", "p-x/c", "p.x/d", "root.txt"] {
        source.external_put(path, "data").await;
    }
    source
}

#[tokio::test]
async fn second_list_reads_the_index() {
    let source = seeded().await;
    let store = cached(&source, &config());

    let first = paths(&store, Some("p")).await;
    assert_eq!(first, ["p/a", "p/sub/b"]);
    assert_eq!(source.lists(), 1);

    assert_eq!(paths(&store, Some("p")).await, first);
    assert_eq!(paths(&store, Some("p/sub")).await, ["p/sub/b"]);
    assert_eq!(paths(&store, Some("p-x")).await, ["p-x/c"]);
    assert_eq!(source.lists(), 1, "one listing of the source fills the full cache");
}

#[tokio::test]
async fn head_after_a_listing_reads_the_index() {
    let source = seeded().await;
    let store = cached(&source, &config());
    paths(&store, None).await;

    let meta = store.head(&Path::from("p/a")).await.unwrap();
    assert_eq!(meta.size, 4);
    let missing = store.head(&Path::from("p/none")).await.unwrap_err();
    assert!(matches!(missing, Error::NotFound { .. }));
    assert_eq!(source.gets(), 0);

    let status = store.handle().status();
    assert!(status.synced);
    assert_eq!(status.entries, 5);
    assert!(status.hits >= 2);
}

#[tokio::test]
async fn head_before_a_listing_reads_the_source() {
    let source = seeded().await;
    let store = cached(&source, &config());
    store.head(&Path::from("p/a")).await.unwrap();
    assert_eq!(source.gets(), 1);
    assert_eq!(source.lists(), 0, "a metadata call does not start a listing");
}

#[tokio::test]
async fn writes_show_in_the_next_list() {
    let source = seeded().await;
    let store = cached(&source, &config());
    paths(&store, None).await;

    store.put(&Path::from("p/new"), "12345".into()).await.unwrap();
    store.copy(&Path::from("p/a"), &Path::from("p/copy")).await.unwrap();
    store.rename(&Path::from("root.txt"), &Path::from("p/moved")).await.unwrap();
    store.delete(&Path::from("p/sub/b")).await.unwrap();
    let upload = store.put_multipart(&Path::from("p/multi")).await.unwrap();
    let mut writer = WriteMultipart::new(upload);
    writer.write(b"multipart data");
    writer.finish().await.unwrap();

    assert_eq!(
        paths(&store, Some("p")).await,
        ["p/a", "p/copy", "p/moved", "p/multi", "p/new"]
    );
    assert_eq!(store.head(&Path::from("p/new")).await.unwrap().size, 5);
    assert_eq!(store.head(&Path::from("p/multi")).await.unwrap().size, 14);
    assert!(store.head(&Path::from("root.txt")).await.is_err());
    assert_eq!(source.lists(), 1);
}

#[tokio::test]
async fn cached_metadata_equals_source_metadata() {
    let source = seeded().await;
    let store = cached(&source, &config());
    paths(&store, None).await;
    store.put(&Path::from("p/new"), "12345".into()).await.unwrap();

    for path in ["p/a", "p/new"] {
        let location = Path::from(path);
        let known = store.head(&location).await.unwrap();
        let real = source.inner.head(&location).await.unwrap();
        assert_eq!(known, real, "{path}");
    }
}

#[tokio::test]
async fn concurrent_first_lists_share_one_source_listing() {
    let source = seeded().await;
    let store = cached(&source, &config());
    let results = join_all((0..8).map(|_| paths(&store, None))).await;
    assert!(results.iter().all(|listing| listing.len() == 5));
    assert_eq!(source.lists(), 1);
}

#[tokio::test]
async fn a_dropped_caller_does_not_stop_the_first_listing() {
    let source = seeded().await;
    let store = cached(&source, &config());
    source.hold_lists.store(true, Ordering::SeqCst);

    let mut listing = store.list(None);
    let early = tokio::time::timeout(Duration::from_millis(50), listing.next()).await;
    assert!(early.is_err(), "the source listing is on hold");
    drop(listing);

    source.hold_lists.store(false, Ordering::SeqCst);
    source.release.notify_one();
    let handle = store.handle();
    for _ in 0..200 {
        if handle.status().synced {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(handle.status().synced);
    assert_eq!(paths(&store, None).await.len(), 5);
    assert_eq!(source.lists(), 1);
}

#[tokio::test]
async fn listing_is_sorted_and_respects_segments() {
    let source = seeded().await;
    let store = cached(&source, &config());

    assert_eq!(
        paths(&store, None).await,
        ["p-x/c", "p.x/d", "p/a", "p/sub/b", "root.txt"]
    );
    assert_eq!(paths(&store, Some("p")).await, ["p/a", "p/sub/b"]);
    assert!(paths(&store, Some("p/a")).await.is_empty(), "a file is not a prefix");

    let after: Vec<String> = store
        .list_with_offset(Some(&Path::from("p")), &Path::from("p/a"))
        .map(|meta| meta.unwrap().location.to_string())
        .collect()
        .await;
    assert_eq!(after, ["p/sub/b"], "the offset is exclusive");

    let below_prefix: Vec<String> = store
        .list_with_offset(Some(&Path::from("p")), &Path::from("a"))
        .map(|meta| meta.unwrap().location.to_string())
        .collect()
        .await;
    assert_eq!(below_prefix, ["p/a", "p/sub/b"]);
}

#[tokio::test]
async fn delimiter_listing_reads_the_index() {
    let source = seeded().await;
    let store = cached(&source, &config());
    paths(&store, None).await;

    let root = store.list_with_delimiter(None).await.unwrap();
    let folders: Vec<String> = root.common_prefixes.iter().map(ToString::to_string).collect();
    assert_eq!(folders, ["p-x", "p.x", "p"], "folders follow the key order");
    assert_eq!(root.objects.len(), 1);
    assert_eq!(root.objects[0].location.as_ref(), "root.txt");

    let folder = store.list_with_delimiter(Some(&Path::from("p"))).await.unwrap();
    assert_eq!(folder.common_prefixes, [Path::from("p/sub")]);
    assert_eq!(folder.objects.len(), 1);
    assert_eq!(source.lists(), 1);
}

#[tokio::test]
async fn a_read_corrects_a_stale_entry() {
    let source = seeded().await;
    let store = cached(&source, &config());
    paths(&store, None).await;

    source.external_put("p/a", "a longer payload").await;
    assert_eq!(store.head(&Path::from("p/a")).await.unwrap().size, 4, "stale until a read");
    store.get(&Path::from("p/a")).await.unwrap();
    assert_eq!(store.head(&Path::from("p/a")).await.unwrap().size, 16);

    source.external_delete("p/sub/b").await;
    assert!(store.get(&Path::from("p/sub/b")).await.is_err());
    assert_eq!(paths(&store, Some("p")).await, ["p/a"]);
}

#[tokio::test]
async fn head_uncached_reads_the_source() {
    let source = seeded().await;
    let store = cached(&source, &config());
    paths(&store, None).await;
    source.external_put("p/external", "data").await;

    assert!(store.head(&Path::from("p/external")).await.is_err());
    let meta = head_uncached(&store, &Path::from("p/external")).await.unwrap();
    assert_eq!(meta.size, 4);
    assert_eq!(
        paths(&store, Some("p")).await,
        ["p/a", "p/external", "p/sub/b"],
        "the source read corrects the index"
    );
}

#[tokio::test]
async fn bypass_segments_read_the_source() {
    let source = seeded().await;
    source.external_put("t/_delta_log/0.json", "{}").await;
    let store = cached(&source, &config());
    paths(&store, None).await;
    let before = source.lists();

    source.external_put("t/_delta_log/1.json", "{}").await;
    assert_eq!(paths(&store, Some("t/_delta_log")).await.len(), 2);
    assert_eq!(source.lists(), before + 1);
    store.head(&Path::from("t/_delta_log/1.json")).await.unwrap();
    assert_eq!(source.gets(), 1);
}

#[tokio::test]
async fn clear_drops_the_index() {
    let source = seeded().await;
    let store = cached(&source, &config());
    paths(&store, None).await;
    source.external_put("p/late", "data").await;

    store.handle().clear();
    assert_eq!(store.handle().status().entries, 0);
    assert!(paths(&store, Some("p")).await.contains(&"p/late".to_string()));
    assert_eq!(source.lists(), 2);
}

#[tokio::test]
async fn wrap_respects_the_enable_setting() {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (plain, none) = CachedObjectStore::wrap(Arc::clone(&inner), &StoreCacheConfig::default());
    assert!(none.is_none());
    assert_eq!(plain.to_string(), inner.to_string());
    let (wrapped, handle) = CachedObjectStore::wrap(inner, &config());
    assert!(handle.is_some());
    assert!(wrapped.to_string().starts_with("CachedObjectStore("));
}

fn fresh() -> CachedObjectStore {
    CachedObjectStore::new(Arc::new(InMemory::new()), &config())
}

/// The object_store conformance suite. `warm` covers the root first.
async fn conformance(warm: bool) {
    let prepare = |store: CachedObjectStore| async move {
        if warm {
            paths(&store, None).await;
        }
        store
    };
    integration::put_get_delete_list(&prepare(fresh()).await).await;
    integration::get_opts(&prepare(fresh()).await).await;
    integration::put_opts(&prepare(fresh()).await, true).await;
    integration::list_uses_directories_correctly(&prepare(fresh()).await).await;
    integration::list_with_delimiter(&prepare(fresh()).await).await;
    integration::rename_and_copy(&prepare(fresh()).await).await;
    integration::copy_if_not_exists(&prepare(fresh()).await).await;
    integration::copy_rename_nonexistent_object(&prepare(fresh()).await).await;
    integration::stream_get(&prepare(fresh()).await).await;
    integration::list_with_offset_exclusivity(&prepare(fresh()).await).await;
}

#[tokio::test]
async fn conformance_with_a_cold_cache() {
    conformance(false).await;
}

#[tokio::test]
async fn conformance_with_a_warm_cache() {
    conformance(true).await;
}
