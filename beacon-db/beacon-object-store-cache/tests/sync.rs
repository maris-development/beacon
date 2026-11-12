//! Background sync. The tests run on paused time.

mod common;

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use common::{CountingStore, cached, config, paths};
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};

const INTERVAL: Duration = Duration::from_secs(60);

async fn seeded() -> Arc<CountingStore> {
    let source = CountingStore::new();
    source.external_put("p/a", "data").await;
    source.external_put("q/b", "data").await;
    source
}

#[tokio::test(start_paused = true)]
async fn an_external_change_shows_after_one_interval() {
    let source = seeded().await;
    let store = cached(&source, &config());
    paths(&store, None).await;

    source.external_put("p/new", "data").await;
    source.external_delete("q/b").await;
    assert_eq!(paths(&store, None).await, ["p/a", "q/b"], "stale before the sync");

    tokio::time::sleep(INTERVAL + Duration::from_secs(1)).await;
    assert_eq!(paths(&store, None).await, ["p/a", "p/new"]);
    let status = store.handle().status();
    assert!(status.last_sync.is_some());
    assert_eq!(status.sync_errors, 0);
}

#[tokio::test(start_paused = true)]
async fn a_write_during_a_sync_stays() {
    let source = seeded().await;
    let store = cached(&source, &config());
    paths(&store, None).await;

    source.hold_lists.store(true, Ordering::SeqCst);
    let handle = store.handle();
    let sync = tokio::spawn(async move { handle.sync_now().await });
    while source.lists() < 2 {
        tokio::task::yield_now().await;
    }
    // The sync holds a source listing that does not contain these changes.
    store.put(&Path::from("p/during"), "data".into()).await.unwrap();
    store.delete(&Path::from("p/a")).await.unwrap();
    // A read does not wait for the sync, and it shows the writes.
    assert_eq!(paths(&store, None).await, ["p/during", "q/b"]);
    assert!(store.head(&Path::from("p/during")).await.is_ok());
    source.hold_lists.store(false, Ordering::SeqCst);
    source.release.notify_one();
    let report = sync.await.unwrap();

    assert!(report.synced);
    assert_eq!(paths(&store, None).await, ["p/during", "q/b"]);
}

#[tokio::test(start_paused = true)]
async fn a_cache_without_a_good_sync_expires() {
    let source = seeded().await;
    let store = cached(&source, &config());
    paths(&store, None).await;

    source.fail_lists.store(true, Ordering::SeqCst);
    source.external_put("p/new", "data").await;
    tokio::time::sleep(INTERVAL * 2).await;
    assert_eq!(paths(&store, None).await, ["p/a", "q/b"], "the old listing stays in use");
    assert!(store.handle().status().sync_errors >= 1);

    tokio::time::sleep(INTERVAL * 2).await;
    let listing: Vec<_> = futures::StreamExt::collect(store.list(None)).await;
    assert!(listing[0].is_err(), "an expired cache sends the call to the source");

    source.fail_lists.store(false, Ordering::SeqCst);
    assert_eq!(paths(&store, None).await, ["p/a", "p/new", "q/b"]);
}

#[tokio::test(start_paused = true)]
async fn the_sync_task_ends_with_the_store() {
    let source = seeded().await;
    let store = cached(&source, &config());
    paths(&store, None).await;
    let handle = store.handle();
    assert!(Arc::strong_count(&source) > 1);

    drop(store);
    tokio::time::sleep(INTERVAL * 2).await;
    assert_eq!(Arc::strong_count(&source), 1, "no task keeps the source alive");
    assert_eq!(handle.status().entries, 0);
    assert!(!handle.sync_now().await.synced);
}

#[tokio::test(start_paused = true)]
async fn a_manual_sync_compares_the_cache_with_the_source() {
    let source = seeded().await;
    let store = cached(&source, &config());
    paths(&store, Some("p")).await;

    source.external_put("p/new", "data").await;
    source.external_delete("q/b").await;
    let report = store.handle().sync_now().await;
    assert!(report.synced);
    assert_eq!(report.entries, 2);
    assert_eq!(paths(&store, None).await, ["p/a", "p/new"]);
    assert_eq!(source.lists(), 2);
}

#[tokio::test(start_paused = true)]
async fn an_unused_cache_drops_its_index() {
    let source = seeded().await;
    let settings = beacon_object_store_cache::StoreCacheConfig {
        evict_after: INTERVAL * 3,
        ..config()
    };
    let store = cached(&source, &settings);
    paths(&store, None).await;

    // A cache in use keeps its index.
    for _ in 0..5 {
        tokio::time::sleep(INTERVAL).await;
        paths(&store, Some("p")).await;
    }
    assert!(store.handle().status().synced);

    tokio::time::sleep(INTERVAL * 5).await;
    let status = store.handle().status();
    assert!(!status.synced);
    assert_eq!(status.entries, 0);

    // The next listing fills the cache again.
    assert_eq!(paths(&store, None).await, ["p/a", "q/b"]);
    assert!(store.handle().status().synced);
}
