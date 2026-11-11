//! Periodic sync and change events. The tests run on paused time.

mod common;

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use beacon_object_store_cache::StoreEvent;
use common::{CountingStore, cached, config, paths};
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};

const INTERVAL: Duration = Duration::from_secs(60);

fn changed(path: &str) -> StoreEvent {
    StoreEvent::Changed {
        path: path.to_string(),
    }
}

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
    source.hold_lists.store(false, Ordering::SeqCst);
    source.release.notify_one();
    let report = sync.await.unwrap();

    assert_eq!(report.synced, 1);
    assert_eq!(paths(&store, None).await, ["p/during", "q/b"]);
}

#[tokio::test(start_paused = true)]
async fn a_cover_without_a_good_sync_expires() {
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
    assert!(listing[0].is_err(), "an expired cover sends the call to the source");

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
    assert_eq!(handle.sync_now().await.synced, 0);
    assert_eq!(handle.apply(vec![changed("p/a")]).await.ignored, 1);
}

#[tokio::test(start_paused = true)]
async fn an_unused_cover_is_evicted() {
    let source = seeded().await;
    let settings = beacon_object_store_cache::StoreCacheConfig {
        evict_after: INTERVAL * 3,
        ..config()
    };
    let store = cached(&source, &settings);
    paths(&store, Some("p")).await;
    paths(&store, Some("q")).await;

    for _ in 0..5 {
        tokio::time::sleep(INTERVAL).await;
        paths(&store, Some("p")).await;
    }
    assert_eq!(store.handle().status().covered, ["p"]);
    assert_eq!(store.handle().status().entries, 1);
}

#[tokio::test(start_paused = true)]
async fn a_changed_event_updates_one_object() {
    let source = seeded().await;
    let store = cached(&source, &config());
    paths(&store, None).await;
    let handle = store.handle();

    source.external_put("p/new", "12345").await;
    source.external_delete("q/b").await;
    let report = handle
        .apply(vec![changed("p/new"), changed("q/b"), changed("p/new")])
        .await;
    assert_eq!((report.applied, report.ignored, report.failed), (2, 0, 0));
    assert_eq!(paths(&store, None).await, ["p/a", "p/new"]);
    assert_eq!(store.head(&Path::from("p/new")).await.unwrap().size, 5);
}

#[tokio::test(start_paused = true)]
async fn a_changed_event_for_a_moved_folder_finds_its_objects() {
    let source = seeded().await;
    let store = cached(&source, &config());
    paths(&store, None).await;

    // A different program moves folder `p` to `r`.
    source.external_delete("p/a").await;
    source.external_put("r/a", "data").await;
    let report = store.handle().apply(vec![changed("p"), changed("r")]).await;
    assert_eq!(report.applied, 2);
    assert_eq!(paths(&store, None).await, ["q/b", "r/a"]);
}

#[tokio::test(start_paused = true)]
async fn a_prefix_event_lists_the_folder_again() {
    let source = seeded().await;
    let store = cached(&source, &config());
    paths(&store, Some("p")).await;
    let handle = store.handle();

    source.external_put("p/deep/x", "data").await;
    source.external_put("q/ignored", "data").await;
    let prefix = |path: &str| StoreEvent::PrefixChanged {
        path: path.to_string(),
    };
    let report = handle.apply(vec![prefix("p/deep"), prefix("q")]).await;
    assert_eq!((report.applied, report.ignored), (1, 1));
    assert_eq!(paths(&store, Some("p")).await, ["p/a", "p/deep/x"]);

    // An event for a parent folder syncs the covers below it.
    source.external_put("p/later", "data").await;
    assert_eq!(handle.apply(vec![prefix("")]).await.applied, 1);
    assert_eq!(paths(&store, Some("p")).await, ["p/a", "p/deep/x", "p/later"]);
}

#[tokio::test(start_paused = true)]
async fn a_resync_event_compares_all_covers() {
    let source = seeded().await;
    let store = cached(&source, &config());
    paths(&store, None).await;

    source.external_put("p/new", "data").await;
    let report = store.handle().apply(vec![StoreEvent::Resync]).await;
    assert_eq!(report.applied, 1);
    assert_eq!(paths(&store, None).await, ["p/a", "p/new", "q/b"]);
}

#[tokio::test(start_paused = true)]
async fn an_event_outside_a_cover_is_ignored() {
    let source = seeded().await;
    let store = cached(&source, &config());
    paths(&store, Some("p")).await;

    let report = store.handle().apply(vec![changed("q/b")]).await;
    assert_eq!(report.ignored, 1);
    assert_eq!(source.gets(), 0);
}

#[test]
fn events_use_a_tagged_json_form() {
    let event: StoreEvent = serde_json::from_str(r#"{"type":"changed","path":"a/b"}"#).unwrap();
    assert_eq!(event, changed("a/b"));
    let resync: StoreEvent = serde_json::from_str(r#"{"type":"resync"}"#).unwrap();
    assert_eq!(resync, StoreEvent::Resync);
}
