//! File system watcher on a real folder.
#![cfg(feature = "watch")]

mod common;

use std::sync::Arc;
use std::time::Duration;

use beacon_object_store_cache::{
    CachedObjectStore, StoreCacheConfig, WatcherState, network_file_system, watch_local,
};
use common::paths;
use object_store::local::LocalFileSystem;

/// Wait until a listing of the store equals `expected`.
async fn wait_for(store: &CachedObjectStore, expected: &[&str]) {
    let mut last = Vec::new();
    for _ in 0..200 {
        last = paths(store, None).await;
        if last == expected {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("listing stayed {last:?}, expected {expected:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn file_changes_reach_the_cache() {
    let folder = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(folder.path().join("a")).unwrap();
    std::fs::write(folder.path().join("a").join("one.txt"), "1").unwrap();

    // A long interval: only the watcher can update the cache in this test.
    let config = StoreCacheConfig {
        enable: true,
        sync_interval: Duration::from_secs(3600),
        ..StoreCacheConfig::default()
    };
    let local = LocalFileSystem::new_with_prefix(folder.path()).unwrap();
    let store = CachedObjectStore::new(Arc::new(local), &config);
    let watcher = watch_local(store.handle(), folder.path(), Duration::from_millis(50));
    assert!(watcher.started().await, "a local folder gets a watcher");
    assert_eq!(watcher.state(), WatcherState::Running);
    wait_for(&store, &["a/one.txt"]).await;

    std::fs::write(folder.path().join("a").join("two.txt"), "22").unwrap();
    wait_for(&store, &["a/one.txt", "a/two.txt"]).await;

    std::fs::remove_file(folder.path().join("a").join("one.txt")).unwrap();
    wait_for(&store, &["a/two.txt"]).await;

    std::fs::create_dir_all(folder.path().join("b").join("deep")).unwrap();
    std::fs::write(folder.path().join("b").join("deep").join("three.txt"), "333").unwrap();
    wait_for(&store, &["a/two.txt", "b/deep/three.txt"]).await;

    std::fs::rename(folder.path().join("a"), folder.path().join("c")).unwrap();
    wait_for(&store, &["b/deep/three.txt", "c/two.txt"]).await;

    assert_eq!(store.handle().status().covered, [""]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_change_during_the_start_reaches_the_cache() {
    let folder = tempfile::tempdir().unwrap();
    std::fs::write(folder.path().join("one.txt"), "1").unwrap();
    let config = StoreCacheConfig {
        enable: true,
        sync_interval: Duration::from_secs(3600),
        ..StoreCacheConfig::default()
    };
    let local = LocalFileSystem::new_with_prefix(folder.path()).unwrap();
    let store = CachedObjectStore::new(Arc::new(local), &config);
    wait_for(&store, &["one.txt"]).await;

    // No watcher runs for this write. The compare after the start finds it.
    std::fs::write(folder.path().join("two.txt"), "22").unwrap();
    let watcher = watch_local(store.handle(), folder.path(), Duration::from_millis(50));
    assert!(watcher.started().await);
    wait_for(&store, &["one.txt", "two.txt"]).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_missing_folder_stops_the_watcher() {
    let folder = tempfile::tempdir().unwrap();
    let local = LocalFileSystem::new_with_prefix(folder.path()).unwrap();
    let store = CachedObjectStore::new(Arc::new(local), &StoreCacheConfig::default());
    let missing = folder.path().join("not-here");
    let watcher = watch_local(store.handle(), &missing, Duration::from_millis(50));
    assert!(!watcher.started().await);
    assert_eq!(watcher.state(), WatcherState::Stopped);
}

#[test]
fn a_temp_folder_is_not_a_network_file_system() {
    let folder = tempfile::tempdir().unwrap();
    let canonical = std::fs::canonicalize(folder.path()).unwrap();
    assert_eq!(network_file_system(&canonical), None);
}
