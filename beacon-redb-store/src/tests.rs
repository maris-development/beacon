//! Tests for [`RedbStore`].
//!
//! Two layers:
//! 1. Focused unit tests for Beacon-relevant behaviour (round-trip, listing,
//!    conditional writes, multipart, and — critically — persistence across a
//!    reopen, which is the "reload the catalog after a restart" property).
//! 2. `object_store`'s own conformance suite (behind its `integration` feature),
//!    which pins the semantics that are easy to get subtly wrong.

use super::*;
use object_store::{ObjectStoreExt, PutMode, UpdateVersion, path::Path};
use tempfile::TempDir;

/// A fresh store backed by a file inside a temp dir. The `TempDir` is returned
/// so it outlives the store (dropping it would delete the file).
fn store() -> (TempDir, RedbStore) {
    let dir = TempDir::new().expect("tempdir");
    let store = RedbStore::open(dir.path().join("beacon.db")).expect("open store");
    (dir, store)
}

#[tokio::test]
async fn put_get_head_roundtrip() {
    let (_dir, store) = store();
    let path = Path::from("tables/obs/table.json");

    let res = store.put(&path, "hello".into()).await.unwrap();
    assert!(res.e_tag.is_some());

    let got = store.get(&path).await.unwrap().bytes().await.unwrap();
    assert_eq!(got.as_ref(), b"hello");

    let meta = store.head(&path).await.unwrap();
    assert_eq!(meta.size, 5);
    assert_eq!(meta.location, path);
    assert_eq!(meta.e_tag, res.e_tag);
}

#[tokio::test]
async fn overwrite_changes_etag_and_bytes() {
    let (_dir, store) = store();
    let path = Path::from("a");
    let v1 = store.put(&path, "one".into()).await.unwrap();
    let v2 = store.put(&path, "two!".into()).await.unwrap();
    assert_ne!(v1.e_tag, v2.e_tag);
    let got = store.get(&path).await.unwrap().bytes().await.unwrap();
    assert_eq!(got.as_ref(), b"two!");
}

#[tokio::test]
async fn get_range_returns_slice() {
    let (_dir, store) = store();
    let path = Path::from("blob");
    store.put(&path, "0123456789".into()).await.unwrap();

    let mid = store.get_range(&path, 2..5).await.unwrap();
    assert_eq!(mid.as_ref(), b"234");

    let ranges = store.get_ranges(&path, &[0..2, 8..10]).await.unwrap();
    assert_eq!(ranges[0].as_ref(), b"01");
    assert_eq!(ranges[1].as_ref(), b"89");
}

#[tokio::test]
async fn delete_then_get_is_not_found() {
    let (_dir, store) = store();
    let path = Path::from("gone");
    store.put(&path, "x".into()).await.unwrap();
    store.delete(&path).await.unwrap();

    let err = store.get(&path).await.unwrap_err();
    assert!(matches!(err, object_store::Error::NotFound { .. }), "{err}");

    // Deleting a missing object is NotFound, matching LocalFileSystem.
    let err = store.delete(&path).await.unwrap_err();
    assert!(matches!(err, object_store::Error::NotFound { .. }), "{err}");
}

#[tokio::test]
async fn list_and_delimiter() {
    let (_dir, store) = store();
    for p in ["tables/a/table.json", "tables/a/extensions.json", "tables/b/table.json"] {
        store.put(&Path::from(p), "x".into()).await.unwrap();
    }
    store.put(&Path::from("crawlers/c.json"), "x".into()).await.unwrap();

    // Recursive list under a prefix.
    let mut found: Vec<String> = store
        .list(Some(&Path::from("tables")))
        .map(|m| m.unwrap().location.to_string())
        .collect::<Vec<_>>()
        .await;
    found.sort();
    assert_eq!(
        found,
        vec![
            "tables/a/extensions.json",
            "tables/a/table.json",
            "tables/b/table.json",
        ]
    );

    // Delimiter listing at the root groups into directories.
    let res = store.list_with_delimiter(None).await.unwrap();
    let dirs: Vec<String> = res.common_prefixes.iter().map(|p| p.to_string()).collect();
    assert!(dirs.contains(&"tables".to_string()));
    assert!(dirs.contains(&"crawlers".to_string()));
    assert!(res.objects.is_empty(), "root has no direct-child objects");

    // Segment-boundary safety: `tables` must not match a sibling `tables_x`.
    store.put(&Path::from("tables_x/y"), "x".into()).await.unwrap();
    let under_tables: Vec<String> = store
        .list(Some(&Path::from("tables")))
        .map(|m| m.unwrap().location.to_string())
        .collect::<Vec<_>>()
        .await;
    assert!(!under_tables.iter().any(|p| p.starts_with("tables_x")));
}

#[tokio::test]
async fn conditional_create_and_update() {
    let (_dir, store) = store();
    let path = Path::from("cas");

    let v1 = store
        .put_opts(&path, "a".into(), PutMode::Create.into())
        .await
        .unwrap();

    // Create over an existing object fails.
    let err = store
        .put_opts(&path, "b".into(), PutMode::Create.into())
        .await
        .unwrap_err();
    assert!(matches!(err, object_store::Error::AlreadyExists { .. }), "{err}");

    // Update with the current etag succeeds.
    let v2 = store
        .put_opts(
            &path,
            "c".into(),
            PutMode::Update(UpdateVersion { e_tag: v1.e_tag.clone(), version: None }).into(),
        )
        .await
        .unwrap();

    // Update with a stale etag fails with Precondition.
    let err = store
        .put_opts(
            &path,
            "d".into(),
            PutMode::Update(UpdateVersion { e_tag: v1.e_tag, version: None }).into(),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, object_store::Error::Precondition { .. }), "{err}");

    let got = store.get(&path).await.unwrap().bytes().await.unwrap();
    assert_eq!(got.as_ref(), b"c");
    let _ = v2;
}

#[tokio::test]
async fn copy_and_copy_if_not_exists() {
    let (_dir, store) = store();
    let from = Path::from("src");
    let to = Path::from("dst");
    store.put(&from, "payload".into()).await.unwrap();

    store.copy(&from, &to).await.unwrap();
    let got = store.get(&to).await.unwrap().bytes().await.unwrap();
    assert_eq!(got.as_ref(), b"payload");

    // copy_if_not_exists onto an existing destination fails.
    let err = store.copy_if_not_exists(&from, &to).await.unwrap_err();
    assert!(matches!(err, object_store::Error::AlreadyExists { .. }), "{err}");
}

#[tokio::test]
async fn multipart_upload_preserves_call_order() {
    let (_dir, store) = store();
    let path = Path::from("multi");
    let mut upload = store.put_multipart(&path).await.unwrap();
    upload.put_part("first-".into()).await.unwrap();
    upload.put_part("second-".into()).await.unwrap();
    upload.put_part("third".into()).await.unwrap();
    upload.complete().await.unwrap();

    let got = store.get(&path).await.unwrap().bytes().await.unwrap();
    assert_eq!(got.as_ref(), b"first-second-third");
}

/// Small objects (metadata) stay inline in redb; large objects go to the heap.
#[tokio::test]
async fn small_inline_large_heap() {
    let (_dir, store) = store();

    let small = Path::from("tables/t/table.json");
    store.put(&small, vec![7u8; 1024].into()).await.unwrap();
    assert!(!store.is_heap(&small).unwrap(), "small object should be inline");

    let big = Path::from("data/big.lance");
    let payload = vec![42u8; super::HEAP_THRESHOLD + 4096];
    store.put(&big, payload.clone().into()).await.unwrap();
    assert!(store.is_heap(&big).unwrap(), "large object should be in the heap");

    // Zero-copy read path returns the exact bytes.
    let got = store.get(&big).await.unwrap().bytes().await.unwrap();
    assert_eq!(got.len(), payload.len());
    assert_eq!(got.as_ref(), payload.as_slice());

    // Ranges over a heap object.
    let mid = store.get_range(&big, 100..200).await.unwrap();
    assert_eq!(mid.as_ref(), &payload[100..200]);
}

/// A large heap blob must survive a restart (heap bytes + extent in the reopened
/// file), read back byte-identical.
#[tokio::test]
async fn heap_blob_persists_across_reopen() {
    let dir = TempDir::new().unwrap();
    let file = dir.path().join("beacon.db");
    let path = Path::from("data/cube.lance");
    let payload = vec![9u8; super::HEAP_THRESHOLD * 3 + 17];

    {
        let store = RedbStore::open(&file).unwrap();
        store.put(&path, payload.clone().into()).await.unwrap();
        assert!(store.is_heap(&path).unwrap());
    } // release the file lock

    let store = RedbStore::open(&file).unwrap();
    assert!(store.is_heap(&path).unwrap());
    let got = store.get(&path).await.unwrap().bytes().await.unwrap();
    assert_eq!(got.as_ref(), payload.as_slice());
}

/// Multipart of a large object lands in the heap in call order.
#[tokio::test]
async fn multipart_large_goes_to_heap() {
    let (_dir, store) = store();
    let path = Path::from("data/multi.lance");
    let part = vec![3u8; 32 * 1024];

    let mut upload = store.put_multipart(&path).await.unwrap();
    upload.put_part(part.clone().into()).await.unwrap();
    upload.put_part(part.clone().into()).await.unwrap();
    upload.put_part(part.clone().into()).await.unwrap(); // 96 KiB > threshold
    upload.complete().await.unwrap();

    assert!(store.is_heap(&path).unwrap());
    let got = store.get(&path).await.unwrap().bytes().await.unwrap();
    assert_eq!(got.len(), part.len() * 3);
}

/// The catalog must survive a restart: write, close the store (release redb's
/// file lock), reopen the same file, and read it back.
#[tokio::test]
async fn state_persists_across_reopen() {
    let dir = TempDir::new().unwrap();
    let file = dir.path().join("beacon.db");
    let path = Path::from("tables/keep/table.json");

    {
        let store = RedbStore::open(&file).unwrap();
        store.put(&path, "durable".into()).await.unwrap();
    } // store dropped here -> redb lock released

    let store = RedbStore::open(&file).unwrap();
    let got = store.get(&path).await.unwrap().bytes().await.unwrap();
    assert_eq!(got.as_ref(), b"durable");
}

// ---- object_store conformance suite ---------------------------------------

mod conformance {
    use super::store;
    use object_store::integration;

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn put_get_delete_list() {
        let (_dir, store) = store();
        integration::put_get_delete_list(&store).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn get_opts() {
        let (_dir, store) = store();
        integration::get_opts(&store).await;
    }

    /// Exercises conditional puts including the 5-worker × 10-increment CAS race,
    /// which only passes if `Update` is a true atomic compare-and-swap.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn put_opts() {
        let (_dir, store) = store();
        integration::put_opts(&store, true).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn list_with_delimiter() {
        let (_dir, store) = store();
        integration::list_with_delimiter(&store).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn list_uses_directories_correctly() {
        let (_dir, store) = store();
        integration::list_uses_directories_correctly(&store).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn rename_and_copy() {
        let (_dir, store) = store();
        integration::rename_and_copy(&store).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn copy_if_not_exists() {
        let (_dir, store) = store();
        integration::copy_if_not_exists(&store).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn stream_get() {
        let (_dir, store) = store();
        integration::stream_get(&store).await;
    }
}
