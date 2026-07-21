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

/// A rewrite vacuum reclaims dead heap blobs (and redb pages), shrinking the
/// file, while every live object survives byte-identical with its etag intact.
#[tokio::test]
async fn vacuum_reclaims_space_and_preserves_live_objects() {
    let dir = TempDir::new().unwrap();
    let file = dir.path().join("beacon.db");
    let store = RedbStore::open(&file).unwrap();

    let big = |b: u8| vec![b; super::HEAP_THRESHOLD * 4];
    let keep = Path::from("data/keep.lance");
    let churn = Path::from("data/churn.lance");
    let meta = Path::from("tables/t/table.json");
    let gone = Path::from("data/gone.lance");

    store.put(&keep, big(7).into()).await.unwrap();
    // Overwrite `churn` repeatedly, leaking a dead heap blob each time.
    for i in 0..5u8 {
        store.put(&churn, big(i).into()).await.unwrap();
    }
    store.put(&meta, "hi".into()).await.unwrap(); // small -> inline
    store.put(&gone, big(1).into()).await.unwrap();
    store.delete(&gone).await.unwrap(); // leaks its heap blob too

    let keep_etag = store.head(&keep).await.unwrap().e_tag;
    let size_before = std::fs::metadata(&file).unwrap().len();

    let store = store.vacuum(VacuumMode::Rewrite).await.unwrap();

    let size_after = std::fs::metadata(&file).unwrap().len();
    assert!(
        size_after < size_before,
        "vacuum should shrink the file: {size_before} -> {size_after}"
    );

    // Live objects: byte-identical, etag preserved, still heap-stored.
    let got = store.get(&keep).await.unwrap().bytes().await.unwrap();
    assert_eq!(got.as_ref(), big(7).as_slice());
    assert_eq!(store.head(&keep).await.unwrap().e_tag, keep_etag);
    assert!(store.is_heap(&keep).unwrap());
    let got = store.get(&churn).await.unwrap().bytes().await.unwrap();
    assert_eq!(got.as_ref(), big(4).as_slice(), "keeps the last overwrite");
    assert_eq!(store.get(&meta).await.unwrap().bytes().await.unwrap().as_ref(), b"hi");

    // Deleted object stays gone.
    let err = store.get(&gone).await.unwrap_err();
    assert!(matches!(err, object_store::Error::NotFound { .. }), "{err}");

    // The etag counter carried forward: a fresh put gets a new, larger etag.
    let after = store.put(&keep, big(8).into()).await.unwrap();
    assert_ne!(after.e_tag, keep_etag);
}

/// Vacuum consumes the store and needs exclusive ownership: an outstanding clone
/// makes it fail rather than silently rewrite under a live handle.
#[tokio::test]
async fn vacuum_requires_exclusive_access() {
    let (_dir, store) = store();
    store.put(&Path::from("a"), "x".into()).await.unwrap();

    let clone = store.clone();
    let err = store.vacuum(VacuumMode::Rewrite).await.unwrap_err();
    assert!(matches!(err, object_store::Error::Generic { .. }), "{err}");

    // The clone is still usable — vacuum didn't touch the file.
    let got = clone.get(&Path::from("a")).await.unwrap().bytes().await.unwrap();
    assert_eq!(got.as_ref(), b"x");
}

/// `head` and `get` on a missing object both map to `NotFound`, and so does a
/// copy/rename whose source is absent.
#[tokio::test]
async fn missing_object_maps_to_not_found() {
    let (_dir, store) = store();
    let missing = Path::from("nope");

    assert!(matches!(
        store.head(&missing).await.unwrap_err(),
        object_store::Error::NotFound { .. }
    ));
    assert!(matches!(
        store.copy(&missing, &Path::from("dst")).await.unwrap_err(),
        object_store::Error::NotFound { .. }
    ));
    assert!(matches!(
        store.rename(&missing, &Path::from("dst")).await.unwrap_err(),
        object_store::Error::NotFound { .. }
    ));
}

/// Rename is a move: the destination gets the bytes and the source disappears.
#[tokio::test]
async fn rename_moves_and_removes_source() {
    let (_dir, store) = store();
    let from = Path::from("a");
    let to = Path::from("b");
    store.put(&from, "payload".into()).await.unwrap();

    store.rename(&from, &to).await.unwrap();
    let got = store.get(&to).await.unwrap().bytes().await.unwrap();
    assert_eq!(got.as_ref(), b"payload");
    assert!(matches!(
        store.get(&from).await.unwrap_err(),
        object_store::Error::NotFound { .. }
    ));
}

/// A plain copy overwrites an existing destination (default `Overwrite` mode),
/// while the source is left intact.
#[tokio::test]
async fn copy_overwrites_destination_and_keeps_source() {
    let (_dir, store) = store();
    let from = Path::from("src");
    let to = Path::from("dst");
    store.put(&from, "new".into()).await.unwrap();
    store.put(&to, "old".into()).await.unwrap();

    store.copy(&from, &to).await.unwrap();
    assert_eq!(store.get(&to).await.unwrap().bytes().await.unwrap().as_ref(), b"new");
    // Source still present after a copy.
    assert_eq!(store.get(&from).await.unwrap().bytes().await.unwrap().as_ref(), b"new");
}

/// Copying a heap-backed object shares the extent by reference: no bytes are
/// rewritten, both keys are heap-stored, and both read back identically.
#[tokio::test]
async fn copy_of_heap_object_shares_the_extent() {
    let (_dir, store) = store();
    let from = Path::from("data/a.lance");
    let to = Path::from("data/b.lance");
    let payload = vec![5u8; super::HEAP_THRESHOLD + 1];
    store.put(&from, payload.clone().into()).await.unwrap();

    store.copy(&from, &to).await.unwrap();
    assert!(store.is_heap(&from).unwrap());
    assert!(store.is_heap(&to).unwrap());
    assert_eq!(store.get(&to).await.unwrap().bytes().await.unwrap().as_ref(), payload.as_slice());
    // The copy gets a fresh, distinct etag.
    assert_ne!(
        store.head(&from).await.unwrap().e_tag,
        store.head(&to).await.unwrap().e_tag
    );
}

/// A rename in `Create` mode refuses to clobber an existing destination.
#[tokio::test]
async fn rename_if_not_exists_rejects_existing_destination() {
    let (_dir, store) = store();
    let from = Path::from("src");
    let to = Path::from("dst");
    store.put(&from, "x".into()).await.unwrap();
    store.put(&to, "y".into()).await.unwrap();

    let err = store.rename_if_not_exists(&from, &to).await.unwrap_err();
    assert!(matches!(err, object_store::Error::AlreadyExists { .. }), "{err}");
    // The failed rename left the source in place.
    assert!(store.head(&from).await.unwrap().e_tag.is_some());
}

/// `get_ranges` clamps ranges to the object length instead of panicking, and an
/// entirely out-of-bounds range yields an empty slice.
#[tokio::test]
async fn get_ranges_clamps_to_length() {
    let (_dir, store) = store();
    let path = Path::from("blob");
    store.put(&path, "0123456789".into()).await.unwrap();

    let ranges = store.get_ranges(&path, &[8..100, 50..60]).await.unwrap();
    assert_eq!(ranges[0].as_ref(), b"89");
    assert!(ranges[1].is_empty());
}

/// A `get` whose range starts beyond the object is a `Generic` error (mapped
/// from `object_store`'s range validation), not a panic. A range whose end
/// merely exceeds the size is instead clamped to the object.
#[tokio::test]
async fn get_range_start_past_end_errors_but_long_end_is_clamped() {
    use object_store::{GetOptions, GetRange};
    let (_dir, store) = store();
    let path = Path::from("blob");
    store.put(&path, "0123456789".into()).await.unwrap();

    let start_past = GetOptions {
        range: Some(GetRange::Bounded(20..30)),
        ..Default::default()
    };
    let err = store.get_opts(&path, start_past).await.unwrap_err();
    assert!(matches!(err, object_store::Error::Generic { .. }), "{err}");

    // An end past the size is clamped, returning the available tail.
    let long_end = GetOptions {
        range: Some(GetRange::Bounded(8..100)),
        ..Default::default()
    };
    let got = store.get_opts(&path, long_end).await.unwrap().bytes().await.unwrap();
    assert_eq!(got.as_ref(), b"89");
}

/// `list_with_offset` yields only objects strictly after the offset key, in the
/// same prefix.
#[tokio::test]
async fn list_with_offset_skips_up_to_and_including_offset() {
    let (_dir, store) = store();
    for p in ["t/a", "t/b", "t/c", "t/d"] {
        store.put(&Path::from(p), "x".into()).await.unwrap();
    }
    let mut found: Vec<String> = store
        .list_with_offset(Some(&Path::from("t")), &Path::from("t/b"))
        .map(|m| m.unwrap().location.to_string())
        .collect::<Vec<_>>()
        .await;
    found.sort();
    assert_eq!(found, vec!["t/c".to_string(), "t/d".to_string()]);
}

/// `list_with_delimiter` under a nested prefix separates direct-child objects
/// from deeper common prefixes.
#[tokio::test]
async fn list_with_delimiter_under_nested_prefix() {
    let (_dir, store) = store();
    for p in ["tables/a/table.json", "tables/a/sub/data", "tables/direct"] {
        store.put(&Path::from(p), "x".into()).await.unwrap();
    }
    let res = store.list_with_delimiter(Some(&Path::from("tables/a"))).await.unwrap();
    let objects: Vec<String> = res.objects.iter().map(|m| m.location.to_string()).collect();
    let prefixes: Vec<String> = res.common_prefixes.iter().map(|p| p.to_string()).collect();
    assert_eq!(objects, vec!["tables/a/table.json".to_string()]);
    assert_eq!(prefixes, vec!["tables/a/sub".to_string()]);
}

/// A key containing a percent-encoded delimiter round-trips through listing
/// without being re-encoded or split on the encoded `/`.
#[tokio::test]
async fn encoded_delimiter_in_key_round_trips() {
    let (_dir, store) = store();
    // `Path::parse` preserves the already-encoded form; the segment is `b/c`.
    let raw = "a/b%2Fc";
    let location = Path::parse(raw).unwrap();
    store.put(&location, "x".into()).await.unwrap();

    let found: Vec<Path> = store
        .list(Some(&Path::from("a")))
        .map(|m| m.unwrap().location)
        .collect::<Vec<_>>()
        .await;
    assert_eq!(found, vec![location.clone()]);
    // Round-trips back to a readable object.
    assert_eq!(store.get(&location).await.unwrap().bytes().await.unwrap().as_ref(), b"x");
}

/// Aborting a multipart upload discards buffered parts; a subsequent complete
/// (with no parts) writes an empty object rather than the aborted bytes.
#[tokio::test]
async fn multipart_abort_discards_buffered_parts() {
    let (_dir, store) = store();
    let path = Path::from("multi");
    let mut upload = store.put_multipart(&path).await.unwrap();
    upload.put_part("discarded".into()).await.unwrap();
    upload.abort().await.unwrap();
    upload.complete().await.unwrap();

    let got = store.get(&path).await.unwrap().bytes().await.unwrap();
    assert!(got.is_empty(), "aborted parts must not be written");
}

/// Listing with no prefix returns every object; listing under a prefix that
/// matches nothing returns empty (not an error).
#[tokio::test]
async fn list_whole_store_and_empty_prefix() {
    let (_dir, store) = store();
    for p in ["x/1", "y/2", "z/3"] {
        store.put(&Path::from(p), "v".into()).await.unwrap();
    }
    let all: Vec<String> = store
        .list(None)
        .map(|m| m.unwrap().location.to_string())
        .collect::<Vec<_>>()
        .await;
    assert_eq!(all.len(), 3);

    let none: Vec<_> = store
        .list(Some(&Path::from("absent")))
        .collect::<Vec<_>>()
        .await;
    assert!(none.is_empty());
}

/// Overwriting a heap object with a small one flips its payload back to inline,
/// and the new bytes read back correctly (the stale heap blob leaks until GC).
#[tokio::test]
async fn overwrite_heap_with_inline_flips_payload_location() {
    let (_dir, store) = store();
    let path = Path::from("data/x.lance");
    store.put(&path, vec![1u8; super::HEAP_THRESHOLD + 1].into()).await.unwrap();
    assert!(store.is_heap(&path).unwrap());

    store.put(&path, "small".into()).await.unwrap();
    assert!(!store.is_heap(&path).unwrap(), "small overwrite should be inline");
    assert_eq!(store.get(&path).await.unwrap().bytes().await.unwrap().as_ref(), b"small");
}

/// Reopening a store must not reuse an etag issued before the restart: the
/// monotonic sequence counter is persisted, so a fresh put gets a strictly
/// greater etag.
#[tokio::test]
async fn etag_counter_survives_reopen() {
    let dir = TempDir::new().unwrap();
    let file = dir.path().join("beacon.db");
    let path = Path::from("k");

    let first = {
        let store = RedbStore::open(&file).unwrap();
        store.put(&path, "a".into()).await.unwrap().e_tag.unwrap()
    };
    let store = RedbStore::open(&file).unwrap();
    let second = store.put(&path, "b".into()).await.unwrap().e_tag.unwrap();
    assert_ne!(first, second);
    assert!(
        second.parse::<u64>().unwrap() > first.parse::<u64>().unwrap(),
        "etag {second} must exceed pre-restart {first}"
    );
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
