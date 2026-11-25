//! Writes, listings and outside changes seen through the cached local store.

#[path = "support/mod.rs"]
mod support;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use beacon_object_store::cached_local::{CachedLocalFileSystem, CachedLocalOptions};
use futures::TryStreamExt;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use support::{random_file, temp_store};

/// Block cache on, and no revalidation during a test.
fn opts() -> CachedLocalOptions {
    CachedLocalOptions {
        block_size: 4096,
        revalidate_after: Duration::from_secs(3600),
        ..Default::default()
    }
}

/// Reads the whole object at `name`.
async fn read_all(store: &CachedLocalFileSystem, name: &str) -> Vec<u8> {
    store
        .get(&Path::from(name))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap()
        .to_vec()
}

fn is_not_found(e: &object_store::Error) -> bool {
    matches!(e, object_store::Error::NotFound { .. })
}

/// Writes `data` to a temporary file and renames it over `path`, outside the store.
fn replace(path: &std::path::Path, data: &[u8]) {
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, data).unwrap();
    std::fs::rename(&tmp, path).unwrap();
}

#[tokio::test]
async fn put_shows_at_once() {
    let (dir, ours, _) = temp_store(opts());
    random_file(dir.path(), "a.bin", 10_000);
    read_all(&ours, "a.bin").await;
    let new = vec![5u8; 10_000];
    ours.put(&Path::from("a.bin"), new.clone().into())
        .await
        .unwrap();
    assert_eq!(read_all(&ours, "a.bin").await, new);
}

#[tokio::test]
async fn multipart_put_shows_at_once() {
    let (dir, ours, _) = temp_store(opts());
    random_file(dir.path(), "a.bin", 10_000);
    read_all(&ours, "a.bin").await;
    let mut upload = ours.put_multipart(&Path::from("a.bin")).await.unwrap();
    upload.put_part(vec![1u8; 6000].into()).await.unwrap();
    upload.put_part(vec![2u8; 4000].into()).await.unwrap();
    upload.complete().await.unwrap();
    let got = read_all(&ours, "a.bin").await;
    assert_eq!(&got[..6000], &[1u8; 6000][..]);
    assert_eq!(&got[6000..], &[2u8; 4000][..]);
}

#[tokio::test]
async fn delete_shows_at_once() {
    let (dir, ours, _) = temp_store(opts());
    random_file(dir.path(), "a.bin", 10_000);
    read_all(&ours, "a.bin").await;
    ours.delete(&Path::from("a.bin")).await.unwrap();
    let err = ours.get(&Path::from("a.bin")).await.unwrap_err();
    assert!(is_not_found(&err), "{err}");
}

#[tokio::test]
async fn rename_and_copy_show_at_once() {
    let (dir, ours, _) = temp_store(opts());
    random_file(dir.path(), "to.bin", 10_000);
    std::fs::write(dir.path().join("from.bin"), vec![3u8; 7000]).unwrap();
    std::fs::write(dir.path().join("src.bin"), vec![4u8; 9000]).unwrap();
    read_all(&ours, "to.bin").await;
    read_all(&ours, "from.bin").await;
    ours.rename(&Path::from("from.bin"), &Path::from("to.bin"))
        .await
        .unwrap();
    assert_eq!(read_all(&ours, "to.bin").await, vec![3u8; 7000]);
    let err = ours.get(&Path::from("from.bin")).await.unwrap_err();
    assert!(is_not_found(&err), "{err}");
    ours.copy(&Path::from("src.bin"), &Path::from("to.bin"))
        .await
        .unwrap();
    assert_eq!(read_all(&ours, "to.bin").await, vec![4u8; 9000]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn read_racing_put_never_mixes() {
    const LEN: usize = 64 << 10;
    let o = CachedLocalOptions {
        // Revalidate often, so that reads see both versions.
        revalidate_after: Duration::from_millis(1),
        ..opts()
    };
    let (dir, ours, _) = temp_store(o);
    // Same length, so that only the mtime tells the versions apart.
    let a = vec![b'A'; LEN];
    let b = vec![b'B'; LEN];
    std::fs::write(dir.path().join("a.bin"), &a).unwrap();
    let ours = Arc::new(ours);
    let done = Arc::new(AtomicBool::new(false));
    let readers: Vec<_> = (0..32)
        .map(|i| {
            let ours = ours.clone();
            let done = done.clone();
            // Half read past the block size, half read two cached blocks.
            let range = if i % 2 == 0 {
                0..LEN as u64
            } else {
                4000..5000
            };
            tokio::spawn(async move {
                let mut seen = [0usize; 2];
                while !done.load(Ordering::Relaxed) {
                    let got = match ours.get_range(&Path::from("a.bin"), range.clone()).await {
                        Ok(got) => got,
                        // Windows opens can miss a file during a rename over it; the stock store too.
                        Err(object_store::Error::NotFound { .. }) if cfg!(windows) => continue,
                        Err(e) => panic!("{e}"),
                    };
                    match got[0] {
                        b'A' => seen[0] += 1,
                        b'B' => seen[1] += 1,
                        other => panic!("unexpected byte {other}"),
                    }
                    assert!(got.iter().all(|&x| x == got[0]), "mixed read");
                }
                seen
            })
        })
        .collect();
    let start = Instant::now();
    let mut puts = 0;
    while start.elapsed() < Duration::from_secs(2) {
        let data = if puts % 2 == 0 { &b } else { &a };
        ours.put(&Path::from("a.bin"), PutPayload::from(data.clone()))
            .await
            .unwrap();
        puts += 1;
        tokio::task::yield_now().await;
    }
    done.store(true, Ordering::Relaxed);
    let mut seen = [0usize; 2];
    for r in readers {
        let s = r.await.unwrap();
        seen[0] += s[0];
        seen[1] += s[1];
    }
    assert!(puts > 10, "{puts} puts");
    assert!(seen[0] > 0 && seen[1] > 0, "{seen:?}");
}

#[tokio::test]
async fn listing_evicts_changed_file() {
    let (dir, ours, _) = temp_store(opts());
    random_file(dir.path(), "a.bin", 10_000);
    read_all(&ours, "a.bin").await;
    // An unchanged file keeps its handle.
    let _: Vec<_> = ours.list(None).try_collect().await.unwrap();
    read_all(&ours, "a.bin").await;
    assert_eq!(ours.stats().opens, 1);
    let new: Vec<u8> = (0..20_000u32).map(|i| (i % 251) as u8).collect();
    replace(&dir.path().join("a.bin"), &new);
    let listed: Vec<_> = ours.list(None).try_collect().await.unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].size, 20_000);
    // Without the eviction by the listing, the cached handle would answer.
    assert_eq!(ours.head(&Path::from("a.bin")).await.unwrap().size, 20_000);
    let got = ours
        .get_range(&Path::from("a.bin"), 5000..15_000)
        .await
        .unwrap();
    assert_eq!(&got[..], &new[5000..15_000]);
}

#[tokio::test]
async fn listing_hides_nfs_files() {
    let (dir, ours, _) = temp_store(opts());
    std::fs::create_dir(dir.path().join("sub")).unwrap();
    std::fs::create_dir(dir.path().join(".nfsdir")).unwrap();
    for name in [".nfs1234", "sub/.nfs99", "data.nfs.nc", ".nfsdir/x.bin"] {
        std::fs::write(dir.path().join(name), b"x").unwrap();
    }
    let names = |metas: Vec<object_store::ObjectMeta>| {
        let mut v: Vec<String> = metas.into_iter().map(|m| m.location.to_string()).collect();
        v.sort();
        v
    };
    let listed = names(ours.list(None).try_collect().await.unwrap());
    // A file inside a `.nfs` directory keeps its own name.
    assert_eq!(listed, vec![".nfsdir/x.bin", "data.nfs.nc"]);
    let listed = names(
        ours.list_with_offset(None, &Path::from(".a"))
            .try_collect()
            .await
            .unwrap(),
    );
    assert_eq!(listed, vec![".nfsdir/x.bin", "data.nfs.nc"]);
    let sub: Vec<_> = ours
        .list(Some(&Path::from("sub")))
        .try_collect()
        .await
        .unwrap();
    assert!(sub.is_empty(), "{sub:?}");
    let result = ours.list_with_delimiter(None).await.unwrap();
    assert_eq!(names(result.objects), vec!["data.nfs.nc"]);
    let prefixes: Vec<String> = result
        .common_prefixes
        .iter()
        .map(|p| p.to_string())
        .collect();
    assert_eq!(prefixes, vec!["sub"]);
    let result = ours
        .list_with_delimiter(Some(&Path::from("sub")))
        .await
        .unwrap();
    assert!(result.objects.is_empty(), "{:?}", result.objects);
}

#[tokio::test]
async fn file_that_grew_reads_at_once() {
    let (dir, ours, _) = temp_store(opts());
    let mut data = random_file(dir.path(), "a.bin", 10_000);
    read_all(&ours, "a.bin").await;
    let more = vec![9u8; 5000];
    {
        use std::io::Write;
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(dir.path().join("a.bin"))
            .unwrap();
        f.write_all(&more).unwrap();
    }
    data.extend_from_slice(&more);
    let got = ours
        .get_range(&Path::from("a.bin"), 8000..15_000)
        .await
        .unwrap();
    assert_eq!(&got[..], &data[8000..15_000]);
}
