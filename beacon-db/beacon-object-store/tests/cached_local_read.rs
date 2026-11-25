//! The read path of the cached local store, compared with `LocalFileSystem`.

#[path = "support/mod.rs"]
mod support;

use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;

use beacon_object_store::cached_local::CachedLocalOptions;
use bytes::Bytes;
use futures::StreamExt;
use object_store::path::Path;
use object_store::{GetOptions, GetRange, GetResultPayload, ObjectStore, ObjectStoreExt};
use support::{Rng, random_file, temp_store};

/// The variant of an error, with the store name for `Generic`.
fn kind(e: &object_store::Error) -> String {
    match e {
        object_store::Error::Generic { store, .. } => format!("Generic({store})"),
        other => format!("{other:?}")
            .chars()
            .take_while(|c| c.is_alphanumeric())
            .collect(),
    }
}

fn outcome(r: object_store::Result<Bytes>) -> Result<Bytes, String> {
    r.map_err(|e| kind(&e))
}

/// Runs `get_opts` and returns the range and, unless `head`, the bytes.
async fn run_opts(
    store: &dyn ObjectStore,
    location: &Path,
    options: GetOptions,
) -> Result<(Range<u64>, Option<Bytes>), String> {
    let head = options.head;
    let r = store
        .get_opts(location, options)
        .await
        .map_err(|e| kind(&e))?;
    let range = r.range.clone();
    if head {
        return Ok((range, None));
    }
    Ok((range, Some(r.bytes().await.map_err(|e| kind(&e))?)))
}

#[tokio::test]
async fn get_range_matches_stock() {
    let (dir, ours, stock) = temp_store(CachedLocalOptions::default());
    let data = random_file(dir.path(), "a.bin", 3 << 20);
    let loc = Path::from("a.bin");
    let mut rng = Rng::new(1);
    for _ in 0..200 {
        let r = rng.range(data.len() as u64);
        let got = ours.get_range(&loc, r.clone()).await.unwrap();
        let expected = stock.get_range(&loc, r.clone()).await.unwrap();
        assert_eq!(got, expected, "{r:?}");
        assert_eq!(&got[..], &data[r.start as usize..r.end as usize]);
    }
}

#[tokio::test]
async fn get_ranges_matches_stock() {
    let (dir, ours, stock) = temp_store(CachedLocalOptions::default());
    let data = random_file(dir.path(), "a.bin", 3 << 20);
    let loc = Path::from("a.bin");
    let ranges = vec![
        2_000_000..2_100_000,
        10..20,
        0..5000,
        4000..9000,
        10..20,
        3_000_000..3_145_728,
        1_000_000..1_000_001,
        0..5000,
        2_050_000..2_060_000,
    ];
    let got = ours.get_ranges(&loc, &ranges).await.unwrap();
    let expected = stock.get_ranges(&loc, &ranges).await.unwrap();
    assert_eq!(got, expected);
    for (r, b) in ranges.iter().zip(&got) {
        assert_eq!(&b[..], &data[r.start as usize..r.end as usize], "{r:?}");
    }
}

#[tokio::test]
async fn range_at_eof_and_empty_range() {
    let (dir, ours, stock) = temp_store(CachedLocalOptions::default());
    let data = random_file(dir.path(), "a.bin", 1000);
    let loc = Path::from("a.bin");
    let len = data.len() as u64;
    for r in [len - 10..len, len..len, len - 10..len + 1, len + 5..len + 9] {
        let got = outcome(ours.get_range(&loc, r.clone()).await);
        let expected = outcome(stock.get_range(&loc, r.clone()).await);
        assert_eq!(got, expected, "get_range {r:?}");

        let got = ours
            .get_ranges(&loc, std::slice::from_ref(&r))
            .await
            .map_err(|e| kind(&e));
        let expected = stock
            .get_ranges(&loc, std::slice::from_ref(&r))
            .await
            .map_err(|e| kind(&e));
        assert_eq!(got, expected, "get_ranges {r:?}");

        let opts = GetOptions::new().with_range(Some(r.clone()));
        let got = run_opts(&ours, &loc, opts.clone()).await;
        let expected = run_opts(&stock, &loc, opts).await;
        assert_eq!(got, expected, "get_opts {r:?}");
    }
    // Stock `get_range` clamps the end, stock `get_ranges` errors.
    let past = len - 10..len + 1;
    assert_eq!(ours.get_range(&loc, past.clone()).await.unwrap().len(), 10);
    let err = ours.get_ranges(&loc, &[past]).await.unwrap_err();
    assert_eq!(kind(&err), "Generic(LocalFileSystem)", "{err}");
}

#[tokio::test]
async fn full_get_streams_in_chunks() {
    let opts = CachedLocalOptions {
        stream_chunk: 8 << 20,
        ..Default::default()
    };
    let (dir, ours, _stock) = temp_store(opts);
    let data = random_file(dir.path(), "a.bin", 20 << 20);
    let result = ours.get(&Path::from("a.bin")).await.unwrap();
    assert_eq!(result.range, 0..data.len() as u64);
    let GetResultPayload::Stream(mut stream) = result.payload else {
        panic!("payload is not a stream");
    };
    let mut all = Vec::new();
    let mut chunks = 0;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.unwrap();
        assert!(chunk.len() <= 8 << 20, "{}", chunk.len());
        all.extend_from_slice(&chunk);
        chunks += 1;
    }
    assert_eq!(chunks, 3);
    assert!(all == data);
}

#[tokio::test]
async fn get_options_match_stock() {
    let (dir, ours, stock) = temp_store(CachedLocalOptions::default());
    let data = random_file(dir.path(), "a.bin", 5000);
    let loc = Path::from("a.bin");
    let len = data.len() as u64;
    let modified = stock.head(&loc).await.unwrap().last_modified;
    let cases = [
        GetOptions::new().with_head(true),
        GetOptions::new().with_range(Some(GetRange::Offset(100))),
        GetOptions::new().with_range(Some(GetRange::Offset(len))),
        GetOptions::new().with_range(Some(GetRange::Suffix(100))),
        GetOptions::new().with_range(Some(GetRange::Suffix(len + 100))),
        GetOptions::new().with_if_modified_since(Some(modified)),
        GetOptions::new().with_if_modified_since(Some(modified - Duration::from_secs(1))),
        GetOptions::new().with_if_unmodified_since(Some(modified - Duration::from_secs(1))),
        GetOptions::new().with_if_match(Some("bogus")),
        GetOptions::new().with_if_none_match(Some("bogus")),
    ];
    for opts in cases {
        let got = run_opts(&ours, &loc, opts.clone()).await;
        let expected = run_opts(&stock, &loc, opts.clone()).await;
        assert_eq!(got, expected, "{opts:?}");
    }
    // Each store checks ETags against its own ETag.
    for store in [&ours as &dyn ObjectStore, &stock] {
        let tag = store.head(&loc).await.unwrap().e_tag.unwrap();
        let opts = GetOptions::new().with_if_match(Some(tag.clone()));
        let (_, bytes) = run_opts(store, &loc, opts).await.unwrap();
        assert_eq!(&bytes.unwrap()[..], &data[..]);
        let opts = GetOptions::new().with_if_none_match(Some(tag));
        assert_eq!(
            run_opts(store, &loc, opts).await.unwrap_err(),
            "NotModified",
            "{store}"
        );
    }
}

#[tokio::test]
async fn head_matches_stock() {
    let (dir, ours, stock) = temp_store(CachedLocalOptions::default());
    random_file(dir.path(), "a.bin", 5000);
    let loc = Path::from("a.bin");
    let got = ours.head(&loc).await.unwrap();
    let expected = stock.head(&loc).await.unwrap();
    assert_eq!(got.location, expected.location);
    assert_eq!(got.last_modified, expected.last_modified);
    assert_eq!(got.size, expected.size);
    assert_eq!(got.version, expected.version);
    let stock_tag = expected.e_tag.unwrap();
    let (_, mtime_size) = stock_tag.split_once('-').unwrap();
    assert_eq!(got.e_tag.unwrap(), format!("0-{mtime_size}"));
}

#[tokio::test]
async fn missing_file_is_not_found() {
    let (_dir, ours, _stock) = temp_store(CachedLocalOptions::default());
    let loc = Path::from("missing.bin");
    let err = ours.get(&loc).await.unwrap_err();
    assert_eq!(kind(&err), "NotFound", "{err}");
    let err = ours.get_range(&loc, 0..10).await.unwrap_err();
    assert_eq!(kind(&err), "NotFound", "{err}");
    let err = ours.head(&loc).await.unwrap_err();
    assert_eq!(kind(&err), "NotFound", "{err}");
}

#[tokio::test]
async fn one_open_for_many_reads() {
    let (dir, ours, _stock) = temp_store(CachedLocalOptions::default());
    let data = random_file(dir.path(), "a.bin", 100_000);
    let loc = Path::from("a.bin");
    let mut rng = Rng::new(2);
    for _ in 0..500 {
        let r = rng.range(data.len() as u64);
        ours.get_range(&loc, r).await.unwrap();
    }
    assert_eq!(ours.stats().opens, 1);
    assert_eq!(ours.stats().disk_reads, 500);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_reads_share_handle() {
    let (dir, ours, _stock) = temp_store(CachedLocalOptions::default());
    let data = Arc::new(random_file(dir.path(), "a.bin", 1 << 20));
    let ours = Arc::new(ours);
    let tasks: Vec<_> = (0..64)
        .map(|i| {
            let ours = ours.clone();
            let data = data.clone();
            tokio::spawn(async move {
                let loc = Path::from("a.bin");
                let mut rng = Rng::new(100 + i);
                for _ in 0..50 {
                    let r = rng.range(data.len() as u64);
                    let got = ours.get_range(&loc, r.clone()).await.unwrap();
                    assert_eq!(&got[..], &data[r.start as usize..r.end as usize]);
                }
            })
        })
        .collect();
    for t in tasks {
        t.await.unwrap();
    }
    assert_eq!(ours.stats().opens, 1);
}

#[tokio::test]
async fn deleted_file_read_is_not_found() {
    let opts = CachedLocalOptions {
        revalidate_after: Duration::from_millis(50),
        ..Default::default()
    };
    let (dir, ours, _stock) = temp_store(opts);
    random_file(dir.path(), "a.bin", 5000);
    let loc = Path::from("a.bin");
    ours.get_range(&loc, 0..10).await.unwrap();
    std::fs::remove_file(dir.path().join("a.bin")).unwrap();
    tokio::time::sleep(Duration::from_millis(60)).await;
    let err = ours.get_range(&loc, 0..10).await.unwrap_err();
    assert_eq!(kind(&err), "NotFound", "{err}");
}

#[tokio::test]
async fn file_that_shrank_reads_as_stock() {
    for block_size in [0, 4096] {
        let (dir, ours, stock) = temp_store(CachedLocalOptions {
            block_size,
            revalidate_after: Duration::from_secs(3600),
            ..Default::default()
        });
        let data = random_file(dir.path(), "a.bin", 10_000);
        let loc = Path::from("a.bin");
        // The cached handle keeps the old size of 10000 bytes.
        ours.get_range(&loc, 0..10).await.unwrap();
        std::fs::OpenOptions::new()
            .write(true)
            .open(dir.path().join("a.bin"))
            .unwrap()
            .set_len(5_000)
            .unwrap();

        let got = ours.get_range(&loc, 4_000..6_000).await;
        assert_eq!(
            outcome(got),
            outcome(stock.get_range(&loc, 4_000..6_000).await),
            "block_size {block_size}"
        );
        assert_eq!(
            &ours.get_range(&loc, 4_000..4_500).await.unwrap()[..],
            &data[4_000..4_500]
        );
        let ours_ranges = ours.get_ranges(&loc, &[100..200, 4_900..5_100]).await;
        let stock_ranges = stock.get_ranges(&loc, &[100..200, 4_900..5_100]).await;
        assert_eq!(
            ours_ranges.map_err(|e| kind(&e)),
            stock_ranges.map_err(|e| kind(&e)),
            "block_size {block_size}"
        );
    }
}
