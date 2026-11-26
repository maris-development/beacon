//! The block cache of the cached local store.

#[path = "support/mod.rs"]
mod support;

use std::time::Duration;

use beacon_object_store::cached_local::CachedLocalOptions;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use support::{Rng, random_file, temp_store};

/// Options with the given block size and a long revalidation interval.
fn block_opts(block_size: u64) -> CachedLocalOptions {
    CachedLocalOptions {
        block_size,
        revalidate_after: Duration::from_secs(3600),
        ..Default::default()
    }
}

#[tokio::test]
async fn block_reads_match_stock() {
    const B: u64 = 64 << 10;
    let (dir, ours, stock) = temp_store(block_opts(B));
    // Four whole blocks and a short last block.
    let data = random_file(dir.path(), "a.bin", (4 * B + 1234) as usize);
    let loc = Path::from("a.bin");
    let len = data.len() as u64;
    let ranges = [
        10..1000,              // inside one block
        B + 5..B + 60_000,     // inside one block
        B - 100..B + 100,      // across 2 blocks
        60_000..70_000,        // across 2 blocks
        B - 10..2 * B + 10,    // across 3 blocks
        len - 100..len,        // ends at EOF
        4 * B + 10..len,       // inside the short last block
        4 * B..len,            // the whole short last block
        3 * B + 100..len - 10, // across the last 2 blocks
    ];
    // The second pass reads cached blocks.
    for _ in 0..2 {
        for r in &ranges {
            let got = ours.get_range(&loc, r.clone()).await.unwrap();
            let expected = stock.get_range(&loc, r.clone()).await.unwrap();
            assert_eq!(got, expected, "{r:?}");
            assert_eq!(&got[..], &data[r.start as usize..r.end as usize], "{r:?}");
        }
        let got = ours.get_ranges(&loc, &ranges).await.unwrap();
        let expected = stock.get_ranges(&loc, &ranges).await.unwrap();
        assert_eq!(got, expected);
    }
    assert!(ours.stats().block_hits > 0);
}

#[tokio::test]
async fn large_reads_bypass_blocks() {
    const B: u64 = 64 << 10;
    let (dir, ours, _stock) = temp_store(block_opts(B));
    let data = random_file(dir.path(), "a.bin", (4 * B) as usize);
    let loc = Path::from("a.bin");
    let before = ours.stats();
    for r in [0..B, 100..B + 100, 0..3 * B] {
        let got = ours.get_range(&loc, r.clone()).await.unwrap();
        assert_eq!(&got[..], &data[r.start as usize..r.end as usize], "{r:?}");
    }
    let after = ours.stats();
    assert_eq!(after.block_misses, before.block_misses);
    assert_eq!(after.block_hits, before.block_hits);
    assert_eq!(after.disk_reads, before.disk_reads + 3);
}

#[tokio::test]
async fn repeated_reads_hit_cache() {
    let (dir, ours, _stock) = temp_store(block_opts(64 << 10));
    let data = random_file(dir.path(), "a.bin", 4 << 20);
    let loc = Path::from("a.bin");
    let mut rng = Rng::new(3);
    let ranges: Vec<_> = (0..1000)
        .map(|_| {
            let start = rng.below(data.len() as u64 - 4096);
            start..start + 4096
        })
        .collect();
    for r in &ranges {
        let got = ours.get_range(&loc, r.clone()).await.unwrap();
        assert_eq!(&got[..], &data[r.start as usize..r.end as usize]);
    }
    let first = ours.stats();
    for r in &ranges {
        let got = ours.get_range(&loc, r.clone()).await.unwrap();
        assert_eq!(&got[..], &data[r.start as usize..r.end as usize]);
    }
    let second = ours.stats();
    assert_eq!(second.disk_reads, first.disk_reads);
    assert_eq!(second.block_misses, first.block_misses);
    assert!(second.block_hits >= first.block_hits + 1000);
}

#[tokio::test]
async fn adjacent_missing_blocks_one_read() {
    let (dir, ours, _stock) = temp_store(block_opts(4096));
    let data = random_file(dir.path(), "a.bin", 20_000);
    let loc = Path::from("a.bin");
    let got = ours.get_range(&loc, 10..12000).await.unwrap();
    assert_eq!(&got[..], &data[10..12000]);
    assert_eq!(ours.stats().disk_reads, 1);
}

#[tokio::test]
async fn two_missing_blocks_one_read() {
    let (dir, ours, _stock) = temp_store(block_opts(4096));
    let data = random_file(dir.path(), "a.bin", 20_000);
    let loc = Path::from("a.bin");
    // Shorter than a block, so it goes through the block cache.
    let got = ours.get_range(&loc, 4000..4200).await.unwrap();
    assert_eq!(&got[..], &data[4000..4200]);
    let s = ours.stats();
    assert_eq!((s.disk_reads, s.block_misses, s.block_hits), (1, 2, 0));
    // Block 1 is cached, block 2 is not: one read for block 2.
    let got = ours.get_range(&loc, 8000..8300).await.unwrap();
    assert_eq!(&got[..], &data[8000..8300]);
    let s = ours.stats();
    assert_eq!((s.disk_reads, s.block_misses, s.block_hits), (2, 3, 1));
    assert_eq!(s.disk_bytes, 3 * 4096);
}

#[tokio::test]
async fn changed_file_does_not_serve_old_blocks() {
    let opts = CachedLocalOptions {
        block_size: 4096,
        revalidate_after: Duration::from_millis(100),
        ..Default::default()
    };
    let (dir, ours, _stock) = temp_store(opts);
    let p = dir.path().join("a.bin");
    std::fs::write(&p, vec![1u8; 10_000]).unwrap();
    let loc = Path::from("a.bin");
    let got = ours.get_range(&loc, 0..100).await.unwrap();
    assert!(got.iter().all(|&x| x == 1));
    // The cached handle keeps the old file open; the rename replaces the path.
    let tmp = dir.path().join("a.tmp");
    std::fs::write(&tmp, vec![2u8; 12_000]).unwrap();
    std::fs::rename(&tmp, &p).unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;
    let got = ours.get_range(&loc, 0..100).await.unwrap();
    assert_eq!(got.len(), 100);
    assert!(got.iter().all(|&x| x == 2), "old bytes");
}

#[tokio::test]
async fn block_cache_off_when_size_zero() {
    let (dir, ours, _stock) = temp_store(block_opts(0));
    let data = random_file(dir.path(), "a.bin", 100_000);
    let loc = Path::from("a.bin");
    for _ in 0..2 {
        let got = ours.get_range(&loc, 10..5000).await.unwrap();
        assert_eq!(&got[..], &data[10..5000]);
    }
    let s = ours.stats();
    assert_eq!((s.block_hits, s.block_misses), (0, 0));
    assert_eq!(s.disk_reads, 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_block_reads_match() {
    let (dir, ours, _stock) = temp_store(block_opts(4096));
    let data = std::sync::Arc::new(random_file(dir.path(), "a.bin", 256 << 10));
    let ours = std::sync::Arc::new(ours);
    let tasks: Vec<_> = (0..64)
        .map(|i| {
            let ours = ours.clone();
            let data = data.clone();
            tokio::spawn(async move {
                let loc = Path::from("a.bin");
                let mut rng = Rng::new(200 + i);
                for _ in 0..100 {
                    let start = rng.below(data.len() as u64 - 6000);
                    let r = start..start + 1 + rng.below(6000);
                    let got = ours.get_range(&loc, r.clone()).await.unwrap();
                    assert_eq!(&got[..], &data[r.start as usize..r.end as usize]);
                }
            })
        })
        .collect();
    for t in tasks {
        t.await.unwrap();
    }
    assert!(ours.stats().block_hits > 0);
}
