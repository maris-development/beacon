use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::array::Int32Array;
use arrow::record_batch::RecordBatch;
use arrow_schema::{DataType, Field, Schema};
use beacon_binary_format::io_cache::{ArrayIoCache, CacheKey};
use criterion::{Criterion, black_box, criterion_group, criterion_main};
use object_store::path::Path;

fn build_batch() -> RecordBatch {
    let field = Field::new("values", DataType::Int32, false);
    let schema = Arc::new(Schema::new(vec![field]));
    let values = Arc::new(Int32Array::from_iter(0..1024));
    RecordBatch::try_new(schema, vec![values]).expect("record batch creation")
}

fn bench_cache_hits(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().expect("runtime");
    let cache = Arc::new(ArrayIoCache::new(10 * 1024 * 1024));
    let key = CacheKey {
        array_partition_path: Path::from("bench/partition.arrow"),
        group_index: 0,
    };
    let batch = Arc::new(build_batch());
    let warm_batch = batch.clone();

    // Warm the cache so subsequent iterations hit the stored batch.
    runtime
        .block_on(cache.try_get_or_insert_with(key.clone(), move |_key| {
            let batch = warm_batch.clone();
            async move { Ok(Some(batch.as_ref().clone())) }
        }))
        .expect("warm cache");

    c.bench_function("io_cache_hit", |b| {
        let cache = cache.clone();
        let key = key.clone();
        b.to_async(&runtime).iter(move || {
            let cache = cache.clone();
            let key = key.clone();
            async move {
                let result = cache
                    .try_get_or_insert_with(key.clone(), |_key| async move {
                        unreachable!("loader should not run on hits")
                    })
                    .await
                    .expect("cache lookup");
                black_box(result);
            }
        });
    });
}

fn bench_cache_misses(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().expect("runtime");
    let cache = Arc::new(ArrayIoCache::new(10 * 1024 * 1024));
    let counter = Arc::new(AtomicUsize::new(0));
    let batch = Arc::new(build_batch());

    c.bench_function("io_cache_miss", |b| {
        let batch = batch.clone();
        let cache = cache.clone();
        let counter = counter.clone();
        b.to_async(&runtime).iter(move || {
            let batch = batch.clone();
            let cache = cache.clone();
            let counter = counter.clone();
            let idx = counter.fetch_add(1, Ordering::Relaxed);
            let key = CacheKey {
                array_partition_path: Path::from(format!("bench/partition_{idx}.arrow")),
                group_index: 0,
            };
            async move {
                let loader_batch = batch.clone();
                let result = cache
                    .try_get_or_insert_with(key, move |_key| {
                        let batch = loader_batch.clone();
                        async move { Ok(Some(batch.as_ref().clone())) }
                    })
                    .await
                    .expect("cache insert");
                black_box(result);
            }
        });
    });
}

criterion_group!(io_cache_benches, bench_cache_hits, bench_cache_misses);
criterion_main!(io_cache_benches);
