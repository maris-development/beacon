use std::sync::{Arc, OnceLock};

use arrow::array::{ArrayRef, Float64Array, Int32Array};
use arrow_schema::{DataType, Field, FieldRef};
use beacon_binary_format::collection::{CollectionReader, CollectionWriter};
use beacon_binary_format::collection_partition::{
    CollectionPartitionReadOptions, CollectionPartitionWriter, WriterOptions,
};
use beacon_binary_format::io_cache::ArrayIoCache;
use criterion::{Criterion, black_box, criterion_group, criterion_main};
use futures::{StreamExt, stream};
use nd_arrow_array::NdArrowArray;
use nd_arrow_array::dimensions::{Dimension, Dimensions};
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use tempfile::TempDir;
use tokio::runtime::Runtime;

const ENTRY_COUNT: usize = 20_000;
const MAX_GROUP_SIZE: usize = 8 * 1024 * 1024;
const ENTRY_VALUE_COUNT: usize = 10_000;
const IO_CACHE_CAPACITY: usize = 32 * 1024 * 1024;
const READ_CONCURRENCY: usize = 512;

struct CollectionFixture {
    store: Arc<dyn ObjectStore>,
    root: Path,
    temp_dir: TempDir,
}

fn local_object_store(tag: &str) -> (Arc<dyn ObjectStore>, TempDir) {
    // mkdir
    let temp_dir = tempfile::Builder::new()
        .prefix(&format!("bbf-bench-collection-{tag}-"))
        .tempdir()
        .expect("create temp dir");
    let fs = Arc::new(
        LocalFileSystem::new_with_prefix(temp_dir.path()).expect("initialize local object store"),
    );
    (fs, temp_dir)
}

fn make_field(name: &str, data_type: DataType) -> FieldRef {
    Arc::new(Field::new(name, data_type, true))
}

fn nd_from_array(array: ArrayRef) -> NdArrowArray {
    let dimension = Dimension {
        name: "dim0".to_string(),
        size: array.len(),
    };
    NdArrowArray::new(array, Dimensions::MultiDimensional(vec![dimension]))
        .expect("nd array creation")
}

fn nd_i32_payload(seed: usize) -> NdArrowArray {
    let base = seed as i32;
    let values = (0..ENTRY_VALUE_COUNT)
        .map(|offset| base + offset as i32)
        .collect::<Vec<_>>();
    let array: ArrayRef = Arc::new(Int32Array::from(values));
    nd_from_array(array)
}

fn nd_f64_payload(seed: usize) -> NdArrowArray {
    let base = seed as f64;
    let values = (0..ENTRY_VALUE_COUNT)
        .map(|offset| base * 0.1 + offset as f64 * 0.01)
        .collect::<Vec<_>>();
    let array: ArrayRef = Arc::new(Float64Array::from(values));
    nd_from_array(array)
}

async fn populate_collection(
    store: Arc<dyn ObjectStore>,
    collection_root: Path,
    entry_count: usize,
) {
    let temp_field = make_field("temp", DataType::Int32);
    let sal_field = make_field("salinity", DataType::Int32);
    let depth_field = make_field("depth", DataType::Float64);

    let mut partition_writer = CollectionPartitionWriter::new(
        collection_root.clone(),
        store.clone(),
        "partition-0".to_string(),
        WriterOptions {
            max_group_size: MAX_GROUP_SIZE,
        },
    );

    for idx in 0..entry_count {
        let entry_key = format!("entry-{idx:06}");
        let entry_stream = stream::iter(vec![
            (temp_field.clone(), nd_i32_payload(idx)),
            (sal_field.clone(), nd_i32_payload(idx + 1)),
            (depth_field.clone(), nd_f64_payload(idx)),
        ]);
        partition_writer
            .write_entry(&entry_key, entry_stream)
            .await
            .expect("write entry");
    }

    let partition_metadata = partition_writer.finish().await.expect("finish partition");

    let mut collection_writer = CollectionWriter::new(store.clone(), collection_root.clone())
        .await
        .expect("collection writer init");

    collection_writer
        .append_partition(partition_metadata)
        .expect("append partition");
    collection_writer.persist().await.expect("persist metadata");

    black_box(collection_writer.metadata().collection_num_elements);
}

async fn build_collection_fixture(entry_count: usize) -> CollectionFixture {
    let (store, temp_dir) = local_object_store("collection");
    let collection_root = Path::from("collection");
    populate_collection(store.clone(), collection_root.clone(), entry_count).await;
    CollectionFixture {
        store,
        root: collection_root,
        temp_dir,
    }
}

fn cached_collection_fixture(runtime: &Runtime, entry_count: usize) -> Arc<CollectionFixture> {
    Arc::new(runtime.block_on(build_collection_fixture(entry_count)))
}

async fn read_partition(fixture: Arc<CollectionFixture>) {
    let reader = CollectionReader::new(
        fixture.store.clone(),
        fixture.root.clone(),
        ArrayIoCache::new(IO_CACHE_CAPACITY),
    )
    .await
    .expect("collection reader init");

    let partition_reader = reader
        .partition_reader("partition-0")
        .expect("partition metadata exists");

    let scheduler = partition_reader
        .read(
            None,
            CollectionPartitionReadOptions {
                max_concurrent_reads: READ_CONCURRENCY,
            },
        )
        .await
        .expect("schedule partition read");

    let mut stream = scheduler.shared_pollable_stream_ref().await;
    while let Some(batch_result) = stream.next().await {
        let batch = batch_result.expect("batch read");
        black_box(batch);
    }
}

fn bench_collection_partition_write(c: &mut Criterion) {
    let runtime = Runtime::new().expect("tokio runtime");
    let mut group = c.benchmark_group("collection_partition_write");
    group.sample_size(10);
    group.bench_function("write_100k_entries", |b| {
        b.to_async(&runtime).iter(|| async {
            let _fixture = build_collection_fixture(ENTRY_COUNT).await;
        });
    });
    group.finish();
}

fn bench_collection_partition_read(c: &mut Criterion) {
    let runtime = Runtime::new().expect("tokio runtime");
    let fixture = cached_collection_fixture(&runtime, ENTRY_COUNT);

    let mut group = c.benchmark_group("collection_partition_read");
    group.sample_size(20);
    group.bench_function("read_100k_entries", |b| {
        let fixture = fixture.clone();
        b.to_async(&runtime).iter(|| {
            let fixture = fixture.clone();
            async move {
                read_partition(fixture).await;
            }
        });
    });
    println!(
        "Finished reading benchmark with fixture at {:?}",
        fixture.root
    );
    group.finish();
}

criterion_group!(
    collection_partition_benches,
    // bench_collection_partition_write,
    bench_collection_partition_read
);
criterion_main!(collection_partition_benches);
