# beacon-binary-format

Rust primitives for writing and reading the Beacon binary format. The crate exposes
modular building blocks—array partition writers/readers, collection metadata
orchestration, IO caching, and async scheduling—that higher-level services can compose
into ingestion or query pipelines.

## Capabilities

- **Array partitions** – Stream `NdArrowArray` batches into compressed Arrow IPC blobs
  and lazily read them back with pruning index support (`array_partition`).
- **Collection metadata** – Persist `bbf.json` files that track every collection
  partition, enforce schema compatibility, and surface partition readers
  (`collection`, `collection_partition`).
- **Partition groups** – Aggregate multiple partitions belonging to the same logical
  array while keeping cumulative statistics and type promotion logic
  (`array_partition_group`).
- **IO cache** – Deduplicate expensive Arrow IPC fetches via an async LRU cache
  (`io_cache`).
- **Async scheduling** – Bound concurrent entry reads using a cooperative scheduler
  (`stream`).
- **Utilities** – Common helpers for Arrow super-type resolution and metadata
  serialization (`util`).

## Getting Started

Add the crate to your workspace (already part of the monorepo) and enable the
workspace dependencies declared in `Cargo.toml`. All functionality is available on
stable Rust.

### Writing an array partition

```rust
use std::sync::Arc;
use arrow_schema::{DataType, Field};
use beacon_binary_format::array_partition::ArrayPartitionWriter;
use nd_arrow_array::NdArrowArray;
use object_store::{memory::InMemory, path::Path};

# async fn demo() -> beacon_binary_format::error::BBFResult<()> {
let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
let array_path = Path::from("demo/temp");
let mut writer = ArrayPartitionWriter::new(
    store.clone(),
    array_path,
    "temp".to_string(),
    8 * 1024 * 1024,
    Some(DataType::Float64),
    0,
)
.await?;

let temp_field = Field::new("temp", DataType::Float64, true);
let values = NdArrowArray::from_arrow_array(temp_field.data_type().clone(), vec![1.0, 2.0])?;
writer.append_array(Some(values)).await?;
let metadata = writer.finish().await?; // Uploads IPC file + pruning index
# Ok(())
# }
```

### Reading a collection partition

```rust
use beacon_binary_format::collection_partition::{
    CollectionPartitionReader, CollectionPartitionReadOptions
};
use beacon_binary_format::io_cache::ArrayIoCache;
use object_store::{memory::InMemory, path::Path};

# async fn read_partition_example(meta: beacon_binary_format::collection_partition::CollectionPartitionMetadata) -> beacon_binary_format::error::BBFResult<()> {
let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
let partition_root = Path::from("collections/demo/partition-0");
let reader = CollectionPartitionReader::new(
    partition_root,
    store,
    meta,
    ArrayIoCache::new(64 * 1024 * 1024),
);

let scheduler = reader
    .read(None, CollectionPartitionReadOptions { max_concurrent_reads: 32 })
    .await?;
let mut stream = scheduler.shared_pollable_stream_ref().await;
while let Some(batch) = stream.next().await {
    let batch = batch?;
    // process NdRecordBatch for each logical entry
}
# Ok(())
# }
```

## Development

- **Tests**: `cargo test -p beacon-binary-format`
- **Benchmarks**: `cargo bench -p beacon-binary-format collection_partition`
- **Linting**: The workspace relies on `cargo fmt` / `cargo clippy` run from the repo root.

### Repository layout

```
beacon-binary-format/
├── benches/
│   ├── collection_partition.rs   # Criterion benchmarks for collection IO
│   └── io_cache.rs               # Cache hit/miss microbenchmarks
├── src/
│   ├── array_group.rs            # NdArrowArray ↔ Arrow record batch glue
│   ├── array_partition*.rs       # Array partition readers/writers/indexes
│   ├── collection*.rs            # Collection + partition orchestration
│   ├── io_cache.rs               # Async LRU cache wrapper
│   ├── stream.rs                 # Async scheduler helper
│   └── util.rs                   # Arrow utilities
└── Cargo.toml
```

## Design Notes

- The crate deliberately stays low-level: it only assumes access to an Arrow-compatible
  object store and leaves higher-level orchestration (retry, logging, metrics) to
  integrating services.
- Metadata files (`bbf.json`, `apg.json`) are JSON for debuggability. Arrow IPC blobs
  are compressed with ZSTD by default, but writers gracefully fall back to
  uncompressed IPC when required by the environment.
- `ArrayIoCache` uses `moka` under the hood; tune cache size and eviction policies at
  construction time to match workload characteristics.

## License

This crate inherits the workspace license; see the top-level `LICENSE` file.
