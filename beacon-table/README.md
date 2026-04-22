# Beacon Table

A manifest-managed, append-only table format backed by Apache Parquet and integrated with [Apache DataFusion](https://datafusion.apache.org/).

## Overview

`beacon-table` provides `BeaconTable`, a DataFusion `TableProvider` that stores tabular data as a collection of Parquet files managed by a lightweight JSON manifest. It is designed for analytical workloads where data is inserted in bulk and queried frequently.

### Key Features

- **Append-only inserts** — new data is written to a fresh Parquet file per insert and registered in the manifest.
- **Full-table scans** — with optional column projection and limit push-down.
- **Vacuum / compaction** — merges all data files into a single Parquet file, updates the manifest, and deletes the old files.
- **Optimistic concurrency control** — a table-level mutation lock and manifest schema-version checks prevent concurrent mutations from corrupting data.
- **Object-store agnostic** — works with any `object_store` backend (local filesystem, S3, GCS, Azure Blob, in-memory).

## Storage Layout

```
<table_directory>/
    manifest.json                 # Table metadata (schema, file list, indexes)
    data/
        <uuid-1>.parquet          # Data file from first insert
        <uuid-2>.parquet          # Data file from second insert
        ...
```

### Manifest (`manifest.json`)

The manifest is a JSON file that tracks:

| Field            | Description                                                            |
|------------------|------------------------------------------------------------------------|
| `schema`         | Arrow schema shared by all data files                                  |
| `schema_version` | Monotonically increasing version; used for concurrency detection       |
| `data_files`     | Ordered list of data file entries (Parquet path + deletion vectors)     |
| `z_order_index`  | Optional Z-order index configuration (future)                         |

Each data file entry contains:

| Field                    | Description                                          |
|--------------------------|------------------------------------------------------|
| `parquet_file`           | Relative path to the Parquet data file               |
| `deletion_vector_files`  | Paths to deletion vector files (future delete support)|

## Usage

### Creating a Table

```rust
use beacon_table::BeaconTable;
use datafusion::execution::object_store::ObjectStoreUrl;
use object_store::memory::InMemory;
use std::sync::Arc;

let store = Arc::new(InMemory::new());
let url = ObjectStoreUrl::parse("memory://")?;

let table = BeaconTable::new(
    store,
    url,
    "my_table".into(),
    "my_table".into(),
    Some(my_schema),  // None to load from existing manifest
).await?;
```

### Inserting Data

`BeaconTable` implements `TableProvider::insert_into`, so it can be used with DataFusion's standard SQL or DataFrame insert APIs. Only `InsertOp::Append` is supported.

Under the hood, each insert:

1. Writes a new `<uuid>.parquet` file to `<table_dir>/data/`.
2. Acquires the mutation lock.
3. Validates the manifest hasn't changed (optimistic concurrency check).
4. Appends the new `DataFile` entry to the manifest and flushes it.

### Scanning Data

```rust
// Via DataFusion SQL after registering the table:
ctx.register_table("my_table", Arc::new(table))?;
let df = ctx.sql("SELECT * FROM my_table WHERE id > 100").await?;
```

Scans support:

- **Projection** — only the requested columns are read from Parquet.
- **Limit** — a `GlobalLimitExec` is pushed on top of the scan plan.
- **Multi-file** — when multiple data files exist, each is scanned independently and concatenated via `AppendExec`.

### Vacuuming (Compaction)

```rust
let plan = table.vacuum(&session_state).await?;
let results = datafusion::physical_plan::collect(plan, ctx.task_ctx()).await?;
// results contains a single batch with a `count` column (rows compacted)
```

Vacuum:

1. Reads all existing data into a single stream.
2. Writes it to one new Parquet file.
3. Replaces the manifest's file list with the single new file.
4. Deletes old data files and deletion vectors (best-effort).

## Concurrency Model

All mutating operations (insert, vacuum) follow this protocol:

1. **Acquire** the table-level `mutation_handle` mutex (serializes writers).
2. **Validate** that `schema_version` has not changed since the operation was planned (detects interleaved mutations).
3. **Write** data to the object store.
4. **Update and flush** the manifest atomically.

This provides serializable isolation for writers while allowing concurrent readers.

## Architecture

| Module                   | Description                                              |
|--------------------------|----------------------------------------------------------|
| `lib.rs`                 | `BeaconTable`, `TableProvider` impl, `vacuum()` method   |
| `manifest.rs`            | `TableManifest`, `DataFile`, load/flush functions         |
| `insert_exec.rs`         | `InsertSink` — DataFusion `DataSink` for inserts          |
| `vacuum_exec.rs`         | `VacuumExec` — `ExecutionPlan` for compaction             |
| `append_exec.rs`         | `AppendExec` — concatenates multi-file scan streams       |
| `deletion_vector_exec.rs`| `DeletionVectorExec` — row filtering via boolean masks (future) |
| `index_exec.rs`          | `IndexType` enum and `IndexExec` placeholder (future)     |

## Status

| Feature             | Status         |
|---------------------|----------------|
| Append insert       | ✅ Implemented |
| Scan with projection| ✅ Implemented |
| Scan with limit     | ✅ Implemented |
| Vacuum / compaction | ✅ Implemented |
| Deletion vectors    | 🚧 Planned     |
| Z-order indexing    | 🚧 Planned     |
| Schema evolution    | 🚧 Planned     |
