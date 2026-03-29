# beacon-atlas

`beacon-atlas` is a partitioned, Arrow-native storage layer for n-dimensional dataset columns on top of `object_store`.

It is designed for:

- Writing dataset-oriented partitions to cloud/local object stores
- Reading per-dataset slices lazily through Arrow IPC range reads
- Streaming datasets without loading whole partitions in memory
- Maintaining column statistics for pruning/alignment
- Soft-deleting datasets via partition entry flags
- Recasting existing columns to different Arrow data types

This document explains both **how to use it** and **how it works internally**.

---

## Contents

- [Mental model](#mental-model)
- [Public API surface](#public-api-surface)
- [Quickstart](#quickstart)
- [Core workflows](#core-workflows)
	- [Create/open a collection](#createopen-a-collection)
	- [Create a partition and write datasets](#create-a-partition-and-write-datasets)
	- [Read datasets by index](#read-datasets-by-index)
	- [Stream datasets](#stream-datasets)
	- [Read aligned column statistics](#read-aligned-column-statistics)
	- [Soft-delete datasets](#soft-delete-datasets)
	- [Cast columns in-place](#cast-columns-in-place)
- [Storage format (on object store)](#storage-format-on-object-store)
- [Internal architecture](#internal-architecture)
- [Schema behavior and super-typing](#schema-behavior-and-super-typing)
- [Type support and constraints](#type-support-and-constraints)
- [Operational caveats](#operational-caveats)
- [API reference by module](#api-reference-by-module)

---

## Mental model

At a high level:

- A **Collection** is a logical namespace with metadata (`atlas.json`) and a list of partitions.
- A **Partition** stores datasets as column-wise files plus partition metadata (`atlas_partition.json`) and entries (`entries.arrow`).
- A **Dataset** is identified inside a partition by `dataset_index` and `dataset_name` (`__entry_key`).
- A **Column** is persisted as three files:
	- `array.arrow` (flat values in Arrow IPC batches)
	- `layout.arrow` (where each dataset’s values are located in the flat value stream)
	- `statistics.arrow` (min/max/null_count/row_count per dataset)

The read path uses layout metadata + Arrow IPC block range reads to lazily reconstruct dataset arrays.

---

## Public API surface

The crate prelude exports the common user-facing types:

- `Dataset`
- `AtlasCollection`, `AtlasCollectionState`, `CollectionMetadata`, `CollectionPartitionWriter`
- `Column`
- `AtlasSchema`, `AtlasColumn`, `AtlasSuperTypingMode`

Additional operations are under partition ops:

- `partition::ops::write::PartitionWriter`
- `partition::ops::read::ReaderBuilder`
- `partition::ops::stream_read::PartitionStreamReaderBuilder`
- `partition::ops::statistics::PartitionStatisticsBuilder`
- `partition::ops::delete::DeleteDatasetsPartition`
- `partition::ops::cast::CastPartition`

---

## Quickstart

```rust
use std::sync::Arc;

use beacon_atlas::prelude::*;
use beacon_atlas::partition::ops::read::ReaderBuilder;
use beacon_atlas::schema::AtlasSuperTypingMode;
use futures::stream;
use object_store::{memory::InMemory, ObjectStore, path::Path};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
		let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
		let collection_path = Path::from("collections/demo");

		let mut collection = AtlasCollection::create(
				store.clone(),
				collection_path,
				"demo",
				Some("demo collection".to_string()),
				AtlasSuperTypingMode::General,
		).await?;

		let mut partition_writer = collection
				.create_partition("part-00000", Some("first partition"))
				.await?;

		partition_writer
				.writer_mut()?
				.write_dataset_columns(
						"dataset-0",
						stream::iter(vec![
								Column::new_from_vec(
										"temperature".to_string(),
										vec![10.2_f32, 10.8, 11.0],
										vec![3],
										vec!["x".to_string()],
										None,
								)?,
						]),
				)
				.await?;

		let partition = partition_writer.finish().await?;

		let dataset = ReaderBuilder::new(store.clone(), partition).dataset(0).await?;
		println!("dataset has {} arrays", dataset.0.arrays.len());

		Ok(())
}
```

---

## Core workflows

### Create/open a collection

Use one of:

- `AtlasCollection::create(...)` to initialize a new collection and metadata
- `AtlasCollection::open(...)` to load existing metadata + partition schemas

State/snapshot behavior:

- `snapshot()` returns loaded collection state
- `arrow_schema()` returns merged partition schema based on `AtlasSuperTypingMode`
- If a scoped `CollectionPartitionWriter` is dropped before `finish()`, state is marked stale and should be reloaded (`load` / `update_state`)

### Create a partition and write datasets

Options:

- High-level: `AtlasCollection::create_partition(...) -> CollectionPartitionWriter`
- Low-level: construct `PartitionWriter` directly

Writing:

- `PartitionWriter::write_dataset(...)` writes a `Dataset`
- `PartitionWriter::write_dataset_columns(...)` writes stream of `Column`
- Dataset indexes are assigned sequentially (`u32`) in write order
- Reserved `__entry_key` column is written automatically

Finalize:

- `finish()` writes partition metadata + all column files + entries file, then returns a loaded `Partition`

### Read datasets by index

Use:

- `ReaderBuilder::new(object_store, partition)`
- `dataset(index)` returns a read dataset as `partition::ops::read::Dataset`

Notes:

- Reader initializes column readers lazily (via `tokio::sync::OnceCell`)
- If some columns are missing for a specific dataset index, returned schema is sparse (`new_unaligned` path)

### Stream datasets

Use:

- `PartitionStreamReaderBuilder::new(object_store, Arc<Partition>)`
- Optional: `.with_dataset_indexes(...)`, `.with_io_cache(...)`, `.with_projection(...)`
- Terminal: `.create_shareable_stream(buffer_size)`
- Read with `.stream_ref()` iterator

Default behavior:

- Starts from undeleted dataset indexes (`partition.undeleted_dataset_indexes()`)
- Multiple clones of `ShareableCollectionStream` can consume the shared queue

### Read aligned column statistics

Use:

- `PartitionStatisticsBuilder::new(object_store, Arc<Partition>)`
- Optional: `.with_projection(...)`, `.with_io_cache(...)`
- Terminal: `.read_statistics_batches()`

Returned batches are aligned to undeleted dataset indexes:

- If a column statistics file is missing, a synthetic all-null stats batch is generated
- If a dataset is absent in a column stats batch, null rows are inserted for alignment

### Soft-delete datasets

Use:

- `DeleteDatasetsPartition::new(object_store, partition)`
- Chain selectors: `.delete_dataset("name")`, `.delete_dataset_index(idx)`
- Execute with `.execute()`

Behavior:

- Updates deletion flags in `entries.arrow`
- Does **not** physically remove column data files (soft delete)
- Returns reloaded partition state

### Cast columns in-place

Use:

- `CastPartition::new(object_store, partition)`
- Queue casts with `.cast_column("col", target_arrow_type)` (can call multiple times)
- Execute with `.execute()`

Behavior:

- Rewrites each targeted column file with casted values
- Uses safe Arrow casting (`CastOptions { safe: true }`)
- Updates partition schema metadata
- Rejects casting reserved `__entry_key`

---

## Storage format (on object store)

Collection layout:

```text
<collection_dir>/
	atlas.json
	partitions/
		<partition_name>/
			atlas_partition.json
			entries.arrow
			columns/
				<column_path>/
					array.arrow
					layout.arrow
					statistics.arrow
```

### `atlas.json`

Serialized `CollectionMetadata`:

- `name`
- `description`
- `super_typing_mode`
- `partitions` (partition names)

### `atlas_partition.json`

Serialized `PartitionMetadata`:

- `name`
- `description`
- `schema` (`AtlasSchema`)

### `entries.arrow`

Arrow IPC table with fields:

- `dataset_name: Utf8`
- `dataset_index: UInt32`
- `deletion: Boolean`

This is the authoritative index for dataset names, local partition indexes, and delete state.

### Column path mapping rules

`column_name_to_path` maps logical column names to object paths:

- regular name: `temperature` → `columns/temperature`
- dotted path: `a.b.c` → `columns/a/b/c`
- leading dot: `.platform` → `columns/__platform`

### `array.arrow`

- Flat value stream for the column
- Stored as Arrow IPC record batches
- Writer flushes in chunks based on `arrow_chunk_size_by_type(...)`

### `layout.arrow`

Per dataset-array placement metadata (`ArrayLayout`):

- `dataset_index`
- `array_start` (offset into flat column value stream)
- `array_len`
- `array_shape`
- `dimensions`

### `statistics.arrow`

Per dataset summary rows:

- `min`
- `max`
- `null_count`
- `row_count`

`ArrayReader` appends `dataset_index` from layout when not already present.

---

## Internal architecture

### Layering

1. **Collection layer** (`collection.rs`)
	 - Handles collection metadata, partition registration, and merged schema view.
2. **Partition ops layer** (`partition/ops/*`)
	 - Write, read, stream, statistics, delete, cast operations over one partition.
3. **Column layer** (`column/*`)
	 - Wraps array readers/writers and column file naming.
4. **Array layer** (`array/*`)
	 - Implements chunked Arrow storage, layout mapping, stats, and subset reads.
5. **Arrow object-store reader** (`arrow_object_store.rs`)
	 - Range-based IPC reader that parses footer + dictionary blocks and lazily loads record batches.

### Write data flow

For each appended dataset column:

1. `PartitionWriter` assigns `dataset_index` and ensures `__entry_key` is written.
2. `ColumnWriter` delegates to `ArrayWriter`.
3. `ArrayWriter::append_array(...)`:
	 - records `ArrayLayout`
	 - appends statistics row
	 - buffers Arrow array values
4. On flush/finalize:
	 - buffered arrays are concatenated into batch-sized Arrow IPC writes
	 - `array.arrow`, `layout.arrow`, and `statistics.arrow` are uploaded
5. `PartitionWriter::finish()` writes `atlas_partition.json` + `entries.arrow`.

### Read data flow

1. `ArrayReader` loads `layout.arrow` and opens `ArrowObjectStoreReader` for `array.arrow`.
2. `read_dataset_array(dataset_index)` finds `ArrayLayout` and creates lazy `AtlasArrayBackend<T>`.
3. On materialization/subset:
	 - n-D subset is translated into flat ranges
	 - ranges are mapped to batch index + slice offsets
	 - record batches are fetched via range reads (with cache)
	 - array slices are concatenated and converted into `ndarray` values

### Caching

- `IoCache` caches decoded Arrow record batches keyed by `(path, batch_index)`
- Reader/stream/statistics builders accept injected cache; otherwise default size is used

---

## Schema behavior and super-typing

`AtlasSchema::merge_all_with_mode(...)` supports two modes:

- `AtlasSuperTypingMode::General`
	- Uses general Arrow super-typing via `beacon_common::super_typing::super_type_arrow`
- `AtlasSuperTypingMode::GroupBased`
	- Allows merge only if both types are:
		- numeric + numeric, or
		- string/timestamp + string/timestamp

Collection-level `arrow_schema()` is computed by merging partition schemas using the collection mode.

---

## Type support and constraints

### Runtime materialization / cast support in Atlas compat paths

Supported broadly in read/cast compatibility paths:

- `Boolean`
- `Int8/16/32/64`
- `UInt8/16/32/64`
- `Float32/64`
- `Utf8` / `LargeUtf8` (cast path supports both)
- `Timestamp(Nanosecond, _)`

Unsupported types in those paths return explicit errors.

### Null handling

`AtlasArrowCompat` conversion currently rejects null values when converting Arrow arrays into concrete value vectors. This means null-containing arrays can fail in some read/cast materialization paths.

---

## Operational caveats

- Soft delete does not compact/garbage-collect physical column data.
- `PartitionStreamReaderBuilder::with_projection(...)` stores projection, but the current stream construction does not apply projection to emitted datasets.
- `create_shareable_stream(buffer_size)` currently stores `_buffer_size`, but no explicit buffering strategy is enforced beyond queue semantics.
- `DEFAULT_IO_CACHE_BYTES` constant value/comment are inconsistent in source (`256 MiB` value with `64 MiB` comment).
- `ArrayWriter::new(...)` uses `tempfile::tempfile().unwrap()` for one temp-file path (panic risk if tempfile creation fails).
- `ReaderBuilder::dataset(...)` names returned batches as constant `"dataset"`, not the original entry key.

---

## API reference by module

### Root / prelude

- `Dataset`
- `prelude::*` re-exports common collection/column/schema types

### `collection`

- `AtlasCollection`
	- `create`, `new`, `open`, `load`, `update_state`
	- `create_partition_writer`, `create_partition`
	- `snapshot`, `arrow_schema`, `super_typing_mode`
- `CollectionPartitionWriter`
	- `writer_mut`, `finish`
- `CollectionMetadata`, `AtlasCollectionState`

### `partition::ops`

- Write: `PartitionWriter::{new, write_dataset, write_dataset_columns, finish}`
- Read: `ReaderBuilder::{new, arrow_schema, dataset}`
- Stream: `PartitionStreamReaderBuilder::{new, with_projection, with_io_cache, with_dataset_indexes, create_shareable_stream}`
- Stats: `PartitionStatisticsBuilder::{new, with_projection, with_io_cache, read_statistics_batches}`
- Delete: `DeleteDatasetsPartition::{new, delete_dataset, delete_dataset_index, execute}`
- Cast: `CastPartition::{new, cast_column, execute}`

### `column`

- `Column::{new, new_from_vec, name, data_type, array}`
- `ColumnWriter::{new, write_column_array, finalize}`
- `ColumnReader::{new, read_column_array, read_column_statistics}`

### `array`

- `ArrayWriter::{new, append_array, flush, finalize}`
- `ArrayReader::{new_with_cache, new_with_cache_and_statistics, read_dataset_array, read_statistics_batch}`
- `AtlasArrayBackend` (subset-oriented lazy backend)

### `arrow_object_store`

- `ArrowObjectStoreReader::{new, with_projection, num_batches, schema, read_batch}`

---

## Practical guidance

- Prefer the high-level collection APIs for normal lifecycle operations.
- Use operation builders (`Read`, `Stream`, `Statistics`, `Delete`, `Cast`) for targeted partition work.
- Reuse `IoCache` when performing many reads across the same partition/columns.
- Treat dataset indexes as partition-local implementation identifiers; use dataset names (`__entry_key`) for user-facing identity.

