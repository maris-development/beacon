# beacon-nd-array

N-dimensional array primitives and dataset utilities used across Beacon.

This crate provides a typed ND array abstraction (`NdArray<T>`), a dynamic trait object interface (`NdArrayD`), dataset containers (`Dataset`, `AnyDataset`, `RaggedDataset`), and Arrow/DataFusion integration for turning ND data into `RecordBatch` streams.

## What This Crate Does

`beacon-nd-array` is the bridge between:

- ND scientific-style arrays with named dimensions, and
- tabular Arrow/DataFusion execution.

It gives you:

- Typed ND arrays (`NdArray<T>`) with named dimensions and shape.
- A backend abstraction (`ArrayBackend<T>`) for reading subsets asynchronously.
- In-memory backend (`InMemoryArrayBackend`) for simple construction and tests.
- Dataset modeling for both regular and CF ragged layouts.
- RecordBatch streaming for DataFusion consumption.
- Predicate pushdown helpers that convert DataFusion predicates into boolean masks.
- Arrow conversion with fill-value-to-null handling.

## Core Concepts

### 1) ND Array (`NdArray<T>`)

`NdArray<T>` wraps an `ArrayBackend<T>` and carries:

- `shape`: axis sizes
- `dimensions`: axis names
- optional `fill_value`

It supports:

- `subset(...)` for slicing
- `broadcast(...)` for name-aligned expansion to a target shape/dim set
- `clone_into_raw_vec()` for full materialization into a flat vector

### 2) Dynamic ND Array (`NdArrayD`)

`NdArrayD` is the object-safe trait used throughout dataset and Arrow code. It exposes:

- runtime datatype (`NdArrayDataType`)
- shape/dimensions/chunk shape
- async `subset` and `broadcast`
- downcasting via `as_any()`

This lets mixed datatypes live in one dataset map (`IndexMap<String, Arc<dyn NdArrayD>>`).

### 3) Dataset Types

- `Dataset`: generic named collection of arrays plus discovered dimensions.
- `AnyDataset`: enum wrapper that is either:
  - `Regular(Dataset)`, or
  - `Ragged { dataset, ragged }`.
- `RaggedDataset`: CF contiguous ragged view built from `sample_dimension` attributes.

`AnyDataset::try_from_dataset(...)` auto-detects regular vs CF-ragged layout.

## How It Works Internally

### Backend Layer

`ArrayBackend<T>` is responsible for reading subsets of ND data.

Current implementation includes:

- `InMemoryArrayBackend<T>` (ndarray-backed)

The backend validates subset bounds and returns `ndarray::ArrayD<T>` for the requested window.

### ND to Arrow Conversion

`arrow::array::ndarray_to_arrow_array(...)` converts an `NdArrayD` into an Arrow array.

For supported types (bool, numeric, timestamp, string, binary):

- data is materialized as a flat vector,
- Arrow array is created,
- if `fill_value` is set, matching entries are converted to nulls.

### Dataset to RecordBatch Stream

`arrow::batch` provides:

- `dataset_as_record_batch_stream(dataset, batch_size, concurrency, predicate, metrics)` for regular datasets
- `any_dataset_as_record_batch_stream(dataset, batch_size, concurrency, predicate, metrics)` for regular + ragged datasets

`concurrency` is how many chunks (or casts, for ragged) are read concurrently. Each
chunk read is spawned onto the tokio runtime so I/O and Arrow conversion spread across
worker threads, while emission order is preserved. Pass `default_chunk_concurrency()`
for a core-count default, or `1` for fully serial reads.

For regular datasets:

- arrays are broadcast to a common target shape/dimension set,
- chunks are generated from `chunk_shape`,
- each chunk is subset+broadcasted and emitted as a `RecordBatch`.

For ragged datasets:

- row-size variables are used to compute cumulative offsets,
- each emitted batch corresponds to one or more casts,
- instance variables are run-length expanded,
- observation variables are sliced and null-padded where needed,
- row-size bookkeeping arrays are excluded from output schema.

### Predicate Pushdown

`arrow::pushdown_filter::PushdownFilter` walks DataFusion physical expressions and extracts per-column `ValueRange` bounds.

`arrow::pushdown::mask_pushdown(...)` resolves those ranges against 1D typed arrays and computes boolean masks (`Vec<bool>`) using `compute_mask(...)`.

Those masks are used in batch planning (including ragged paths) to skip filtered instances early.

## Data Types

Supported ND datatypes (`NdArrayDataType`):

- Bool
- I8, I16, I32, I64
- U8, U16, U32, U64
- F32, F64
- Timestamp (nanosecond wrapper)
- Binary
- String

The crate also includes type promotion logic (`super_type`) for schema harmonization scenarios.

## Example: Build a Dataset and Stream Batches

```rust
use std::sync::Arc;
use futures::TryStreamExt;
use beacon_nd_array::{NdArray, NdArrayD, dataset::Dataset};
use beacon_nd_array::arrow::batch::{dataset_as_record_batch_stream, default_chunk_concurrency};
use indexmap::IndexMap;

async fn demo() -> anyhow::Result<()> {
    let temperature = NdArray::<f64>::try_new_from_vec_in_mem(
            vec![10.0, 11.0, 12.0, 13.0],
            vec![2, 2],
            vec!["time".to_string(), "depth".to_string()],
            None,
    )?;

    let mut arrays: IndexMap<String, Arc<dyn NdArrayD>> = IndexMap::new();
    arrays.insert("temperature".to_string(), Arc::new(temperature));

    let ds = Dataset::new("example".to_string(), arrays).await;

    let batches = dataset_as_record_batch_stream(ds, usize::MAX, default_chunk_concurrency(), None, None)
            .try_collect::<Vec<_>>()
            .await?;

    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 4);
    Ok(())
}
```

## Module Guide

- `src/lib.rs`
  - `NdArray<T>`, `NdArrayD`
- `src/array/backend`
  - backend abstraction and in-memory backend
- `src/dataset`
  - regular dataset model + CF ragged support
- `src/arrow/array.rs`
  - ND-to-Arrow conversion
- `src/arrow/batch.rs`
  - RecordBatch streaming for regular/ragged datasets
- `src/arrow/pushdown*.rs`
  - predicate range extraction and mask computation
- `src/projection.rs`
  - dataset projection descriptor

## Current Scope

This crate focuses on execution-time ND data handling and conversion for Beacon query paths.

Implemented today:

- ND representation with named dims
- subset + broadcast
- regular and CF ragged dataset handling
- Arrow conversion with null masking from fill values
- DataFusion predicate-range pushdown helpers
- RecordBatch streaming for query engines
