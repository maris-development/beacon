# beacon-nd-arrow

N-dimensional (ND) arrays backed by Apache Arrow arrays, designed for Beacon.

This crate provides:

- `NdArrowArray`: a single ND value (e.g. one row) consisting of a *flat* Arrow array plus explicit dimensions.
- `NdArrowArrayColumn`: an Arrow column encoding where **each row is one ND array**, allowing heterogeneous shapes per row.
- Name-aligned (xarray-style) broadcasting for `NdArrowArray` via `NdArrowArray::broadcast_to`.

## Storage model (Arrow IPC compatible)

To support RecordBatches where each row may have a different shape, dimensions are stored **in the data** (not in schema-level extension metadata).

An ND column is represented as:

```
Struct{
  values:    List<T>,
  dim_names: List<Dictionary<Int32, Utf8>>,
  dim_sizes: List<UInt32>
}
```

The parent field is tagged as an Arrow “extension type” via `Field` metadata:

- `ARROW:extension:name = "beacon.nd"`
- `ARROW:extension:metadata = "{}"` (intentionally dimension-free)

This preserves the “this is an ND column” intent through Arrow IPC without preventing per-row shapes.

## Quick start

### 1) Create a single ND value

```rust
use std::sync::Arc;

use arrow::array::Int32Array;
use beacon_nd_arrow::{NdArrowArray, dimensions::{Dimension, Dimensions}};

let nd = NdArrowArray::new(
    Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6])),
    Dimensions::new(vec![
        Dimension::try_new("y", 2)?,
        Dimension::try_new("x", 3)?,
    ]),
)?;
assert_eq!(nd.dimensions().shape(), vec![2, 3]);
# Ok::<(), beacon_nd_arrow::error::NdArrayError>(())
```

### 2) Broadcast a value

```rust
use std::sync::Arc;

use arrow::array::Int32Array;
use beacon_nd_arrow::{NdArrowArray, dimensions::{Dimension, Dimensions}};

let a = NdArrowArray::new(
    Arc::new(Int32Array::from(vec![1, 2, 3])),
    Dimensions::new(vec![
        Dimension::try_new("y", 1)?,
        Dimension::try_new("x", 3)?,
    ]),
)?;

let b = a.broadcast_to(&Dimensions::new(vec![
    Dimension::try_new("y", 2)?,
    Dimension::try_new("x", 3)?,
]))?;
assert_eq!(b.dimensions().shape(), vec![2, 3]);
# Ok::<(), beacon_nd_arrow::error::NdArrayError>(())
```

## Broadcasting semantics

Broadcasting is **name-aligned** (xarray-style): axes are matched by *dimension name*.

### Rules summary

- Broadcasting never changes the element type; it only changes the logical shape.
- Dimensions must be internally consistent: the product of `dim_sizes` must match the length of the flat `values` array.
- Dimension names are mandatory for non-scalar arrays: every axis must have a non-empty, unique name.
- Axes are compatible if their sizes are equal, or one of them is `1` (which can be expanded).
- The input dim names must be a subset of the target dim names.
- The relative order of the input axes must be preserved (no implicit transpose).
- Any dim missing in the input is treated as size `1` and will broadcast.

This is intentionally “xarray-like”: adding new axes is allowed, but reordering is not.

Example (allowed):

```rust
use std::sync::Arc;

use arrow::array::Int32Array;
use beacon_nd_arrow::{NdArrowArray, dimensions::{Dimension, Dimensions}};

let a = NdArrowArray::new(
    Arc::new(Int32Array::from(vec![1, 2, 3])),
    Dimensions::new(vec![
        Dimension::try_new("time", 1)?,
        Dimension::try_new("x", 3)?,
    ]),
)?;

let target = Dimensions::new(vec![
    Dimension::try_new("station", 2)?,
    Dimension::try_new("time", 1)?,
    Dimension::try_new("x", 3)?,
]);

let b = a.broadcast_to(&target)?;
assert_eq!(b.dimensions().shape(), vec![2, 1, 3]);
# Ok::<(), beacon_nd_arrow::error::NdArrayError>(())
```

Example (rejected due to implicit transpose):

- input dims: `time, x`
- target dims: `x, time`

Even though the same names appear, the order differs and the operation is rejected.

Example (rejected due to incompatible size):

- input dims: `time=2, x=3`
- target dims: `station=2, time=4, x=3`

This fails because `time` cannot be expanded from `2` to `4`.

### 3) Store multiple ND rows in a RecordBatch

```rust
use std::sync::Arc;

use arrow::array::Int32Array;
use arrow::record_batch::RecordBatch;
use arrow_schema::DataType;

use beacon_nd_arrow::{
    NdArrowArray,
    column::NdArrowArrayColumn,
    dimensions::{Dimension, Dimensions},
    extension,
};

let nd1 = NdArrowArray::new(
    Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6])),
    Dimensions::new(vec![
        Dimension::try_new("y", 2)?,
        Dimension::try_new("x", 3)?,
    ]),
)?;
let nd2 = NdArrowArray::new(
    Arc::new(Int32Array::from(vec![7, 8, 9])),
    Dimensions::new(vec![
        Dimension::try_new("y", 1)?,
        Dimension::try_new("x", 3)?,
    ]),
)?;

let column = NdArrowArrayColumn::from_rows(vec![nd1, nd2])?;

let field = extension::nd_column_field("x", DataType::Int32, false)?;
let schema = Arc::new(arrow::datatypes::Schema::new(vec![field]));

let batch = RecordBatch::try_new(schema, vec![column.into_array_ref()])?;
assert_eq!(batch.num_rows(), 2);
# Ok::<(), Box<dyn std::error::Error>>(())
```

## `ndarray` conversions (optional)

This crate can convert to/from `ndarray::ArrayD<T>` behind the `ndarray` feature flag.

Enable it in your `Cargo.toml`:

```toml
beacon-nd-arrow = { version = "0.1", features = ["ndarray"] }
```

### `ndarray -> NdArrowArray`

`ndarray` does not store axis names, but Beacon ND arrays require **named dimensions** for
name-aligned broadcasting. Therefore you must provide `dim_names`.

```rust
use beacon_nd_arrow::{NdArrowArray, ndarray_convert::FromNdarray};

let a = ndarray::ArrayD::from_shape_vec(ndarray::IxDyn(&[2, 3]), (1..=6).collect()).unwrap();
let nd = NdArrowArray::from_ndarray(&a, &vec!["y".to_string(), "x".to_string()]).unwrap();
assert_eq!(nd.dimensions().shape(), vec![2, 3]);
```

Supported storage types include:

- Integers: `i8`, `i16`, `i32`, `i64`, `u8`, `u16`, `u32`, `u64`
- Floats: `f32`, `f64`
- `bool`, `String` (Utf8)
- Timestamps: `chrono::NaiveDateTime`, `chrono::DateTime<chrono::Utc>`

### `NdArrowArray -> ndarray` (strict vs null-filling)

By default, `ToNdarray::to_ndarray()` is **strict** and errors if the Arrow storage contains nulls.
If you want to materialize nulls into an `ndarray`, use `ToNdarrayWithNulls`:

```rust
use beacon_nd_arrow::ndarray_convert::ToNdarrayWithNulls;

let (arr, fill_value) = nd.to_ndarray_with_default_fill().unwrap();
// `fill_value` is the sentinel used to represent Arrow nulls.
```

Default fill values:

- Integers: `T::MAX` (e.g. `i64::MAX`, `u8::MAX`)
- Floats: `NaN`
- `bool`: `false`
- `String`: empty string
- Timestamps: Unix epoch (`1970-01-01T00:00:00Z`)

## Notes on size & performance

- `dim_names` is dictionary-encoded to avoid repeating common dimension strings across rows.
- `dim_sizes` uses `UInt32`. If you need dimension sizes > `u32::MAX`, this encoding will reject such data.
- For maximum compression with IPC+ZSTD, consider grouping rows with similar shapes together before writing.

## Test: IPC ZSTD size smoke test

There is an ignored test that prints the file size for a synthetic batch of 1000 ND rows:

```
cargo test -p beacon-nd-arrow ipc_zstd_compressed_file_size_1000_rows -- --ignored --nocapture
```
