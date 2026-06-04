//! ND (N-dimensional) Arrow arrays for Beacon.
//!
//! This crate provides a small, production-oriented abstraction for representing
//! N-dimensional arrays backed by Arrow arrays.
//!
//! Key features:
//! - Stores ND arrays row-wise in Arrow IPC as a nested column:
//!   `Struct{ values: List<T>, dim_names: List<Dictionary<Int32, Utf8>>, dim_sizes: List<UInt32> }`.
//! - Provides broadcasting support (xarray-style, name-aligned) via
//!   virtual broadcast views with efficient windowed `take`.

pub mod array;
pub mod batch;
pub mod stream;

pub use array::NdArrowArrayDispatch;
pub use batch::NdRecordBatch;
pub use stream::{NdToArrowPipeOptions, pipe_nd_record_batch_stream};
