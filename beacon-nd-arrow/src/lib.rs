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
//!
//! ## Quick start
//!
//! Create a single ND value:
//!
//! ```
//! use std::sync::Arc;
//! use arrow::array::Int32Array;
//! use beacon_nd_arrow::array::{
//!     NdArrowArray,
//!     backend::{ArrayBackend, mem::InMemoryArrayBackend},
//! };
//! use arrow::datatypes::DataType;
//!
//! let values: Arc<dyn arrow::array::Array> = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6]));
//! let backend: Arc<dyn ArrayBackend> = Arc::new(InMemoryArrayBackend::new(
//!     values,
//!     vec![2, 3],
//!     vec!["y".to_string(), "x".to_string()],
//! ));
//! let nd = NdArrowArray::new(backend, DataType::Int32)?;
//! assert_eq!(nd.shape(), &[2, 3]);
//! assert_eq!(nd.dimensions(), &["y".to_string(), "x".to_string()]);
//! # Ok::<(), anyhow::Error>(())
//! ```
//!
//! Broadcast one named dimension into a higher-rank target:
//!
//! ```
//! use std::sync::Arc;
//! use beacon_nd_arrow::array::{
//!     NdArrowArray,
//!     backend::{ArrayBackend, mem::InMemoryArrayBackend},
//! };
//! use arrow::array::Int32Array;
//! use arrow::datatypes::DataType;
//!
//! let values: Arc<dyn arrow::array::Array> = Arc::new(Int32Array::from(vec![10, 20, 30]));
//! let backend: Arc<dyn ArrayBackend> = Arc::new(InMemoryArrayBackend::new(
//!     values,
//!     vec![3],
//!     vec!["lon".to_string()],
//! ));
//! let lon = NdArrowArray::new(backend, DataType::Int32)?;
//! let view = lon.broadcast(&[2, 3], &["time".to_string(), "lon".to_string()])?;
//! assert_eq!(view.shape(), &[2, 3]);
//! # Ok::<(), anyhow::Error>(())
//! ```

pub mod array;
pub mod batch;
pub mod stream;

pub use array::NdArrowArray;
pub use batch::NdRecordBatch;
pub use stream::NdSendableBatchStream;

#[cfg(feature = "ndarray")]
pub mod ndarray_convert;
