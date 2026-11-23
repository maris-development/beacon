//! The dataset contract of the Beacon ND framework.
//!
//! A format crate implements [`NdDataset`] for one opened file.
//! Each variable of the file is an [`NdVariable`] that reads one [`Region`] at a time.
//! A scan cuts the dataset into a [`ChunkGrid`] and reads each chunk with [`read_chunk`].
//!
//! # Example
//!
//! ```
//! # #[tokio::main] async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! use std::sync::Arc;
//! use arrow::array::Float64Array;
//! use beacon_nd_dataset::{ChunkGrid, GridSelection, MemoryDataset, MemoryVariable, read_chunk, select_grid};
//! use nd_arrow_array::{Dimension, Dimensions, NdArrowArray};
//! let time = Dimensions::try_new([Dimension::new("time", 3)])?;
//! let values = Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0]));
//! let temp = MemoryVariable::new("temp", NdArrowArray::try_new(values.clone(), time.clone())?);
//! let salt = MemoryVariable::new("salt", NdArrowArray::try_new(values, time)?);
//! let file = Arc::new(MemoryDataset::try_new("a.nd", vec![temp, salt])?);
//! let dataset = select_grid(file, &GridSelection::PerFile)?;
//! let grid = ChunkGrid::try_new(dataset.as_ref(), 8192)?;
//! let batch = read_chunk(dataset.as_ref(), &grid, &grid.chunks()[0]).await?;
//! assert_eq!((batch.num_columns(), batch.num_rows()), (2, 3));
//! # Ok(()) }
//! ```
#![warn(missing_docs)]

pub mod attributes;
pub mod dataset;
pub mod error;
pub mod grid;
pub mod memory;
pub mod region;
pub mod schema;
pub mod statistics;
pub mod view;

pub use attributes::attribute_columns;
pub use dataset::{NdDataset, NdVariable, validate};
pub use error::{Error, Result};
pub use grid::{ChunkGrid, read_chunk};
pub use memory::{MemoryDataset, MemoryVariable};
pub use region::Region;
pub use schema::{DatasetSchema, VariableInfo};
pub use statistics::value_range;
pub use view::{GridSelection, default_grid, select_columns, select_grid};
