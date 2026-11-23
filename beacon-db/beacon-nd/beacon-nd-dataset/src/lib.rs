//! The dataset contract of the Beacon ND framework.
//!
//! A format crate implements [`NdDataset`] for one opened file.
//! Each variable of the file is an [`NdVariable`] that reads one [`Region`] at a time.
//! A scan cuts the dataset into a [`ChunkGrid`] and reads each chunk with a [`ChunkReader`].
//!
//! # Example
//!
//! ```
//! # #[tokio::main] async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! use std::sync::Arc;
//! use arrow::array::Float64Array;
//! use beacon_nd_dataset::{ChunkGrid, ChunkReader, GridSelection, MemoryDataset, MemoryVariable};
//! use beacon_nd_dataset::{Dimension, Dimensions, NdArrowArray, select_grid};
//! let time = Dimensions::try_new([Dimension::new("time", 3)])?;
//! let values = Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0]));
//! let temp = MemoryVariable::new("temp", NdArrowArray::try_new(values.clone(), time.clone())?);
//! let salt = MemoryVariable::new("salt", NdArrowArray::try_new(values, time)?);
//! let file = Arc::new(MemoryDataset::try_new("a.nd", vec![temp, salt])?);
//! let dataset = select_grid(file, &GridSelection::PerFile)?;
//! let grid = ChunkGrid::try_new(dataset.as_ref(), 8192)?;
//! let reader = ChunkReader::try_new(dataset.as_ref(), grid)?;
//! let batch = reader.read(&reader.grid().chunk(0)).await?;
//! assert_eq!((batch.num_columns(), batch.num_rows()), (2, 3));
//! # Ok(()) }
//! ```
#![warn(missing_docs)]

pub mod attributes;
pub mod dataset;
pub mod error;
pub mod grid;
pub mod memory;
pub mod read;
pub mod region;
pub mod schema;
pub mod statistics;
pub mod view;

pub use attributes::attribute_columns;
pub use dataset::{NdDataset, NdVariable, validate};
pub use error::{Error, Result};
pub use grid::ChunkGrid;
pub use memory::{MemoryDataset, MemoryVariable};
pub use nd_arrow_array::{Attributes, Dimension, Dimensions, NdArrowArray, NdRecordBatch};
pub use read::ChunkReader;
pub use region::Region;
pub use schema::{DatasetSchema, VariableInfo};
pub use statistics::value_range;
pub use view::{GridSelection, default_grid, select_columns, select_grid};
