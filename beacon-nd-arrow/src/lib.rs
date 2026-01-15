//! ND (N-dimensional) Arrow arrays for Beacon.
//!
//! This crate provides a small, production-oriented abstraction for representing
//! N-dimensional arrays backed by Arrow arrays.
//!
//! Key features:
//! - Stores ND arrays row-wise in Arrow IPC as a nested column:
//!   `Struct{ values: List<T>, dim_names: List<Dictionary<Int32, Utf8>>, dim_sizes: List<UInt32> }`.
//! - Provides broadcasting support (xarray-style, name-aligned) and the ability to
//!   materialize broadcasted arrays.
//!
//! ## Quick start
//!
//! Create a single ND value:
//!
//! ```
//! use std::sync::Arc;
//! use arrow::array::Int32Array;
//! use beacon_nd_arrow::{NdArrowArray, dimensions::{Dimension, Dimensions}};
//!
//! let nd = NdArrowArray::new(
//!     Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6])),
//!     Dimensions::new(vec![
//!         Dimension::try_new("y", 2)?,
//!         Dimension::try_new("x", 3)?,
//!     ]),
//! )?;
//! assert_eq!(nd.dimensions().shape(), vec![2, 3]);
//! # Ok::<(), beacon_nd_arrow::error::NdArrayError>(())
//! ```
//!
//! Store multiple ND rows in a RecordBatch using `column::NdArrowArrayColumn`:
//!
//! ```
//! use std::sync::Arc;
//! use arrow::array::Int32Array;
//! use arrow::record_batch::RecordBatch;
//! use arrow_schema::DataType;
//!
//! use beacon_nd_arrow::{
//!   NdArrowArray,
//!   column::NdArrowArrayColumn,
//!   dimensions::{Dimension, Dimensions},
//!   extension,
//! };
//!
//! let nd1 = NdArrowArray::new(
//!   Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6])),
//!   Dimensions::new(vec![
//!     Dimension::try_new("y", 2)?,
//!     Dimension::try_new("x", 3)?,
//!   ]),
//! )?;
//! let nd2 = NdArrowArray::new(
//!   Arc::new(Int32Array::from(vec![7, 8, 9])),
//!   Dimensions::new(vec![
//!     Dimension::try_new("y", 1)?,
//!     Dimension::try_new("x", 3)?,
//!   ]),
//! )?;
//!
//! let column = NdArrowArrayColumn::from_rows(vec![nd1, nd2])?;
//! let field = extension::nd_column_field("x", DataType::Int32, false)?;
//! let schema = Arc::new(arrow::datatypes::Schema::new(vec![field]));
//! let batch = RecordBatch::try_new(schema, vec![column.into_array_ref()])?;
//! assert_eq!(batch.num_rows(), 2);
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

pub mod batch;
pub mod broadcast;
pub mod column;
pub mod dimensions;
pub mod error;
pub mod extension;

use std::{ops::Deref, sync::Arc};

use arrow::array::ArrayRef;
use arrow_schema::DataType;

use crate::{dimensions::Dimensions, error::NdArrayError};

/// A single ND array value (e.g. one row).
///
/// To store multiple ND values (rows) in a RecordBatch / Arrow IPC, use
/// `column::NdArrowArrayColumn`.
#[derive(Debug, Clone)]
pub struct NdArrowArray {
    values: ArrayRef,
    dimensions: Dimensions,
}

impl NdArrowArray {
    /// Create a new ND array value from a *flat* storage Arrow array and dimensions.
    ///
    /// This represents a single ND array (e.g. a single row value). To store multiple
    /// ND arrays in an Arrow column (e.g. multiple rows), use `column::NdArrowArrayColumn`.
    pub fn new(values: ArrayRef, dimensions: Dimensions) -> Result<Self, NdArrayError> {
        dimensions.validate()?;

        // Validate the dimensions against the array shape
        if values.len() != dimensions.total_flat_size() {
            return Err(NdArrayError::MisalignedArrayDimensions(
                values.len(),
                dimensions,
            ));
        }

        Ok(Self { values, dimensions })
    }

    /// Construct a scalar NULL ND array.
    pub fn new_null_scalar(data_type: Option<DataType>) -> Result<Self, NdArrayError> {
        let storage = match data_type {
            Some(dt) => arrow::array::new_null_array(&dt, 1),
            None => Arc::new(arrow::array::NullArray::new(1)),
        };
        Self::new(storage, Dimensions::Scalar)
    }

    /// Returns the flat storage values.
    pub fn values(&self) -> &ArrayRef {
        &self.values
    }

    /// Returns the dimensions.
    pub fn dimensions(&self) -> &Dimensions {
        &self.dimensions
    }

    /// Returns the underlying storage array (the physical Arrow representation).
    pub fn storage_array(&self) -> ArrayRef {
        self.values.clone()
    }

    /// Broadcast this array to `target` and materialize it as a new ND array.
    ///
    /// ```
    /// use std::sync::Arc;
    /// use arrow::array::Int32Array;
    /// use beacon_nd_arrow::{NdArrowArray, dimensions::{Dimension, Dimensions}};
    ///
    /// let a = NdArrowArray::new(
    ///     Arc::new(Int32Array::from(vec![1, 2, 3])),
    ///     Dimensions::new(vec![
    ///         Dimension::try_new("y", 1)?,
    ///         Dimension::try_new("x", 3)?,
    ///     ]),
    /// )?;
    ///
    /// let b = a.broadcast_to(&Dimensions::new(vec![
    ///     Dimension::try_new("y", 2)?,
    ///     Dimension::try_new("x", 3)?,
    /// ]))?;
    /// let b_arr = b.values().as_any().downcast_ref::<Int32Array>().unwrap();
    /// assert_eq!(b_arr.values(), &[1, 2, 3, 1, 2, 3]);
    /// # Ok::<(), beacon_nd_arrow::error::NdArrayError>(())
    /// ```
    pub fn broadcast_to(&self, target: &Dimensions) -> Result<Self, NdArrayError> {
        broadcast::broadcast_nd_array(self, target)
    }
}

impl Deref for NdArrowArray {
    type Target = ArrayRef;

    fn deref(&self) -> &Self::Target {
        &self.values
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::Int32Array;
    use arrow::record_batch::RecordBatch;
    use arrow_ipc::CompressionType;
    use arrow_schema::DataType;
    use tempfile::NamedTempFile;

    use crate::{
        NdArrowArray,
        column::NdArrowArrayColumn,
        dimensions::{Dimension, Dimensions},
        extension,
    };

    #[test]
    fn extension_field_metadata_roundtrip() {
        let field = extension::nd_column_field("x", DataType::Int32, true).unwrap();
        assert!(extension::is_nd_column_field(&field));
        assert_eq!(
            extension::nd_storage_type_from_field(&field).unwrap(),
            DataType::Int32
        );
    }

    #[test]
    fn arrow_ipc_roundtrip_preserves_nd_extension_field_metadata() {
        let nd1 = NdArrowArray::new(
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6])),
            Dimensions::new(vec![
                Dimension::try_new("y", 2).unwrap(),
                Dimension::try_new("x", 3).unwrap(),
            ]),
        )
        .unwrap();
        let nd2 = NdArrowArray::new(
            Arc::new(Int32Array::from(vec![7, 8, 9])),
            Dimensions::new(vec![
                Dimension::try_new("y", 1).unwrap(),
                Dimension::try_new("x", 3).unwrap(),
            ]),
        )
        .unwrap();

        let rows = vec![nd1.clone(), nd2.clone(), nd1.clone()];
        let column = NdArrowArrayColumn::from_rows(rows).unwrap();
        let field = extension::nd_column_field("x", DataType::Int32, false).unwrap();
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![field.clone()]));
        let batch = RecordBatch::try_new(schema.clone(), vec![column.into_array_ref()]).unwrap();

        // Write IPC file
        let tmp = NamedTempFile::new().unwrap();
        {
            let options = arrow_ipc::writer::IpcWriteOptions::default()
                .try_with_compression(Some(CompressionType::ZSTD))
                .unwrap();
            let mut writer = arrow_ipc::writer::FileWriter::try_new_with_options(
                tmp.reopen().unwrap(),
                &schema,
                options,
            )
            .unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }

        // Read IPC file
        let mut reader =
            arrow_ipc::reader::FileReader::try_new(std::fs::File::open(tmp.path()).unwrap(), None)
                .unwrap();
        let read_schema = reader.schema();
        assert_eq!(read_schema.fields().len(), 1);

        let read_field = read_schema.field(0);
        assert!(extension::is_nd_column_field(read_field));
        assert_eq!(
            extension::nd_storage_type_from_field(read_field).unwrap(),
            DataType::Int32
        );

        let read_batch = reader.next().unwrap().unwrap();
        let col = NdArrowArrayColumn::try_from_array(read_batch.column(0).clone()).unwrap();

        assert_eq!(col.len(), 3);

        let r0 = col.row(0).unwrap();
        assert_eq!(r0.dimensions().shape(), vec![2, 3]);
        let r0_vals = r0.values().as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(r0_vals.values(), &[1, 2, 3, 4, 5, 6]);

        let r1 = col.row(1).unwrap();
        assert_eq!(r1.dimensions().shape(), vec![1, 3]);
        let r1_vals = r1.values().as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(r1_vals.values(), &[7, 8, 9]);
    }

    /// "Benchmark-ish" smoke test to inspect compressed IPC size.
    ///
    /// Run with:
    /// `cargo test -p beacon-nd-arrow ipc_zstd_compressed_file_size_1000_rows -- --ignored --nocapture`
    #[test]
    #[ignore]
    fn ipc_zstd_compressed_file_size_1000_rows() {
        // One row is a 2D array flattened row-major.
        let per_row_values: Vec<i32> = (0..1024).map(|i| (i % 10) as i32).collect();
        let nd = NdArrowArray::new(
            Arc::new(Int32Array::from(per_row_values)),
            Dimensions::new(vec![
                Dimension::try_new("y", 32).unwrap(),
                Dimension::try_new("x", 32).unwrap(),
            ]),
        )
        .unwrap();

        let rows = (0..1000).map(|_| nd.clone()).collect::<Vec<_>>();
        let column = NdArrowArrayColumn::from_rows(rows).unwrap();
        let field = extension::nd_column_field("x", DataType::Int32, false).unwrap();
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![field]));
        let batch = RecordBatch::try_new(schema.clone(), vec![column.into_array_ref()]).unwrap();

        let tmp = NamedTempFile::new().unwrap();
        {
            let options = arrow_ipc::writer::IpcWriteOptions::default()
                .try_with_compression(Some(CompressionType::ZSTD))
                .unwrap();
            let mut writer = arrow_ipc::writer::FileWriter::try_new_with_options(
                tmp.reopen().unwrap(),
                &schema,
                options,
            )
            .unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }

        let bytes = tmp.path().metadata().unwrap().len();
        println!(
            "ZSTD IPC size for 1000 rows (shape 32x32, i32): {} bytes (~{:.2} KiB)",
            bytes,
            (bytes as f64) / 1024.0
        );
    }
}
