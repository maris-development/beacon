//! N-dimensional Arrow arrays and their `beacon.nd` record batch encoding.
//!
//! Beacon's nd formats (zarr, netcdf) are logically grids of variables over
//! shared named dimensions. Flattening a grid to a table broadcasts every
//! variable onto the full dimension cross-product. This module holds the data
//! types that keep columns *un-broadcast*:
//!
//! - [`NdArrowArray`]: a flat Arrow array plus named C-order [`Dimensions`].
//! - [`BroadcastMap`]: a broadcast expressed as stride arithmetic; no data
//!   moves.
//! - [`NdRecordBatch`]: columns (each on a subset of the dimensions) over a
//!   shared target grid.
//!
//! The physical operators and optimizer rules live in `beacon-nd-execution`.

pub mod array;
pub mod batch;
pub mod broadcast;
pub mod dimensions;
pub mod encoding;

pub use array::NdArrowArray;
pub use batch::NdRecordBatch;
pub use broadcast::BroadcastMap;
pub use dimensions::{Dimension, Dimensions};
pub use encoding::{
    decode_nd_record_batch, encode_flat_batch_as_nd, encode_nd_record_batch, encoded_schema,
    infer_target, is_nd_encoded, logical_schema, nd_encoded_field, nd_encoded_field_of,
    nd_encoded_type,
};

