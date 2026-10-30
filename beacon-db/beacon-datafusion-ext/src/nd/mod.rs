//! N-dimensional Arrow arrays and their `beacon.nd` record batch encoding.
//!
//! A re-export of `beacon-nd-encoding`, so the format crates and the plugin SDK
//! share one set of nd types. The broadcast, the materialization, the physical
//! operators and the optimizer rules live in `beacon-nd-execution`.

pub use beacon_nd_encoding::{array, batch, dimensions, encoding};

pub use beacon_nd_encoding::{Dimension, Dimensions, NdArrowArray, NdRecordBatch};
pub use beacon_nd_encoding::encoding::{
    decode_nd_record_batch, encode_flat_batch_as_nd, encode_nd_record_batch, encoded_schema,
    infer_target, is_nd_encoded, logical_schema, nd_encoded_field, nd_encoded_field_of,
    nd_encoded_type,
};
