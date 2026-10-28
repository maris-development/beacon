//! N-dimensional Arrow arrays and the `beacon.nd` encoding that carries them
//! through ordinary Arrow record batches.
//!
//! The plugin SDK re-exports this crate as `beacon_plugin_sdk::nd`. Beacon itself
//! still uses its own copy in `beacon_datafusion_ext::nd`; both must keep the same
//! encoding.

pub mod array;
pub mod batch;
pub mod dimensions;
pub mod encoding;

pub use array::NdArrowArray;
pub use batch::NdRecordBatch;
pub use dimensions::{Dimension, Dimensions};
