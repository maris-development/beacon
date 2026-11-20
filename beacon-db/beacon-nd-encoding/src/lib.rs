//! N-dimensional Arrow arrays and the `beacon.nd` encoding that carries them
//! through ordinary Arrow record batches.
//!
//! The plugin SDK re-exports this crate as `beacon_plugin_sdk::nd`, and Beacon
//! re-exports it as `beacon_datafusion_ext::nd`. The broadcast and the
//! materialization live in `beacon-nd-execution`, so this crate needs no compute
//! kernels.

pub mod array;
pub mod batch;
pub mod dimensions;
pub mod encoding;

pub use array::NdArrowArray;
pub use batch::NdRecordBatch;
pub use dimensions::{Dimension, Dimensions};
