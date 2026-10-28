//! N-dimensional Arrow arrays and the `beacon.nd` encoding that carries them
//! through ordinary Arrow record batches.

pub mod array;
pub mod batch;
pub mod dimensions;
pub mod encoding;

pub use array::NdArrowArray;
pub use batch::NdRecordBatch;
pub use dimensions::{Dimension, Dimensions};
