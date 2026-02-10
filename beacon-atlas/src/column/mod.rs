use std::sync::Arc;

use crate::array::{Array, store::ChunkStore};

pub mod encoding;
pub mod reader;
pub mod writer;

pub enum Column {
    Attribute,
    Array(Array<Arc<dyn ChunkStore>>),
}
