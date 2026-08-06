//! The immutable segment: layout, writer, and reader.

pub mod format;
pub mod reader;
pub mod values;
pub mod writer;

pub use reader::{ColumnStats, SegmentReader};
pub use values::{normalize_type, storage_type};
pub use writer::{ColumnStat, FinishedSegment, SegmentBuilder};
