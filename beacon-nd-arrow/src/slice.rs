use std::ops::Range;

/// Slice selector for a single dimension.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NdIndex {
    /// Select a single index (dimension is removed).
    Index(usize),
    /// Select a contiguous range (dimension is retained with `len`).
    Slice { start: usize, len: usize },
}

impl NdIndex {
    /// Create a slice with `start` and `len`.
    pub fn slice(start: usize, len: usize) -> Self {
        NdIndex::Slice { start, len }
    }
}

impl From<usize> for NdIndex {
    fn from(value: usize) -> Self {
        NdIndex::Index(value)
    }
}

impl From<Range<usize>> for NdIndex {
    fn from(value: Range<usize>) -> Self {
        let len = value.end.saturating_sub(value.start);
        NdIndex::Slice {
            start: value.start,
            len,
        }
    }
}

pub(crate) fn row_major_strides(shape: &[usize]) -> Vec<usize> {
    if shape.is_empty() {
        return vec![];
    }
    let mut strides = vec![0usize; shape.len()];
    let mut stride = 1usize;
    for (i, dim) in shape.iter().enumerate().rev() {
        strides[i] = stride;
        stride = stride.saturating_mul(*dim);
    }
    strides
}