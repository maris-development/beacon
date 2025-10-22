#[derive(Clone, Debug)]
pub struct ArraySlicePushDown {
    pub dimension: String,
    pub start: Option<usize>,
    pub end: Option<usize>,
}

impl ArraySlicePushDown {
    pub fn new(dimension: String, start: Option<usize>, end: Option<usize>) -> Self {
        Self {
            dimension,
            start,
            end,
        }
    }

    pub fn dimension(&self) -> &str {
        &self.dimension
    }

    pub fn start(&self) -> Option<usize> {
        self.start
    }

    pub fn end(&self) -> Option<usize> {
        self.end
    }

    pub fn overlapping_range(
        &self,
        array_range: std::ops::Range<usize>,
    ) -> Option<std::ops::Range<usize>> {
        match (self.start, self.end) {
            (Some(push_start), Some(push_end)) => {
                let push_end = push_end + 1; // Make end inclusive
                let start = array_range.start.max(push_start);
                let end = array_range.end.min(push_end);
                if start < end { Some(start..end) } else { None }
            }
            (Some(push_start), None) => {
                let start = array_range.start.max(push_start);
                if start < array_range.end {
                    Some(start..array_range.end)
                } else {
                    None
                }
            }
            (None, Some(push_end)) => {
                let push_end = push_end + 1; // Make end inclusive
                let end = array_range.end.min(push_end);
                if array_range.start < end {
                    Some(array_range.start..end)
                } else {
                    None
                }
            }
            (None, None) => Some(array_range),
        }
    }
}
