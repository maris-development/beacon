pub mod file;
pub mod gridded;
pub mod ragged;

use std::{collections::HashMap, ops::Range, sync::Arc};

use datafusion::common::ColumnStatistics;

use crate::{NdArrayD, array::subset::ArraySubset, statistics::file::column_statistics};

/// The statistics of each range of the 1-D `array`, in the order of `ranges`.
pub(crate) async fn range_statistics(
    array: &Arc<dyn NdArrayD>,
    ranges: &[Range<usize>],
) -> anyhow::Result<Vec<ColumnStatistics>> {
    // Read the array once; each range then slices the in-memory copy.
    let values = array
        .subset(ArraySubset::new(vec![0], array.shape()))
        .await?;
    // Chunks often share a range, so compute each range once.
    let mut by_range: HashMap<Range<usize>, ColumnStatistics> = HashMap::new();
    let mut result = Vec::with_capacity(ranges.len());
    for range in ranges {
        if !by_range.contains_key(range) {
            let slice = values
                .subset(ArraySubset::new(vec![range.start], vec![range.len()]))
                .await?;
            by_range.insert(range.clone(), column_statistics(slice.as_ref()).await);
        }
        result.push(by_range[range].clone());
    }
    Ok(result)
}
