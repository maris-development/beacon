//! Container row counts for DataFusion pruning, from per-column counts.
//!
//! DataFusion asks for one row count per container, the same for every column.
//! Beacon records a row count per column, and in an nd file the variables of
//! one container can differ in length. A missing column also records a null
//! count and a row count of 1, which reads as "every value is null".
//!
//! Pruning only compares the null count of a column with the row count. So
//! [`ContainerCounts`] takes the largest row count of the columns as the
//! container row count. It then reports an all-null column with a null count
//! equal to that row count. Both answers that pruning needs stay exact:
//! "the column has no nulls" and "the column has only nulls".

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, UInt64Array};
use arrow::datatypes::{DataType, UInt64Type};

/// One row count per container, from the row counts of the columns.
#[derive(Debug, Clone)]
pub struct ContainerCounts {
    row_count: Arc<UInt64Array>,
}

impl ContainerCounts {
    /// The largest known row count of `row_counts` in each of `num_containers`.
    ///
    /// A container stays null when no column knows its row count. An array that
    /// does not cast to `UInt64` or has a different length adds nothing.
    pub fn new<'a>(num_containers: usize, row_counts: impl IntoIterator<Item = &'a ArrayRef>) -> Self {
        let mut largest: Vec<Option<u64>> = vec![None; num_containers];
        for counts in row_counts {
            let Some(counts) = as_u64(counts, num_containers) else {
                continue;
            };
            for (slot, count) in largest.iter_mut().zip(counts.iter()) {
                if let Some(count) = count {
                    *slot = Some(slot.map_or(count, |known| known.max(count)));
                }
            }
        }
        Self {
            row_count: Arc::new(UInt64Array::from(largest)),
        }
    }

    /// The row count of each container, for `PruningStatistics::row_counts`.
    pub fn row_counts(&self) -> ArrayRef {
        self.row_count.clone()
    }

    /// The null counts of a column that is null in every container.
    pub fn all_null(&self) -> ArrayRef {
        self.row_count.clone()
    }

    /// The null counts of one column, measured against the container row count.
    ///
    /// A container where the column has only nulls reports the container row
    /// count. Other null counts stay as they are. `None` when the arrays do not
    /// cast to `UInt64` or have a different length.
    pub fn null_counts(&self, null_count: &ArrayRef, row_count: &ArrayRef) -> Option<ArrayRef> {
        let rows = self.row_count.len();
        let null_count = as_u64(null_count, rows)?;
        let row_count = as_u64(row_count, rows)?;
        let adjusted: UInt64Array = null_count
            .iter()
            .zip(row_count.iter())
            .zip(self.row_count.iter())
            .map(|((nulls, column_rows), container_rows)| match (nulls, column_rows) {
                (Some(nulls), Some(column_rows)) if nulls == column_rows => container_rows,
                _ => nulls,
            })
            .collect();
        Some(Arc::new(adjusted))
    }
}

fn as_u64(array: &ArrayRef, len: usize) -> Option<UInt64Array> {
    if array.len() != len {
        return None;
    }
    let cast = arrow::compute::cast(array, &DataType::UInt64).ok()?;
    Some(cast.as_primitive::<UInt64Type>().clone())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn counts(values: &[Option<u64>]) -> ArrayRef {
        Arc::new(UInt64Array::from(values.to_vec()))
    }

    fn values(array: &ArrayRef) -> Vec<Option<u64>> {
        array.as_primitive::<UInt64Type>().iter().collect()
    }

    #[test]
    fn the_container_row_count_is_the_largest_column_row_count() {
        let short = counts(&[Some(10), None, Some(1)]);
        let long = counts(&[Some(100), None, None]);
        let containers = ContainerCounts::new(3, [&short, &long]);
        assert_eq!(values(&containers.row_counts()), vec![Some(100), None, Some(1)]);
    }

    #[test]
    fn an_all_null_column_reports_the_container_row_count() {
        let rows = counts(&[Some(10), Some(10), Some(1)]);
        let grid = counts(&[Some(100), Some(100), Some(100)]);
        let containers = ContainerCounts::new(3, [&rows, &grid]);

        // Container 0: no nulls. Container 1: some nulls. Container 2: the
        // column is missing, which records 1 null out of 1 row.
        let nulls = counts(&[Some(0), Some(4), Some(1)]);
        let adjusted = containers.null_counts(&nulls, &rows).unwrap();
        assert_eq!(values(&adjusted), vec![Some(0), Some(4), Some(100)]);
    }

    #[test]
    fn unknown_counts_stay_unknown() {
        let rows = counts(&[None, Some(5)]);
        let containers = ContainerCounts::new(2, [&rows]);
        let nulls = counts(&[None, None]);
        let adjusted = containers.null_counts(&nulls, &rows).unwrap();
        assert_eq!(values(&adjusted), vec![None, None]);
    }

    #[test]
    fn a_column_of_the_wrong_length_adds_nothing() {
        let wrong = counts(&[Some(7)]);
        let containers = ContainerCounts::new(2, [&wrong]);
        assert_eq!(values(&containers.row_counts()), vec![None, None]);
        assert!(containers.null_counts(&wrong, &wrong).is_none());
    }
}
