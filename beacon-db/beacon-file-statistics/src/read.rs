//! Pruning statistics for a list of files, read from a statistics table.
//!
//! Each path fails open: an unknown file, a file that changed since its
//! analysis, an unknown column or a failed cast gives null statistics, and the
//! pruning engine keeps the file.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, BooleanArray, RecordBatch, new_null_array};
use arrow::datatypes::{DataType, Schema, UInt64Type};
use beacon_datafusion_ext::container_counts::ContainerCounts;
use datafusion::common::Column;
use datafusion::common::pruning::PruningStatistics;
use datafusion::scalar::ScalarValue;
use futures::TryStreamExt;
use lance::Dataset;

use crate::layout::{Layout, ROW_COUNT, STAMP, StatisticsColumn, range_struct};

/// Marks the end of a chain of positions that share a stamp.
const END: usize = usize::MAX;

/// Pruning statistics for a list of files. Container `i` is file `i` of the request.
pub(crate) struct StampStatistics {
    containers: usize,
    columns: HashMap<String, ColumnRows>,
    counts: ContainerCounts,
}

/// The statistics of one column, with row `i` for container `i`.
struct ColumnRows {
    min: ArrayRef,
    max: ArrayRef,
    null_count: ArrayRef,
    row_count: ArrayRef,
}

impl std::fmt::Debug for StampStatistics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StampStatistics")
            .field("containers", &self.containers)
            .field("columns", &self.columns.len())
            .finish()
    }
}

impl StampStatistics {
    /// Statistics with `containers` containers and no columns. A prune over them keeps each file.
    fn unknown(containers: usize) -> Self {
        Self {
            containers,
            columns: HashMap::new(),
            counts: ContainerCounts::new(containers, std::iter::empty()),
        }
    }
}

impl PruningStatistics for StampStatistics {
    fn min_values(&self, column: &Column) -> Option<ArrayRef> {
        self.columns.get(column.name()).map(|c| Arc::clone(&c.min))
    }

    fn max_values(&self, column: &Column) -> Option<ArrayRef> {
        self.columns.get(column.name()).map(|c| Arc::clone(&c.max))
    }

    fn num_containers(&self) -> usize {
        self.containers
    }

    fn null_counts(&self, column: &Column) -> Option<ArrayRef> {
        let c = self.columns.get(column.name())?;
        self.counts.null_counts(&c.null_count, &c.row_count)
    }

    fn row_counts(&self) -> Option<ArrayRef> {
        Some(self.counts.row_counts())
    }

    /// A min/max range cannot answer this, so the predicate uses the range tests.
    fn contained(&self, _column: &Column, _values: &HashSet<ScalarValue>) -> Option<BooleanArray> {
        None
    }
}

/// The statistics of the columns of `schema` for the files with `stamps`.
///
/// One projected scan reads the stamp, the row count and the requested columns
/// of each row, and a hash join on the stamp places each row at the positions
/// of its file in `stamps`.
///
/// # Errors
///
/// Returns an error when the scan fails.
pub(crate) async fn read(
    dataset: &Dataset,
    layout: &Layout,
    stamps: &[u64],
    schema: &Schema,
) -> anyhow::Result<StampStatistics> {
    let containers = stamps.len();
    let wanted: Vec<(&DataType, &StatisticsColumn)> = schema
        .fields()
        .iter()
        .filter_map(|field| Some((field.data_type(), layout.column(field.name())?)))
        .collect();
    if containers == 0 || wanted.is_empty() {
        return Ok(StampStatistics::unknown(containers));
    }

    let mut projection = vec![STAMP, ROW_COUNT];
    projection.extend(wanted.iter().map(|(_, column)| column.stored.as_str()));
    let mut scanner = dataset.scan();
    scanner.project(&projection)?;
    let batches: Vec<RecordBatch> = scanner.try_into_stream().await?.try_collect().await?;

    let indices = join(stamps, &batches);
    let gather = |name: &str, data_type: &DataType| -> anyhow::Result<ArrayRef> {
        // Source 0 is one null, for each file without a row.
        let mut sources: Vec<ArrayRef> = vec![new_null_array(data_type, 1)];
        for batch in &batches {
            let column = batch
                .column_by_name(name)
                .ok_or_else(|| anyhow::anyhow!("the statistics scan has no column '{name}'"))?;
            sources.push(Arc::clone(column));
        }
        let sources: Vec<&dyn Array> = sources.iter().map(|source| source.as_ref()).collect();
        Ok(arrow::compute::interleave(&sources, &indices)?)
    };

    let row_count = gather(ROW_COUNT, &DataType::UInt64)?;
    let mut columns = HashMap::with_capacity(wanted.len());
    for (target, column) in wanted {
        let range = gather(&column.stored, &range_struct(&column.data_type))?;
        let range = range.as_struct();
        // A type the predicate cannot compare with drops the column, which keeps each file.
        let (Ok(min), Ok(max)) = (
            arrow::compute::cast(range.column(0), target),
            arrow::compute::cast(range.column(1), target),
        ) else {
            continue;
        };
        columns.insert(
            column.name.clone(),
            ColumnRows {
                min,
                max,
                null_count: Arc::clone(range.column(2)),
                row_count: Arc::clone(&row_count),
            },
        );
    }
    let counts = ContainerCounts::new(containers, std::iter::once(&row_count));
    Ok(StampStatistics {
        containers,
        columns,
        counts,
    })
}

/// For each position in `stamps`, the `(source, row)` of its row in `batches`.
///
/// Source `b + 1` is batch `b`. A stamp without a row gets `(0, 0)`, the null source.
fn join(stamps: &[u64], batches: &[RecordBatch]) -> Vec<(usize, usize)> {
    // The first position of each stamp, and a chain to the next position with
    // the same stamp, so a repeated file needs no list of its own.
    let mut first: HashMap<u64, usize> = HashMap::with_capacity(stamps.len());
    let mut next = vec![END; stamps.len()];
    for (position, stamp) in stamps.iter().enumerate().rev() {
        if let Some(previous) = first.insert(*stamp, position) {
            next[position] = previous;
        }
    }

    let mut indices = vec![(0, 0); stamps.len()];
    for (source, batch) in batches.iter().enumerate() {
        let Some(column) = batch.column_by_name(STAMP) else {
            continue;
        };
        for (row, stamp) in column
            .as_primitive::<UInt64Type>()
            .values()
            .iter()
            .enumerate()
        {
            let mut position = first.get(stamp).copied().unwrap_or(END);
            while position != END {
                indices[position] = (source + 1, row);
                position = next[position];
            }
        }
    }
    indices
}

#[cfg(test)]
mod tests {
    use arrow::array::UInt64Array;
    use arrow::datatypes::Field;

    use super::*;

    fn batch(stamps: &[u64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            STAMP,
            DataType::UInt64,
            false,
        )]));
        RecordBatch::try_new(
            schema,
            vec![Arc::new(UInt64Array::from(stamps.to_vec())) as ArrayRef],
        )
        .expect("a batch")
    }

    #[test]
    fn the_join_places_each_row_at_each_position_of_its_stamp() {
        let batches = [batch(&[10, 20]), batch(&[30])];

        let indices = join(&[30, 99, 10, 30], &batches);

        assert_eq!(indices, [(2, 0), (0, 0), (1, 0), (2, 0)]);
    }

    #[test]
    fn unknown_statistics_keep_their_container_count() {
        let statistics = StampStatistics::unknown(3);

        assert_eq!(statistics.num_containers(), 3);
        assert!(statistics.min_values(&Column::from_name("TEMP")).is_none());
        assert_eq!(statistics.row_counts().expect("row counts").len(), 3);
    }
}
