//! Reading pruning statistics for a list of stamps.
//!
//! Each path fails open: a stamp with no row, an unknown column or a failed
//! cast gives null statistics, and the pruning engine keeps the file.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BooleanArray, UInt64Array, new_null_array};
use arrow::datatypes::{DataType, Field, SchemaRef};
use beacon_datafusion_ext::container_counts::ContainerCounts;
use datafusion::common::Column;
use datafusion::common::pruning::PruningStatistics;
use datafusion::scalar::ScalarValue;
use redb::{Database, ReadableDatabase, ReadableTable};

use super::StatisticsStore;
use super::error::Result;
use super::stat::{ColumnStat, StatScalar, scalars_to_array};
use super::tables::{COLUMNS, ColumnId, STATS, decode_column_id, stamp_of, stat_key};
use crate::stamp::Stamp;

/// Up to this many stamps, a column read looks up each stamp. Above it, the
/// read scans the column, which costs less than many lookups.
const POINT_READ_LIMIT: usize = 4096;

/// Each stamp with its position in the request, sorted by stamp.
type Order = [(Stamp, usize)];

/// Pruning statistics for a list of stamps. Container `i` is stamp `i` of the request.
pub struct StampStatistics {
    containers: usize,
    columns: HashMap<String, ColumnRows>,
    /// One row count for each container, for all columns.
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

impl StatisticsStore {
    /// The pruning statistics of the columns of `schema` for `stamps`.
    ///
    /// `stamps` can be in any order, and can repeat a stamp. Container `i` is
    /// `stamps[i]`. Each field of `schema` gives the type that the predicate
    /// compares with, and each column is read on its own blocking thread.
    ///
    /// # Errors
    ///
    /// Returns an error when a read fails or a stored value does not decode.
    pub async fn get(&self, schema: &SchemaRef, stamps: &[Stamp]) -> Result<StampStatistics> {
        let containers = stamps.len();
        let mut order: Vec<(Stamp, usize)> = stamps.iter().copied().zip(0..).collect();
        order.sort_unstable();
        // An `Arc` lets each blocking task share the order without a copy.
        let order: Arc<Order> = order.into();
        let reads = schema.fields().iter().map(|field| {
            let field = Arc::clone(field);
            let order = Arc::clone(&order);
            self.blocking(move |store| {
                let rows = read_column(&store.database, &field, &order, containers)?;
                Ok(rows.map(|rows| (field.name().clone(), rows)))
            })
        });
        let columns: HashMap<String, ColumnRows> = futures::future::try_join_all(reads)
            .await?
            .into_iter()
            .flatten()
            .collect();
        let counts = ContainerCounts::new(containers, columns.values().map(|c| &c.row_count));
        Ok(StampStatistics {
            containers,
            columns,
            counts,
        })
    }
}

/// The rows of one column, or `None` when no stamp has a usable row.
fn read_column(
    database: &Database,
    field: &Field,
    order: &Order,
    containers: usize,
) -> Result<Option<ColumnRows>> {
    let read = database.begin_read()?;
    let column_id = read
        .open_table(COLUMNS)?
        .get(field.name().as_bytes())?
        .map(|value| decode_column_id(value.value()))
        .transpose()?;
    let Some(column_id) = column_id else {
        return Ok(None);
    };
    let stats = read.open_table(STATS)?;
    let found = if order.len() <= POINT_READ_LIMIT {
        point_reads(&stats, column_id, order)?
    } else {
        scan(&stats, column_id, order)?
    };
    Ok(gather(found, field.data_type(), containers))
}

/// The stored rows of `order`, by one lookup for each stamp.
///
/// # Returns
///
/// `(position, statistics)` for each requested stamp that has a row.
fn point_reads(
    stats: &impl ReadableTable<&'static [u8], &'static [u8]>,
    column_id: ColumnId,
    order: &Order,
) -> Result<Vec<(usize, ColumnStat)>> {
    let mut found = Vec::new();
    for (stamp, position) in order {
        if let Some(value) = stats.get(stat_key(column_id, *stamp).as_slice())? {
            found.push((*position, bincode::deserialize(value.value())?));
        }
    }
    Ok(found)
}

/// The stored rows of `order`, by one scan over the column from the first stamp to the last.
fn scan(
    stats: &impl ReadableTable<&'static [u8], &'static [u8]>,
    column_id: ColumnId,
    order: &Order,
) -> Result<Vec<(usize, ColumnStat)>> {
    let (Some((first, _)), Some((last, _))) = (order.first(), order.last()) else {
        return Ok(Vec::new());
    };
    let start = stat_key(column_id, *first);
    let end = stat_key(column_id, *last);
    let mut found = Vec::new();
    let mut next = 0;
    for entry in stats.range(start.as_slice()..=end.as_slice())? {
        let (key, value) = entry?;
        let stamp = stamp_of(key.value())?;
        // Both lists ascend, so the join is a merge. Only a match decodes its value.
        while order.get(next).is_some_and(|(wanted, _)| *wanted < stamp) {
            next += 1;
        }
        let matches = order[next..]
            .iter()
            .take_while(|(wanted, _)| *wanted == stamp);
        let positions: Vec<usize> = matches.map(|(_, position)| *position).collect();
        if let Some((last, others)) = positions.split_last() {
            let stat: ColumnStat = bincode::deserialize(value.value())?;
            found.extend(others.iter().map(|position| (*position, stat.clone())));
            found.push((*last, stat));
            next += positions.len();
        }
    }
    Ok(found)
}

/// The rows of one stored type of one column.
struct TypeGroup {
    data_type: DataType,
    positions: Vec<usize>,
    mins: Vec<StatScalar>,
    maxes: Vec<StatScalar>,
}

/// Places the found rows at their positions, and casts min and max to `target`.
///
/// The rows of a stored type that does not cast to `target` read as null.
/// `None` when no row is usable.
fn gather(
    found: Vec<(usize, ColumnStat)>,
    target: &DataType,
    containers: usize,
) -> Option<ColumnRows> {
    let mut null_counts: Vec<Option<u64>> = vec![None; containers];
    let mut row_counts: Vec<Option<u64>> = vec![None; containers];
    // Files can disagree on the column type; normally there is one group.
    let mut groups: Vec<TypeGroup> = Vec::new();
    for (position, stat) in found {
        null_counts[position] = stat.null_count;
        row_counts[position] = stat.row_count;
        let index = match groups.iter().position(|g| g.data_type == stat.data_type) {
            Some(index) => index,
            None => {
                groups.push(TypeGroup {
                    data_type: stat.data_type,
                    positions: Vec::new(),
                    mins: Vec::new(),
                    maxes: Vec::new(),
                });
                groups.len() - 1
            }
        };
        let group = &mut groups[index];
        group.positions.push(position);
        group.mins.push(stat.min);
        group.maxes.push(stat.max);
    }

    // Source 0 is one null, for each container with no usable row.
    let mut mins: Vec<ArrayRef> = vec![new_null_array(target, 1)];
    let mut maxes: Vec<ArrayRef> = vec![new_null_array(target, 1)];
    let mut indices = vec![(0usize, 0usize); containers];
    for group in groups {
        let cast = |values: &[StatScalar]| {
            let array = scalars_to_array(values, &group.data_type).ok()?;
            arrow::compute::cast(&array, target).ok()
        };
        let (Some(min), Some(max)) = (cast(&group.mins), cast(&group.maxes)) else {
            continue;
        };
        let source = mins.len();
        mins.push(min);
        maxes.push(max);
        for (row, position) in group.positions.iter().enumerate() {
            indices[*position] = (source, row);
        }
    }
    if mins.len() == 1 {
        return None;
    }

    let interleave = |sources: &[ArrayRef]| {
        let sources: Vec<&dyn Array> = sources.iter().map(|source| source.as_ref()).collect();
        arrow::compute::interleave(&sources, &indices).ok()
    };
    Some(ColumnRows {
        min: interleave(&mins)?,
        max: interleave(&maxes)?,
        null_count: Arc::new(UInt64Array::from(null_counts)),
        row_count: Arc::new(UInt64Array::from(row_counts)),
    })
}

#[cfg(test)]
mod tests {
    use arrow::array::AsArray;
    use arrow::datatypes::{Float64Type, Int64Type, Schema};

    use super::*;
    use crate::store::FileRecord;
    use crate::store::tests::{record, stamp, store, url};

    fn schema(data_type: DataType) -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("TEMP", data_type, true)]))
    }

    fn stamps(files: &[u64]) -> Vec<Stamp> {
        files.iter().copied().map(stamp).collect()
    }

    fn mins(statistics: &StampStatistics) -> Vec<Option<f64>> {
        let min = statistics
            .min_values(&Column::from_name("TEMP"))
            .expect("a column");
        min.as_primitive::<Float64Type>().iter().collect()
    }

    #[tokio::test]
    async fn containers_follow_the_request_order() {
        let store = store().await;
        store
            .put(vec![record(1, 10.0), record(2, 20.0), record(3, 30.0)])
            .await
            .expect("stored");

        let statistics = store
            .get(&schema(DataType::Float64), &stamps(&[3, 9, 1, 3]))
            .await
            .expect("read");

        assert_eq!(statistics.num_containers(), 4);
        assert_eq!(
            mins(&statistics),
            [Some(30.0), None, Some(10.0), Some(30.0)]
        );
        let rows = statistics.row_counts().expect("row counts");
        assert_eq!(rows.null_count(), 1, "the unknown stamp has no row count");
    }

    #[tokio::test]
    async fn point_reads_and_the_scan_find_the_same_rows() {
        // Arrange: more stamps than the point read limit, with a repeat.
        let store = store().await;
        let records: Vec<FileRecord> = (0..POINT_READ_LIMIT as u64 + 10)
            .map(|file| record(file * 2, file as f64))
            .collect();
        store.put(records).await.expect("stored");
        let few = stamps(&[8_018, 1, 2, 4_000, 0]);
        let mut many = few.clone();
        many.extend(stamps(&(0..POINT_READ_LIMIT as u64).collect::<Vec<_>>()));

        // Act
        let looked_up = store.get(&schema(DataType::Float64), &few).await;
        let scanned = store.get(&schema(DataType::Float64), &many).await;

        // Assert
        let looked_up = mins(&looked_up.expect("read"));
        let scanned = mins(&scanned.expect("read"));
        assert_eq!(
            looked_up,
            [Some(4_009.0), None, Some(1.0), Some(2_000.0), Some(0.0)]
        );
        assert_eq!(scanned[..5], looked_up[..]);
        assert_eq!(scanned[5 + 4], Some(2.0), "file 4 is the third stored file");
    }

    #[tokio::test]
    async fn a_column_with_two_types_casts_each_group() {
        let store = store().await;
        let mut ints = record(2, 0.0);
        ints.columns[0].1 = ColumnStat {
            min: StatScalar::I64(7),
            max: StatScalar::I64(9),
            null_count: None,
            row_count: None,
            data_type: DataType::Int64,
        };
        store.put(vec![record(1, 1.5), ints]).await.expect("stored");

        let as_floats = store
            .get(&schema(DataType::Float64), &stamps(&[1, 2]))
            .await
            .expect("read");
        let as_ints = store
            .get(&schema(DataType::Int64), &stamps(&[2]))
            .await
            .expect("read");

        assert_eq!(mins(&as_floats), [Some(1.5), Some(7.0)]);
        let max = as_ints
            .max_values(&Column::from_name("TEMP"))
            .expect("a column");
        assert_eq!(max.as_primitive::<Int64Type>().value(0), 9);
    }

    #[tokio::test]
    async fn a_type_that_does_not_cast_gives_no_column() {
        let store = store().await;
        store.put(vec![record(1, 0.0)]).await.expect("stored");
        let structure = DataType::Struct(vec![Field::new("a", DataType::Int32, true)].into());

        let statistics = store
            .get(&schema(structure), &stamps(&[1]))
            .await
            .expect("read");

        assert!(statistics.min_values(&Column::from_name("TEMP")).is_none());
    }

    #[tokio::test]
    async fn an_unknown_column_or_a_removed_stamp_gives_no_column() {
        let store = store().await;
        store.put(vec![record(1, 0.0)]).await.expect("stored");
        store
            .retain(&url("memory"), HashSet::new())
            .await
            .expect("retained");

        let removed = store
            .get(&schema(DataType::Float64), &stamps(&[1]))
            .await
            .expect("read");
        let other = Arc::new(Schema::new(vec![Field::new(
            "PSAL",
            DataType::Float64,
            true,
        )]));
        let unknown = store.get(&other, &stamps(&[1])).await.expect("read");

        assert!(removed.min_values(&Column::from_name("TEMP")).is_none());
        assert!(unknown.min_values(&Column::from_name("PSAL")).is_none());
        assert_eq!(
            removed.num_containers(),
            1,
            "each stamp keeps its container"
        );
    }
}
