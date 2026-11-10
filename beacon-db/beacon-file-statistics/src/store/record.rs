//! The statistics of one file version, as the store takes them.

use arrow::datatypes::{DataType, Schema};
use datafusion::common::{ColumnStatistics, Statistics};

use super::stat::ColumnStat;
use crate::stamp::Stamp;

/// The statistics of one analyzed file version.
#[derive(Debug, Clone, PartialEq)]
pub struct FileRecord {
    pub(super) stamp: Stamp,
    pub(super) columns: Vec<(String, ColumnStat)>,
}

impl FileRecord {
    /// The record of the file with `stamp`, from the statistics a format reports.
    ///
    /// The column statistics match the fields of `schema` by position. A column
    /// with no min and no max, or with a type the store keeps no range for, is
    /// left out.
    pub fn new(stamp: Stamp, schema: &Schema, statistics: &Statistics) -> Self {
        let num_rows = statistics.num_rows.get_value().map(|n| *n as u64);
        let columns = schema
            .fields()
            .iter()
            .zip(&statistics.column_statistics)
            .filter_map(|(field, column)| {
                to_column_stat(column, field.data_type(), num_rows)
                    .map(|stat| (field.name().clone(), stat))
            })
            .collect();
        Self { stamp, columns }
    }

    /// The record of a file whose analysis failed. The store keeps it with no
    /// statistics, so the same version is not analyzed again.
    pub fn failed(stamp: Stamp) -> Self {
        Self {
            stamp,
            columns: Vec::new(),
        }
    }

    /// The stamp of the file version.
    pub fn stamp(&self) -> Stamp {
        self.stamp
    }

    /// The number of columns with statistics, for tests outside the store.
    #[cfg(test)]
    pub(crate) fn num_columns(&self) -> usize {
        self.columns.len()
    }
}

/// One column's statistics, or `None` when it has no min and no max, or no storage type.
///
/// An absent count stays absent. DataFusion prunes `IS NOT NULL` on
/// `null_count != row_count`, so two unknown counts stored as zero read as
/// "each value is null" and drop a file full of values.
///
/// `get_value` accepts `Exact` and `Inexact`, as DataFusion's own pruning does.
/// That is correct only while an inexact bound is wider than the true bound.
fn to_column_stat(
    column: &ColumnStatistics,
    data_type: &DataType,
    num_rows: Option<u64>,
) -> Option<ColumnStat> {
    let min = column.min_value.get_value();
    let max = column.max_value.get_value();
    if min.is_none() && max.is_none() {
        return None;
    }
    let min = min.and_then(|value| value.to_array().ok());
    let max = max.and_then(|value| value.to_array().ok());
    ColumnStat::from_arrays(
        min.as_ref(),
        max.as_ref(),
        column.null_count.get_value().map(|n| *n as u64),
        num_rows,
        data_type,
    )
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::Field;
    use datafusion::common::stats::Precision;
    use datafusion::scalar::ScalarValue;

    use super::*;
    use crate::store::stat::StatScalar;

    fn record(statistics: Statistics) -> FileRecord {
        let schema = Schema::new(vec![
            Field::new("TEMP", DataType::Float64, true),
            Field::new("PSAL", DataType::Float64, true),
        ]);
        FileRecord::new(Stamp::from_parts(0, 1), &schema, &statistics)
    }

    fn known(min: f64, max: f64, nulls: usize) -> ColumnStatistics {
        ColumnStatistics {
            null_count: Precision::Exact(nulls),
            max_value: Precision::Exact(ScalarValue::Float64(Some(max))),
            min_value: Precision::Exact(ScalarValue::Float64(Some(min))),
            ..Default::default()
        }
    }

    #[test]
    fn statistics_match_the_schema_by_position() {
        let record = record(Statistics {
            num_rows: Precision::Exact(1_000),
            total_byte_size: Precision::Absent,
            column_statistics: vec![known(0.0, 10.0, 3), known(34.0, 35.0, 0)],
        });

        let names: Vec<&str> = record.columns.iter().map(|(n, _)| n.as_str()).collect();
        assert_eq!(names, ["TEMP", "PSAL"]);
        let temp = &record.columns[0].1;
        assert_eq!(temp.min, StatScalar::F64(0.0));
        assert_eq!(temp.null_count, Some(3));
        assert_eq!(temp.row_count, Some(1_000));
    }

    #[test]
    fn a_column_with_no_range_gives_no_row() {
        let record = record(Statistics {
            num_rows: Precision::Exact(10),
            total_byte_size: Precision::Absent,
            column_statistics: vec![known(1.0, 2.0, 0), ColumnStatistics::default()],
        });

        assert_eq!(record.columns.len(), 1);
        assert_eq!(record.columns[0].0, "TEMP");
    }

    #[test]
    fn an_absent_count_stays_absent_and_inexact_bounds_stay() {
        let record = record(Statistics {
            num_rows: Precision::Absent,
            total_byte_size: Precision::Absent,
            column_statistics: vec![
                ColumnStatistics {
                    min_value: Precision::Inexact(ScalarValue::Float64(Some(1.0))),
                    max_value: Precision::Inexact(ScalarValue::Float64(Some(9.0))),
                    ..Default::default()
                },
                ColumnStatistics::default(),
            ],
        });

        let stat = &record.columns[0].1;
        assert_eq!(stat.max, StatScalar::F64(9.0));
        assert_eq!(stat.null_count, None);
        assert_eq!(stat.row_count, None);
    }

    #[test]
    fn a_failed_record_has_no_columns() {
        let record = FileRecord::failed(Stamp::from_parts(0, 1));
        assert!(record.columns.is_empty());
        assert_eq!(record.stamp(), Stamp::from_parts(0, 1));
    }
}
