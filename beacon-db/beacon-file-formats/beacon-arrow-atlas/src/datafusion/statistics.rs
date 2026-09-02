//! The column ranges of a whole collection, for the file analyzer.
//!
//! A collection reports one range per column, folded over its live datasets out
//! of the footer. It costs no array read: the writer computed each dataset's
//! minimum and maximum while it staged the data, and the open already holds
//! them.
//!
//! # Why a wrong answer here is worse than no answer
//!
//! A recorded range prunes whole collections before a scan opens them, so a
//! range that is too narrow silently deletes matching rows from an answer.
//! Every path below reports unknown unless it can *prove* the bound. Unknown is
//! always legal: it only means Beacon reads what it might have skipped.

use arrow::datatypes::{DataType, Schema};
use atlas::{Atlas, StatValue};
use datafusion::common::{ColumnStatistics, Statistics, stats::Precision};
use datafusion::scalar::ScalarValue;

/// The statistics of one collection, in `table_schema` order.
pub fn collection_statistics(atlas: &Atlas, table_schema: &Schema) -> Statistics {
    let arrays = atlas.list_arrays();
    let live = atlas.dataset_count();

    let mut statistics = Statistics::default();
    for field in table_schema.fields() {
        let range = if arrays.iter().any(|array| array == field.name()) {
            column_range(atlas, field.name(), field.data_type(), live)
        } else {
            // An attribute column would need one `DatasetView` per dataset, and
            // each of those is a linear scan of the footer. The query-time
            // pruning index pays that where it is bounded and worth it; a
            // background pass over every collection is not the place.
            None
        };

        statistics = statistics.add_column_statistics(match range {
            Some((min, max)) => ColumnStatistics::new_unknown()
                .with_min_value(Precision::Exact(min))
                .with_max_value(Precision::Exact(max)),
            None => ColumnStatistics::new_unknown(),
        });
    }
    statistics
}

/// The range of one array column over every live dataset, or `None`.
///
/// # Every live dataset must report
///
/// A dataset that declares an array and never writes it has no statistics
/// entry, and its cells read back as the array's fill — or, when it declares
/// none, as zeros that nothing nulls. Folding only the datasets that *do*
/// report would then produce a range those zeros sit outside of, and pruning
/// would drop the collection for a query that matches them.
///
/// The footer cannot tell "declares it and never wrote it" from "does not
/// declare it" without a view per dataset, which is a linear scan each. So the
/// count is the proof: a bound is claimed only when every live dataset reported
/// one. A uniform collection — which is what `atlas create` writes, and what
/// this format exists for — satisfies that; a heterogeneous one goes unknown
/// and is read in full.
fn column_range(
    atlas: &Atlas,
    column: &str,
    target: &DataType,
    live: usize,
) -> Option<(ScalarValue, ScalarValue)> {
    let per_dataset = atlas.array_stats_by_dataset(column);
    if per_dataset.is_empty() || per_dataset.len() != live {
        return None;
    }

    let mut low: Option<ScalarValue> = None;
    let mut high: Option<ScalarValue> = None;
    for (_, stats) in per_dataset {
        // One dataset without a bound leaves the column unbounded: its values
        // may lie anywhere.
        let min = bound(stats.min.as_ref(), target)?;
        let max = bound(stats.max.as_ref(), target)?;
        low = Some(match low {
            Some(held) => smaller(held, min)?,
            None => min,
        });
        high = Some(match high {
            Some(held) => larger(held, max)?,
            None => max,
        });
    }
    Some((low?, high?))
}

/// One statistic as a scalar of the table's type, or `None` when it proves
/// nothing: absent, `NaN` — which sorts last and bounds nothing — or a value
/// that will not cast.
fn bound(value: Option<&StatValue>, target: &DataType) -> Option<ScalarValue> {
    let canonical = match value? {
        StatValue::Int(v) => ScalarValue::Int64(Some(*v)),
        StatValue::UInt(v) => ScalarValue::UInt64(Some(*v)),
        StatValue::Float(v) if v.is_nan() => return None,
        StatValue::Float(v) => ScalarValue::Float64(Some(*v)),
        StatValue::TimestampNs(v) => ScalarValue::TimestampNanosecond(Some(*v), None),
        StatValue::Bytes(bytes) => match std::str::from_utf8(bytes) {
            Ok(text) => ScalarValue::Utf8(Some(text.to_string())),
            Err(_) => ScalarValue::Binary(Some(bytes.clone())),
        },
    };
    let cast = canonical.cast_to(target).ok()?;
    // A cast that lands on null has lost the value, and a null bounds nothing.
    if cast.is_null() { None } else { Some(cast) }
}

/// The lower of two bounds, or `None` when they do not compare.
fn smaller(held: ScalarValue, next: ScalarValue) -> Option<ScalarValue> {
    match held.partial_cmp(&next)? {
        std::cmp::Ordering::Greater => Some(next),
        _ => Some(held),
    }
}

/// The higher of two bounds, or `None` when they do not compare.
fn larger(held: ScalarValue, next: ScalarValue) -> Option<ScalarValue> {
    match held.partial_cmp(&next)? {
        std::cmp::Ordering::Less => Some(next),
        _ => Some(held),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support;
    use arrow::datatypes::Field;

    fn schema(fields: Vec<Field>) -> Schema {
        Schema::new(fields)
    }

    fn range(
        statistics: &Statistics,
        index: usize,
    ) -> (Precision<ScalarValue>, Precision<ScalarValue>) {
        let column = &statistics.column_statistics[index];
        (column.min_value.clone(), column.max_value.clone())
    }

    /// A uniform collection — every dataset holding every array — reports the
    /// union of its datasets' ranges.
    #[tokio::test]
    async fn a_uniform_collection_reports_the_union_of_its_datasets() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::ranged(tmp.path(), 10).await;
        let atlas = test_support::open(tmp.path()).await;

        let statistics = collection_statistics(
            &atlas,
            &schema(vec![Field::new("temperature", DataType::Float32, true)]),
        );
        let (min, max) = range(&statistics, 0);
        // d0 starts at 0 and d9 ends at 93.
        assert_eq!(min, Precision::Exact(ScalarValue::Float32(Some(0.0))));
        assert_eq!(max, Precision::Exact(ScalarValue::Float32(Some(93.0))));
    }

    /// A column only some datasets declare goes unknown, because a dataset that
    /// declared it and never wrote it is indistinguishable from one that never
    /// declared it, and the first reads back as values this fold cannot see.
    #[tokio::test]
    async fn a_column_not_every_dataset_reports_goes_unknown() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::two_datasets(tmp.path()).await;
        let atlas = test_support::open(tmp.path()).await;

        let statistics = collection_statistics(
            &atlas,
            &schema(vec![
                // winter alone declares `cycle`.
                Field::new("cycle", DataType::Int32, true),
                // both declare `temperature`.
                Field::new("temperature", DataType::Float32, true),
            ]),
        );
        assert_eq!(range(&statistics, 0).0, Precision::Absent);
        assert_eq!(
            range(&statistics, 1).0,
            Precision::Exact(ScalarValue::Float32(Some(1.0))),
            "a column every dataset reports still bounds"
        );
        assert_eq!(
            range(&statistics, 1).1,
            Precision::Exact(ScalarValue::Float32(Some(22.0))),
        );
    }

    /// A column two datasets type differently is folded on the table's type.
    #[tokio::test]
    async fn a_mixed_dtype_column_folds_on_the_table_type() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::widening(tmp.path()).await;
        let atlas = test_support::open(tmp.path()).await;

        let statistics = collection_statistics(
            &atlas,
            &schema(vec![Field::new("value", DataType::Float64, true)]),
        );
        let (min, max) = range(&statistics, 0);
        // a holds [1, 2] as Int16 and b holds [3.5, 4.5] as Float32.
        assert_eq!(min, Precision::Exact(ScalarValue::Float64(Some(1.0))));
        assert_eq!(max, Precision::Exact(ScalarValue::Float64(Some(4.5))));
    }

    /// A column the collection does not hold, and a column whose values are
    /// text, both report unknown rather than a guess.
    #[tokio::test]
    async fn an_unknown_column_reports_unknown() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::ranged(tmp.path(), 4).await;
        let atlas = test_support::open(tmp.path()).await;

        let statistics = collection_statistics(
            &atlas,
            &schema(vec![
                Field::new("ghost", DataType::Float32, true),
                Field::new(".platform", DataType::Utf8, true),
            ]),
        );
        assert_eq!(range(&statistics, 0).0, Precision::Absent);
        assert_eq!(
            range(&statistics, 1).0,
            Precision::Absent,
            "an attribute is not measured here"
        );
    }

    #[tokio::test]
    async fn an_empty_collection_bounds_nothing() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::empty(tmp.path()).await;
        let atlas = test_support::open(tmp.path()).await;

        let statistics = collection_statistics(
            &atlas,
            &schema(vec![Field::new("temperature", DataType::Float32, true)]),
        );
        assert_eq!(range(&statistics, 0).0, Precision::Absent);
    }

    /// A deleted dataset counts toward nothing: neither the fold nor the count
    /// that guards it.
    #[tokio::test]
    async fn a_deleted_dataset_leaves_the_range() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::ranged(tmp.path(), 10).await;
        let atlas = test_support::open(tmp.path()).await;
        atlas.delete_dataset("d9").await.unwrap();

        // Reopen, so the handle reads the mask that was just written.
        let atlas = test_support::open(tmp.path()).await;
        let statistics = collection_statistics(
            &atlas,
            &schema(vec![Field::new("temperature", DataType::Float32, true)]),
        );
        assert_eq!(
            range(&statistics, 0).1,
            Precision::Exact(ScalarValue::Float32(Some(83.0))),
            "d9 held the values up to 93 and is gone"
        );
    }

    // ── the pieces ──────────────────────────────────────────────────────

    #[test]
    fn a_nan_bound_proves_nothing() {
        assert!(bound(Some(&StatValue::Float(f64::NAN)), &DataType::Float64).is_none());
    }

    #[test]
    fn a_bound_that_will_not_cast_proves_nothing() {
        assert!(
            bound(
                Some(&StatValue::Bytes(b"argo".to_vec())),
                &DataType::Float64
            )
            .is_none()
        );
    }

    #[test]
    fn an_absent_bound_proves_nothing() {
        assert!(bound(None, &DataType::Float64).is_none());
    }
}
