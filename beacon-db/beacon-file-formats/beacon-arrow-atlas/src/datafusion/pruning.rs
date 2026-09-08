//! Dropping the datasets a predicate cannot match, from what is in memory.
//!
//! # One index, not a decision per dataset
//!
//! A collection can hold millions of datasets. Evaluating a predicate against
//! each one in turn would cost millions of evaluations. Instead the opener
//! builds one [`PruningIndex`] over the collection: one row per live dataset,
//! and one column of typed Arrow statistics per column the predicate names.
//! DataFusion's [`PruningPredicate`] then judges the whole collection in one
//! vectorised pass, and the result is one bit per dataset.
//!
//! The inputs are the opener's own column views. A variable's segment records
//! the statistics of every dataset that wrote it, and an attribute view holds
//! every dataset's value. Both are in memory once the views exist, so the
//! index costs no I/O. Reading the views rather than asking the collection
//! again also keeps pruning on the columns the scan reads: a column resolves
//! one way, in [`column_views`](super::opener::column_views).
//!
//! # A column the dataset lacks
//!
//! The scan reads such a column as nulls, so the index says so:
//! `null_count == row_count`. DataFusion then drops the dataset for `x > 5`
//! and for `x IS NOT NULL`, and keeps it for `x IS NULL`.
//!
//! The writer counts a cell nobody wrote as null too. With a fill value that is
//! what the scan reads, and the counts hold. Without one the scan reads zeros,
//! so an entry with unwritten cells and no fill value says nothing about its
//! values. Its counts stay unknown, and its dataset stays in.
//!
//! # Pruning is only ever an optimization
//!
//! Every path here fails open: an error, a predicate the engine cannot use, or
//! a bound that will not cast all leave the datasets in. A dataset that
//! survives is still filtered row by row above the scan, so a hiccup here
//! costs time and never a row.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, UInt64Array, new_null_array};
use arrow::datatypes::{DataType, FieldRef, SchemaRef};
use atlas::{ArrayFile, Attr, StatValue};
use datafusion::common::Column;
use datafusion::common::pruning::PruningStatistics;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::utils::collect_columns;
use datafusion::physical_optimizer::pruning::PruningPredicate;
use datafusion::scalar::ScalarValue;
use indexmap::IndexMap;

use super::opener::AtlasColumnView;

/// The datasets of `names` that `predicate` could still match, in order.
///
/// `logical_schema` must type every column the predicate names, which the
/// scan's own projected schema does: a filter that stays above the scan forces
/// its columns into the projection. `views` must resolve those columns the way
/// the scan reads them.
///
/// Fails open to `names` on anything it cannot prove.
pub(crate) async fn prune_datasets(
    views: &Arc<IndexMap<FieldRef, Option<AtlasColumnView>>>,
    names: Vec<String>,
    predicate: &Arc<dyn PhysicalExpr>,
    logical_schema: &SchemaRef,
) -> Vec<String> {
    let Ok(pruning) = PruningPredicate::try_new(Arc::clone(predicate), Arc::clone(logical_schema))
    else {
        // The engine cannot use this predicate shape.
        return names;
    };
    let referenced = collect_columns(pruning.orig_expr());
    if referenced.is_empty() || names.is_empty() {
        return names;
    }

    // A column the predicate names, with the type the table gives it.
    let wanted: Vec<(String, DataType)> = referenced
        .iter()
        .filter_map(|column| {
            let (field, _) = views
                .iter()
                .find(|(field, _)| field.name() == column.name())?;
            Some((column.name().to_string(), field.data_type().clone()))
        })
        .collect();
    if wanted.is_empty() {
        // Nothing the predicate names is a column of the scan.
        return names;
    }

    // The pivot is pure CPU over what is in memory, and a million rows is
    // real work, so it does not run on the async runtime.
    let names: Arc<[String]> = names.into();
    let (views, rows) = (Arc::clone(views), Arc::clone(&names));
    let built = tokio::task::spawn_blocking(move || build_index(&views, &rows, &wanted)).await;
    let Ok(index) = built else {
        return names.to_vec();
    };

    match pruning.prune(&index) {
        Ok(kept) => names
            .iter()
            .zip(kept)
            .filter(|(_, keep)| *keep)
            .map(|(name, _)| name.clone())
            .collect(),
        Err(e) => {
            tracing::debug!("atlas pruning fell back to reading every dataset: {e}");
            names.to_vec()
        }
    }
}

// ─── The index ───────────────────────────────────────────────────────────────

/// One column's statistics, one row per dataset.
struct StatColumn {
    min: ArrayRef,
    max: ArrayRef,
    null_count: ArrayRef,
    row_count: ArrayRef,
}

/// A collection's statistics, pivoted into columns of equal length.
struct PruningIndex {
    rows: usize,
    columns: HashMap<String, StatColumn>,
}

impl PruningStatistics for PruningIndex {
    fn min_values(&self, column: &Column) -> Option<ArrayRef> {
        self.columns.get(column.name()).map(|c| Arc::clone(&c.min))
    }

    fn max_values(&self, column: &Column) -> Option<ArrayRef> {
        self.columns.get(column.name()).map(|c| Arc::clone(&c.max))
    }

    fn null_counts(&self, column: &Column) -> Option<ArrayRef> {
        self.columns
            .get(column.name())
            .map(|c| Arc::clone(&c.null_count))
    }

    fn row_counts(&self, column: &Column) -> Option<ArrayRef> {
        self.columns
            .get(column.name())
            .map(|c| Arc::clone(&c.row_count))
    }

    fn num_containers(&self) -> usize {
        self.rows
    }

    fn contained(
        &self,
        _column: &Column,
        _values: &std::collections::HashSet<ScalarValue>,
    ) -> Option<arrow::array::BooleanArray> {
        // An attribute's value is exact, so an `IN` list could prune on one.
        // Not yet: every column here reports a range, and a range says nothing
        // about membership.
        None
    }
}

// ─── Building it ─────────────────────────────────────────────────────────────

/// Pivot the views into one [`StatColumn`] per wanted column.
fn build_index(
    views: &IndexMap<FieldRef, Option<AtlasColumnView>>,
    names: &[String],
    wanted: &[(String, DataType)],
) -> PruningIndex {
    let columns = wanted
        .iter()
        .filter_map(|(column, target)| {
            let (_, view) = views.iter().find(|(field, _)| field.name() == column)?;
            let packed = match view {
                // No dataset declares the column. The scan reads nulls.
                None => all_null_column(names.len(), target),
                Some(AtlasColumnView::Array { segment }) => {
                    pack_array_column(segment, names, target)
                }
                Some(AtlasColumnView::GlobalAttribute { map })
                | Some(AtlasColumnView::VariableAttribute { map, .. }) => {
                    pack_attribute_column(map, names, target)
                }
            };
            Some((column.clone(), packed))
        })
        .collect();

    PruningIndex {
        rows: names.len(),
        columns,
    }
}

/// One array column, from the segment that holds the variable.
///
/// A dataset the segment has no entry for does not declare the array. The scan
/// reads it as nulls, and `null_count == row_count` says so. An entry without
/// statistics, or one whose unwritten cells read as zeros rather than as the
/// nulls the writer counted, says nothing, and that dataset stays in.
fn pack_array_column(segment: &ArrayFile, names: &[String], target: &DataType) -> StatColumn {
    let rows = names.len();
    let null = null_of(target);
    let mut mins = vec![null.clone(); rows];
    let mut maxes = vec![null.clone(); rows];
    let mut null_counts: Vec<Option<u64>> = vec![None; rows];
    let mut row_counts: Vec<Option<u64>> = vec![None; rows];

    for (row, name) in names.iter().enumerate() {
        let Some(info) = segment.array(name) else {
            null_counts[row] = Some(1);
            row_counts[row] = Some(1);
            continue;
        };
        let Some(stats) = info.stats.as_ref() else {
            continue;
        };
        if info.fill_value.is_none() && stats.null_count > 0 {
            // The nulls the writer counted are cells nobody wrote. Without a
            // fill value the scan reads them as zeros, which the bounds do
            // not cover either.
            continue;
        }
        mins[row] = stat_to_scalar(stats.min.as_ref(), target, &null);
        maxes[row] = stat_to_scalar(stats.max.as_ref(), target, &null);
        null_counts[row] = Some(stats.null_count);
        row_counts[row] = Some(stats.row_count);
    }

    StatColumn {
        min: scalars_to_array(mins, rows, target),
        max: scalars_to_array(maxes, rows, target),
        null_count: Arc::new(UInt64Array::from(null_counts)),
        row_count: Arc::new(UInt64Array::from(row_counts)),
    }
}

/// One attribute column.
///
/// An attribute's value is exact, so it is both the minimum and the maximum of
/// its dataset, on the one cell the scan reads. That prunes an equality on a
/// dataset-level attribute, the platform a file came from, say. A dataset
/// without the key reads as null, and the counts say so. A list, a `NaN`, or a
/// value that will not cast bounds nothing, and that dataset stays in.
fn pack_attribute_column(
    values: &IndexMap<String, Attr>,
    names: &[String],
    target: &DataType,
) -> StatColumn {
    let rows = names.len();
    let null = null_of(target);
    let mut bounds = vec![null.clone(); rows];
    let mut null_counts: Vec<Option<u64>> = vec![None; rows];
    let mut row_counts: Vec<Option<u64>> = vec![None; rows];

    for (row, name) in names.iter().enumerate() {
        let Some(attr) = values.get(name) else {
            null_counts[row] = Some(1);
            row_counts[row] = Some(1);
            continue;
        };
        let Some(scalar) = attr_to_scalar(attr).and_then(|scalar| scalar.cast_to(target).ok())
        else {
            continue;
        };
        bounds[row] = scalar;
        null_counts[row] = Some(0);
        row_counts[row] = Some(1);
    }

    StatColumn {
        min: scalars_to_array(bounds.clone(), rows, target),
        max: scalars_to_array(bounds, rows, target),
        null_count: Arc::new(UInt64Array::from(null_counts)),
        row_count: Arc::new(UInt64Array::from(row_counts)),
    }
}

/// A column every dataset reads as null.
fn all_null_column(rows: usize, target: &DataType) -> StatColumn {
    let ones = || Arc::new(UInt64Array::from(vec![1u64; rows])) as ArrayRef;
    StatColumn {
        min: new_null_array(target, rows),
        max: new_null_array(target, rows),
        null_count: ones(),
        row_count: ones(),
    }
}

/// The null of `target`, or the untyped null for a type that has none.
fn null_of(target: &DataType) -> ScalarValue {
    ScalarValue::try_from(target).unwrap_or(ScalarValue::Null)
}

/// Pack scalars into one typed array, or a column of nulls when they will not.
fn scalars_to_array(values: Vec<ScalarValue>, rows: usize, target: &DataType) -> ArrayRef {
    ScalarValue::iter_to_array(values).unwrap_or_else(|_| new_null_array(target, rows))
}

/// An atlas statistic as a scalar of the table's own type.
///
/// A value that will not cast, and a `NaN` bound, both read as null. `NaN`
/// sorts last under `total_cmp`, so a `NaN` maximum says nothing about the
/// values below it, and claiming it as a bound would drop rows.
fn stat_to_scalar(value: Option<&StatValue>, target: &DataType, null: &ScalarValue) -> ScalarValue {
    let canonical = match value {
        Some(StatValue::Int(v)) => ScalarValue::Int64(Some(*v)),
        Some(StatValue::UInt(v)) => ScalarValue::UInt64(Some(*v)),
        Some(StatValue::Float(v)) if v.is_nan() => return null.clone(),
        Some(StatValue::Float(v)) => ScalarValue::Float64(Some(*v)),
        Some(StatValue::TimestampNs(v)) => ScalarValue::TimestampNanosecond(Some(*v), None),
        Some(StatValue::Bytes(bytes)) => match std::str::from_utf8(bytes) {
            Ok(text) => ScalarValue::Utf8(Some(text.to_string())),
            Err(_) => ScalarValue::Binary(Some(bytes.clone())),
        },
        None => return null.clone(),
    };
    canonical.cast_to(target).unwrap_or_else(|_| null.clone())
}

/// An attribute value as a scalar, or `None` for a list, which bounds nothing.
fn attr_to_scalar(attr: &Attr) -> Option<ScalarValue> {
    Some(match attr {
        Attr::Bool(v) => ScalarValue::Boolean(Some(*v)),
        Attr::Int8(v) => ScalarValue::Int8(Some(*v)),
        Attr::Int16(v) => ScalarValue::Int16(Some(*v)),
        Attr::Int32(v) => ScalarValue::Int32(Some(*v)),
        Attr::Int64(v) => ScalarValue::Int64(Some(*v)),
        Attr::UInt8(v) => ScalarValue::UInt8(Some(*v)),
        Attr::UInt16(v) => ScalarValue::UInt16(Some(*v)),
        Attr::UInt32(v) => ScalarValue::UInt32(Some(*v)),
        Attr::UInt64(v) => ScalarValue::UInt64(Some(*v)),
        Attr::Float32(v) if v.is_nan() => return None,
        Attr::Float32(v) => ScalarValue::Float32(Some(*v)),
        Attr::Float64(v) if v.is_nan() => return None,
        Attr::Float64(v) => ScalarValue::Float64(Some(*v)),
        Attr::String(v) => ScalarValue::Utf8(Some(v.clone())),
        Attr::Binary(v) => ScalarValue::Binary(Some(v.clone())),
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{Field, Schema};
    use atlas::Atlas;
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::expressions::{
        BinaryExpr, Column as ColumnExpr, IsNullExpr, Literal,
    };

    use super::*;
    use crate::datafusion::opener::column_views;
    use crate::test_support;

    fn schema(name: &str, data_type: DataType) -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new(name, data_type, true)]))
    }

    fn binary(column: &str, op: Operator, value: ScalarValue) -> Arc<dyn PhysicalExpr> {
        Arc::new(BinaryExpr::new(
            Arc::new(ColumnExpr::new(column, 0)),
            op,
            Arc::new(Literal::new(value)),
        ))
    }

    fn is_null(column: &str) -> Arc<dyn PhysicalExpr> {
        Arc::new(IsNullExpr::new(Arc::new(ColumnExpr::new(column, 0))))
    }

    /// The datasets a predicate leaves in, in listing order. The views are the
    /// ones the scan would read.
    async fn kept(
        atlas: &Arc<Atlas>,
        predicate: Arc<dyn PhysicalExpr>,
        schema: SchemaRef,
    ) -> Vec<String> {
        let views = Arc::new(column_views(atlas, &schema).await.unwrap());
        prune_datasets(&views, atlas.list_datasets(), &predicate, &schema).await
    }

    // ── the index over array statistics ─────────────────────────────────

    /// The ranged fixture gives dataset `d{i}` the values `[10i, 10i+3]`, so a
    /// threshold has an answer that can be written down.
    #[tokio::test]
    async fn only_the_datasets_whose_range_reaches_the_threshold_survive() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::ranged(tmp.path(), 10).await;
        let atlas = test_support::open(tmp.path()).await;

        let survivors = kept(
            &atlas,
            binary(
                "temperature",
                Operator::Gt,
                ScalarValue::Float32(Some(45.0)),
            ),
            schema("temperature", DataType::Float32),
        )
        .await;
        assert_eq!(survivors, vec!["d5", "d6", "d7", "d8", "d9"]);
    }

    #[tokio::test]
    async fn a_predicate_nothing_can_meet_prunes_everything() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::ranged(tmp.path(), 6).await;
        let atlas = test_support::open(tmp.path()).await;

        let survivors = kept(
            &atlas,
            binary(
                "temperature",
                Operator::Gt,
                ScalarValue::Float32(Some(10_000.0)),
            ),
            schema("temperature", DataType::Float32),
        )
        .await;
        assert!(survivors.is_empty(), "{survivors:?}");
    }

    #[tokio::test]
    async fn a_predicate_everything_meets_prunes_nothing() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::ranged(tmp.path(), 6).await;
        let atlas = test_support::open(tmp.path()).await;

        let survivors = kept(
            &atlas,
            binary(
                "temperature",
                Operator::GtEq,
                ScalarValue::Float32(Some(0.0)),
            ),
            schema("temperature", DataType::Float32),
        )
        .await;
        assert_eq!(survivors, atlas.list_datasets());
    }

    /// A deleted dataset is not in the list, so it is neither judged nor read.
    /// The segment still holds its entry, and that entry is never looked at.
    #[tokio::test]
    async fn a_deleted_dataset_is_neither_judged_nor_read() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::ranged(tmp.path(), 6).await;
        let atlas = test_support::open(tmp.path()).await;
        atlas.delete_dataset("d0").await.unwrap();

        let survivors = kept(
            &atlas,
            binary(
                "temperature",
                Operator::GtEq,
                ScalarValue::Float32(Some(0.0)),
            ),
            schema("temperature", DataType::Float32),
        )
        .await;
        assert_eq!(survivors, vec!["d1", "d2", "d3", "d4", "d5"]);
    }

    // ── mixed and awkward types ─────────────────────────────────────────

    /// Two datasets that type one array differently still prune: every bound is
    /// cast to the column's table type before it is compared.
    #[tokio::test]
    async fn a_mixed_dtype_column_is_cast_before_it_is_compared() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::widening(tmp.path()).await;
        let atlas = test_support::open(tmp.path()).await;
        // Int16 and Float32 merge to Float64 under the session rule.
        let schema = schema("value", DataType::Float64);

        // a holds [1, 2] and b holds [3.5, 4.5].
        assert_eq!(
            kept(
                &atlas,
                binary("value", Operator::Gt, ScalarValue::Float64(Some(3.0))),
                Arc::clone(&schema)
            )
            .await,
            vec!["b"]
        );
        assert_eq!(
            kept(
                &atlas,
                binary("value", Operator::Lt, ScalarValue::Float64(Some(3.0))),
                Arc::clone(&schema)
            )
            .await,
            vec!["a"]
        );
        assert!(
            kept(
                &atlas,
                binary("value", Operator::Gt, ScalarValue::Float64(Some(100.0))),
                schema
            )
            .await
            .is_empty()
        );
    }

    /// A dataset-level attribute is exact, so an equality on it prunes from
    /// what is in memory alone.
    #[tokio::test]
    async fn an_attribute_predicate_prunes() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::ranged(tmp.path(), 6).await;
        let atlas = test_support::open(tmp.path()).await;

        let survivors = kept(
            &atlas,
            binary(
                ".platform",
                Operator::Eq,
                ScalarValue::Utf8(Some("p3".to_string())),
            ),
            schema(".platform", DataType::Utf8),
        )
        .await;
        assert_eq!(survivors, vec!["d3"]);
    }

    // ── a column the dataset lacks ──────────────────────────────────────

    /// `summer` never set `year`. The scan reads the column as null for it, so
    /// an equality drops it and an `IS NULL` keeps it alone.
    #[tokio::test]
    async fn a_missing_attribute_reads_as_null_and_prunes_as_null() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::two_datasets(tmp.path()).await;
        let atlas = test_support::open(tmp.path()).await;
        let schema = schema(".year", DataType::Int64);

        assert_eq!(
            kept(
                &atlas,
                binary(".year", Operator::Eq, ScalarValue::Int64(Some(2024))),
                Arc::clone(&schema)
            )
            .await,
            vec!["winter"]
        );
        assert_eq!(kept(&atlas, is_null(".year"), schema).await, vec!["summer"]);
    }

    /// Only `a` declares `flag`, and it holds [7, 8]. `b` reads the column as
    /// null, so a comparison drops it and an `IS NULL` keeps it alone.
    #[tokio::test]
    async fn a_dataset_that_lacks_the_array_reads_as_null_and_prunes_as_null() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::widening(tmp.path()).await;
        let atlas = test_support::open(tmp.path()).await;
        let schema = schema("flag", DataType::Int32);

        assert_eq!(
            kept(
                &atlas,
                binary("flag", Operator::Gt, ScalarValue::Int32(Some(5))),
                Arc::clone(&schema)
            )
            .await,
            vec!["a"]
        );
        assert!(
            kept(
                &atlas,
                binary("flag", Operator::Gt, ScalarValue::Int32(Some(100))),
                Arc::clone(&schema)
            )
            .await
            .is_empty(),
            "a is ruled out by its range, b by its nulls"
        );
        assert_eq!(kept(&atlas, is_null("flag"), schema).await, vec!["b"]);
    }

    /// A column no dataset declares is null everywhere the scan looks.
    #[tokio::test]
    async fn a_column_no_dataset_declares_is_null_everywhere() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::ranged(tmp.path(), 4).await;
        let atlas = test_support::open(tmp.path()).await;
        let schema = schema("ghost", DataType::Float32);

        assert!(
            kept(
                &atlas,
                binary("ghost", Operator::Gt, ScalarValue::Float32(Some(0.0))),
                Arc::clone(&schema)
            )
            .await
            .is_empty()
        );
        assert_eq!(
            kept(&atlas, is_null("ghost"), schema).await,
            atlas.list_datasets()
        );
    }

    /// `d` declares `value` with no fill value and never writes it. The writer
    /// counted both cells as null, yet the scan reads them as zeros, so the
    /// index must not trust the count. `d` stays in. `w` wrote [5, 6], and
    /// its statistics rule it out.
    #[tokio::test]
    async fn a_declared_array_nobody_wrote_is_never_pruned() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::declared_unwritten(tmp.path()).await;
        let atlas = test_support::open(tmp.path()).await;

        let survivors = kept(
            &atlas,
            binary("value", Operator::Eq, ScalarValue::Int32(Some(0))),
            schema("value", DataType::Int32),
        )
        .await;
        assert_eq!(survivors, vec!["d"]);
    }

    // ── failing open ────────────────────────────────────────────────────

    #[tokio::test]
    async fn a_collection_with_no_datasets_prunes_nothing() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::empty(tmp.path()).await;
        let atlas = test_support::open(tmp.path()).await;

        let survivors = kept(
            &atlas,
            binary("temperature", Operator::Gt, ScalarValue::Float32(Some(0.0))),
            schema("temperature", DataType::Float32),
        )
        .await;
        assert!(survivors.is_empty(), "nothing in, nothing out");
    }

    // ── the pieces ──────────────────────────────────────────────────────

    #[test]
    fn a_nan_bound_is_no_bound() {
        let null = ScalarValue::Float64(None);
        let nan = stat_to_scalar(Some(&StatValue::Float(f64::NAN)), &DataType::Float64, &null);
        assert!(nan.is_null(), "NaN sorts last, so it bounds nothing");
    }

    #[test]
    fn a_bound_that_will_not_cast_is_no_bound() {
        let null = ScalarValue::Int32(None);
        let text = stat_to_scalar(
            Some(&StatValue::Bytes(b"not a number".to_vec())),
            &DataType::Int32,
            &null,
        );
        assert!(text.is_null());
    }

    #[test]
    fn a_text_bound_survives_as_text() {
        let null = ScalarValue::Utf8(None);
        let text = stat_to_scalar(
            Some(&StatValue::Bytes(b"argo".to_vec())),
            &DataType::Utf8,
            &null,
        );
        assert_eq!(text, ScalarValue::Utf8(Some("argo".to_string())));
    }

    #[test]
    fn a_list_attribute_bounds_nothing() {
        assert!(attr_to_scalar(&Attr::Int32List(vec![1, 2])).is_none());
        assert!(attr_to_scalar(&Attr::Float64(f64::NAN)).is_none());
        assert_eq!(
            attr_to_scalar(&Attr::String("p1".into())),
            Some(ScalarValue::Utf8(Some("p1".into())))
        );
    }

    /// The index is built by hand here. Writing that many real datasets would
    /// take minutes and prove nothing extra. What this pins is that the
    /// evaluation is one pass over Arrow arrays rather than a decision per
    /// dataset.
    #[test]
    fn a_large_index_is_judged_in_one_pass() {
        use arrow::array::Float64Array;

        const ROWS: usize = 200_000;
        const THRESHOLD: f64 = 199_000.0;

        // Row i covers [i, i + 1], so exactly the rows above the threshold
        // survive.
        let mins: Float64Array = (0..ROWS).map(|row| Some(row as f64)).collect();
        let maxes: Float64Array = (0..ROWS).map(|row| Some(row as f64 + 1.0)).collect();
        let counts: UInt64Array = (0..ROWS).map(|_| Some(0u64)).collect();
        let rows: UInt64Array = (0..ROWS).map(|_| Some(1u64)).collect();

        let index = PruningIndex {
            rows: ROWS,
            columns: HashMap::from([(
                "temperature".to_string(),
                StatColumn {
                    min: Arc::new(mins),
                    max: Arc::new(maxes),
                    null_count: Arc::new(counts),
                    row_count: Arc::new(rows),
                },
            )]),
        };

        let pruning = PruningPredicate::try_new(
            binary(
                "temperature",
                Operator::Gt,
                ScalarValue::Float64(Some(THRESHOLD)),
            ),
            schema("temperature", DataType::Float64),
        )
        .expect("the predicate is prunable");

        let kept = pruning.prune(&index).expect("one pass over the index");
        assert_eq!(kept.len(), ROWS);
        // Row i survives when its maximum, i + 1, exceeds the threshold, so the
        // survivors are the rows from the threshold onward.
        let expected = ROWS - THRESHOLD as usize;
        assert_eq!(kept.iter().filter(|keep| **keep).count(), expected);
    }
}
