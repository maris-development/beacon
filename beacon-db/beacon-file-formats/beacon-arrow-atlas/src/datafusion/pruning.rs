//! Dropping the datasets a predicate cannot match, from the collection footer.
//!
//! # One index, not a decision per dataset
//!
//! A collection can hold millions of datasets. Evaluating a predicate against
//! each one in turn would cost millions of evaluations, and each would need its
//! own `DatasetView` — a linear scan of the footer — so the pass would be
//! quadratic before it did any work.
//!
//! Instead the first opener that reaches a collection builds one
//! [`PruningIndex`] over it: one row per live dataset, and one column of typed
//! Arrow statistics per column the predicate names. DataFusion's
//! [`PruningPredicate`] then evaluates the whole collection in one vectorised
//! pass, and the result is a bit per dataset that every partition reads.
//!
//! The statistics come from the footer the open already held, so the build
//! costs no I/O. An array column costs one linear pass through
//! [`Atlas::array_stats_by_dataset`], with no view and no name lookup at all.
//!
//! # Pruning is only ever an optimization
//!
//! Every path here fails open: an error, a predicate the engine cannot use, a
//! column with no statistics, or a bound that will not cast all leave the
//! datasets in. A dataset that survives is still filtered row by row above the
//! scan, so a hiccup here costs time and never a row.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use atlas::{Atlas, Attr, StatValue};
use datafusion::common::Column;
use datafusion::common::pruning::PruningStatistics;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::utils::collect_columns;
use datafusion::physical_optimizer::pruning::PruningPredicate;
use datafusion::scalar::ScalarValue;

/// Above this many datasets, an attribute column stays out of the index.
///
/// An array column costs one footer pass. An attribute has no bulk accessor in
/// the reader, so its value needs one `DatasetView` per dataset and each of
/// those is a linear scan — the pass is quadratic. It is worth paying on a
/// collection of thousands and not on one of millions, and the only cost of
/// skipping it is that a predicate on that attribute prunes nothing.
///
/// `Atlas::attribute_by_dataset` upstream would remove the limit.
const ATTRIBUTE_INDEX_LIMIT: usize = 100_000;

// ─── What a scan does with the answer ────────────────────────────────────────

/// Which datasets of one collection a predicate could still match.
#[derive(Debug)]
pub enum CandidateFilter {
    /// Pruning did not apply. Every dataset is read.
    KeepAll,
    /// One bit per dataset, in the order the plan listed them.
    Rows {
        kept: BooleanArray,
        /// The dataset at each row, so a listing that changed under the plan is
        /// detected rather than mis-indexed.
        names: Vec<String>,
    },
}

impl CandidateFilter {
    /// Whether the dataset at `position` is worth reading.
    ///
    /// The name is checked against the row it indexes. A collection is
    /// immutable, but its deletion mask is not, so a delete between the plan
    /// and the open would shift every row after it. A mismatch keeps the
    /// dataset: the filter above the scan decides it either way.
    pub fn keeps(&self, position: usize, dataset: &str) -> bool {
        match self {
            Self::KeepAll => true,
            Self::Rows { kept, names } => match names.get(position) {
                Some(name) if name == dataset => kept.value(position),
                _ => true,
            },
        }
    }

    /// Whether an index was built at all, as opposed to pruning not applying.
    pub fn is_index(&self) -> bool {
        matches!(self, Self::Rows { .. })
    }

    /// How many datasets this filter drops. For diagnostics.
    pub fn pruned(&self) -> usize {
        match self {
            Self::KeepAll => 0,
            Self::Rows { kept, .. } => kept.len() - kept.true_count(),
        }
    }

    /// How many rows the index behind this filter holds.
    pub fn rows(&self) -> usize {
        match self {
            Self::KeepAll => 0,
            Self::Rows { kept, .. } => kept.len(),
        }
    }
}

/// Each collection's [`CandidateFilter`], computed once per scan.
///
/// Keyed by the container's path. The predicate and the schema are fixed for a
/// scan, so the container identifies the answer. Every partition's opener holds
/// a clone of this cache, and the clones share one store: the first opener to
/// reach a collection builds the index while the rest await the same future.
#[derive(Clone)]
pub struct PruneCache {
    cache: moka::future::Cache<String, Arc<CandidateFilter>>,
}

impl PruneCache {
    pub fn new() -> Self {
        Self {
            cache: moka::future::Cache::builder().max_capacity(256).build(),
        }
    }

    /// The memoized filter for `key`, computing it with `init` on first use.
    /// Concurrent callers for one key share the one computation.
    pub async fn get_or_compute<F>(&self, key: String, init: F) -> Arc<CandidateFilter>
    where
        F: std::future::Future<Output = Arc<CandidateFilter>>,
    {
        self.cache.get_with(key, init).await
    }
}

impl Default for PruneCache {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for PruneCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PruneCache").finish_non_exhaustive()
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
    ) -> Option<BooleanArray> {
        // An attribute's value is exact, so an `IN` list could prune on one.
        // Not yet: every column here reports a range, and a range says nothing
        // about membership.
        None
    }
}

// ─── Building it ─────────────────────────────────────────────────────────────

/// The logical schema behind an nd-encoded one.
///
/// A scan carries its columns as `beacon.nd` structs, and a predicate is
/// written against the values inside them. A field whose type does not decode
/// keeps its own type, which simply leaves it unprunable.
pub fn logical_schema(encoded: &Schema) -> SchemaRef {
    let fields: Vec<Arc<Field>> = encoded
        .fields()
        .iter()
        .map(|field| {
            let value_type = beacon_datafusion_ext::nd::encoding::nd_value_type(field.data_type())
                .unwrap_or_else(|_| field.data_type().clone());
            Arc::new(Field::new(field.name(), value_type, true))
        })
        .collect();
    Arc::new(Schema::new(fields))
}

/// Which datasets of `atlas` could satisfy `predicate`.
///
/// `logical_schema` must type every column the predicate names, which the
/// scan's own projected schema does: a filter that stays above the scan forces
/// its columns into the projection.
///
/// Fails open to [`CandidateFilter::KeepAll`] on anything it cannot prove.
pub async fn candidate_filter(
    atlas: &Arc<Atlas>,
    predicate: &Arc<dyn PhysicalExpr>,
    logical_schema: &SchemaRef,
) -> CandidateFilter {
    let Ok(pruning) = PruningPredicate::try_new(Arc::clone(predicate), Arc::clone(logical_schema))
    else {
        // The engine cannot use this predicate shape.
        return CandidateFilter::KeepAll;
    };

    let referenced = collect_columns(pruning.orig_expr());
    if referenced.is_empty() {
        return CandidateFilter::KeepAll;
    }

    let names = atlas.list_datasets();
    if names.is_empty() {
        return CandidateFilter::KeepAll;
    }

    // The pivot is pure CPU over data already in memory. A million rows is real
    // work, so it does not run on the async runtime.
    let atlas = Arc::clone(atlas);
    let schema = Arc::clone(logical_schema);
    let wanted: Vec<String> = referenced
        .iter()
        .map(|column| column.name().to_string())
        .collect();

    let built = tokio::task::spawn_blocking(move || {
        let index = build_index(&atlas, &names, &wanted, &schema);
        (names, index)
    })
    .await;

    let Ok((names, index)) = built else {
        return CandidateFilter::KeepAll;
    };
    if index.columns.is_empty() {
        // Nothing the predicate names has statistics, so nothing can be ruled
        // out.
        return CandidateFilter::KeepAll;
    }

    match pruning.prune(&index) {
        Ok(kept) => CandidateFilter::Rows {
            kept: BooleanArray::from(kept),
            names,
        },
        Err(e) => {
            tracing::debug!("atlas pruning fell back to reading every dataset: {e}");
            CandidateFilter::KeepAll
        }
    }
}

/// Pivot the footer into one [`StatColumn`] per column that has statistics.
fn build_index(
    atlas: &Atlas,
    names: &[String],
    wanted: &[String],
    schema: &SchemaRef,
) -> PruningIndex {
    // Where each dataset sits, so a footer pass in write order can be scattered
    // into rows without a search.
    let row_of: HashMap<&str, usize> = names
        .iter()
        .enumerate()
        .map(|(row, name)| (name.as_str(), row))
        .collect();
    let arrays = atlas.list_arrays();

    let mut columns = HashMap::new();
    for column in wanted {
        let Ok(field) = schema.field_with_name(column) else {
            continue;
        };
        let target = field.data_type();

        let packed = if arrays.iter().any(|array| array == column) {
            Some(pack_array_column(
                atlas,
                names.len(),
                &row_of,
                column,
                target,
            ))
        } else {
            pack_attribute_column(atlas, names, &row_of, column, target)
        };
        if let Some(packed) = packed {
            columns.insert(column.clone(), packed);
        }
    }

    PruningIndex {
        rows: names.len(),
        columns,
    }
}

/// One array column, from one linear pass over the footer.
///
/// A dataset with no entry for the array keeps a null bound and unknown counts.
/// That is what a dataset which does not declare the array looks like, and it
/// is also what one that declared it and never wrote it looks like — both must
/// stay in, and a null does exactly that.
fn pack_array_column(
    atlas: &Atlas,
    rows: usize,
    row_of: &HashMap<&str, usize>,
    column: &str,
    target: &DataType,
) -> StatColumn {
    let null = ScalarValue::try_from(target).unwrap_or(ScalarValue::Null);
    let mut mins = vec![null.clone(); rows];
    let mut maxes = vec![null.clone(); rows];
    let mut null_counts: Vec<Option<u64>> = vec![None; rows];
    let mut row_counts: Vec<Option<u64>> = vec![None; rows];

    for (dataset, stats) in atlas.array_stats_by_dataset(column) {
        let Some(&row) = row_of.get(dataset.as_str()) else {
            continue;
        };
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

/// One attribute column, or `None` when it is not worth the pass.
///
/// An attribute's value is exact, so it is both the minimum and the maximum of
/// its dataset. That prunes an equality on a dataset-level attribute — the
/// platform a file came from, say — out of the footer alone.
fn pack_attribute_column(
    atlas: &Atlas,
    names: &[String],
    row_of: &HashMap<&str, usize>,
    column: &str,
    target: &DataType,
) -> Option<StatColumn> {
    if names.len() > ATTRIBUTE_INDEX_LIMIT {
        tracing::debug!(
            datasets = names.len(),
            column,
            "not indexing an attribute over a collection this large; see ATTRIBUTE_INDEX_LIMIT"
        );
        return None;
    }

    let rows = names.len();
    let null = ScalarValue::try_from(target).unwrap_or(ScalarValue::Null);
    let mut values = vec![null.clone(); rows];
    let mut null_counts: Vec<Option<u64>> = vec![None; rows];
    let mut seen = false;

    for name in names {
        let Some(&row) = row_of.get(name.as_str()) else {
            continue;
        };
        let Ok(view) = atlas.dataset(name) else {
            continue;
        };
        let Some(value) = attribute_of(&view, column) else {
            continue;
        };
        let Some(scalar) = attr_to_scalar(&value) else {
            continue;
        };
        values[row] = scalar.cast_to(target).unwrap_or_else(|_| null.clone());
        // One value, and it is not the fill of anything.
        null_counts[row] = Some(0);
        seen = true;
    }

    if !seen {
        return None;
    }

    let min = scalars_to_array(values.clone(), rows, target);
    let max = scalars_to_array(values, rows, target);
    Some(StatColumn {
        min,
        max,
        null_count: Arc::new(UInt64Array::from(null_counts)),
        // An attribute is one value broadcast over whatever grid the dataset
        // has, so its row count is not the dataset's. Unknown is honest.
        row_count: Arc::new(UInt64Array::from(vec![None::<u64>; rows])),
    })
}

/// The attribute a column name refers to, dataset-level or per-array.
fn attribute_of(view: &atlas::DatasetView, column: &str) -> Option<Attr> {
    if let Some(key) = column.strip_prefix('.') {
        return view.get_attribute(key);
    }
    // An array name and an attribute key may both hold dots, so every split is
    // a candidate.
    for (index, character) in column.char_indices() {
        if character == '.' {
            let (array, rest) = column.split_at(index);
            if let Some(value) = view.get_array_attribute(array, &rest[1..]) {
                return Some(value);
            }
        }
    }
    None
}

/// Pack scalars into one typed array, or a column of nulls when they will not.
fn scalars_to_array(values: Vec<ScalarValue>, rows: usize, target: &DataType) -> ArrayRef {
    ScalarValue::iter_to_array(values)
        .unwrap_or_else(|_| arrow::array::new_null_array(target, rows))
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
        Attr::TimestampNanoseconds(v) => ScalarValue::TimestampNanosecond(Some(*v), None),
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support;
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::expressions::{BinaryExpr, Column as ColumnExpr, Literal};

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

    /// The datasets a predicate leaves in, in listing order.
    async fn kept(
        atlas: &Arc<Atlas>,
        predicate: Arc<dyn PhysicalExpr>,
        schema: SchemaRef,
    ) -> Vec<String> {
        let filter = candidate_filter(atlas, &predicate, &schema).await;
        atlas
            .list_datasets()
            .into_iter()
            .enumerate()
            .filter(|(position, name)| filter.keeps(*position, name))
            .map(|(_, name)| name)
            .collect()
    }

    // ── the index over array statistics ─────────────────────────────────

    /// The ranged fixture gives dataset `d{i}` the values `[10i, 10i+3]`, so a
    /// threshold has an answer that can be written down.
    #[tokio::test]
    async fn only_the_datasets_whose_range_reaches_the_threshold_survive() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::ranged(tmp.path(), 10).await;
        let atlas = test_support::open(tmp.path()).await;
        let schema = schema("temperature", DataType::Float32);

        let survivors = kept(
            &atlas,
            binary(
                "temperature",
                Operator::Gt,
                ScalarValue::Float32(Some(45.0)),
            ),
            schema,
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

    /// The index holds one row per live dataset, in listing order, and the
    /// filter reads it by position.
    #[tokio::test]
    async fn the_index_holds_one_row_per_live_dataset() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::ranged(tmp.path(), 10).await;
        let atlas = test_support::open(tmp.path()).await;

        let filter = candidate_filter(
            &atlas,
            &binary(
                "temperature",
                Operator::Gt,
                ScalarValue::Float32(Some(45.0)),
            ),
            &schema("temperature", DataType::Float32),
        )
        .await;
        assert_eq!(filter.rows(), 10);
        assert_eq!(filter.pruned(), 5);
    }

    /// A deleted dataset has no row at all, and the rows after it shift up.
    /// That is why the filter checks the name it was given against the row it
    /// indexes.
    #[tokio::test]
    async fn a_delete_shifts_the_rows_and_the_name_check_catches_it() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::ranged(tmp.path(), 6).await;
        let atlas = test_support::open(tmp.path()).await;
        atlas.delete_dataset("d0").await.unwrap();

        let filter = candidate_filter(
            &atlas,
            &binary(
                "temperature",
                Operator::Gt,
                ScalarValue::Float32(Some(45.0)),
            ),
            &schema("temperature", DataType::Float32),
        )
        .await;
        assert_eq!(filter.rows(), 5, "the deleted dataset has no row");

        // Row 0 is now d1. A plan made before the delete would ask about d0
        // there, and that must not read as d1's answer.
        assert!(
            filter.keeps(0, "d0"),
            "a name that does not match its row is kept, not mis-indexed"
        );
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

    /// A dataset-level attribute is exact, so an equality on it prunes from the
    /// footer alone.
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

    // ── failing open ────────────────────────────────────────────────────

    #[tokio::test]
    async fn a_column_with_no_statistics_prunes_nothing() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::ranged(tmp.path(), 4).await;
        let atlas = test_support::open(tmp.path()).await;

        let survivors = kept(
            &atlas,
            binary("ghost", Operator::Gt, ScalarValue::Float32(Some(0.0))),
            schema("ghost", DataType::Float32),
        )
        .await;
        assert_eq!(survivors, atlas.list_datasets());
    }

    /// A column one dataset declares and another does not: the one without it
    /// has no bound, so it stays in and its rows are decided above the scan.
    #[tokio::test]
    async fn a_dataset_that_lacks_the_column_is_never_pruned_on_it() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::widening(tmp.path()).await;
        let atlas = test_support::open(tmp.path()).await;

        // Only `a` declares `flag`, and it holds [7, 8].
        let survivors = kept(
            &atlas,
            binary("flag", Operator::Gt, ScalarValue::Int32(Some(100))),
            schema("flag", DataType::Int32),
        )
        .await;
        assert_eq!(survivors, vec!["b"], "a is ruled out, b cannot be");
    }

    #[tokio::test]
    async fn a_collection_with_no_datasets_prunes_nothing() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::empty(tmp.path()).await;
        let atlas = test_support::open(tmp.path()).await;

        let filter = candidate_filter(
            &atlas,
            &binary("temperature", Operator::Gt, ScalarValue::Float32(Some(0.0))),
            &schema("temperature", DataType::Float32),
        )
        .await;
        assert!(matches!(filter, CandidateFilter::KeepAll));
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
            attr_to_scalar(&Attr::Int64(7)),
            Some(ScalarValue::Int64(Some(7)))
        );
    }

    /// The whole point of an index: a collection of hundreds of thousands of
    /// datasets is judged in one vectorised pass.
    ///
    /// The index is built by hand here. Writing that many real datasets would
    /// take minutes and prove nothing extra — what this pins is that the
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

    /// The scan's schema is nd-encoded; a predicate is written against the
    /// values inside it.
    #[test]
    fn the_logical_schema_unwraps_the_encoding() {
        let logical = Schema::new(vec![Field::new("temperature", DataType::Float32, true)]);
        let encoded = beacon_datafusion_ext::nd::encoded_schema(&logical);
        assert_ne!(
            encoded.field(0).data_type(),
            &DataType::Float32,
            "the encoded form is a struct"
        );
        assert_eq!(
            logical_schema(&encoded).field(0).data_type(),
            &DataType::Float32
        );
    }
}
