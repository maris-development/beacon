//! Optional dataset-level predicate pruning.
//!
//! atlas co-locates each column's per-dataset statistics in one `.af` stats
//! file, and [`Atlas::pruning_index`](atlas::Atlas::pruning_index) pivots the
//! requested columns into flat, per-dataset (min, max, null_count, row_count)
//! buffers with a single read per column. Feeding those to DataFusion's
//! [`PruningPredicate`] yields, for an arbitrary predicate, the set of datasets
//! whose ranges could still satisfy it — so a selective `WHERE` over a 1M+
//! dataset store skips opening the datasets that provably can't match.
//!
//! Pruning is a pure optimization: it only ever drops datasets that cannot
//! contain a matching row, and every path **fails open** (returns the full
//! input on any error, unsupported predicate, or missing statistic) so a query
//! can never lose a real row to a pruning hiccup. The not-fully-consumed filter
//! kept above the scan re-checks whatever survives.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, UInt64Array};
use arrow::datatypes::{DataType, SchemaRef};
use atlas::{Atlas, ColumnKey, MergedSchema, PruningIndex, StatVal};
use datafusion::common::Column;
use datafusion::common::pruning::PruningStatistics;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::utils::collect_columns;
use datafusion::physical_optimizer::pruning::PruningPredicate;
use datafusion::scalar::ScalarValue;

/// Which of a store's datasets a predicate could match — the result of pruning
/// the whole store once, so every partition's opener can reuse it.
#[derive(Debug, Clone)]
pub enum CandidateFilter {
    /// Pruning didn't apply (fail-open) — keep every dataset.
    KeepAll,
    /// Only these dataset names could match; everything else is prunable.
    Only(HashSet<String>),
}

impl CandidateFilter {
    /// Restrict `names` to the datasets that survive pruning, preserving order.
    pub fn retain(&self, names: Vec<String>) -> Vec<String> {
        match self {
            CandidateFilter::KeepAll => names,
            CandidateFilter::Only(set) => {
                names.into_iter().filter(|n| set.contains(n)).collect()
            }
        }
    }
}

/// Per-store memo of the [`CandidateFilter`], so a store is pruned **once** per
/// query rather than once per scan partition.
///
/// Keyed by marker path; the predicate and schema are fixed for a given source,
/// so the marker identifies the result. Concurrent partition openers coalesce on
/// the shared [`moka`] entry — the first computes, the rest await it.
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

    /// Return the memoized filter for `key`, computing it via `init` on first
    /// use. Concurrent callers for the same key share one computation.
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

/// Prune a whole store: the set of dataset names whose statistics leave them
/// able to satisfy `predicate`. Fails open to [`CandidateFilter::KeepAll`] on any
/// error, unsupported predicate, or unmapped column, so a real row is never lost.
///
/// `schema` must contain every column the predicate references with its table
/// type — the scan's projected output schema does (a not-fully-consumed filter
/// forces its columns into the projection).
pub async fn candidate_set(
    atlas: &Arc<Atlas>,
    predicate: &Arc<dyn PhysicalExpr>,
    schema: &SchemaRef,
) -> CandidateFilter {
    match try_candidates(atlas, predicate, schema).await {
        Ok(Some(set)) => CandidateFilter::Only(set),
        Ok(None) | Err(_) => CandidateFilter::KeepAll,
    }
}

/// Keep only the datasets in `names` that could satisfy `predicate`. A thin
/// convenience over [`candidate_set`] for one-shot callers and tests.
pub async fn retain_candidates(
    atlas: &Arc<Atlas>,
    names: Vec<String>,
    predicate: &Arc<dyn PhysicalExpr>,
    schema: &SchemaRef,
) -> Vec<String> {
    candidate_set(atlas, predicate, schema).await.retain(names)
}

async fn try_candidates(
    atlas: &Arc<Atlas>,
    predicate: &Arc<dyn PhysicalExpr>,
    schema: &SchemaRef,
) -> datafusion::error::Result<Option<HashSet<String>>> {
    let Ok(pruning_predicate) = PruningPredicate::try_new(predicate.clone(), schema.clone()) else {
        return Ok(None); // predicate shape unsupported by the pruning engine
    };

    let referenced = collect_columns(pruning_predicate.orig_expr());
    if referenced.is_empty() {
        return Ok(None);
    }

    // Map referenced table columns to atlas column keys, skipping any we can't
    // resolve (they simply won't contribute statistics → never prune on them).
    let merged = atlas.merged_schema();
    let keyed: Vec<(String, ColumnKey)> = referenced
        .iter()
        .filter_map(|col| column_key(&merged, col.name()).map(|k| (col.name().to_string(), k)))
        .collect();
    if keyed.is_empty() {
        return Ok(None);
    }

    // `pruning_index` reads and pivots each column's stats file concurrently
    // (atlas bounds it to the CPU count). We then pack each column's per-dataset
    // stats into DataFusion `ScalarValue` arrays — also one task per column, so
    // the whole build is column-parallel end to end.
    let keys: Vec<ColumnKey> = keyed.iter().map(|(_, k)| k.clone()).collect();
    let index = Arc::new(atlas.pruning_index(&keys).await.map_err(|e| {
        datafusion::error::DataFusionError::Execution(format!(
            "Failed to build atlas pruning index: {e}"
        ))
    })?);
    let rows = index.rows();

    let mut handles = Vec::with_capacity(keyed.len());
    for (name, key) in keyed {
        let index = index.clone();
        let schema = schema.clone();
        handles.push(tokio::task::spawn_blocking(move || {
            pack_column(&index, &key, &name, &schema).map(|packed| (name, packed))
        }));
    }
    let mut columns: HashMap<String, PackedStats> = HashMap::new();
    for handle in handles {
        if let Ok(Some((name, packed))) = handle.await {
            columns.insert(name, packed);
        }
    }

    let stats = AtlasPruningStatistics {
        num_containers: rows,
        columns,
    };
    let mask = pruning_predicate.prune(&stats).map_err(|e| {
        datafusion::error::DataFusionError::Execution(format!("Failed to prune atlas datasets: {e}"))
    })?;

    // Row ordinals the predicate couldn't rule out → their dataset names.
    let mut candidates: HashSet<String> = HashSet::new();
    for (row, keep) in mask.iter().enumerate() {
        if *keep
            && let Some(name) = index.dataset_name(row)
        {
            candidates.insert(name.to_string());
        }
    }
    Ok(Some(candidates))
}

/// Pack one column's per-dataset statistics into [`PackedStats`] (min/max as
/// `ScalarValue` arrays, plus count arrays). Pure CPU; run on a blocking task so
/// a wide, million-row pack doesn't stall the async runtime. `None` if the
/// column is absent or its datasets disagree on type.
///
/// Each dataset stores its stats in its *own* dtype, so a column whose datasets
/// disagree (e.g. `Int16` in one, `Float32` in another) yields mixed `StatVal`
/// variants. Every value is cast to `target` — the column's type in the merged
/// (super-typed) table schema, which is also the type
/// [`PruningPredicate`](datafusion::physical_optimizer::pruning::PruningPredicate)
/// compares against — so the per-container min/max arrays are homogeneous and
/// the comparison is well-defined.
fn pack_column(
    index: &PruningIndex,
    key: &ColumnKey,
    name: &str,
    schema: &SchemaRef,
) -> Option<PackedStats> {
    let view = index.view(key)?;
    let rows = index.rows();
    // `target` is the merged/super-typed table type for this column (the schema
    // is derived from `Atlas::merged_schema`), so casting to it lifts every
    // dataset's native-typed stat onto the common comparison type.
    let target = schema
        .field_with_name(name)
        .map(|f| f.data_type().clone())
        .unwrap_or(DataType::Null);
    let null_scalar = ScalarValue::try_from(&target).unwrap_or(ScalarValue::Null);

    let mut mins = Vec::with_capacity(rows);
    let mut maxes = Vec::with_capacity(rows);
    let mut null_counts: Vec<Option<u64>> = Vec::with_capacity(rows);
    let mut row_counts: Vec<Option<u64>> = Vec::with_capacity(rows);
    for row in 0..rows {
        if view.is_present(row) {
            mins.push(stat_to_scalar(view.min(row), &target, &null_scalar));
            maxes.push(stat_to_scalar(view.max(row), &target, &null_scalar));
            null_counts.push(Some(view.null_count(row)));
            row_counts.push(Some(view.row_count(row)));
        } else {
            // Column absent from this dataset: leave counts unknown so the
            // predicate can't prune it (its rows read back null-filled, and the
            // filter above the scan decides). Never claim "all null" — that
            // could wrongly drop an `IS NULL` match.
            mins.push(null_scalar.clone());
            maxes.push(null_scalar.clone());
            null_counts.push(None);
            row_counts.push(None);
        }
    }

    let min = ScalarValue::iter_to_array(mins).ok()?;
    let max = ScalarValue::iter_to_array(maxes).ok()?;
    Some(PackedStats {
        min,
        max,
        null_count: Arc::new(UInt64Array::from(null_counts)),
        row_count: Arc::new(UInt64Array::from(row_counts)),
    })
}

/// Resolve a table column name to the atlas [`ColumnKey`] whose statistics back
/// it, using the merged schema to disambiguate. `None` if it maps to nothing
/// prunable (an unknown name, or a `{array}.{attr}` whose parts don't resolve).
fn column_key(merged: &MergedSchema, name: &str) -> Option<ColumnKey> {
    if merged.arrays.contains_key(name) {
        return Some(ColumnKey::array(name));
    }
    if merged.global_attributes.contains_key(name) {
        return Some(ColumnKey::global_attr(name));
    }
    // `{array}.{attr}` — try each `.` split, since both an array name and an
    // attribute key may themselves contain dots.
    for (i, c) in name.char_indices() {
        if c == '.' {
            let (array, rest) = name.split_at(i);
            let attr = &rest[1..];
            if let Some(marr) = merged.arrays.get(array)
                && marr.attributes.contains_key(attr)
            {
                return Some(ColumnKey::array_attr(array, attr));
            }
        }
    }
    None
}

/// Convert an atlas [`StatVal`] to a `ScalarValue` cast to `target`, or
/// `null_scalar` when absent or on any conversion failure.
fn stat_to_scalar(
    value: Option<&StatVal>,
    target: &DataType,
    null_scalar: &ScalarValue,
) -> ScalarValue {
    let canonical = match value {
        Some(StatVal::Int(x)) => ScalarValue::Int64(Some(*x)),
        Some(StatVal::UInt(x)) => ScalarValue::UInt64(Some(*x)),
        Some(StatVal::Float(x)) => ScalarValue::Float64(Some(*x)),
        Some(StatVal::Bytes(b)) => match std::str::from_utf8(b) {
            Ok(s) => ScalarValue::Utf8(Some(s.to_string())),
            Err(_) => ScalarValue::Binary(Some(b.clone())),
        },
        Some(StatVal::TimestampNs(x)) => ScalarValue::TimestampNanosecond(Some(*x), None),
        None => return null_scalar.clone(),
    };
    canonical.cast_to(target).unwrap_or_else(|_| null_scalar.clone())
}

struct PackedStats {
    min: ArrayRef,
    max: ArrayRef,
    null_count: ArrayRef,
    row_count: ArrayRef,
}

/// [`PruningStatistics`] with one container per dataset row slot, backed by the
/// atlas pruning index.
struct AtlasPruningStatistics {
    num_containers: usize,
    columns: HashMap<String, PackedStats>,
}

impl PruningStatistics for AtlasPruningStatistics {
    fn min_values(&self, column: &Column) -> Option<ArrayRef> {
        self.columns.get(column.name()).map(|c| c.min.clone())
    }

    fn max_values(&self, column: &Column) -> Option<ArrayRef> {
        self.columns.get(column.name()).map(|c| c.max.clone())
    }

    fn null_counts(&self, column: &Column) -> Option<ArrayRef> {
        self.columns.get(column.name()).map(|c| c.null_count.clone())
    }

    fn row_counts(&self, column: &Column) -> Option<ArrayRef> {
        self.columns.get(column.name()).map(|c| c.row_count.clone())
    }

    fn num_containers(&self) -> usize {
        self.num_containers
    }

    fn contained(
        &self,
        _column: &Column,
        _values: &HashSet<ScalarValue>,
    ) -> Option<BooleanArray> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{Field, Schema};
    use atlas::Atlas;
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::expressions::{binary, col, lit};
    use object_store::ObjectStore;
    use object_store::local::LocalFileSystem;
    use object_store::path::Path as OsPath;

    async fn ranged_atlas(n: usize) -> (tempfile::TempDir, Arc<Atlas>) {
        let tmp = tempfile::tempdir().unwrap();
        crate::reader::test_support::build_ranged_store(tmp.path(), n).await;
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(tmp.path()).unwrap());
        let atlas = Atlas::open(store, OsPath::from("")).await.unwrap();
        (tmp, Arc::new(atlas))
    }

    fn temperature_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new(
            "temperature",
            DataType::Float32,
            true,
        )]))
    }

    fn gt(schema: &SchemaRef, threshold: f32) -> Arc<dyn PhysicalExpr> {
        binary(
            col("temperature", schema).unwrap(),
            Operator::Gt,
            lit(ScalarValue::Float32(Some(threshold))),
            schema,
        )
        .unwrap()
    }

    #[tokio::test]
    async fn retains_only_datasets_whose_range_can_match() {
        // 10 datasets: d{i}.temperature ∈ [10i, 10i+3].
        let (_tmp, atlas) = ranged_atlas(10).await;
        let schema = temperature_schema();
        let names = atlas.list_datasets();

        // `> 45` matches d5..d9 (ranges 50-53 … 90-93); d0..d4 (max 43) are pruned.
        let kept = retain_candidates(&atlas, names.clone(), &gt(&schema, 45.0), &schema).await;
        assert_eq!(kept, vec!["d5", "d6", "d7", "d8", "d9"]);
    }

    #[tokio::test]
    async fn impossible_predicate_prunes_everything() {
        let (_tmp, atlas) = ranged_atlas(6).await;
        let schema = temperature_schema();
        let kept =
            retain_candidates(&atlas, atlas.list_datasets(), &gt(&schema, 1_000.0), &schema).await;
        assert!(kept.is_empty(), "no dataset reaches 1000: {kept:?}");
    }

    #[tokio::test]
    async fn permissive_predicate_keeps_everything() {
        let (_tmp, atlas) = ranged_atlas(6).await;
        let schema = temperature_schema();
        let all = atlas.list_datasets();
        let kept = retain_candidates(&atlas, all.clone(), &gt(&schema, -1.0), &schema).await;
        assert_eq!(kept, all, "every dataset can exceed -1");
    }

    #[tokio::test]
    async fn mixed_dtype_column_casts_to_merged_type_then_prunes() {
        // The widening fixture stores `value` as Int16 in `a` ([1,2]) and
        // Float32 in `b` ([3.5,4.5]); the merged column type is Float32. Pruning
        // must cast `a`'s Int16 stats up to Float32 before comparing, or the two
        // datasets' min/max buffers wouldn't even be a single Arrow array.
        let tmp = tempfile::tempdir().unwrap();
        crate::reader::test_support::build_widening_store(tmp.path()).await;
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(tmp.path()).unwrap());
        let atlas = Arc::new(Atlas::open(store, OsPath::from("")).await.unwrap());

        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Float32,
            true,
        )]));
        let value_gt = |t: f32| {
            binary(
                col("value", &schema).unwrap(),
                Operator::Gt,
                lit(ScalarValue::Float32(Some(t))),
                &schema,
            )
            .unwrap()
        };
        let names = atlas.list_datasets(); // [a, b]

        // `> 3`: a (max 2, cast from Int16) is pruned; b (max 4.5) survives.
        assert_eq!(
            retain_candidates(&atlas, names.clone(), &value_gt(3.0), &schema).await,
            vec!["b"]
        );
        // `> 10`: neither can match.
        assert!(
            retain_candidates(&atlas, names.clone(), &value_gt(10.0), &schema)
                .await
                .is_empty()
        );
        // `> 1.5`: a's cast max (2.0) still qualifies, so both survive.
        assert_eq!(
            retain_candidates(&atlas, names.clone(), &value_gt(1.5), &schema).await,
            names
        );
    }

    #[tokio::test]
    async fn unmappable_column_fails_open() {
        // A predicate on a column with no atlas statistics must keep every input.
        let (_tmp, atlas) = ranged_atlas(4).await;
        let schema = Arc::new(Schema::new(vec![Field::new(
            "ghost",
            DataType::Float32,
            true,
        )]));
        let pred = binary(
            col("ghost", &schema).unwrap(),
            Operator::Gt,
            lit(ScalarValue::Float32(Some(0.0))),
            &schema,
        )
        .unwrap();
        let all = atlas.list_datasets();
        let kept = retain_candidates(&atlas, all.clone(), &pred, &schema).await;
        assert_eq!(kept, all, "unknown column must not prune anything");
    }
}
