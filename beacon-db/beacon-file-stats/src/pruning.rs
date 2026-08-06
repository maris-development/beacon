//! Turning stored statistics into a DataFusion [`PruningPredicate`] input.
//!
//! This is what the store is for: given a predicate and a list of candidate
//! files, drop the ones whose recorded ranges say they cannot hold a matching
//! row. Only the columns the predicate names are read, so a three-column `WHERE`
//! over a store with 160K column names costs three blocks per surviving segment.
//!
//! # Fail open, always
//!
//! Every path here returns the full candidate list on any error, unsupported
//! predicate shape, unknown column, or failed cast. Pruning may only ever drop a
//! file that provably cannot match. A file wrongly dropped is a silently wrong
//! answer; a file wrongly kept costs one scan the optimizer would have skipped.
//!
//! # Duplicate rows
//!
//! A file re-analyzed after a change appears in more than one segment. Segments
//! are folded oldest first, so the newest row for a file wins. The old row is
//! not wrong, just stale, and preferring the newest keeps the range tightest.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BooleanArray, UInt32Array};
use arrow::datatypes::{DataType, SchemaRef};
use datafusion::common::Column;
use datafusion::common::pruning::PruningStatistics;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::utils::collect_columns;
use datafusion::physical_optimizer::pruning::PruningPredicate;
use datafusion::scalar::ScalarValue;

use crate::segment::ColumnStats;
use crate::store::FileStatsStore;
use crate::types::FileId;

/// Statistics for a set of candidate files, one row per file, in the caller's
/// order.
pub struct FileStatsPruningStatistics {
    rows: usize,
    columns: HashMap<String, PackedColumn>,
}

struct PackedColumn {
    min: ArrayRef,
    max: ArrayRef,
    null_count: ArrayRef,
    row_count: ArrayRef,
}

impl std::fmt::Debug for FileStatsPruningStatistics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileStatsPruningStatistics")
            .field("rows", &self.rows)
            .field("columns", &self.columns.len())
            .finish()
    }
}

impl PruningStatistics for FileStatsPruningStatistics {
    fn min_values(&self, column: &Column) -> Option<ArrayRef> {
        self.columns.get(column.name()).map(|c| c.min.clone())
    }

    fn max_values(&self, column: &Column) -> Option<ArrayRef> {
        self.columns.get(column.name()).map(|c| c.max.clone())
    }

    fn num_containers(&self) -> usize {
        self.rows
    }

    fn null_counts(&self, column: &Column) -> Option<ArrayRef> {
        self.columns.get(column.name()).map(|c| c.null_count.clone())
    }

    fn row_counts(&self, column: &Column) -> Option<ArrayRef> {
        self.columns.get(column.name()).map(|c| c.row_count.clone())
    }

    /// Not answerable from a min/max range, so the predicate falls back to the
    /// range tests. Returning `None` is the fail-open answer.
    fn contained(&self, _column: &Column, _values: &HashSet<ScalarValue>) -> Option<BooleanArray> {
        None
    }
}

/// The files in `candidates` whose statistics leave them able to satisfy
/// `predicate`.
///
/// `candidates` must be ascending. `schema` must carry every column the
/// predicate names, with the type the predicate compares against.
///
/// Returns every candidate unchanged when pruning cannot apply.
pub async fn prune_files(
    store: &FileStatsStore,
    predicate: &Arc<dyn PhysicalExpr>,
    schema: &SchemaRef,
    candidates: &[FileId],
) -> Vec<FileId> {
    match try_prune(store, predicate, schema, candidates).await {
        Ok(Some(kept)) => kept,
        Ok(None) => candidates.to_vec(),
        Err(error) => {
            tracing::debug!(%error, "file statistics pruning did not apply; keeping every file");
            candidates.to_vec()
        }
    }
}

async fn try_prune(
    store: &FileStatsStore,
    predicate: &Arc<dyn PhysicalExpr>,
    schema: &SchemaRef,
    candidates: &[FileId],
) -> crate::Result<Option<Vec<FileId>>> {
    if candidates.is_empty() {
        return Ok(None);
    }
    let Ok(pruning_predicate) = PruningPredicate::try_new(predicate.clone(), schema.clone()) else {
        return Ok(None); // a shape the pruning engine cannot use
    };

    let referenced = collect_columns(pruning_predicate.orig_expr());
    if referenced.is_empty() {
        return Ok(None);
    }

    let range = (candidates[0], candidates[candidates.len() - 1]);

    // The predicate's columns are independent, so fetch them together rather
    // than in a queue. A three-column `WHERE` otherwise costs three times one
    // column's segment reads purely because they were written as a loop.
    let wanted: Vec<(String, DataType)> = referenced
        .iter()
        .filter_map(|column| {
            schema
                .field_with_name(column.name())
                .ok()
                .map(|field| (column.name().to_string(), field.data_type().clone()))
        })
        .collect();

    let fetched: Vec<(String, DataType, Vec<ColumnStats>)> =
        futures::future::join_all(wanted.into_iter().map(|(name, data_type)| async move {
            let segments = store
                .column_stats_by_name(&name, range)
                .await
                .unwrap_or_default();
            (name, data_type, segments)
        }))
        .await
        .into_iter()
        .filter(|(_, _, segments)| !segments.is_empty())
        .collect();

    // The suppression lookup is only worth paying for once we know there is
    // something to prune on, and on a store still being ingested there is not.
    if fetched.is_empty() {
        return Ok(None);
    }

    // A file whose content changed since it was analyzed still has rows in the
    // segments, and those rows describe content that is gone. Pruning on them
    // drops files the new content would have matched. Treat them as having no
    // statistics at all, which keeps them.
    let suppressed = store
        .registry()
        .suppressed_in_range(range)
        .unwrap_or_default();
    // Empty means "every candidate is trustworthy", which is the steady state.
    // Allocating and walking a million-entry mask to say so is pure cost.
    let trusted = Arc::new(if suppressed.is_empty() {
        Vec::new()
    } else {
        trust_mask(candidates, &suppressed)
    });

    // Packing casts, concatenates and gathers over one row per candidate, so at
    // a million files it is real CPU work with no await in it. `spawn_blocking`
    // keeps it off the async workers, and running the columns together means a
    // three-column predicate pays for one column's pack, not three.
    //
    // The candidate list is shared rather than cloned: a million ids is 8 MB,
    // and copying that per column to avoid an `Arc` would cost more than the
    // pack.
    let shared_candidates = Arc::new(candidates.to_vec());
    let packs = futures::future::join_all(fetched.into_iter().map(
        |(name, data_type, segments)| {
            let candidates = shared_candidates.clone();
            let trusted = trusted.clone();
            tokio::task::spawn_blocking(move || {
                (name, pack(&segments, &data_type, &candidates, &trusted))
            })
        },
    ))
    .await;

    let mut columns: HashMap<String, PackedColumn> = HashMap::new();
    for outcome in packs {
        match outcome {
            Ok((name, Some(packed))) => {
                columns.insert(name, packed);
            }
            // A column that cannot be cast to the predicate's type contributes
            // nothing, which keeps every file.
            Ok((_, None)) => {}
            Err(error) => {
                tracing::debug!(%error, "a statistics packing task panicked; skipping the column");
            }
        }
    }

    if columns.is_empty() {
        return Ok(None);
    }

    let statistics = FileStatsPruningStatistics {
        rows: candidates.len(),
        columns,
    };
    let Ok(mask) = pruning_predicate.prune(&statistics) else {
        return Ok(None);
    };

    let kept: Vec<FileId> = candidates
        .iter()
        .zip(mask)
        .filter_map(|(id, keep)| keep.then_some(*id))
        .collect();
    Ok(Some(kept))
}

/// Fold every segment's rows for one column onto the candidate rows.
///
/// Values are gathered with `take`, so a candidate with no statistics simply
/// takes a null index and reads back null, which the pruning engine treats as
/// unknown. `None` when the column cannot be cast to the type the predicate
/// compares against.
/// `false` at every candidate row whose statistics must not be trusted.
///
/// Both inputs are ascending, so this is a merge. An empty return from the
/// caller means every row is trustworthy; see [`is_trusted`].
fn trust_mask(candidates: &[FileId], suppressed: &[FileId]) -> Vec<bool> {
    let mut mask = vec![true; candidates.len()];
    let (mut row, mut other) = (0usize, 0usize);
    while row < candidates.len() && other < suppressed.len() {
        match candidates[row].cmp(&suppressed[other]) {
            std::cmp::Ordering::Less => row += 1,
            std::cmp::Ordering::Greater => other += 1,
            std::cmp::Ordering::Equal => {
                mask[row] = false;
                row += 1;
                other += 1;
            }
        }
    }
    mask
}

fn pack(
    segments: &[ColumnStats],
    target: &DataType,
    candidates: &[FileId],
    trusted: &[bool],
) -> Option<PackedColumn> {
    // Both sides are sorted ascending, so the join is a merge, not a hash. That
    // matters at scale: a hash map over a million candidates costs ~50 MB and an
    // allocation per entry, for a walk that needs neither.
    let mut indices: Vec<Option<u32>> = vec![None; candidates.len()];

    // Concatenate the segments, casting each to the predicate's type. Segments
    // arrive oldest first, and a later row for the same file overwrites an
    // earlier one, so the newest statistic wins.
    let mut mins: Vec<ArrayRef> = Vec::with_capacity(segments.len());
    let mut maxes: Vec<ArrayRef> = Vec::with_capacity(segments.len());
    let mut null_counts: Vec<ArrayRef> = Vec::with_capacity(segments.len());
    let mut row_counts: Vec<ArrayRef> = Vec::with_capacity(segments.len());

    let mut offset = 0u32;
    for segment in segments {
        let min = arrow::compute::cast(&segment.min, target).ok()?;
        let max = arrow::compute::cast(&segment.max, target).ok()?;
        mins.push(min);
        maxes.push(max);
        null_counts.push(segment.null_count.clone());
        row_counts.push(segment.row_count.clone());

        let (mut row, mut within) = (0usize, 0usize);
        while row < candidates.len() && within < segment.file_ids.len() {
            match candidates[row].cmp(&segment.file_ids[within]) {
                std::cmp::Ordering::Less => row += 1,
                std::cmp::Ordering::Greater => within += 1,
                std::cmp::Ordering::Equal => {
                    if is_trusted(trusted, row) {
                        indices[row] = Some(offset + within as u32);
                    }
                    row += 1;
                    within += 1;
                }
            }
        }
        offset += segment.len() as u32;
    }

    let min = concat(&mins)?;
    let max = concat(&maxes)?;
    // Nullable, and stays nullable through the gather: an unknown count must
    // reach the pruning engine as unknown, not as zero.
    let null_count = concat(&null_counts)?;
    let row_count = concat(&row_counts)?;

    let indices = UInt32Array::from_iter(indices);

    Some(PackedColumn {
        min: arrow::compute::take(&min, &indices, None).ok()?,
        max: arrow::compute::take(&max, &indices, None).ok()?,
        null_count: arrow::compute::take(&null_count, &indices, None).ok()?,
        row_count: arrow::compute::take(&row_count, &indices, None).ok()?,
    })
}

/// An empty mask means nothing is suppressed, which is the steady state.
#[inline]
fn is_trusted(mask: &[bool], row: usize) -> bool {
    mask.is_empty() || mask[row]
}

fn concat(arrays: &[ArrayRef]) -> Option<ArrayRef> {
    let refs: Vec<&dyn Array> = arrays.iter().map(|a| a.as_ref()).collect();
    arrow::compute::concat(&refs).ok()
}

