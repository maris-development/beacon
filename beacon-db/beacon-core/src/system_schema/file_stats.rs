//! `beacon.system.file_stats`, `beacon.system.file_stats_segments`, and the
//! `file_statistics(path)` table function — the background statistics subsystem
//! as SQL.
//!
//! The subsystem runs on a timer, in the background, and every way it can
//! underperform is quiet by construction. A format that computes no ranges
//! analyzes cleanly and contributes nothing. netCDF does the same unless the
//! Rust reader is on. A file that fails leaves the queue and says so only in a
//! log line. None of that is an error, and none of it is visible from a query
//! plan — the plan simply shows every file being read, exactly as it did before
//! the subsystem existed.
//!
//! `column_count` is the tell, and these tables are what make it readable:
//!
//! ```sql
//! SELECT format,
//!        count(*)                                        AS files,
//!        sum(CASE WHEN column_count = 0 THEN 1 ELSE 0 END) AS barren
//! FROM beacon.system.file_stats
//! GROUP BY format;
//! ```
//!
//! ```text
//! netcdf  | 840000 | 840000    <- use_rust_reader is off
//! odv     |  12000 |  12000    <- ODV computes no ranges
//! parquet |  50000 |      0    <- working
//! ```
//!
//! The segments table answers the other question a live node cannot otherwise
//! be asked: whether the prefix batching is producing narrow segments. That is
//! the property the whole skip depends on, and the one most likely to be quietly
//! wrong on an unfamiliar layout.

use std::sync::Arc;

use arrow::{
    array::{Array, ArrayRef, StringArray, UInt32Array, UInt64Array},
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
    util::display::{ArrayFormatter, FormatOptions},
};
use beacon_common::table_function::parse_glob_paths_arg;
use beacon_file_stats::FileState;
use datafusion::{
    catalog::{TableFunctionImpl, TableProvider},
    common::{plan_err, Result as DFResult},
    error::DataFusionError,
    prelude::Expr,
};

use super::table::{Snapshot, SystemTable};

fn file_stats_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("path", DataType::Utf8, false),
        Field::new("file_id", DataType::UInt64, false),
        Field::new("state", DataType::Utf8, false),
        Field::new("format", DataType::Utf8, false),
        // Zero is the interesting value: the file was analyzed and the format
        // had no ranges to give.
        Field::new("column_count", DataType::UInt32, false),
        Field::new("num_rows", DataType::UInt64, true),
        Field::new("total_byte_size", DataType::UInt64, true),
        Field::new("stats_epoch", DataType::UInt64, false),
        Field::new("size", DataType::UInt64, false),
        Field::new("last_modified_millis", DataType::Int64, false),
    ]))
}

fn segments_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("segment", DataType::Utf8, false),
        // Write order. Recency, and what "newest wins" means when a file appears
        // in more than one segment.
        Field::new("seq", DataType::UInt64, false),
        Field::new("min_file_id", DataType::UInt64, false),
        Field::new("max_file_id", DataType::UInt64, false),
        Field::new("num_files", DataType::UInt64, false),
        // Narrow is good: a predicate on a column a segment does not hold skips
        // it without a read.
        Field::new("num_columns", DataType::UInt64, false),
    ]))
}

fn state_name(state: FileState) -> &'static str {
    match state {
        FileState::Pending => "Pending",
        FileState::Analyzed => "Analyzed",
        FileState::Failed => "Failed",
        FileState::Stale => "Stale",
        FileState::Deleted => "Deleted",
    }
}

/// `beacon.system.file_stats` — one row per file the registry knows.
///
/// The store is resolved per scan rather than captured, because the table is
/// registered before the subsystem starts, and may be registered on a runtime
/// where it never starts at all. No store means no rows, which reads the same as
/// "nothing has been analyzed" and is the honest answer either way.
pub(super) fn file_stats_table(handle: beacon_file_stats::FileStatsHandle) -> SystemTable {
    let snapshot: Snapshot = Arc::new(move || {
        let handle = handle.clone();
        Box::pin(async move {
            let Some(store) = handle.get().cloned() else {
                return empty(file_stats_schema());
            };
            let records = tokio::task::spawn_blocking(move || store.registry().scan_records())
                .await
                .map_err(|e| {
                    datafusion::error::DataFusionError::Execution(format!(
                        "file statistics scan panicked: {e}"
                    ))
                })?
                .unwrap_or_default();

            let paths: Vec<&str> = records.iter().map(|(_, r)| r.path.as_str()).collect();
            let ids: Vec<u64> = records.iter().map(|(id, _)| *id).collect();
            let states: Vec<&str> = records.iter().map(|(_, r)| state_name(r.state)).collect();
            let formats: Vec<&str> = records.iter().map(|(_, r)| r.format.as_str()).collect();
            let columns: Vec<u32> = records.iter().map(|(_, r)| r.column_count).collect();
            let rows: Vec<Option<u64>> = records.iter().map(|(_, r)| r.num_rows).collect();
            let bytes: Vec<Option<u64>> = records.iter().map(|(_, r)| r.total_byte_size).collect();
            let epochs: Vec<u64> = records.iter().map(|(_, r)| r.stats_epoch).collect();
            let sizes: Vec<u64> = records.iter().map(|(_, r)| r.size).collect();
            let modified: Vec<i64> = records
                .iter()
                .map(|(_, r)| r.last_modified_millis)
                .collect();

            RecordBatch::try_new(
                file_stats_schema(),
                vec![
                    Arc::new(StringArray::from(paths)) as ArrayRef,
                    Arc::new(UInt64Array::from(ids)),
                    Arc::new(StringArray::from(states)),
                    Arc::new(StringArray::from(formats)),
                    Arc::new(UInt32Array::from(columns)),
                    Arc::new(UInt64Array::from(rows)),
                    Arc::new(UInt64Array::from(bytes)),
                    Arc::new(UInt64Array::from(epochs)),
                    Arc::new(UInt64Array::from(sizes)),
                    Arc::new(arrow::array::Int64Array::from(modified)),
                ],
            )
            .map_err(Into::into)
        })
    });
    SystemTable::new(file_stats_schema(), snapshot)
}

/// `beacon.system.file_stats_segments` — one row per segment, from the manifest.
pub(super) fn segments_table(handle: beacon_file_stats::FileStatsHandle) -> SystemTable {
    let snapshot: Snapshot = Arc::new(move || {
        let handle = handle.clone();
        Box::pin(async move {
            let Some(store) = handle.get().cloned() else {
                return empty(segments_schema());
            };
            let segments = store.segments().await;

            let names: Vec<&str> = segments.iter().map(|s| s.name.as_str()).collect();
            let seqs: Vec<u64> = segments.iter().map(|s| s.seq).collect();
            let mins: Vec<u64> = segments.iter().map(|s| s.min_file_id).collect();
            let maxes: Vec<u64> = segments.iter().map(|s| s.max_file_id).collect();
            let files: Vec<u64> = segments.iter().map(|s| s.num_files).collect();
            let columns: Vec<u64> = segments.iter().map(|s| s.column_ids.len() as u64).collect();

            RecordBatch::try_new(
                segments_schema(),
                vec![
                    Arc::new(StringArray::from(names)) as ArrayRef,
                    Arc::new(UInt64Array::from(seqs)),
                    Arc::new(UInt64Array::from(mins)),
                    Arc::new(UInt64Array::from(maxes)),
                    Arc::new(UInt64Array::from(files)),
                    Arc::new(UInt64Array::from(columns)),
                ],
            )
            .map_err(Into::into)
        })
    });
    SystemTable::new(segments_schema(), snapshot)
}

/// No store: the subsystem is off, or has not started. Zero rows says so without
/// pretending it is an error.
fn empty(schema: SchemaRef) -> DFResult<RecordBatch> {
    Ok(RecordBatch::new_empty(schema))
}

// ── file_statistics(path) ───────────────────────────────────────────────────

/// Files one call may report on.
///
/// A segment holds a row per file per column, so a wide glob multiplies out fast
/// — 10K files of 200 columns is two million rows materialized in one batch.
/// The cap turns that into an error naming the count, rather than an instance
/// that stops answering.
const MAX_FILES_PER_CALL: usize = 1_000;

fn file_statistics_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("path", DataType::Utf8, false),
        Field::new("column", DataType::Utf8, false),
        // The type the range is held in, which is the file's own type for that
        // column rather than any merged table type.
        Field::new("data_type", DataType::Utf8, false),
        // Rendered from the column's own type. Null means the recorded bound is
        // null, which is what a column of only nulls produces.
        Field::new("min", DataType::Utf8, true),
        Field::new("max", DataType::Utf8, true),
        Field::new("null_count", DataType::UInt64, true),
        Field::new("row_count", DataType::UInt64, true),
        // Which segment answered. Two segments can hold the same file; the newest
        // wins, and this says which one that was.
        Field::new("segment", DataType::Utf8, false),
    ]))
}

/// `file_statistics('path')` — every column range Beacon recorded for a file.
///
/// The per-file counterpart to `beacon.system.file_stats`, which summarizes one
/// row per file. This one opens the segments and reports what is actually stored,
/// which is the only way to answer "will this prune, and on what". Accepts a
/// glob, so a whole dataset directory can be checked in one call:
///
/// ```sql
/// SELECT * FROM file_statistics('argo/2024/01/profile_001.nc');
/// SELECT * FROM file_statistics('argo/2024/**');
/// ```
pub(crate) struct FileStatisticsFunc {
    handle: beacon_file_stats::FileStatsHandle,
}

impl std::fmt::Debug for FileStatisticsFunc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileStatisticsFunc").finish_non_exhaustive()
    }
}

impl FileStatisticsFunc {
    pub(crate) fn new(handle: beacon_file_stats::FileStatsHandle) -> Self {
        Self { handle }
    }
}

impl TableFunctionImpl for FileStatisticsFunc {
    fn call(&self, args: &[Expr]) -> DFResult<Arc<dyn TableProvider>> {
        let patterns = parse_glob_paths_arg(args, "file_statistics")?;
        if patterns.is_empty() {
            return plan_err!("file_statistics requires a path: file_statistics('argo/a.nc')");
        }

        // The subsystem is either on or it is not; the handle is filled at
        // startup. Saying so beats returning zero rows, which reads as "this
        // file has no statistics" and sends the operator looking in the wrong
        // place.
        let Some(store) = self.handle.get().cloned() else {
            return plan_err!(
                "file statistics are not enabled on this runtime; set BEACON_FILE_STATS_ENABLE=true"
            );
        };

        let files = resolve_files(&store, &patterns)?;
        let schema = file_statistics_schema();
        let snapshot: Snapshot = {
            let schema = schema.clone();
            Arc::new(move || {
                let store = store.clone();
                let files = files.clone();
                let schema = schema.clone();
                Box::pin(async move { file_statistics_batch(store, files, schema).await })
            })
        };
        Ok(Arc::new(FileStatisticsTable(SystemTable::new(
            schema, snapshot,
        ))))
    }
}

/// The files a call covers, resolved once at planning time so a typo fails
/// there rather than as an empty result.
fn resolve_files(
    store: &Arc<beacon_file_stats::FileStatsStore>,
    patterns: &[String],
) -> DFResult<Vec<(u64, String)>> {
    let mut files: Vec<(u64, String)> = Vec::new();

    for pattern in patterns {
        if pattern.contains(['*', '?', '[']) {
            let matcher = glob::Pattern::new(pattern)
                .map_err(|e| DataFusionError::Plan(format!("invalid path pattern: {e}")))?;
            let records = store
                .registry()
                .scan_records()
                .map_err(|e| DataFusionError::Execution(format!("file statistics scan: {e}")))?;
            files.extend(
                records
                    .into_iter()
                    .filter(|(_, record)| matcher.matches(&record.path))
                    .map(|(id, record)| (id, record.path)),
            );
        } else {
            let id = store
                .registry()
                .file_id(pattern)
                .map_err(|e| DataFusionError::Execution(format!("file statistics lookup: {e}")))?;
            let Some(id) = id else {
                return plan_err!(
                    "no file '{pattern}' in the file statistics registry; \
                     `SELECT path FROM beacon.system.file_stats` lists what is known"
                );
            };
            files.push((id, pattern.clone()));
        }
    }

    files.sort_by(|a, b| a.1.cmp(&b.1));
    files.dedup_by(|a, b| a.0 == b.0);

    if files.is_empty() {
        return plan_err!(
            "no file matches {patterns:?} in the file statistics registry; \
             `SELECT path FROM beacon.system.file_stats` lists what is known"
        );
    }
    if files.len() > MAX_FILES_PER_CALL {
        return plan_err!(
            "{} files match {patterns:?}; file_statistics reports on at most \
             {MAX_FILES_PER_CALL}. Narrow the pattern.",
            files.len()
        );
    }
    Ok(files)
}

/// Reads the segments and renders one row per file per column.
async fn file_statistics_batch(
    store: Arc<beacon_file_stats::FileStatsStore>,
    files: Vec<(u64, String)>,
    schema: SchemaRef,
) -> DFResult<RecordBatch> {
    let mut paths: Vec<String> = Vec::new();
    let mut columns: Vec<String> = Vec::new();
    let mut types: Vec<String> = Vec::new();
    let mut mins: Vec<Option<String>> = Vec::new();
    let mut maxes: Vec<Option<String>> = Vec::new();
    let mut null_counts: Vec<Option<u64>> = Vec::new();
    let mut row_counts: Vec<Option<u64>> = Vec::new();
    let mut segments: Vec<String> = Vec::new();

    for (file_id, path) in files {
        let stats = store
            .file_column_stats(file_id)
            .await
            .map_err(|e| DataFusionError::Execution(format!("file statistics for {path}: {e}")))?;

        // Column names come from the registry, which is redb: a blocking read,
        // so it is resolved off the async worker in one hop rather than per row.
        let ids: Vec<u32> = stats.iter().map(|stat| stat.column_id).collect();
        let names = {
            let store = store.clone();
            tokio::task::spawn_blocking(move || {
                ids.into_iter()
                    .map(|id| store.registry().column_name(id).ok().flatten())
                    .collect::<Vec<Option<String>>>()
            })
            .await
            .map_err(|e| DataFusionError::Execution(format!("column name lookup: {e}")))?
        };

        let mut rows: Vec<usize> = (0..stats.len()).collect();
        rows.sort_by(|a, b| names[*a].cmp(&names[*b]));
        for row in rows {
            let stat = &stats[row];
            paths.push(path.clone());
            columns.push(
                names[row]
                    .clone()
                    .unwrap_or_else(|| format!("column#{}", stat.column_id)),
            );
            types.push(stat.data_type.to_string());
            mins.push(render(&stat.min)?);
            maxes.push(render(&stat.max)?);
            null_counts.push(stat.null_count);
            row_counts.push(stat.row_count);
            segments.push(stat.segment.clone());
        }
    }

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(paths)) as ArrayRef,
            Arc::new(StringArray::from(columns)),
            Arc::new(StringArray::from(types)),
            Arc::new(StringArray::from(mins)),
            Arc::new(StringArray::from(maxes)),
            Arc::new(UInt64Array::from(null_counts)),
            Arc::new(UInt64Array::from(row_counts)),
            Arc::new(StringArray::from(segments)),
        ],
    )
    .map_err(Into::into)
}

/// Renders a one-element bound in its own type, so a timestamp reads as a
/// timestamp rather than as the integer underneath it.
fn render(value: &ArrayRef) -> DFResult<Option<String>> {
    if value.is_empty() || value.is_null(0) {
        return Ok(None);
    }
    let options = FormatOptions::default();
    let formatter = ArrayFormatter::try_new(value.as_ref(), &options)?;
    Ok(Some(formatter.value(0).to_string()))
}

/// The provider `file_statistics(...)` returns.
///
/// A newtype over [`SystemTable`] purely so the authorization walk can recognize
/// it: the function has no schema in the plan, so the name-based metadata gate
/// cannot see it. Everything else delegates.
#[derive(Debug)]
pub(crate) struct FileStatisticsTable(SystemTable);

#[async_trait::async_trait]
impl TableProvider for FileStatisticsTable {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.0.schema()
    }

    fn table_type(&self) -> datafusion::logical_expr::TableType {
        self.0.table_type()
    }

    async fn scan(
        &self,
        state: &dyn datafusion::catalog::Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        self.0.scan(state, projection, filters, limit).await
    }
}
