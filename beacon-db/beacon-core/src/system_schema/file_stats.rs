//! `beacon.system.file_stats` and `beacon.system.file_stats_segments` — the
//! background statistics subsystem as SQL.
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
    array::{ArrayRef, StringArray, UInt32Array, UInt64Array},
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use beacon_file_stats::FileState;
use datafusion::common::Result as DFResult;

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
