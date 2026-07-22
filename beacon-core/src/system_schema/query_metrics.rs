//! `beacon.system.query_metrics` — the metrics recorded for executed queries.

use std::{collections::HashMap, sync::Arc};

use arrow::{
    array::{ArrayRef, StringArray, UInt64Array},
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use datafusion::common::Result as DFResult;
use parking_lot::Mutex;

use crate::metrics::ConsolidatedMetrics;

use super::table::{Snapshot, SystemTable};

/// The runtime's recorded per-query metrics, keyed by query id.
pub type QueryMetricsMap = Arc<Mutex<HashMap<uuid::Uuid, ConsolidatedMetrics>>>;

/// The scalar counters become typed columns; everything with an open-ended shape
/// (the query itself, both logical plans, the physical-plan metric tree, the
/// file list) stays a single JSON string column. Modelling those as nested Arrow
/// types would pin the table schema to DataFusion's plan and metric
/// representations, which change between versions.
pub(super) fn query_metrics_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("query_id", DataType::Utf8, false),
        Field::new("query", DataType::Utf8, false),
        Field::new("input_rows", DataType::UInt64, false),
        Field::new("input_bytes", DataType::UInt64, false),
        Field::new("result_num_rows", DataType::UInt64, false),
        Field::new("result_size_in_bytes", DataType::UInt64, false),
        Field::new("execution_time_ms", DataType::UInt64, false),
        Field::new("file_paths", DataType::Utf8, false),
        Field::new("parsed_logical_plan", DataType::Utf8, false),
        Field::new("optimized_logical_plan", DataType::Utf8, false),
        Field::new("node_metrics", DataType::Utf8, false),
    ]))
}

fn json_string(value: &serde_json::Value) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| "null".to_string())
}

fn query_metrics_batch(mut rows: Vec<ConsolidatedMetrics>) -> DFResult<RecordBatch> {
    // The source is a `HashMap`, whose iteration order varies per scan. Sorting by
    // query id makes scans deterministic. It is not a chronological order —
    // `ConsolidatedMetrics` carries no timestamp, so "most recent queries" is not
    // expressible until one is added.
    rows.sort_by_key(|metrics| metrics.query_id);

    let query_ids: Vec<String> = rows
        .iter()
        .map(|metrics| metrics.query_id.to_string())
        .collect();
    let queries: Vec<String> = rows.iter().map(|m| json_string(&m.query)).collect();
    let input_rows: Vec<u64> = rows.iter().map(|m| m.input_rows).collect();
    let input_bytes: Vec<u64> = rows.iter().map(|m| m.input_bytes).collect();
    let result_num_rows: Vec<u64> = rows.iter().map(|m| m.result_num_rows).collect();
    let result_sizes: Vec<u64> = rows.iter().map(|m| m.result_size_in_bytes).collect();
    let execution_times: Vec<u64> = rows.iter().map(|m| m.execution_time_ms).collect();
    let file_paths: Vec<String> = rows
        .iter()
        .map(|m| serde_json::to_string(&m.file_paths).unwrap_or_else(|_| "[]".to_string()))
        .collect();
    let parsed_plans: Vec<String> = rows
        .iter()
        .map(|m| json_string(&m.parsed_logical_plan))
        .collect();
    let optimized_plans: Vec<String> = rows
        .iter()
        .map(|m| json_string(&m.optimized_logical_plan))
        .collect();
    let node_metrics: Vec<String> = rows
        .iter()
        .map(|m| serde_json::to_string(&m.node_metrics).unwrap_or_else(|_| "null".to_string()))
        .collect();

    let columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(query_ids)),
        Arc::new(StringArray::from(queries)),
        Arc::new(UInt64Array::from(input_rows)),
        Arc::new(UInt64Array::from(input_bytes)),
        Arc::new(UInt64Array::from(result_num_rows)),
        Arc::new(UInt64Array::from(result_sizes)),
        Arc::new(UInt64Array::from(execution_times)),
        Arc::new(StringArray::from(file_paths)),
        Arc::new(StringArray::from(parsed_plans)),
        Arc::new(StringArray::from(optimized_plans)),
        Arc::new(StringArray::from(node_metrics)),
    ];

    Ok(RecordBatch::try_new(query_metrics_schema(), columns)?)
}

/// `beacon.system.query_metrics`, reading the runtime's live metrics map.
pub(super) fn query_metrics_table(metrics: QueryMetricsMap) -> SystemTable {
    let snapshot: Snapshot = Arc::new(move || {
        // Clone under the lock and build the batch outside it: the map is on the
        // query-completion path and must not be held across the row build.
        let rows: Vec<ConsolidatedMetrics> = metrics.lock().values().cloned().collect();
        query_metrics_batch(rows)
    });

    SystemTable::new(query_metrics_schema(), snapshot)
}
