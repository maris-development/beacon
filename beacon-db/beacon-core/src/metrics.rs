//! Metrics tracking for query execution.
//!
//! This module provides structures and utilities to track and consolidate
//! metrics during query planning and execution, including input/output statistics,
//! logical/physical plans, and per-node metrics.

use std::{
    collections::HashMap,
    sync::{Arc, atomic::AtomicU64},
};

use datafusion::{logical_expr::LogicalPlan, physical_plan::ExecutionPlan};
use parking_lot::{Mutex, RwLock};

/// Consolidated metrics for a query execution.
///
/// This struct is serializable and contains all relevant metrics and plans
/// for a completed query.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ConsolidatedMetrics {
    /// Number of input rows processed.
    pub input_rows: u64,
    /// Number of input bytes processed.
    pub input_bytes: u64,
    /// Number of rows in the result.
    pub result_num_rows: u64,
    /// Size of the result in bytes.
    pub result_size_in_bytes: u64,
    /// List of file paths accessed.
    pub file_paths: Vec<String>,
    /// Total execution time in milliseconds.
    pub execution_time_ms: u64,
    /// The original query as JSON.
    pub query: serde_json::Value,
    /// Unique identifier for the query.
    pub query_id: uuid::Uuid,
    /// Parsed logical plan as JSON.
    pub parsed_logical_plan: serde_json::Value,
    /// Optimized logical plan as JSON.
    pub optimized_logical_plan: serde_json::Value,
    /// Metrics for each node in the physical plan.
    pub node_metrics: NodeMetrics,
}

/// Tracks metrics during query execution.
///
/// This struct is thread-safe and allows concurrent updates to metrics.
#[derive(Debug)]
pub struct MetricsTracker {
    pub input_rows: AtomicU64,
    pub input_bytes: AtomicU64,
    pub result_rows: AtomicU64,
    pub result_size_in_bytes: AtomicU64,
    pub start_time: std::time::Instant,
    pub query: serde_json::Value,
    pub query_id: uuid::Uuid,
    pub parsed_logical_plan: Arc<Mutex<Option<LogicalPlan>>>,
    pub optimized_logical_plan: Arc<Mutex<Option<LogicalPlan>>>,
    pub file_paths: Arc<Mutex<Vec<String>>>,
    pub physical_plan: Arc<RwLock<Option<Arc<dyn ExecutionPlan>>>>,
}

impl MetricsTracker {
    /// Create a new metrics tracker for a query.
    pub fn new(input_query: serde_json::Value, query_id: uuid::Uuid) -> Arc<Self> {
        Arc::new(Self {
            start_time: std::time::Instant::now(),
            query: input_query,
            query_id,
            input_rows: AtomicU64::new(0),
            input_bytes: AtomicU64::new(0),
            result_rows: AtomicU64::new(0),
            result_size_in_bytes: AtomicU64::new(0),
            file_paths: Arc::new(Mutex::new(vec![])),
            parsed_logical_plan: Arc::new(Mutex::new(None)),
            optimized_logical_plan: Arc::new(Mutex::new(None)),
            physical_plan: Arc::new(RwLock::new(None)),
        })
    }

    /// Set the parsed logical plan.
    pub fn set_logical_plan(&self, plan: &LogicalPlan) {
        *self.parsed_logical_plan.lock() = Some(plan.clone());
    }

    /// Set the optimized logical plan.
    pub fn set_optimized_logical_plan(&self, plan: &LogicalPlan) {
        *self.optimized_logical_plan.lock() = Some(plan.clone());
    }

    /// Set the physical execution plan.
    pub fn set_physical_plan(&self, plan: Arc<dyn ExecutionPlan>) {
        *self.physical_plan.write() = Some(plan);
    }

    /// Add to the count of input rows.
    pub fn add_input_rows(&self, rows: u64) {
        self.input_rows
            .fetch_add(rows, std::sync::atomic::Ordering::Relaxed);
    }

    /// Add to the count of input bytes.
    pub fn add_input_bytes(&self, bytes: u64) {
        self.input_bytes
            .fetch_add(bytes, std::sync::atomic::Ordering::Relaxed);
    }

    /// Add to the count of output rows.
    pub fn add_output_rows(&self, rows: u64) {
        self.result_rows
            .fetch_add(rows, std::sync::atomic::Ordering::Relaxed);
    }

    /// Add to the count of output bytes.
    pub fn add_output_bytes(&self, bytes: u64) {
        self.result_size_in_bytes
            .fetch_add(bytes, std::sync::atomic::Ordering::Relaxed);
    }

    /// Add file paths accessed during execution.
    pub fn add_file_paths(&self, paths: Vec<String>) {
        self.file_paths.lock().extend(paths);
    }

    /// Consolidate all metrics into a serializable struct.
    pub fn get_consolidated_metrics(&self) -> ConsolidatedMetrics {
        // The physical plan is optional: callers that only track output
        // rows/bytes (e.g. the unified query path) never register one.
        let physical_plan = self.physical_plan.read().clone();

        let logical_plan_json = self
            .parsed_logical_plan
            .lock()
            .as_ref()
            .map(|plan| serde_json::from_str(&format!("{}", plan.display_pg_json())).unwrap())
            .unwrap_or_default();

        let optimized_logical_plan_json = self
            .optimized_logical_plan
            .lock()
            .as_ref()
            .map(|plan| serde_json::from_str(&format!("{}", plan.display_pg_json())).unwrap())
            .unwrap_or_default();

        ConsolidatedMetrics {
            query_id: self.query_id,
            query: self.query.clone(),
            input_rows: self.input_rows.load(std::sync::atomic::Ordering::Relaxed),
            input_bytes: self.input_bytes.load(std::sync::atomic::Ordering::Relaxed),
            result_num_rows: self.result_rows.load(std::sync::atomic::Ordering::Relaxed),
            result_size_in_bytes: self
                .result_size_in_bytes
                .load(std::sync::atomic::Ordering::Relaxed),
            file_paths: self.file_paths.lock().clone(),
            parsed_logical_plan: logical_plan_json,
            optimized_logical_plan: optimized_logical_plan_json,
            node_metrics: physical_plan
                .as_ref()
                .map(|plan| collect_metrics_json(plan.as_ref()))
                .unwrap_or_default(),
            execution_time_ms: self.start_time.elapsed().as_millis() as u64,
        }
    }
}

/// Metrics for a node in the physical execution plan.
///
/// Includes operator name, metrics, and child nodes.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct NodeMetrics {
    /// Operator name.
    pub operator: String,
    /// Metrics for this node.
    pub metrics: HashMap<String, serde_json::Value>,
    /// Metrics for child nodes.
    pub children: Vec<NodeMetrics>,
}

/// Recursively collect metrics from an execution plan node.
fn collect_metrics_json(plan: &dyn ExecutionPlan) -> NodeMetrics {
    let mut metrics_map = HashMap::new();

    if let Some(metrics_set) = plan.metrics() {
        for metric in metrics_set.iter() {
            metrics_map.insert(
                metric.value().name().to_string(),
                serde_json::to_value(metric.value().as_usize()).unwrap(),
            );
        }
    }

    let children = plan
        .children()
        .into_iter()
        .map(|child| collect_metrics_json(child.as_ref()))
        .collect();

    NodeMetrics {
        operator: plan.name().to_string(),
        metrics: metrics_map,
        children,
    }
}

/// Render an executed physical plan as PostgreSQL-style pgjson annotated with
/// per-node runtime metrics — DataFusion's "Plan with Metrics" shape.
///
/// The output is the array form consumed by pgjson visualizers (e.g. dalibo):
/// a single-element array wrapping the root `"Plan"` node, with children nested
/// recursively under `"Plans"`. Newer DataFusion produces this natively via
/// `EXPLAIN (ANALYZE, FORMAT pgjson)`, which the pinned DataFusion 53 rejects;
/// this backports the same shape by walking the already-executed plan.
///
/// The plan must have been run to completion first so its metrics are populated.
pub fn explain_analyze_pg_json(plan: &dyn ExecutionPlan) -> serde_json::Value {
    serde_json::json!([{ "Plan": node_to_pg_json(plan) }])
}

/// Convert a single physical plan node (and its subtree) into a pgjson node.
fn node_to_pg_json(plan: &dyn ExecutionPlan) -> serde_json::Value {
    let details = format!(
        "{}",
        datafusion::physical_plan::displayable(plan).one_line()
    );

    let mut node = serde_json::json!({
        "Node Type": plan.name(),
        // `one_line` renders with a trailing newline; trim it for a clean detail.
        "Details": details.trim_end(),
    });

    if let Some(metrics_set) = plan.metrics() {
        // Aggregate across partitions, mirroring how `AnalyzeExec` reports metrics.
        let metrics_set = metrics_set.aggregate_by_name();

        if let Some(rows) = metrics_set.output_rows() {
            node["Actual Rows"] = serde_json::json!(rows);
        }
        // `elapsed_compute` is nanoseconds; PostgreSQL's "Actual Total Time" is ms.
        if let Some(nanos) = metrics_set.elapsed_compute() {
            node["Actual Total Time"] = serde_json::json!(nanos as f64 / 1_000_000.0);
        }

        // Every remaining metric goes under `Extras` (the two promoted above are
        // excluded so they are not duplicated).
        let mut extras = serde_json::Map::new();
        for metric in metrics_set.iter() {
            let name = metric.value().name();
            if name == "output_rows" || name == "elapsed_compute" {
                continue;
            }
            extras.insert(name.to_string(), serde_json::json!(metric.value().as_usize()));
        }
        if !extras.is_empty() {
            node["Extras"] = serde_json::Value::Object(extras);
        }
    }

    let children: Vec<serde_json::Value> = plan
        .children()
        .into_iter()
        .map(|child| node_to_pg_json(child.as_ref()))
        .collect();
    node["Plans"] = serde_json::Value::Array(children);

    node
}

/// Extended:
/// Extend the metrics tracker to expose the files as a tracer we can use for BBF
impl MetricsTracker {
    /// Get a tracer that can be passed to BBFSource to track files read
    pub fn get_as_file_tracer(&self) -> Arc<Mutex<Vec<String>>> {
        self.file_paths.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use datafusion::prelude::SessionContext;

    /// Run a small query to completion and return its (metrics-populated)
    /// physical plan. `explain_analyze_pg_json` documents that the plan must have
    /// been executed first, so the tests exercise it in that state.
    async fn executed_plan() -> Arc<dyn ExecutionPlan> {
        let ctx = SessionContext::new();
        let df = ctx
            .sql("SELECT a, count(*) FROM (VALUES (1), (1), (2)) AS t(a) GROUP BY a")
            .await
            .unwrap();
        let plan = df.create_physical_plan().await.unwrap();
        datafusion::physical_plan::collect(plan.clone(), ctx.task_ctx())
            .await
            .unwrap();
        plan
    }

    /// The pgjson output must be the array-wrapped `"Plan"` shape that pgjson
    /// visualizers expect, with children nested under `"Plans"` — this backports a
    /// format the pinned DataFusion cannot emit itself, so nothing else pins it.
    #[tokio::test]
    async fn explain_analyze_pg_json_has_the_visualizer_shape() {
        let plan = executed_plan().await;
        let json = explain_analyze_pg_json(plan.as_ref());

        let root = json
            .as_array()
            .and_then(|array| array.first())
            .and_then(|entry| entry.get("Plan"))
            .expect("output must be [{\"Plan\": ...}]");
        assert_eq!(root["Node Type"], plan.name());
        // `one_line` renders with a trailing newline; the detail must be trimmed.
        let details = root["Details"].as_str().unwrap();
        assert!(!details.ends_with('\n'), "details not trimmed: {details:?}");
        // The subtree is present and recursively shaped the same way.
        let children = root["Plans"].as_array().expect("Plans must be an array");
        assert_eq!(children.len(), plan.children().len());
        assert!(children.iter().all(|child| child.get("Plans").is_some()));
    }

    /// `elapsed_compute` is reported in nanoseconds but PostgreSQL's "Actual Total
    /// Time" is milliseconds, and the two promoted metrics must not be duplicated
    /// into `Extras` — both are easy to get wrong and invisible in the output.
    #[tokio::test]
    async fn promoted_metrics_are_converted_and_not_duplicated() {
        let plan = executed_plan().await;
        let json = explain_analyze_pg_json(plan.as_ref());
        let root = &json[0]["Plan"];

        assert_eq!(root["Actual Rows"], serde_json::json!(2));
        if let Some(extras) = root.get("Extras") {
            assert!(extras.get("output_rows").is_none());
            assert!(extras.get("elapsed_compute").is_none());
        }

        // Nanoseconds -> milliseconds: a sub-second query must land well under 1ms
        // worth of milliseconds, never in the millions (the raw nanosecond count).
        if let Some(time) = root.get("Actual Total Time") {
            let time = time.as_f64().expect("time must be a number");
            assert!(time < 1_000.0, "elapsed_compute was not converted: {time}");
        }
    }

    /// The physical plan is optional on the tracker (the unified query path only
    /// records output rows/bytes), so consolidation must still produce a complete
    /// record rather than panicking on the missing plan.
    #[test]
    fn consolidates_without_a_physical_or_logical_plan() {
        let query_id = uuid::Uuid::new_v4();
        let tracker = MetricsTracker::new(serde_json::json!({"sql": "SELECT 1"}), query_id);
        tracker.add_output_rows(3);
        tracker.add_output_bytes(128);
        tracker.add_file_paths(vec!["argo/a.parquet".to_string()]);

        let metrics = tracker.get_consolidated_metrics();
        assert_eq!(metrics.query_id, query_id);
        assert_eq!(metrics.result_num_rows, 3);
        assert_eq!(metrics.result_size_in_bytes, 128);
        assert_eq!(metrics.file_paths, vec!["argo/a.parquet".to_string()]);
        assert_eq!(metrics.parsed_logical_plan, serde_json::Value::Null);
        assert_eq!(metrics.node_metrics.operator, "");
        assert!(metrics.node_metrics.children.is_empty());
    }

    /// The file tracer shares the tracker's path list rather than copying it, so
    /// files a source records while streaming still land in the query's metrics.
    #[test]
    fn the_file_tracer_writes_through_to_the_tracker() {
        let tracker = MetricsTracker::new(serde_json::Value::Null, uuid::Uuid::new_v4());
        tracker
            .get_as_file_tracer()
            .lock()
            .push("argo/b.parquet".to_string());
        assert_eq!(
            tracker.get_consolidated_metrics().file_paths,
            vec!["argo/b.parquet".to_string()]
        );
    }
}
