//! Metrics tracking for query execution in Beacon Planner.
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
        let physical_plan = self.physical_plan.read().clone().unwrap();

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
            node_metrics: collect_metrics_json(physical_plan.as_ref()),
            execution_time_ms: self.start_time.elapsed().as_millis() as u64,
        }
    }
}

/// Metrics for a node in the physical execution plan.
///
/// Includes operator name, metrics, and child nodes.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
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

/// Extended:
/// Extend the metrics tracker to expose the files as a tracer we can use for BBF
impl MetricsTracker {
    /// Get a tracer that can be passed to BBFSource to track files read
    pub fn get_as_file_tracer(&self) -> Arc<Mutex<Vec<String>>> {
        self.file_paths.clone()
    }
}
