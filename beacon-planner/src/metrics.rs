use std::{
    collections::HashMap,
    sync::{Arc, atomic::AtomicU64},
};

use datafusion::{
    datasource::file_format::csv::CsvSink,
    logical_expr::LogicalPlan,
    physical_plan::{ExecutionPlan, filter::FilterExec, insert::DataSinkExec},
};
use parking_lot::{Mutex, RwLock};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ConsolidatedMetrics {
    pub input_rows: u64,
    pub input_bytes: u64,
    pub output_rows: u64,
    pub output_bytes: u64,
    pub file_paths: Vec<String>,
    pub logical_plan: serde_json::Value,
    pub optimized_logical_plan: serde_json::Value,
    pub node_metrics: NodeMetrics,
}

#[derive(Debug)]
pub struct MetricsTracker {
    pub input_rows: AtomicU64,
    pub input_bytes: AtomicU64,
    pub output_rows: AtomicU64,
    pub output_bytes: AtomicU64,

    pub logical_plan: Arc<Mutex<Option<LogicalPlan>>>,
    pub optimized_logical_plan: Arc<Mutex<Option<LogicalPlan>>>,

    pub file_paths: Arc<Mutex<Vec<String>>>,
    pub physical_plan: Arc<RwLock<Option<Arc<dyn ExecutionPlan>>>>,
}

impl MetricsTracker {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            input_rows: AtomicU64::new(0),
            input_bytes: AtomicU64::new(0),
            output_rows: AtomicU64::new(0),
            output_bytes: AtomicU64::new(0),
            file_paths: Arc::new(Mutex::new(vec![])),
            logical_plan: Arc::new(Mutex::new(None)),
            optimized_logical_plan: Arc::new(Mutex::new(None)),
            physical_plan: Arc::new(RwLock::new(None)),
        })
    }
    pub fn set_logical_plan(&self, plan: &LogicalPlan) {
        // Use a Mutex to ensure thread safety when accessing the logical plan
        let mut logical_plan = self.logical_plan.lock();
        // Set the logical plan
        *logical_plan = Some(plan.clone());
    }
    pub fn set_optimized_logical_plan(&self, plan: &LogicalPlan) {
        let mut optimized_logical_plan = self.optimized_logical_plan.lock();
        *optimized_logical_plan = Some(plan.clone());
    }
    pub fn set_physical_plan(&self, plan: Arc<dyn ExecutionPlan>) {
        // Use a RwLock to ensure thread safety when accessing the physical plan
        let mut physical_plan = self.physical_plan.write();
        // Set the physical plan
        *physical_plan = Some(plan);
    }
    pub fn add_input_rows(&self, rows: u64) {
        self.input_rows
            .fetch_add(rows, std::sync::atomic::Ordering::Relaxed);
    }
    pub fn add_input_bytes(&self, bytes: u64) {
        self.input_bytes
            .fetch_add(bytes, std::sync::atomic::Ordering::Relaxed);
    }
    pub fn add_output_rows(&self, rows: u64) {
        self.output_rows
            .fetch_add(rows, std::sync::atomic::Ordering::Relaxed);
    }
    pub fn add_output_bytes(&self, bytes: u64) {
        self.output_bytes
            .fetch_add(bytes, std::sync::atomic::Ordering::Relaxed);
    }
    pub fn add_file_paths(&self, paths: Vec<String>) {
        let mut file_paths = self.file_paths.lock();
        for path in paths {
            if !file_paths.contains(&path) {
                file_paths.push(path);
            }
        }
    }
    pub fn get_consolidated_metrics(&self) -> ConsolidatedMetrics {
        // Get the metrics from the execution plan
        let physical_plan_opt = self.physical_plan.read();
        let physical_plan = physical_plan_opt.clone().unwrap();

        let logical_plan_json = self
            .logical_plan
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
            input_rows: self.input_rows.load(std::sync::atomic::Ordering::Relaxed),
            input_bytes: self.input_bytes.load(std::sync::atomic::Ordering::Relaxed),
            output_rows: self.output_rows.load(std::sync::atomic::Ordering::Relaxed),
            output_bytes: self.output_bytes.load(std::sync::atomic::Ordering::Relaxed),
            file_paths: self.file_paths.lock().clone(),
            logical_plan: logical_plan_json,
            optimized_logical_plan: optimized_logical_plan_json,
            node_metrics: collect_metrics_json(physical_plan.as_ref()),
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct NodeMetrics {
    operator: String,
    metrics: HashMap<String, serde_json::Value>,
    children: Vec<NodeMetrics>,
}

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
