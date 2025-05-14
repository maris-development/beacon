use parking_lot::Mutex;
use std::sync::{atomic::AtomicU64, Arc};

pub mod planner;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MetricsTracker {
    input_rows: AtomicU64,
    input_bytes: AtomicU64,
    output_rows: AtomicU64,
    output_bytes: AtomicU64,

    file_paths: Arc<Mutex<Vec<String>>>,
}

impl MetricsTracker {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            input_rows: AtomicU64::new(0),
            input_bytes: AtomicU64::new(0),
            output_rows: AtomicU64::new(0),
            output_bytes: AtomicU64::new(0),
            file_paths: Arc::new(Mutex::new(vec![])),
        })
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
}
