use std::sync::Arc;

use datafusion::{
    datasource::physical_plan::{CsvExec, ParquetExec},
    physical_plan::ExecutionPlan,
};

use super::MetricsTracker;

pub struct TrackingPlanner {
    tracker: Arc<MetricsTracker>,
}

impl TrackingPlanner {
    pub fn new(tracker: Arc<MetricsTracker>) -> Self {
        Self { tracker }
    }

    fn wrap_file_scans(&self, plan: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        if let Some(csv) = plan.as_any().downcast_ref::<CsvExec>() {
            let files = csv
                .base_config()
                .file_groups
                .iter()
                .flat_map(|group| group.iter())
                .map(|f| f.object_meta.location.to_string())
                .collect::<Vec<_>>();
            // Add the file paths to the tracker
            self.tracker.add_file_paths(files);
        } else if let Some(parquet) = plan.as_any().downcast_ref::<ParquetExec>() {
            let files = parquet
                .base_config()
                .file_groups
                .iter()
                .flat_map(|group| group.iter())
                .map(|f| f.object_meta.location.to_string())
                .collect::<Vec<_>>();

            self.tracker.add_file_paths(files);
        }
        // Recurse into children
        let new_children: Vec<_> = plan
            .children()
            .into_iter()
            .map(|c| self.wrap_file_scans(c.clone()))
            .collect();

        plan.clone().with_new_children(new_children).unwrap_or(plan)
    }
}
