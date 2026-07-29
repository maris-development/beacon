use datafusion::physical_plan::metrics::{Count, ExecutionPlanMetricsSet, MetricBuilder};

#[derive(Debug, Clone)]
pub struct BBFGlobalMetrics {
    row_count: Count,
}

impl BBFGlobalMetrics {
    pub fn new(metrics_set: ExecutionPlanMetricsSet) -> Self {
        let builder = MetricBuilder::new(&metrics_set);
        let row_count = builder.global_counter("bbf_total_row_count");

        Self { row_count }
    }

    pub fn add_rows(&self, count: usize) {
        self.row_count.add(count);
    }
}
