use beacon_output::OutputFormat;
use datafusion::logical_expr::LogicalPlan;

pub struct BeaconQueryPlan {
    pub inner_datafusion_plan: LogicalPlan,
    pub output: OutputFormat,
}

impl BeaconQueryPlan {
    pub fn new(inner_datafusion_plan: LogicalPlan, output: OutputFormat) -> Self {
        Self {
            inner_datafusion_plan,
            output,
        }
    }
}
