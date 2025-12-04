use datafusion::logical_expr::LogicalPlan;

use crate::output::QueryOutputFile;

pub struct ParsedPlan {
    pub datafusion_plan: LogicalPlan,
    pub output_file: Option<QueryOutputFile>,
}

impl ParsedPlan {
    pub fn new(datafusion_plan: LogicalPlan, output_file: Option<QueryOutputFile>) -> Self {
        Self {
            datafusion_plan,
            output_file,
        }
    }
}
