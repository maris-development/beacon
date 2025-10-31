use datafusion::logical_expr::LogicalPlan;

use crate::output::QueryOutputFile;

pub struct ParsedPlan {
    pub datafusion_plan: LogicalPlan,
    pub output: Option<QueryOutputFile>,
}

impl ParsedPlan {
    pub fn new(datafusion_plan: LogicalPlan, output: Option<QueryOutputFile>) -> Self {
        Self {
            datafusion_plan,
            output,
        }
    }
}
