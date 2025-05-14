use datafusion::logical_expr::LogicalPlan;

use crate::output::QueryOutputBuffer;

pub struct ParsedPlan {
    pub datafusion_plan: LogicalPlan,
    pub output: QueryOutputBuffer,
}

impl ParsedPlan {
    pub fn new(datafusion_plan: LogicalPlan, output: QueryOutputBuffer) -> Self {
        Self {
            datafusion_plan,
            output,
        }
    }
}
