use datafusion::logical_expr::LogicalPlan;

use crate::output::QueryOutput;

pub struct ParsedPlan {
    pub datafusion_plan: LogicalPlan,
    pub output: Option<QueryOutput>,
}

impl ParsedPlan {
    pub fn new(datafusion_plan: LogicalPlan, output: Option<QueryOutput>) -> Self {
        Self {
            datafusion_plan,
            output,
        }
    }
}
