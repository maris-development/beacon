use datafusion::{
    arrow::datatypes::Schema,
    execution::SessionState,
    prelude::{lit, Expr},
};

use super::Filter;

#[derive(Debug, Clone, utoipa::ToSchema, serde::Serialize, serde::Deserialize)]
pub struct Or(pub Vec<Filter>);

impl Or {
    pub fn parse(
        &self,
        session_state: &SessionState,
        schema: &Schema,
    ) -> datafusion::error::Result<Expr> {
        self.0
            .iter()
            .map(|f| f.parse(session_state, schema))
            .fold(Ok(lit(false)), |acc, expr| {
                acc.and_then(|acc| expr.map(|expr| acc.or(expr)))
            })
    }
}
