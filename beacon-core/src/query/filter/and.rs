use datafusion::{
    arrow::datatypes::Schema,
    execution::SessionState,
    prelude::{lit, Expr},
};

#[derive(Debug, Clone, utoipa::ToSchema, serde::Serialize, serde::Deserialize)]
pub struct And(pub Vec<super::Filter>);

impl And {
    pub fn parse(
        &self,
        session_state: &SessionState,
        schema: &Schema,
    ) -> datafusion::error::Result<Expr> {
        self.0
            .iter()
            .map(|f| f.parse(session_state, schema))
            .fold(Ok(lit(true)), |acc, expr| {
                acc.and_then(|acc| expr.map(|expr| acc.and(expr)))
            })
    }
}
