use datafusion::{arrow::datatypes::Schema, execution::SessionState, prelude::Expr};

use super::parse_column_name;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct IsNull {
    #[serde(alias = "for_query_parameter")]
    pub column: String,
}

impl IsNull {
    pub fn parse(
        &self,
        _session_state: &SessionState,
        _schema: &Schema,
    ) -> datafusion::error::Result<Expr> {
        let column = parse_column_name(&self.column);
        Ok(column.is_null())
    }
}
