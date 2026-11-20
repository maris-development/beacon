use chrono::NaiveDateTime;
use datafusion::{
    arrow::datatypes::Schema,
    execution::SessionState,
    logical_expr::lit,
    prelude::{lit_timestamp_nano, Expr},
};

use crate::filter::try_coerce_number_to_schema;

use super::{get_column_type, parse_column_name};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[serde(untagged)]
pub enum Eq {
    Number {
        #[serde(alias = "for_query_parameter")]
        column: String,
        eq: f64,
    },
    Timestamp {
        #[serde(alias = "for_query_parameter")]
        column: String,
        eq: NaiveDateTime,
    },
    String {
        #[serde(alias = "for_query_parameter")]
        column: String,
        eq: String,
    },
}

impl Eq {
    pub fn parse(
        &self,
        _session_state: &SessionState,
        schema: &Schema,
    ) -> datafusion::error::Result<Expr> {
        match self {
            Eq::Number { eq, column } => {
                let column_type = get_column_type(schema, &column);
                let column = parse_column_name(&column);

                match column_type {
                    Some(dtype) => {
                        let coerced_lit = try_coerce_number_to_schema(*eq, &dtype);
                        Ok(column.eq(coerced_lit))
                    }
                    None => Ok(column.eq(lit(*eq))),
                }
            }
            Eq::Timestamp { eq, column } => {
                let column = parse_column_name(&column);
                Ok(column.eq(lit_timestamp_nano(eq.timestamp_nanos())))
            }
            Eq::String { eq, column } => {
                let column = parse_column_name(&column);
                Ok(column.eq(lit(eq)))
            }
        }
    }
}
