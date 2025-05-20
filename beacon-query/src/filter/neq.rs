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
pub enum Neq {
    Number {
        #[serde(alias = "for_query_parameter")]
        column: String,
        #[serde(alias = "not_eq")]
        #[serde(alias = "not_equal")]
        neq: f64,
    },
    Timestamp {
        #[serde(alias = "for_query_parameter")]
        column: String,
        #[serde(alias = "not_eq")]
        #[serde(alias = "not_equal")]
        neq: NaiveDateTime,
    },
    String {
        #[serde(alias = "for_query_parameter")]
        column: String,
        #[serde(alias = "not_eq")]
        #[serde(alias = "not_equal")]
        neq: String,
    },
}

impl Neq {
    pub fn parse(
        &self,
        _session_state: &SessionState,
        schema: &Schema,
    ) -> datafusion::error::Result<Expr> {
        match self {
            Neq::Number { neq, column } => {
                let column_type = get_column_type(schema, &column);
                let column = parse_column_name(&column);

                match column_type {
                    Some(dtype) => {
                        let coerced_lit = try_coerce_number_to_schema(*neq, &dtype);
                        Ok(column.not_eq(coerced_lit))
                    }
                    None => Ok(column.not_eq(lit(*neq))),
                }
            }
            Neq::Timestamp { neq, column } => {
                let column = parse_column_name(&column);
                Ok(column.not_eq(lit_timestamp_nano(neq.timestamp_nanos())))
            }
            Neq::String { neq, column } => {
                let column = parse_column_name(&column);
                Ok(column.not_eq(lit(neq)))
            }
        }
    }
}
