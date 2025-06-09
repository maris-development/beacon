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
pub enum Gt {
    Number {
        #[serde(alias = "for_query_parameter")]
        column: String,
        gt: f64,
    },
    Timestamp {
        #[serde(alias = "for_query_parameter")]
        column: String,
        gt: NaiveDateTime,
    },
    String {
        #[serde(alias = "for_query_parameter")]
        column: String,
        gt: String,
    },
}

impl Gt {
    pub fn parse(
        &self,
        _session_state: &SessionState,
        schema: &Schema,
    ) -> datafusion::error::Result<Expr> {
        match self {
            Gt::Number { gt, column } => {
                let column_type = get_column_type(schema, &column);
                let column = parse_column_name(&column);

                match column_type {
                    Some(dtype) => {
                        let coerced_lit = try_coerce_number_to_schema(*gt, &dtype);
                        Ok(column.gt(coerced_lit))
                    }
                    None => Ok(column.gt(lit(*gt))),
                }
            }
            Gt::Timestamp { gt, column } => {
                let column = parse_column_name(&column);
                Ok(column.gt(lit_timestamp_nano(gt.timestamp_nanos())))
            }
            Gt::String { gt, column } => {
                let column = parse_column_name(&column);
                Ok(column.gt(lit(gt)))
            }
        }
    }
}
