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
pub enum Lt {
    Number {
        #[serde(alias = "for_query_parameter")]
        column: String,
        lt: f64,
    },
    Timestamp {
        #[serde(alias = "for_query_parameter")]
        column: String,
        lt: NaiveDateTime,
    },
    String {
        #[serde(alias = "for_query_parameter")]
        column: String,
        lt: String,
    },
}

impl Lt {
    pub fn parse(
        &self,
        _session_state: &SessionState,
        schema: &Schema,
    ) -> datafusion::error::Result<Expr> {
        match self {
            Lt::Number { lt, column } => {
                let column_type = get_column_type(schema, &column);
                let column = parse_column_name(&column);

                match column_type {
                    Some(dtype) => {
                        let coerced_lit = try_coerce_number_to_schema(*lt, &dtype);
                        Ok(column.lt(coerced_lit))
                    }
                    None => Ok(column.lt(lit(*lt))),
                }
            }
            Lt::Timestamp { lt, column } => {
                let column = parse_column_name(&column);
                Ok(column.lt(lit_timestamp_nano(lt.timestamp_nanos())))
            }
            Lt::String { lt, column } => {
                let column = parse_column_name(&column);
                Ok(column.lt(lit(lt)))
            }
        }
    }
}
