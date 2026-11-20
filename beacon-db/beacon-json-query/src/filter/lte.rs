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
pub enum Lteq {
    Number {
        #[serde(alias = "for_query_parameter")]
        column: String,
        #[serde(alias = "max")]
        lt_eq: f64,
    },
    Timestamp {
        #[serde(alias = "for_query_parameter")]
        column: String,
        #[serde(alias = "max")]
        lt_eq: NaiveDateTime,
    },
    String {
        #[serde(alias = "for_query_parameter")]
        column: String,
        #[serde(alias = "max")]
        lt_eq: String,
    },
}

impl Lteq {
    pub fn parse(
        &self,
        _session_state: &SessionState,
        schema: &Schema,
    ) -> datafusion::error::Result<Expr> {
        match self {
            Lteq::Number { lt_eq, column } => {
                let column_type = get_column_type(schema, &column);
                let column = parse_column_name(&column);

                match column_type {
                    Some(dtype) => {
                        let coerced_lit = try_coerce_number_to_schema(*lt_eq, &dtype);
                        Ok(column.lt_eq(coerced_lit))
                    }
                    None => Ok(column.lt_eq(lit(*lt_eq))),
                }
            }
            Lteq::Timestamp { lt_eq, column } => {
                let column = parse_column_name(&column);
                Ok(column.lt_eq(lit_timestamp_nano(lt_eq.timestamp_nanos())))
            }
            Lteq::String { lt_eq, column } => {
                let column = parse_column_name(&column);
                Ok(column.lt_eq(lit(lt_eq)))
            }
        }
    }
}
