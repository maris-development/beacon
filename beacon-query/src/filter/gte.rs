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
pub enum Gteq {
    Number {
        #[serde(alias = "for_query_parameter")]
        column: String,
        #[serde(alias = "min")]
        gt_eq: f64,
    },
    Timestamp {
        #[serde(alias = "for_query_parameter")]
        column: String,
        #[serde(alias = "min")]
        gt_eq: NaiveDateTime,
    },
    String {
        #[serde(alias = "for_query_parameter")]
        column: String,
        #[serde(alias = "min")]
        gt_eq: String,
    },
}

impl Gteq {
    pub fn parse(
        &self,
        _session_state: &SessionState,
        schema: &Schema,
    ) -> datafusion::error::Result<Expr> {
        match self {
            Gteq::Number { gt_eq, column } => {
                let column_type = get_column_type(schema, &column);
                let column = parse_column_name(&column);

                match column_type {
                    Some(dtype) => {
                        let coerced_lit = try_coerce_number_to_schema(*gt_eq, &dtype);
                        Ok(column.gt_eq(coerced_lit))
                    }
                    None => Ok(column.gt_eq(lit(*gt_eq))),
                }
            }
            Gteq::Timestamp { gt_eq, column } => {
                let column = parse_column_name(&column);
                Ok(column.gt_eq(lit_timestamp_nano(gt_eq.timestamp_nanos())))
            }
            Gteq::String { gt_eq, column } => {
                let column = parse_column_name(&column);
                Ok(column.gt_eq(lit(gt_eq)))
            }
        }
    }
}
