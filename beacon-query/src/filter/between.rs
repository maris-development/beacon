use datafusion::prelude::{lit, lit_timestamp_nano};

use crate::filter::{get_column_type, parse_column_name};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub enum Between {
    Number {
        #[serde(alias = "for_query_parameter")]
        column: String,
        #[serde(alias = "max")]
        gt_eq: f64,
        #[serde(alias = "min")]
        lt_eq: f64,
    },
    Timestamp {
        #[serde(alias = "for_query_parameter")]
        column: String,
        #[serde(alias = "max")]
        gt_eq: chrono::NaiveDateTime,
        #[serde(alias = "min")]
        lt_eq: chrono::NaiveDateTime,
    },
    String {
        #[serde(alias = "for_query_parameter")]
        column: String,
        #[serde(alias = "max")]
        gt_eq: String,
        #[serde(alias = "min")]
        lt_eq: String,
    },
}

impl Between {
    pub fn parse(
        &self,
        _session_state: &datafusion::execution::SessionState,
        schema: &datafusion::arrow::datatypes::Schema,
    ) -> datafusion::error::Result<datafusion::logical_expr::Expr> {
        match self {
            Between::Number {
                column,
                gt_eq,
                lt_eq,
            } => {
                let column_type = get_column_type(schema, &column);
                let column = parse_column_name(&column);

                match column_type {
                    Some(dtype) => {
                        let coerced_lit =
                            crate::filter::try_coerce_number_to_schema(*lt_eq, &dtype);
                        let coerced_lit2 =
                            crate::filter::try_coerce_number_to_schema(*gt_eq, &dtype);
                        Ok(column.between(coerced_lit, coerced_lit2))
                    }
                    None => Ok(column.between(lit(*lt_eq), lit(*gt_eq))),
                }
            }
            Between::Timestamp {
                column,
                gt_eq,
                lt_eq,
            } => {
                let column = super::parse_column_name(column);
                Ok(column.between(
                    lit_timestamp_nano(lt_eq.timestamp_nanos()),
                    lit_timestamp_nano(gt_eq.timestamp_nanos()),
                ))
            }
            Between::String {
                column,
                gt_eq,
                lt_eq,
            } => {
                let column = super::parse_column_name(column);
                Ok(column.between(lit(lt_eq), lit(gt_eq)))
            }
        }
    }
}
