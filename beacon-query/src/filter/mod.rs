use and::And;
use datafusion::arrow::datatypes::Schema;
use datafusion::common::Column;
use datafusion::execution::SessionState;
use datafusion::logical_expr::*;
use datafusion::{arrow::datatypes::DataType, prelude::Expr};
use gt::Gt;
use gte::Gteq;
use is_not_null::IsNotNull;
use is_null::IsNull;
use lt::Lt;
use lte::Lteq;
use or::Or;

pub mod and;
pub mod between;
pub mod eq;
pub mod geo_json;
pub mod gt;
pub mod gte;
pub mod is_not_null;
pub mod is_null;
pub mod lt;
pub mod lte;
pub mod or;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum Filter {
    #[serde(alias = "and")]
    And(And),
    #[serde(alias = "or")]
    Or(Or),
    #[serde(
        alias = "is_not_null",
        alias = "skip_fill_values",
        alias = "skip_missing"
    )]
    IsNotNull(IsNotNull),
    #[serde(alias = "is_null")]
    IsNull(IsNull),
    #[serde(untagged)]
    Between(between::Between),
    #[serde(untagged)]
    Eq(eq::Eq),
    #[serde(untagged)]
    Gt(Gt),
    #[serde(untagged)]
    Gteq(Gteq),
    #[serde(untagged)]
    Lt(Lt),
    #[serde(untagged)]
    Lteq(Lteq),
    #[serde(untagged)]
    GeoJson(geo_json::GeoJsonFilter),
}

impl Filter {
    pub fn parse(
        &self,
        session_state: &SessionState,
        schema: &Schema,
    ) -> datafusion::error::Result<Expr> {
        match self {
            Filter::And(and) => and.parse(session_state, schema),
            Filter::Or(or) => or.parse(session_state, schema),
            Filter::Between(between) => between.parse(session_state, schema),
            Filter::Gt(gt) => gt.parse(session_state, schema),
            Filter::Gteq(gteq) => gteq.parse(session_state, schema),
            Filter::IsNotNull(is_not_null) => is_not_null.parse(session_state, schema),
            Filter::IsNull(is_null) => is_null.parse(session_state, schema),
            Filter::Lt(lt) => lt.parse(session_state, schema),
            Filter::Lteq(lteq) => lteq.parse(session_state, schema),
            Filter::Eq(eq) => eq.parse(session_state, schema),
            Filter::GeoJson(geo_json_filter) => geo_json_filter.parse(session_state, schema),
        }
    }
}

pub(crate) fn try_coerce_number_to_schema(literal: f64, dtype: &DataType) -> Expr {
    let result = match dtype {
        DataType::Int8 => cast::i8(literal).map(|v| datafusion::logical_expr::lit(v)),
        DataType::Int16 => cast::i16(literal).map(|v| datafusion::logical_expr::lit(v)),
        DataType::Int32 => cast::i32(literal).map(|v| datafusion::logical_expr::lit(v)),
        DataType::Int64 => cast::i64(literal).map(|v| datafusion::logical_expr::lit(v)),
        DataType::UInt8 => cast::u8(literal).map(|v| datafusion::logical_expr::lit(v)),
        DataType::UInt16 => cast::u16(literal).map(|v| datafusion::logical_expr::lit(v)),
        DataType::UInt32 => cast::u32(literal).map(|v| datafusion::logical_expr::lit(v)),
        DataType::UInt64 => cast::u64(literal).map(|v| datafusion::logical_expr::lit(v)),
        DataType::Float32 => cast::f32(literal).map(|v| datafusion::logical_expr::lit(v)),
        _ => Ok(lit(literal)),
    };

    let optionally_coerced_expr = result.unwrap_or_else(|_| lit(literal));
    optionally_coerced_expr
}

pub(crate) fn get_column_type(
    schema: &datafusion::arrow::datatypes::Schema,
    column_name: &str,
) -> Option<DataType> {
    schema
        .field_with_name(column_name)
        .ok()
        .map(|field| field.data_type().clone())
}

pub(crate) fn parse_column_name(name: &str) -> Expr {
    col(Column::from_name(name))
}
