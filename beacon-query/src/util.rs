use datafusion::{
    common::Column,
    prelude::{col, Expr},
};

pub(crate) fn parse_column_name(name: &str) -> Expr {
    col(Column::from_name(name))
}
