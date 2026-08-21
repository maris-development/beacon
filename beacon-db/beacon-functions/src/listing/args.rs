//! Reading the positional literals a table function is called with.
//!
//! Both listing functions take the same shapes, so the readers live once.

use datafusion::{prelude::Expr, scalar::ScalarValue};

/// A `Utf8` literal argument, or `None` when absent.
pub(super) fn string_arg(args: &[Expr], index: usize) -> Option<String> {
    match args.get(index) {
        Some(Expr::Literal(ScalarValue::Utf8(value), _)) => value.clone(),
        _ => None,
    }
}

/// A non-negative integer literal argument, or `None` when absent.
pub(super) fn usize_arg(args: &[Expr], index: usize) -> Option<usize> {
    match args.get(index) {
        Some(Expr::Literal(scalar, _)) => match scalar {
            ScalarValue::Int64(Some(v)) if *v >= 0 => Some(*v as usize),
            ScalarValue::UInt64(Some(v)) => Some(*v as usize),
            ScalarValue::Int32(Some(v)) if *v >= 0 => Some(*v as usize),
            ScalarValue::UInt32(Some(v)) => Some(*v as usize),
            _ => None,
        },
        _ => None,
    }
}

