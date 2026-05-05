use std::sync::Arc;

use datafusion::scalar::ScalarValue;

use crate::{NdArrayD, datatypes::NdArrayDataType};

pub fn is_pushdown_candidate<'a>(array: &'a Arc<dyn NdArrayD>) -> bool {
    let dims = array.dimensions();
    let shape = array.shape();

    if dims.len() != 1 || shape[0] == 0 {
        return false;
    }

    if !is_pushdown_type(&array.datatype()) {
        return false;
    }

    true
}

pub fn is_pushdown_candidate_ragged(
    array: &Arc<dyn NdArrayD>,
    instance_dimension: &String,
) -> bool {
    let dims = array.dimensions();
    let shape = array.shape();

    if dims.len() != 1 || shape[0] == 0 {
        return false;
    }

    if !is_pushdown_type(&array.datatype()) {
        return false;
    }

    if &dims[0] == instance_dimension {
        return true;
    }

    false
}

pub fn is_pushdown_type(dt: &NdArrayDataType) -> bool {
    matches!(
        dt,
        NdArrayDataType::I8
            | NdArrayDataType::I16
            | NdArrayDataType::I32
            | NdArrayDataType::I64
            | NdArrayDataType::U8
            | NdArrayDataType::U16
            | NdArrayDataType::U32
            | NdArrayDataType::U64
            | NdArrayDataType::F32
            | NdArrayDataType::F64
            | NdArrayDataType::Timestamp
    )
}

/// A value range for a single column, using DataFusion `ScalarValue` natively.
/// Bounds are tightened as multiple predicates are encountered.
#[derive(Debug, Clone)]
pub struct ValueRange {
    pub min: Option<(ScalarValue, bool)>, // (value, inclusive)
    pub max: Option<(ScalarValue, bool)>,
}

impl ValueRange {
    pub fn empty() -> Self {
        Self {
            min: None,
            max: None,
        }
    }

    /// Tighten this range with a lower bound.
    pub fn with_lower(&mut self, value: ScalarValue, inclusive: bool) {
        match &self.min {
            None => self.min = Some((value, inclusive)),
            Some((existing, existing_inc)) => {
                if let Some(ord) = existing.partial_cmp(&value) {
                    use std::cmp::Ordering::*;
                    match ord {
                        Less => self.min = Some((value, inclusive)),
                        Equal => self.min = Some((value, *existing_inc && inclusive)),
                        Greater => {} // keep tighter existing
                    }
                }
            }
        }
    }

    /// Tighten this range with an upper bound.
    pub fn with_upper(&mut self, value: ScalarValue, inclusive: bool) {
        match &self.max {
            None => self.max = Some((value, inclusive)),
            Some((existing, existing_inc)) => {
                if let Some(ord) = existing.partial_cmp(&value) {
                    use std::cmp::Ordering::*;
                    match ord {
                        Greater => self.max = Some((value, inclusive)),
                        Equal => self.max = Some((value, *existing_inc && inclusive)),
                        Less => {} // keep tighter existing
                    }
                }
            }
        }
    }
}
