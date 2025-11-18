use std::collections::HashMap;

use beacon_arrow_zarr::array_slice_pushdown::ArraySlicePushDown;

#[derive(Debug, Clone)]
pub enum ArraySlicePushDownResult {
    Prune,
    PushDown(ArraySlicePushDown),
    Retain,
}

impl ArraySlicePushDownResult {
    pub fn optimize(pushdowns: Vec<ArraySlicePushDownResult>) -> Vec<ArraySlicePushDownResult> {
        let mut left_over = vec![];
        let mut dimension_pushdowns = HashMap::new();

        for pushdown in pushdowns {
            match pushdown {
                ArraySlicePushDownResult::Prune => {
                    left_over.push(ArraySlicePushDownResult::Prune);
                }
                ArraySlicePushDownResult::PushDown(slice_pushdown) => {
                    dimension_pushdowns
                        .entry(slice_pushdown.dimension.clone())
                        .and_modify(|existing_pushdown: &mut ArraySlicePushDown| {
                            // Coalesce the pushdown
                            existing_pushdown.start =
                                slice_pushdown.start.max(existing_pushdown.start);
                            existing_pushdown.end = slice_pushdown.end.min(existing_pushdown.end);
                        })
                        .or_insert(slice_pushdown);
                }
                ArraySlicePushDownResult::Retain => {
                    left_over.push(ArraySlicePushDownResult::Retain);
                }
            }
        }

        for (_, pushdown) in dimension_pushdowns {
            // Only keep pushdowns where start < end
            if pushdown.start < pushdown.end {
                left_over.push(ArraySlicePushDownResult::PushDown(pushdown));
            } else {
                left_over.push(ArraySlicePushDownResult::Prune);
            }
        }

        left_over
    }

    pub fn should_prune(pushdowns: &[ArraySlicePushDownResult]) -> bool {
        pushdowns
            .iter()
            .any(|p| matches!(p, ArraySlicePushDownResult::Prune))
    }

    pub fn get_slice_pushdowns(
        pushdowns: &[ArraySlicePushDownResult],
    ) -> HashMap<String, ArraySlicePushDown> {
        let mut result = HashMap::new();
        for pushdown in pushdowns {
            if let ArraySlicePushDownResult::PushDown(slice_pushdown) = pushdown {
                result.insert(slice_pushdown.dimension.clone(), slice_pushdown.clone());
            }
        }
        result
    }
}
