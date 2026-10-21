//! Chunk pruning of the morsel scan: the pushdown predicate runs on the 0-D and 1-D arrays it names.

use std::{collections::HashMap, ops::Range, sync::Arc};

use arrow::{
    array::{Array, ArrayRef, AsArray, BooleanArray},
    buffer::BooleanBuffer,
    compute::cast,
    datatypes::{Schema, SchemaRef},
    record_batch::RecordBatch,
};
use beacon_datafusion_ext::nd::optimizer::is_pushable_expr;
use datafusion::{
    physical_expr::{
        split_conjunction,
        utils::{collect_columns, reassign_expr_columns},
    },
    physical_plan::PhysicalExpr,
    scalar::ScalarValue,
};
use indexmap::IndexMap;

use crate::{
    NdArrayD, array::subset::ArraySubset, arrow::array::ndarray_to_arrow_array, dataset_v2::Dataset,
};

pub async fn prune_mask_dimensions(
    arrays: &IndexMap<String, Arc<dyn NdArrayD>>,
    predicate: Arc<dyn PhysicalExpr>,
) -> anyhow::Result<HashMap<String, BooleanArray>> {
    // Split predicate into conjunctions
    let predicates = split_conjunction(&predicate);

    // Load all the arrays referenced by the predicate that are 0/1 dimensional
    let mut dimension_grouped_arrays: HashMap<Option<String>, HashMap<String, ArrayRef>> =
        HashMap::new();
    let mut dimension_masks: HashMap<Option<String>, BooleanArray> = HashMap::new();
    for col in collect_columns(&predicate) {
        if let Some(array) = arrays.get(col.name()) {
            let array_dimensions = array.dimensions();
            if array_dimensions.len() <= 1 {
                let dimension = &array_dimensions.get(0);
                let arrow_arr = ndarray_to_arrow_array(array.as_ref()).await?;

                dimension_grouped_arrays
                    .entry(dimension.cloned())
                    .or_insert_with(HashMap::new)
                    .insert(col.name().to_string(), arrow_arr);
            }
        }
    }

    // At this point, `dimension_grouped_arrays` contains all 0-D and 1-D arrays grouped by their dimension.
    for (dimension_name, size) in &dataset.dimensions {
        if !dimension_masks.contains_key(dimension_name) {
            let bool_buffer = BooleanBuffer::new_set(*size);
            dimension_masks.insert(
                dimension_name.clone(),
                BooleanArray::new(bool_buffer.clone(), None),
            );
        }
    }

    // Start pruning and apply the resulting masks to the dimension masks

    todo!()
    // Return the computed dimension masks
    // Ok(dimension_masks)
}
