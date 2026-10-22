//! Chunk pruning of the morsel scan: the pushdown predicate runs on the 0-D and 1-D arrays it names.

use std::{
    collections::{HashMap, hash_map::Entry},
    sync::Arc,
};

use arrow::{
    array::{Array, ArrayRef, AsArray, BooleanArray},
    compute::{and, cast, prep_null_mask_filter},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use beacon_datafusion_ext::nd::optimizer::is_pushable_expr;
use datafusion::{
    logical_expr::Operator,
    physical_expr::{
        expressions::BinaryExpr,
        split_conjunction,
        utils::{collect_columns, reassign_expr_columns},
    },
    physical_plan::PhysicalExpr,
};
use indexmap::IndexMap;

use crate::{NdArrayD, array::subset::ArraySubset, arrow::array::ndarray_to_arrow_array};

/// The keep mask of each dimension. The `None` key holds the mask of the 0-D arrays.
#[derive(Debug, Default)]
pub struct DimensionMasks(HashMap<Option<String>, BooleanArray>);

impl DimensionMasks {
    /// Check if a subset can hold a wanted row.
    ///
    /// A false 0-D mask rejects each subset. A dimension without a mask keeps the subset.
    ///
    /// # Arguments
    ///
    /// * `dimensions` - The dimension of each axis of `subset`
    /// * `subset` - The start and shape of the subset on each axis
    pub fn keeps(&self, dimensions: &[String], subset: &ArraySubset) -> bool {
        if self.0.get(&None).is_some_and(|mask| mask.true_count() == 0) {
            return false;
        }
        dimensions
            .iter()
            .zip(subset.start.iter().zip(&subset.shape))
            .all(|(dimension, (&start, &len))| {
                self.0
                    .get(&Some(dimension.clone()))
                    .is_none_or(|mask| mask.slice(start, len).true_count() > 0)
            })
    }
}

/// Compute a keep mask for each dimension from the 0-D and 1-D arrays that the predicate names.
///
/// The `None` key holds the mask of the 0-D arrays, with length 1.
/// A dimension without a mask keeps all of its indices.
///
/// # Arguments
///
/// * `arrays` - The arrays of the dataset, by name
/// * `predicate` - The pushdown predicate
///
/// # Errors
///
/// Returns an error when an array does not load or a predicate does not evaluate.
pub async fn prune_mask_dimensions(
    arrays: &IndexMap<String, Arc<dyn NdArrayD>>,
    predicate: Arc<dyn PhysicalExpr>,
) -> anyhow::Result<DimensionMasks> {
    let schema = Schema::new(
        arrays
            .iter()
            .map(|(name, array)| Field::new(name, array.datatype().into(), true))
            .collect::<Vec<_>>(),
    );

    let mut masks = HashMap::new();
    for (name, array_predicate) in predicates_by_array(&predicate, &schema)? {
        let array = &arrays[&name];
        let dimensions = array.dimensions();
        if dimensions.len() > 1 {
            continue;
        }
        let values = ndarray_to_arrow_array(array.as_ref()).await?;
        let mask = evaluate_mask(&name, &array_predicate, values)?;
        match masks.entry(dimensions.first().cloned()) {
            Entry::Occupied(mut held) => {
                let held = held.get_mut();
                *held = and(held, &mask)?;
            }
            Entry::Vacant(slot) => {
                slot.insert(mask);
            }
        }
    }
    Ok(DimensionMasks(masks))
}

/// Collect the pushable conjuncts that name one array, joined with AND for each array.
///
/// Each predicate points its column at index 0, so it runs on a batch that holds only that array.
fn predicates_by_array(
    predicate: &Arc<dyn PhysicalExpr>,
    schema: &Schema,
) -> anyhow::Result<HashMap<String, Arc<dyn PhysicalExpr>>> {
    let mut predicate_by_array: HashMap<String, Arc<dyn PhysicalExpr>> = HashMap::new();
    for conjunct in split_conjunction(predicate) {
        if !is_pushable_expr(conjunct) {
            continue;
        }
        let columns = collect_columns(conjunct);
        let [column] = columns.iter().collect::<Vec<_>>()[..] else {
            continue;
        };
        let Ok(field) = schema.field_with_name(column.name()) else {
            continue;
        };

        let array_schema = Schema::new(vec![field.clone()]);
        let array_predicate = reassign_expr_columns(Arc::clone(conjunct), &array_schema)?;
        let array_predicate = match predicate_by_array.remove(column.name()) {
            Some(held) => Arc::new(BinaryExpr::new(held, Operator::And, array_predicate)),
            None => array_predicate,
        };
        predicate_by_array.insert(column.name().to_string(), array_predicate);
    }
    Ok(predicate_by_array)
}

/// Run a predicate from [`predicates_by_array`] on the values of its array.
///
/// A null result is false, as in a WHERE clause.
fn evaluate_mask(
    name: &str,
    predicate: &Arc<dyn PhysicalExpr>,
    values: ArrayRef,
) -> anyhow::Result<BooleanArray> {
    let rows = values.len();
    let schema = Arc::new(Schema::new(vec![Field::new(
        name,
        values.data_type().clone(),
        true,
    )]));
    let batch = RecordBatch::try_new(schema, vec![values])?;
    let result = predicate.evaluate(&batch)?.into_array(rows)?;
    anyhow::ensure!(
        result.len() == rows,
        "predicate on '{name}' gives {} values for {rows} rows",
        result.len()
    );
    let result = cast(&result, &DataType::Boolean)?;
    let result = result.as_boolean();
    // prep_null_mask_filter panics on an array without nulls
    Ok(if result.null_count() > 0 {
        prep_null_mask_filter(result)
    } else {
        result.clone()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::NdArray;
    use arrow::array::Int32Array;
    use datafusion::physical_expr::expressions::{binary, col, in_list, lit};

    fn schema() -> Schema {
        Schema::new(vec![
            Field::new("lat", DataType::Int32, true),
            Field::new("lon", DataType::Int32, true),
        ])
    }

    fn compare(name: &str, op: Operator, value: i32) -> Arc<dyn PhysicalExpr> {
        let schema = schema();
        binary(col(name, &schema).unwrap(), op, lit(value), &schema).unwrap()
    }

    fn all(conjuncts: Vec<Arc<dyn PhysicalExpr>>) -> Arc<dyn PhysicalExpr> {
        conjuncts
            .into_iter()
            .reduce(|left, right| Arc::new(BinaryExpr::new(left, Operator::And, right)))
            .unwrap()
    }

    fn nd(values: Vec<i32>, dimensions: &[&str]) -> Arc<dyn NdArrayD> {
        let shape = if dimensions.is_empty() {
            vec![]
        } else {
            vec![values.len()]
        };
        let dimensions = dimensions.iter().map(|d| d.to_string()).collect();
        Arc::new(NdArray::try_new_from_vec_in_mem(values, shape, dimensions, None).unwrap())
    }

    fn mask_of(
        predicate: &Arc<dyn PhysicalExpr>,
        name: &str,
        values: Vec<Option<i32>>,
    ) -> Vec<bool> {
        let predicates = predicates_by_array(predicate, &schema()).unwrap();
        let mask = evaluate_mask(name, &predicates[name], Arc::new(Int32Array::from(values)));
        mask.unwrap().iter().map(Option::unwrap).collect()
    }

    #[test]
    fn joins_conjuncts_of_one_array() {
        let predicate = all(vec![
            compare("lat", Operator::Gt, 10),
            compare("lat", Operator::Lt, 20),
        ]);
        let predicates = predicates_by_array(&predicate, &schema()).unwrap();
        assert_eq!(predicates.len(), 1);
        assert_eq!(
            mask_of(&predicate, "lat", vec![Some(5), Some(15), Some(25)]),
            vec![false, true, false]
        );
    }

    #[test]
    fn skips_conjunct_on_two_arrays() {
        let schema = schema();
        let predicate = binary(
            col("lat", &schema).unwrap(),
            Operator::Gt,
            col("lon", &schema).unwrap(),
            &schema,
        )
        .unwrap();
        assert!(predicates_by_array(&predicate, &schema).unwrap().is_empty());
    }

    #[test]
    fn skips_conjunct_that_is_not_pushable() {
        let schema = schema();
        let predicate =
            in_list(col("lat", &schema).unwrap(), vec![lit(1)], &false, &schema).unwrap();
        assert!(predicates_by_array(&predicate, &schema).unwrap().is_empty());
    }

    #[test]
    fn skips_column_that_is_not_an_array() {
        let predicate = compare("lat", Operator::Gt, 10);
        let schema = Schema::new(vec![Field::new("lon", DataType::Int32, true)]);
        assert!(predicates_by_array(&predicate, &schema).unwrap().is_empty());
    }

    #[test]
    fn null_result_drops_the_row() {
        let predicate = compare("lat", Operator::Gt, 10);
        assert_eq!(
            mask_of(&predicate, "lat", vec![Some(15), None]),
            vec![true, false]
        );
    }

    #[test]
    fn scalar_array_gives_mask_of_length_one() {
        let predicate = compare("lat", Operator::Gt, 10);
        assert_eq!(mask_of(&predicate, "lat", vec![Some(5)]), vec![false]);
    }

    fn grid_masks() -> DimensionMasks {
        DimensionMasks(HashMap::from([
            (
                Some("x".to_string()),
                BooleanArray::from(vec![false, false, true, true]),
            ),
            (Some("y".to_string()), BooleanArray::from(vec![true, false])),
        ]))
    }

    #[test]
    fn keeps_a_subset_with_a_wanted_index_on_each_axis() {
        let dimensions = ["x".to_string(), "y".to_string()];
        let keeps = |start: Vec<usize>, shape: Vec<usize>| {
            grid_masks().keeps(&dimensions, &ArraySubset::new(start, shape))
        };
        assert!(!keeps(vec![0, 0], vec![2, 2]));
        assert!(keeps(vec![2, 0], vec![2, 2]));
        assert!(!keeps(vec![2, 1], vec![2, 1]));
        assert!(keeps(vec![1, 0], vec![2, 1]));
    }

    #[test]
    fn keeps_a_subset_on_a_dimension_without_mask() {
        let subset = ArraySubset::new(vec![0], vec![2]);
        assert!(grid_masks().keeps(&["z".to_string()], &subset));
    }

    #[test]
    fn false_scalar_mask_rejects_each_subset() {
        let mut masks = grid_masks();
        masks.0.insert(None, BooleanArray::from(vec![false]));
        let subset = ArraySubset::new(vec![2], vec![2]);
        assert!(!masks.keeps(&["x".to_string()], &subset));
    }

    #[tokio::test]
    async fn masks_by_dimension() {
        let arrays: IndexMap<String, Arc<dyn NdArrayD>> = IndexMap::from([
            ("lat".to_string(), nd(vec![5, 15, 25, 35], &["obs"])),
            ("lon".to_string(), nd(vec![1, 2, 3, 4], &["obs"])),
            ("depth".to_string(), nd(vec![100], &[])),
            ("temp".to_string(), nd(vec![1, 2, 3, 4], &["x", "y"])),
        ]);
        let schema = Schema::new(
            ["lat", "lon", "depth", "temp"]
                .map(|name| Field::new(name, DataType::Int32, true))
                .to_vec(),
        );
        let compare = |name: &str, op: Operator, value: i32| {
            binary(col(name, &schema).unwrap(), op, lit(value), &schema).unwrap()
        };
        let predicate = all(vec![
            compare("lat", Operator::Gt, 10),
            compare("lon", Operator::Lt, 4),
            compare("depth", Operator::Lt, 50),
            compare("temp", Operator::Gt, 2),
        ]);

        let masks = prune_mask_dimensions(&arrays, predicate).await.unwrap();
        let values = |key: Option<&str>| -> Vec<bool> {
            masks.0[&key.map(String::from)]
                .iter()
                .map(Option::unwrap)
                .collect()
        };
        assert_eq!(masks.0.len(), 2);
        assert_eq!(values(Some("obs")), vec![false, true, true, false]);
        assert_eq!(values(None), vec![false]);
    }
}
