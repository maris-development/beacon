//! The chunk prune: the chunks of a file that no row of the predicate can come from.
//!
//! The prune evaluates each conjunct of the predicate on the values of the
//! rank-0 variables and the 1-D coordinates that it names. The DataFusion expression adapter rewrites
//! the conjunct from the table types to the file types, with the casts of the
//! scan. So a mask is false only where the filter above the scan rejects each
//! row.
//!
//! The prune only skips work. The filter above the scan applies the predicate
//! again, so a chunk that stays is always correct. When a mask is in doubt,
//! the prune builds no mask.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, BooleanArray, RecordBatch, RecordBatchOptions};
use arrow::compute::{and, prep_null_mask_filter};
use arrow::datatypes::{Field, Schema, SchemaRef};
use beacon_nd_dataset::{ChunkGrid, NdDataset, NdVariable, Region};
use datafusion::physical_expr::utils::collect_columns;
use datafusion::physical_expr::{PhysicalExpr, split_conjunction};
use datafusion::physical_expr_adapter::{
    DefaultPhysicalExprAdapterFactory, PhysicalExprAdapterFactory,
};
use datafusion::physical_expr_common::physical_expr::{is_volatile, snapshot_physical_expr};

/// What the predicate rejects in one file.
#[derive(Debug, Default)]
pub(crate) struct Masks {
    /// True when a conjunct rejects each row of the file.
    no_row: bool,
    /// The keep mask of each dimension that a conjunct bounds, with the name of the dimension.
    dimensions: Vec<(String, BooleanArray)>,
}

impl Masks {
    /// Add the keep mask of one conjunct.
    fn add(&mut self, mask: ConjunctMask) {
        match mask {
            ConjunctMask::File(keep) => self.no_row |= !keep,
            ConjunctMask::Dimension(name, mask) => {
                match self.dimensions.iter_mut().find(|(known, _)| *known == name) {
                    // A row must meet each conjunct. The two masks have the size of the dimension.
                    Some((_, known)) => {
                        if let Ok(both) = and(known, &mask) {
                            *known = both;
                        }
                    }
                    None => self.dimensions.push((name, mask)),
                }
            }
        }
    }
}

/// The keep mask of one conjunct.
enum ConjunctMask {
    /// The conjunct names no rank-1 variable. False when it rejects each row of the file.
    File(bool),
    /// The conjunct names coordinates of this dimension. One value for each index.
    Dimension(String, BooleanArray),
}

/// The 1-D coordinates that the prune read, by name.
type Coordinates = HashMap<String, ArrayRef>;

/// Evaluate the conjuncts of `predicate` on the 1-D coordinates of `dataset`.
///
/// `table_schema` holds the logical type of each file column of the scan.
/// A conjunct gives a mask when each of its columns is in `table_schema`, and
/// each column that the file has is a rank-0 variable, or a rank-1 variable
/// on one and the same dimension. A conjunct with no rank-1 variable has one
/// value for the file, so it can reject the whole file.
/// A column that the file does not have is null, as in the scan.
/// A conjunct that cannot be evaluated is left out, so the scan reads each
/// chunk that it bounds.
pub(crate) async fn chunk_masks(
    dataset: &dyn NdDataset,
    predicate: &Arc<dyn PhysicalExpr>,
    table_schema: &SchemaRef,
) -> Masks {
    // A dynamic filter is evaluated in its state of this moment.
    let predicate =
        snapshot_physical_expr(Arc::clone(predicate)).unwrap_or_else(|_| Arc::clone(predicate));
    let mut masks = Masks::default();
    let mut coordinates = Coordinates::new();
    for conjunct in split_conjunction(&predicate) {
        match conjunct_mask(dataset, conjunct, table_schema, &mut coordinates).await {
            Ok(mask) => masks.add(mask),
            Err(reason) => tracing::debug!("no prune mask for '{conjunct}': {reason}"),
        }
    }
    masks
}

/// True when no row of `chunk` can meet the predicate.
///
/// The chunk is pruned when a conjunct rejects each row of the file, or when
/// the mask of one of its axes has no `true` in the range of the chunk on that axis.
pub(crate) fn is_pruned(masks: &Masks, grid: &ChunkGrid, chunk: &Region) -> bool {
    masks.no_row
        || grid
            .dimensions()
            .iter()
            .enumerate()
            .any(|(axis, dimension)| {
                let Some((_, mask)) = masks
                    .dimensions
                    .iter()
                    .find(|(name, _)| name == dimension.name())
                else {
                    return false;
                };
                let (Some(&start), Some(&length)) = (chunk.start.get(axis), chunk.shape.get(axis))
                else {
                    return false;
                };
                // A chunk outside the mask is not pruned.
                match start.checked_add(length) {
                    Some(end) if end <= mask.len() => {
                        mask.values().slice(start, length).count_set_bits() == 0
                    }
                    _ => false,
                }
            })
}

/// The keep mask of `conjunct`, or the reason why it has none.
async fn conjunct_mask(
    dataset: &dyn NdDataset,
    conjunct: &Arc<dyn PhysicalExpr>,
    table_schema: &SchemaRef,
    coordinates: &mut Coordinates,
) -> Result<ConjunctMask, String> {
    // A volatile function gives another value in the filter above the scan.
    if is_volatile(conjunct) {
        return Err("it has a volatile function".to_string());
    }

    let mut dimension: Option<(String, usize)> = None;
    let mut fields = Vec::new();
    let mut arrays = Vec::new();
    for column in collect_columns(conjunct) {
        let name = column.name();
        // The type of a column that the scan does not project is not known here.
        let Ok(table_field) = table_schema.field_with_name(name) else {
            return Err(format!("'{name}' is not a file column of the scan"));
        };
        // The scan gives nulls for a column that the file does not have. The adapter does the same.
        let Some(variable) = dataset.variable(name) else {
            continue;
        };
        // The adapter casts a nested type by field name, and the scan does not.
        if table_field.data_type().is_nested() || variable.data_type().is_nested() {
            return Err(format!("'{name}' has a nested type"));
        }
        // A rank-0 variable has one value for each row of the file.
        let size = match variable.dimensions().as_slice() {
            [] => 1,
            [axis] => {
                match &dimension {
                    Some((known, _)) if known != axis.name() => {
                        return Err(format!(
                            "it has the two dimensions '{known}' and '{}'",
                            axis.name()
                        ));
                    }
                    Some(_) => {}
                    None => dimension = Some((axis.name().to_string(), axis.size())),
                }
                axis.size()
            }
            _ => return Err(format!("'{name}' has a rank above 1")),
        };
        if size == 0 {
            return Err(format!("'{name}' is empty"));
        }
        let values = read_coordinate(variable.as_ref(), coordinates).await?;
        if values.len() != size {
            return Err(format!(
                "'{name}' has {} values in place of {size}",
                values.len()
            ));
        }
        fields.push(Field::new(name, variable.data_type().clone(), true));
        arrays.push(values);
    }

    // A conjunct with no rank-1 column has one value for the whole file.
    let rows = dimension.as_ref().map_or(1, |(_, size)| *size);
    // Repeat each rank-0 value on each row of the dimension.
    for (values, field) in arrays.iter_mut().zip(&fields) {
        if values.len() != rows {
            *values = datafusion::scalar::ScalarValue::try_from_array(values, 0)
                .and_then(|value| value.to_array_of_size(rows))
                .map_err(|e| format!("'{}': {e}", field.name()))?;
        }
    }
    let file_schema = Arc::new(Schema::new(fields));
    let options = RecordBatchOptions::new().with_row_count(Some(rows));
    let batch = RecordBatch::try_new_with_options(Arc::clone(&file_schema), arrays, &options)
        .map_err(|e| e.to_string())?;

    // The adapter puts the cast of the scan around each column, and a null for a missing column.
    let adapted = DefaultPhysicalExprAdapterFactory
        .create(Arc::clone(table_schema), file_schema)
        .and_then(|adapter| adapter.rewrite(Arc::clone(conjunct)))
        .map_err(|e| e.to_string())?;
    let result = adapted
        .evaluate(&batch)
        .and_then(|value| value.into_array(rows))
        .map_err(|e| e.to_string())?;
    if result.len() != rows {
        return Err(format!("it gives {} values for {rows} rows", result.len()));
    }
    let mask = result
        .as_boolean_opt()
        .ok_or_else(|| format!("its type is {}, not Boolean", result.data_type()))?;
    // The filter rejects a row whose conjunct is null.
    let mask = match mask.nulls() {
        Some(_) => prep_null_mask_filter(mask),
        None => mask.clone(),
    };

    Ok(match dimension {
        Some((name, _)) => ConjunctMask::Dimension(name, mask),
        None => ConjunctMask::File(mask.value(0)),
    })
}

/// The values of the whole of `variable`. Each coordinate is read one time.
async fn read_coordinate(
    variable: &dyn NdVariable,
    coordinates: &mut Coordinates,
) -> Result<ArrayRef, String> {
    if let Some(values) = coordinates.get(variable.name()) {
        return Ok(Arc::clone(values));
    }
    let array = variable
        .read(&Region::full(variable.dimensions()))
        .await
        .map_err(|e| e.to_string())?;
    let values = Arc::clone(array.values());
    coordinates.insert(variable.name().to_string(), Arc::clone(&values));
    Ok(values)
}

#[cfg(test)]
mod tests {
    use arrow::array::{
        Float32Array, Float64Array, Int32Array, Int64Array, StringArray, StructArray,
        TimestampSecondArray, UInt64Array,
    };
    use arrow::datatypes::{DataType, TimeUnit};
    use beacon_nd_dataset::{Dimension, Dimensions, MemoryDataset, MemoryVariable, NdArrowArray};
    use datafusion::common::DFSchema;
    use datafusion::logical_expr::{Expr, cast, col, lit, not};
    use datafusion::prelude::{SessionContext, random};
    use datafusion::scalar::ScalarValue;

    use super::*;
    use crate::testing::{grid_dataset, leveled_dataset};

    /// A table schema with a nullable field for each of `fields`.
    fn schema_of(fields: &[(&str, DataType)]) -> SchemaRef {
        Arc::new(Schema::new(
            fields
                .iter()
                .map(|(name, data_type)| Field::new(*name, data_type.clone(), true))
                .collect::<Vec<_>>(),
        ))
    }

    /// The physical form of `expr` over `table_schema`, with the type coercion of a query.
    fn physical(expr: Expr, table_schema: &SchemaRef) -> Arc<dyn PhysicalExpr> {
        let schema = DFSchema::try_from(table_schema.as_ref().clone()).unwrap();
        SessionContext::new()
            .create_physical_expr(expr, &schema)
            .unwrap()
    }

    /// A dataset with one 1-D variable `name` on the dimension `name`.
    fn coordinate(name: &str, values: ArrayRef) -> MemoryDataset {
        let dimensions = Dimensions::try_new([Dimension::new(name, values.len())]).unwrap();
        let variable =
            MemoryVariable::new(name, NdArrowArray::try_new(values, dimensions).unwrap());
        MemoryDataset::try_new("test.nd", vec![variable]).unwrap()
    }

    /// The masks of `expr` on `dataset`, with the file type of each variable as its table type.
    async fn masks_of(dataset: &MemoryDataset, expr: Expr) -> Masks {
        let fields: Vec<Field> = dataset
            .variable_names()
            .iter()
            .filter_map(|name| dataset.variable(name))
            .map(|variable| Field::new(variable.name(), variable.data_type().clone(), true))
            .collect();
        let table_schema = Arc::new(Schema::new(fields));
        chunk_masks(dataset, &physical(expr, &table_schema), &table_schema).await
    }

    /// The masks of `expr` on `dataset`, with the table schema `table`.
    async fn masks_in(dataset: &MemoryDataset, expr: Expr, table: &[(&str, DataType)]) -> Masks {
        let table_schema = schema_of(table);
        chunk_masks(dataset, &physical(expr, &table_schema), &table_schema).await
    }

    /// The one mask of `masks`, as booleans.
    fn only_mask(masks: &Masks, dimension: &str) -> Vec<bool> {
        assert!(!masks.no_row, "{masks:?}");
        assert_eq!(masks.dimensions.len(), 1, "{masks:?}");
        assert_eq!(masks.dimensions[0].0, dimension);
        assert_eq!(masks.dimensions[0].1.null_count(), 0);
        masks.dimensions[0].1.iter().map(Option::unwrap).collect()
    }

    /// True when `masks` prunes nothing.
    fn is_empty(masks: &Masks) -> bool {
        !masks.no_row && masks.dimensions.is_empty()
    }

    /// The masks with one mask for `dimension`.
    fn masks_with(dimension: &str, mask: Vec<bool>) -> Masks {
        Masks {
            no_row: false,
            dimensions: vec![(dimension.to_string(), BooleanArray::from(mask))],
        }
    }

    /// The starts of the chunks of `grid_dataset` at batch size 12 that `masks` prunes.
    fn pruned_starts(masks: &Masks) -> Vec<Vec<usize>> {
        let grid = ChunkGrid::try_new(&grid_dataset("a.nd"), 12).unwrap();
        grid.chunks()
            .iter()
            .filter(|chunk| is_pruned(masks, &grid, chunk))
            .map(|chunk| chunk.start.clone())
            .collect()
    }

    fn seconds() -> DataType {
        DataType::Timestamp(TimeUnit::Second, None)
    }

    const F: bool = false;
    const T: bool = true;

    // `lat` of the grid fixture runs from 0.0 to 7.0, in 2 chunks of 4.

    #[tokio::test]
    async fn a_filter_no_row_meets_prunes_every_chunk() {
        let dataset = grid_dataset("a.nd");
        let masks = masks_of(&dataset, col("lat").gt(lit(100.0))).await;
        assert_eq!(pruned_starts(&masks).len(), 4);
    }

    #[tokio::test]
    async fn a_filter_every_row_meets_prunes_nothing() {
        let dataset = grid_dataset("a.nd");
        let masks = masks_of(&dataset, col("lat").gt(lit(-1.0))).await;
        assert!(pruned_starts(&masks).is_empty());
    }

    #[tokio::test]
    async fn a_filter_on_the_upper_half_keeps_half_the_chunks() {
        let dataset = grid_dataset("a.nd");
        let masks = masks_of(&dataset, col("lat").gt(lit(3.5))).await;
        assert_eq!(pruned_starts(&masks), vec![vec![0, 0, 0], vec![0, 0, 3]]);
    }

    #[tokio::test]
    async fn a_bound_is_inclusive_or_exclusive_as_the_operator_says() {
        let dataset = grid_dataset("a.nd");
        let masks = masks_of(&dataset, col("lat").gt_eq(lit(4.0))).await;
        assert_eq!(only_mask(&masks, "lat"), [F, F, F, F, T, T, T, T]);
        let masks = masks_of(&dataset, col("lat").gt(lit(4.0))).await;
        assert_eq!(only_mask(&masks, "lat"), [F, F, F, F, F, T, T, T]);
    }

    #[tokio::test]
    async fn two_conjuncts_on_one_dimension_meet() {
        let dataset = grid_dataset("a.nd");
        let expr = col("lat").gt(lit(1.0)).and(col("lat").lt_eq(lit(5.0)));
        let masks = masks_of(&dataset, expr).await;
        assert_eq!(only_mask(&masks, "lat"), [F, F, T, T, T, T, F, F]);
    }

    #[tokio::test]
    async fn conjuncts_on_two_dimensions_give_two_masks() {
        let dataset = grid_dataset("a.nd");
        let expr = col("lat").gt(lit(3.5)).and(col("lon").gt(lit(-1.0)));
        let masks = masks_of(&dataset, expr).await;
        assert_eq!(masks.dimensions.len(), 2, "{masks:?}");
        assert_eq!(pruned_starts(&masks), vec![vec![0, 0, 0], vec![0, 0, 3]]);
    }

    #[tokio::test]
    async fn an_or_on_one_dimension_keeps_the_rows_of_each_side() {
        let dataset = grid_dataset("a.nd");
        let expr = col("lat").lt(lit(1.0)).or(col("lat").gt(lit(6.0)));
        let masks = masks_of(&dataset, expr).await;
        assert_eq!(only_mask(&masks, "lat"), [T, F, F, F, F, F, F, T]);
    }

    #[tokio::test]
    async fn an_or_on_two_dimensions_gives_no_mask() {
        let dataset = grid_dataset("a.nd");
        let expr = col("lat").gt(lit(100.0)).or(col("lon").gt(lit(100.0)));
        assert!(is_empty(&masks_of(&dataset, expr).await));
    }

    #[tokio::test]
    async fn an_in_list_and_a_not_are_evaluated() {
        let dataset = grid_dataset("a.nd");
        let expr = col("lat").in_list(vec![lit(1.0), lit(6.0)], false);
        let masks = masks_of(&dataset, expr).await;
        assert_eq!(only_mask(&masks, "lat"), [F, T, F, F, F, F, T, F]);
        let masks = masks_of(&dataset, not(col("lat").gt(lit(1.0)))).await;
        assert_eq!(only_mask(&masks, "lat"), [T, T, F, F, F, F, F, F]);
    }

    #[tokio::test]
    async fn a_string_coordinate_gets_a_mask() {
        let dataset = coordinate("s", Arc::new(StringArray::from(vec!["a", "b", "c"])));
        let masks = masks_of(&dataset, col("s").gt(lit("a"))).await;
        assert_eq!(only_mask(&masks, "s"), [F, T, T]);
    }

    #[tokio::test]
    async fn a_literal_of_another_type_does_not_prune_a_matching_row() {
        let dataset = coordinate("n", Arc::new(Int32Array::from(vec![0, 1, 2, 3])));
        let masks = masks_of(&dataset, col("n").gt(lit(0.5))).await;
        assert_eq!(only_mask(&masks, "n"), [F, T, T, T]);
    }

    #[tokio::test]
    async fn a_null_coordinate_value_is_not_kept() {
        let values = Float64Array::from(vec![Some(1.0), None, Some(9.0)]);
        let dataset = coordinate("x", Arc::new(values));
        let masks = masks_of(&dataset, col("x").gt(lit(0.0))).await;
        assert_eq!(only_mask(&masks, "x"), [T, F, T]);
    }

    #[tokio::test]
    async fn a_null_coordinate_value_is_kept_by_is_null() {
        let values = Float64Array::from(vec![Some(1.0), None, Some(9.0)]);
        let dataset = coordinate("x", Arc::new(values));
        let masks = masks_of(&dataset, col("x").is_null()).await;
        assert_eq!(only_mask(&masks, "x"), [F, T, F]);
    }

    #[tokio::test]
    async fn a_nan_follows_the_total_order() {
        let dataset = coordinate("x", Arc::new(Float64Array::from(vec![1.0, f64::NAN])));
        let masks = masks_of(&dataset, col("x").gt(lit(5.0))).await;
        assert_eq!(only_mask(&masks, "x"), [F, T]);
    }

    #[tokio::test]
    async fn a_rank_2_variable_gets_no_mask_and_no_read() {
        let dataset = grid_dataset("a.nd");
        let masks = masks_of(&dataset, col("temp").gt(lit(5.0))).await;
        assert!(is_empty(&masks));
        assert_eq!(dataset.reads("temp"), 0);
    }

    #[tokio::test]
    async fn a_coordinate_in_two_conjuncts_is_read_one_time() {
        let dataset = grid_dataset("a.nd");
        let expr = col("lat").gt(lit(1.0)).and(col("lat").lt_eq(lit(5.0)));
        masks_of(&dataset, expr).await;
        assert_eq!(dataset.reads("lat"), 1);
    }

    #[tokio::test]
    async fn a_column_that_is_not_a_file_column_of_the_scan_gets_no_mask() {
        let dataset = coordinate("x", Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0])));
        let table_schema = schema_of(&[("x", DataType::Float64)]);
        let predicate = physical(col("x").gt(lit(1.5)), &table_schema);
        // The scan schema holds only `y`.
        let scan_schema = schema_of(&[("y", DataType::Float64)]);
        let masks = chunk_masks(&dataset, &predicate, &scan_schema).await;
        assert!(is_empty(&masks));
        assert_eq!(dataset.reads("x"), 0);
    }

    #[tokio::test]
    async fn a_column_the_file_does_not_have_is_null_and_prunes_the_file() {
        let dataset = grid_dataset("a.nd");
        let table = [("lat", DataType::Float64), ("depth", DataType::Float64)];
        let masks = masks_in(&dataset, col("depth").gt(lit(5.0)), &table).await;
        assert!(masks.no_row);
        assert_eq!(pruned_starts(&masks).len(), 4);
        assert_eq!(dataset.reads("lat") + dataset.reads("temp"), 0);
    }

    #[tokio::test]
    async fn a_column_the_file_does_not_have_meets_is_null() {
        let dataset = grid_dataset("a.nd");
        let table = [("lat", DataType::Float64), ("depth", DataType::Float64)];
        let masks = masks_in(&dataset, col("depth").is_null(), &table).await;
        assert!(pruned_starts(&masks).is_empty());
        // The missing column is null beside a coordinate too.
        let expr = col("depth").gt(lit(5.0)).or(col("lat").gt(lit(5.5)));
        let masks = masks_in(&dataset, expr, &table).await;
        assert_eq!(only_mask(&masks, "lat"), [F, F, F, F, F, F, T, T]);
    }

    /// The grid fixture with the rank-0 Float64 variable `level`.
    fn grid_with_level(level: Option<f64>) -> MemoryDataset {
        leveled_dataset("a.nd", level)
    }

    #[tokio::test]
    async fn a_rank_0_variable_that_fails_the_filter_prunes_the_file() {
        let dataset = grid_with_level(Some(2.0));
        let masks = masks_of(&dataset, col("level").eq(lit(5.0))).await;
        assert!(masks.no_row);
        assert_eq!(pruned_starts(&masks).len(), 4);
        assert_eq!(dataset.reads("temp") + dataset.reads("lat"), 0);
    }

    #[tokio::test]
    async fn a_rank_0_variable_that_meets_the_filter_prunes_nothing() {
        let dataset = grid_with_level(Some(2.0));
        let masks = masks_of(&dataset, col("level").eq(lit(2.0))).await;
        assert!(is_empty(&masks));
    }

    #[tokio::test]
    async fn a_null_rank_0_variable_prunes_the_file_but_meets_is_null() {
        let dataset = grid_with_level(None);
        let masks = masks_of(&dataset, col("level").eq(lit(2.0))).await;
        assert!(masks.no_row);
        let masks = masks_of(&dataset, col("level").is_null()).await;
        assert!(is_empty(&masks));
    }

    #[tokio::test]
    async fn a_rank_0_variable_beside_a_coordinate_is_repeated_on_the_dimension() {
        let dataset = grid_with_level(Some(2.0));
        // The level side is false, so only the lat side keeps rows.
        let expr = col("level").eq(lit(5.0)).or(col("lat").gt(lit(5.5)));
        let masks = masks_of(&dataset, expr).await;
        assert_eq!(only_mask(&masks, "lat"), [F, F, F, F, F, F, T, T]);
        // The level side is true, so each row stays.
        let expr = col("level").eq(lit(2.0)).or(col("lat").gt(lit(5.5)));
        let masks = masks_of(&dataset, expr).await;
        assert_eq!(only_mask(&masks, "lat"), [T; 8]);
    }

    #[tokio::test]
    async fn a_rank_0_variable_compares_in_the_table_type() {
        let value: ArrayRef = Arc::new(Int32Array::from(vec![2]));
        let level = MemoryVariable::new("level", NdArrowArray::try_new_scalar(value).unwrap());
        let dataset = MemoryDataset::try_new("a.nd", vec![level]).unwrap();
        let table = [("level", DataType::Float64)];
        let masks = masks_in(&dataset, col("level").gt(lit(2.5)), &table).await;
        assert!(masks.no_row);
        let masks = masks_in(&dataset, col("level").gt(lit(1.5)), &table).await;
        assert!(is_empty(&masks));
    }

    #[tokio::test]
    async fn a_volatile_conjunct_gets_no_mask() {
        let dataset = grid_dataset("a.nd");
        let masks = masks_of(&dataset, col("lat").gt(random() + lit(100.0))).await;
        assert!(is_empty(&masks));
        assert_eq!(dataset.reads("lat"), 0);
    }

    #[tokio::test]
    async fn a_coordinate_with_a_nested_type_gets_no_mask() {
        let field = Arc::new(Field::new("b", DataType::Int32, true));
        let child: ArrayRef = Arc::new(Int32Array::from(vec![1, 2]));
        let values = StructArray::from(vec![(field, child)]);
        let dataset = coordinate("s", Arc::new(values));
        let masks = masks_of(&dataset, col("s").is_null()).await;
        assert!(is_empty(&masks));
        assert_eq!(dataset.reads("s"), 0);
    }

    // The casts. Each mask must keep every row that the filter above the scan accepts.

    #[tokio::test]
    async fn a_cast_that_truncates_keeps_the_rows_it_maps_to_the_value() {
        let dataset = coordinate("x", Arc::new(Float64Array::from(vec![5.0, 5.5, 6.0])));
        let expr = cast(col("x"), DataType::Int32).eq(lit(5));
        let masks = masks_of(&dataset, expr).await;
        // The cast gives 5 for 5.5, so the filter accepts that row.
        assert_eq!(only_mask(&masks, "x"), [T, T, F]);
    }

    #[tokio::test]
    async fn an_int64_compare_is_exact_above_two_to_the_53() {
        let big = 1i64 << 53;
        let dataset = coordinate("n", Arc::new(Int64Array::from(vec![big - 1, big, big + 1])));
        let masks = masks_of(&dataset, col("n").gt(lit(big))).await;
        assert_eq!(only_mask(&masks, "n"), [F, F, T]);
    }

    #[tokio::test]
    async fn a_uint64_coordinate_above_two_to_the_63_keeps_each_matching_row() {
        let big = 1u64 << 63;
        let values = UInt64Array::from(vec![0, big + 10, u64::MAX]);
        let dataset = coordinate("n", Arc::new(values));
        let masks = masks_of(&dataset, col("n").gt(lit(big))).await;
        assert_eq!(only_mask(&masks, "n"), [F, T, T]);
    }

    #[tokio::test]
    async fn a_table_type_wider_than_the_file_type_compares_in_the_table_type() {
        let dataset = coordinate("x", Arc::new(Float32Array::from(vec![1.0, 2.0, 3.0])));
        let table = [("x", DataType::Float64)];
        let masks = masks_in(&dataset, col("x").gt(lit(1.5)), &table).await;
        assert_eq!(only_mask(&masks, "x"), [F, T, T]);
    }

    #[tokio::test]
    async fn a_table_type_narrower_than_the_file_type_compares_the_cast_values() {
        // In Float32, 16777217 is 16777216, so the filter rejects that row.
        let values = Float64Array::from(vec![16_777_216.0, 16_777_217.0, 16_777_218.0]);
        let dataset = coordinate("x", Arc::new(values));
        let table = [("x", DataType::Float32)];
        let masks = masks_in(&dataset, col("x").gt(lit(16_777_216.0_f32)), &table).await;
        assert_eq!(only_mask(&masks, "x"), [F, F, T]);
    }

    #[tokio::test]
    async fn a_timestamp_compares_in_the_unit_of_the_table() {
        let dataset = coordinate("t", Arc::new(TimestampSecondArray::from(vec![0, 1, 2, 3])));
        let table = [("t", DataType::Timestamp(TimeUnit::Millisecond, None))];
        let bound = lit(ScalarValue::TimestampMillisecond(Some(1500), None));
        let masks = masks_in(&dataset, col("t").gt(bound), &table).await;
        assert_eq!(only_mask(&masks, "t"), [F, F, T, T]);
    }

    #[tokio::test]
    async fn a_timestamp_literal_of_a_finer_unit_keeps_each_matching_row() {
        let dataset = coordinate("t", Arc::new(TimestampSecondArray::from(vec![0, 1, 2, 3])));
        let bound = lit(ScalarValue::TimestampMillisecond(Some(1500), None));
        let masks = masks_in(&dataset, col("t").gt(bound), &[("t", seconds())]).await;
        assert_eq!(only_mask(&masks, "t"), [F, F, T, T]);
    }

    #[tokio::test]
    async fn a_cast_that_fails_gives_no_mask() {
        // The year 9999 in seconds does not fit in nanoseconds.
        let values = TimestampSecondArray::from(vec![0, 253_402_300_799]);
        let dataset = coordinate("t", Arc::new(values));
        let table = [("t", DataType::Timestamp(TimeUnit::Nanosecond, None))];
        let bound = lit(ScalarValue::TimestampNanosecond(Some(1), None));
        let masks = masks_in(&dataset, col("t").gt(bound), &table).await;
        assert!(is_empty(&masks));
    }

    // The chunk check.

    #[test]
    fn a_chunk_with_no_mask_on_its_axes_is_not_pruned() {
        let masks = masks_with("depth", vec![F; 4]);
        assert!(pruned_starts(&masks).is_empty());
    }

    #[test]
    fn a_chunk_at_the_edge_is_checked_and_a_chunk_past_the_mask_is_kept() {
        let grid = ChunkGrid::try_new(&grid_dataset("a.nd"), 12).unwrap();
        let masks = masks_with("lat", vec![T, T, T, T, F, F, F, F]);
        // The chunk ends at lat 8, the edge of the dimension.
        assert!(is_pruned(
            &masks,
            &grid,
            &Region::new(vec![0, 4, 0], vec![1, 4, 3])
        ));
        assert!(!is_pruned(
            &masks,
            &grid,
            &Region::new(vec![0, 6, 0], vec![1, 4, 3])
        ));
        assert!(!is_pruned(
            &masks,
            &grid,
            &Region::new(vec![0, 9, 0], vec![1, 1, 3])
        ));
        assert!(!is_pruned(
            &masks,
            &grid,
            &Region::new(vec![0, usize::MAX, 0], vec![1, 2, 3])
        ));
    }
}
