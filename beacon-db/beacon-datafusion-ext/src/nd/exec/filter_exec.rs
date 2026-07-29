//! Filter evaluated *before* broadcast.
//!
//! A `WHERE` predicate is element-wise: whether grid cell `(t, y, x)` is kept
//! depends only on its input columns at `(t, y, x)`. So instead of broadcasting
//! every column onto the full grid and then filtering (a plain `FilterExec`
//! above [`NdBroadcastExec`]), each conjunct is evaluated on the *minimal*
//! sub-grid its inputs span — its footprint — and the resulting boolean mask is
//! lifted to the target grid. The conjunct masks are combined (null → excluded)
//! into the set of retained target cells, which rides the nd batch as a
//! [`selection`](NdRecordBatch::selection).
//!
//! No column data moves here: the filter attaches an index array.
//! [`NdBroadcastExec`] then fuses the broadcast with the selection into one
//! gather per column, so the filtered-out cross-product is never materialized —
//! and every operator above the broadcast sees only the surviving rows.

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use arrow::array::{Array, BooleanArray, UInt64Array};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::metrics::{
    BaselineMetrics, Count, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet,
};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use futures::StreamExt;

use crate::nd::batch::NdRecordBatch;

use super::expr_column::{NdExprColumn, ProjectMetrics};
use super::{NdBroadcastExec, NdExecutionPlan, SendableNdBatchStream, as_nd_plan};

/// Per-partition counters recorded while filtering.
struct FilterMetrics {
    /// Target cells seen before filtering (∑ grid sizes, honoring any inbound
    /// selection).
    input_rows: Count,
    /// Cells removed by the predicate.
    rows_pruned: Count,
    /// Footprint evaluation work of the conjuncts (shared with projection).
    project: ProjectMetrics,
}

/// Applies a conjunction of element-wise predicates over un-broadcast nd
/// batches, recording the result as a grid selection instead of dropping
/// columns. Requires an nd-aware child and is itself nd-aware, so it slots
/// between [`NdSourceExec`](super::NdSourceExec) and [`NdBroadcastExec`].
#[derive(Debug, Clone)]
pub struct NdFilterExec {
    /// nd-aware child producing the input nd batches.
    input: Arc<dyn ExecutionPlan>,
    /// Predicate conjuncts, ANDed together (each must be boolean).
    predicates: Vec<Arc<dyn PhysicalExpr>>,
    /// Per-conjunct evaluation plan (derived from `predicates`).
    columns: Vec<NdExprColumn>,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl NdFilterExec {
    /// Build a filter over an nd-aware `input`. `predicates` are the conjuncts to
    /// apply (ANDed); each must be evaluable against `input`'s schema and yield a
    /// boolean. The output schema equals the input schema — a filter selects
    /// rows, it does not change columns.
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        predicates: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Self> {
        if as_nd_plan(&input).is_none() {
            return Err(DataFusionError::Plan(format!(
                "NdFilterExec requires an nd-aware input, got {}",
                input.name()
            )));
        }
        if predicates.is_empty() {
            return Err(DataFusionError::Plan(
                "NdFilterExec requires at least one predicate".to_string(),
            ));
        }
        let input_schema = input.schema();
        let columns = predicates
            .iter()
            .map(|expr| NdExprColumn::build(&input_schema, expr))
            .collect::<Result<Vec<_>>>()?;

        // A filter preserves its input's columns and ordering; only row count
        // and statistics change, so reuse the child's plan properties.
        let properties = input.properties().clone();
        Ok(Self {
            input,
            predicates,
            columns,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    pub fn predicates(&self) -> &[Arc<dyn PhysicalExpr>] {
        &self.predicates
    }

    /// Filter one nd batch: compute the retained target cells and attach them as
    /// the batch's selection (intersected with any inbound selection).
    fn filter_batch(&self, batch: &NdRecordBatch, metrics: &FilterMetrics) -> Result<NdRecordBatch> {
        metrics.input_rows.add(batch.num_rows());
        let retained = retained_indices(&self.columns, batch, &metrics.project)?;
        metrics
            .rows_pruned
            .add(batch.num_rows().saturating_sub(retained.len()));
        batch.clone().with_selection(Some(retained))
    }
}

/// Evaluate the conjuncts over one nd batch and return the retained target-cell
/// indices (row-major). Each conjunct is evaluated on its footprint, its boolean
/// mask is broadcast onto the target grid, and the masks are ANDed — a null
/// predicate value excludes the cell. The result is intersected with any
/// selection a child already accumulated, so it is always a subset of the
/// batch's current rows.
fn retained_indices(
    columns: &[NdExprColumn],
    batch: &NdRecordBatch,
    metrics: &ProjectMetrics,
) -> Result<UInt64Array> {
    let target = batch.target();
    let n = target.num_elements();

    // `keep[i]` starts true and is ANDed with each conjunct's mask over the full
    // target grid, so all conjuncts combine in one coordinate system.
    let mut keep = vec![true; n];
    for column in columns {
        let masked = column.project(batch, target, metrics)?;
        let broadcast = masked.materialize(target)?;
        let mask = broadcast
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| {
                DataFusionError::Plan(
                    "NdFilterExec predicate did not evaluate to a boolean".to_string(),
                )
            })?;
        for (i, slot) in keep.iter_mut().enumerate() {
            *slot &= mask.is_valid(i) && mask.value(i);
        }
    }

    Ok(match batch.selection() {
        Some(sel) => sel
            .values()
            .iter()
            .copied()
            .filter(|&t| keep[t as usize])
            .collect(),
        None => (0..n as u64).filter(|&t| keep[t as usize]).collect(),
    })
}

impl DisplayAs for NdFilterExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let preds: Vec<String> = self.predicates.iter().map(|p| p.to_string()).collect();
        write!(f, "NdFilterExec: predicate=[{}]", preds.join(" AND "))
    }
}

impl ExecutionPlan for NdFilterExec {
    fn name(&self) -> &str {
        "NdFilterExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let [input] = <[_; 1]>::try_from(children).map_err(|_| {
            DataFusionError::Internal("NdFilterExec expects exactly one child".to_string())
        })?;
        Ok(Arc::new(Self::try_new(input, self.predicates.clone())?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // Like the other nd operators, the real output is the un-broadcast nd
        // stream from `execute_nd`; a standalone execution wraps this node in an
        // `NdBroadcastExec` to materialize. In a real plan an `NdBroadcastExec`
        // sits above and pulls `execute_nd` directly, so this path is unused.
        NdBroadcastExec::try_new(Arc::new(self.clone()))?.execute(partition, context)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

impl NdExecutionPlan for NdFilterExec {
    fn execute_nd(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableNdBatchStream> {
        let baseline = BaselineMetrics::new(&self.metrics, partition);
        let filter_metrics = FilterMetrics {
            input_rows: MetricBuilder::new(&self.metrics).counter("input_rows", partition),
            rows_pruned: MetricBuilder::new(&self.metrics).counter("rows_pruned", partition),
            project: ProjectMetrics {
                elements_evaluated: MetricBuilder::new(&self.metrics)
                    .counter("elements_evaluated", partition),
                elements_saved: MetricBuilder::new(&self.metrics)
                    .counter("elements_saved", partition),
                broadcasts: MetricBuilder::new(&self.metrics)
                    .counter("implicit_broadcasts", partition),
            },
        };
        let this = self.clone();
        let stream = as_nd_plan(&self.input)
            .expect("validated in try_new")
            .execute_nd(partition, context)?
            .map(move |item| {
                let _timer = baseline.elapsed_compute().timer();
                let batch = item?;
                let filtered = this.filter_batch(&batch, &filter_metrics)?;
                baseline.record_output(filtered.num_rows());
                Ok(filtered)
            });
        Ok(Box::pin(stream))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{AsArray, Int32Array};
    use arrow::datatypes::{DataType, Field, Int32Type, Schema, SchemaRef};
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::expressions::{binary, col, lit};

    use crate::nd::array::NdArrowArray;
    use crate::nd::dimensions::{Dimension, Dimensions};

    use super::*;

    fn dims(spec: &[(&str, usize)]) -> Dimensions {
        Dimensions::try_new(
            spec.iter()
                .map(|(name, size)| Dimension::new(*name, *size))
                .collect(),
        )
        .unwrap()
    }

    /// Grid (lat=3, lon=2): a `lat` coord, a `lon` coord, and a full-rank
    /// `temp{lat,lon}` data variable.
    fn test_batch() -> (SchemaRef, NdRecordBatch) {
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("lat", DataType::Int32, true),
            Field::new("lon", DataType::Int32, true),
            Field::new("temp", DataType::Int32, true),
        ]));
        let lat = NdArrowArray::try_new(
            Arc::new(Int32Array::from(vec![10, 20, 30])),
            dims(&[("lat", 3)]),
        )
        .unwrap();
        let lon =
            NdArrowArray::try_new(Arc::new(Int32Array::from(vec![1, 2])), dims(&[("lon", 2)]))
                .unwrap();
        let temp = NdArrowArray::try_new(
            Arc::new(Int32Array::from(vec![0, 1, 2, 3, 4, 5])),
            dims(&[("lat", 3), ("lon", 2)]),
        )
        .unwrap();
        let batch = NdRecordBatch::try_new(
            schema.clone(),
            vec![lat, lon, temp],
            dims(&[("lat", 3), ("lon", 2)]),
        )
        .unwrap();
        (schema, batch)
    }

    fn no_metrics() -> ProjectMetrics {
        ProjectMetrics {
            elements_evaluated: Count::new(),
            elements_saved: Count::new(),
            broadcasts: Count::new(),
        }
    }

    /// Build the per-conjunct evaluation plan and compute the retained cells.
    fn select(schema: &SchemaRef, batch: &NdRecordBatch, preds: Vec<Arc<dyn PhysicalExpr>>) -> Vec<u64> {
        let columns = preds
            .iter()
            .map(|expr| NdExprColumn::build(schema, expr))
            .collect::<Result<Vec<_>>>()
            .unwrap();
        retained_indices(&columns, batch, &no_metrics())
            .unwrap()
            .values()
            .to_vec()
    }

    /// A single-axis predicate (`lat > 15`) selects whole lat-slices: cells
    /// where lat ∈ {20, 30}, i.e. target rows 2,3,4,5 of the C-order grid.
    #[test]
    fn single_axis_predicate_selects_slices() {
        let (schema, batch) = test_batch();
        let pred = binary(col("lat", &schema).unwrap(), Operator::Gt, lit(15i32), &schema).unwrap();
        assert_eq!(select(&schema, &batch, vec![pred]), vec![2, 3, 4, 5]);

        // Materializing the selected batch gathers exactly those cells.
        let out = batch
            .with_selection(Some(UInt64Array::from(vec![2u64, 3, 4, 5])))
            .unwrap()
            .materialize()
            .unwrap();
        assert_eq!(
            out.column(0).as_primitive::<Int32Type>().values(),
            &[20, 20, 30, 30]
        );
        assert_eq!(
            out.column(2).as_primitive::<Int32Type>().values(),
            &[2, 3, 4, 5]
        );
    }

    /// A cross-axis predicate (`lat + lon > 22`) selects an arbitrary,
    /// non-factorizable subset of the grid — handled the same way.
    #[test]
    fn cross_axis_predicate_selects_arbitrary_cells() {
        let (schema, batch) = test_batch();
        // Grid cells (lat,lon): (10,1)=11,(10,2)=12,(20,1)=21,(20,2)=22,
        // (30,1)=31,(30,2)=32. `>22` keeps cells 4,5 (lat=30).
        let pred = binary(
            binary(
                col("lat", &schema).unwrap(),
                Operator::Plus,
                col("lon", &schema).unwrap(),
                &schema,
            )
            .unwrap(),
            Operator::Gt,
            lit(22i32),
            &schema,
        )
        .unwrap();
        assert_eq!(select(&schema, &batch, vec![pred]), vec![4, 5]);
    }

    /// Two conjuncts intersect: `lat > 15 AND lon = 2` keeps cells 3 and 5.
    #[test]
    fn conjuncts_intersect() {
        let (schema, batch) = test_batch();
        let p1 = binary(col("lat", &schema).unwrap(), Operator::Gt, lit(15i32), &schema).unwrap();
        let p2 = binary(col("lon", &schema).unwrap(), Operator::Eq, lit(2i32), &schema).unwrap();
        assert_eq!(select(&schema, &batch, vec![p1, p2]), vec![3, 5]);
    }

    /// A filter over an already-selected batch intersects with the inbound
    /// selection rather than replacing it.
    #[test]
    fn intersects_inbound_selection() {
        let (schema, batch) = test_batch();
        let batch = batch
            .with_selection(Some(UInt64Array::from(vec![0u64, 2, 4])))
            .unwrap();
        // lat > 15 keeps target rows 2,3,4,5; intersect with {0,2,4} → {2,4}.
        let pred = binary(col("lat", &schema).unwrap(), Operator::Gt, lit(15i32), &schema).unwrap();
        assert_eq!(select(&schema, &batch, vec![pred]), vec![2, 4]);
    }

    /// Two conjuncts on the *same* axis form a range: `lat > 10 AND lat < 30`
    /// keeps lat = 20, i.e. cells 2 and 3.
    #[test]
    fn same_axis_conjuncts_form_a_range() {
        let (schema, batch) = test_batch();
        let lo = binary(col("lat", &schema).unwrap(), Operator::Gt, lit(10i32), &schema).unwrap();
        let hi = binary(col("lat", &schema).unwrap(), Operator::Lt, lit(30i32), &schema).unwrap();
        assert_eq!(select(&schema, &batch, vec![lo, hi]), vec![2, 3]);
    }

    /// A predicate on the *inner* axis only (`lon = 2`) tiles across lat: cells
    /// 1, 3, 5.
    #[test]
    fn inner_axis_predicate_tiles() {
        let (schema, batch) = test_batch();
        let pred = binary(col("lon", &schema).unwrap(), Operator::Eq, lit(2i32), &schema).unwrap();
        assert_eq!(select(&schema, &batch, vec![pred]), vec![1, 3, 5]);
    }

    /// A predicate on a full-rank data variable (`temp >= 3`) selects an
    /// arbitrary subset directly on the grid: cells 3, 4, 5.
    #[test]
    fn data_variable_predicate_selects_cells() {
        let (schema, batch) = test_batch();
        let pred =
            binary(col("temp", &schema).unwrap(), Operator::GtEq, lit(3i32), &schema).unwrap();
        assert_eq!(select(&schema, &batch, vec![pred]), vec![3, 4, 5]);
    }

    /// A single conjunct containing an `OR` (not split) unions its branches:
    /// `lat < 15 OR lon = 2` keeps cells 0,1 (lat=10) ∪ 1,3,5 (lon=2) = {0,1,3,5}.
    #[test]
    fn or_predicate_unions_branches() {
        let (schema, batch) = test_batch();
        let pred = binary(
            binary(col("lat", &schema).unwrap(), Operator::Lt, lit(15i32), &schema).unwrap(),
            Operator::Or,
            binary(col("lon", &schema).unwrap(), Operator::Eq, lit(2i32), &schema).unwrap(),
            &schema,
        )
        .unwrap();
        assert_eq!(select(&schema, &batch, vec![pred]), vec![0, 1, 3, 5]);
    }

    /// A cross-axis conjunct intersected with a single-axis one:
    /// `lat + lon > 22 AND lon = 1` keeps only cell 4.
    #[test]
    fn cross_axis_and_single_axis_intersect() {
        let (schema, batch) = test_batch();
        let cross = binary(
            binary(
                col("lat", &schema).unwrap(),
                Operator::Plus,
                col("lon", &schema).unwrap(),
                &schema,
            )
            .unwrap(),
            Operator::Gt,
            lit(22i32),
            &schema,
        )
        .unwrap();
        let inner = binary(col("lon", &schema).unwrap(), Operator::Eq, lit(1i32), &schema).unwrap();
        assert_eq!(select(&schema, &batch, vec![cross, inner]), vec![4]);
    }

    /// A predicate no cell satisfies yields an empty selection.
    #[test]
    fn predicate_selecting_nothing_is_empty() {
        let (schema, batch) = test_batch();
        let pred = binary(col("lat", &schema).unwrap(), Operator::Gt, lit(100i32), &schema).unwrap();
        assert_eq!(select(&schema, &batch, vec![pred]), Vec::<u64>::new());
    }

    /// A predicate every cell satisfies retains the whole grid, in order.
    #[test]
    fn predicate_selecting_everything_keeps_all() {
        let (schema, batch) = test_batch();
        let pred = binary(col("lat", &schema).unwrap(), Operator::GtEq, lit(10i32), &schema).unwrap();
        assert_eq!(select(&schema, &batch, vec![pred]), vec![0, 1, 2, 3, 4, 5]);
    }

    /// A cell where the predicate evaluates to NULL is excluded (SQL `WHERE`
    /// keeps only TRUE). With `lat = [10, null, 30]`, `lat > 15` drops the null
    /// slice and keeps only the lat=30 cells.
    #[test]
    fn null_predicate_value_excludes_cell() {
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("lat", DataType::Int32, true),
            Field::new("lon", DataType::Int32, true),
        ]));
        let lat = NdArrowArray::try_new(
            Arc::new(Int32Array::from(vec![Some(10), None, Some(30)])),
            dims(&[("lat", 3)]),
        )
        .unwrap();
        let lon =
            NdArrowArray::try_new(Arc::new(Int32Array::from(vec![1, 2])), dims(&[("lon", 2)]))
                .unwrap();
        let batch = NdRecordBatch::try_new(
            schema.clone(),
            vec![lat, lon],
            dims(&[("lat", 3), ("lon", 2)]),
        )
        .unwrap();

        let pred = binary(col("lat", &schema).unwrap(), Operator::Gt, lit(15i32), &schema).unwrap();
        assert_eq!(select(&schema, &batch, vec![pred]), vec![4, 5]);
    }
}
