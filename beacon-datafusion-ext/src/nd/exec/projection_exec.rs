//! Projection evaluated *before* broadcast.
//!
//! A projection expression is element-wise: the value at grid cell `(t, y, x)`
//! depends only on its input columns' values at `(t, y, x)`. So instead of
//! broadcasting every input column onto the full grid and then evaluating (the
//! job of a plain `ProjectionExec` above [`NdBroadcastExec`]), we evaluate each
//! expression on the *minimal* sub-grid its inputs span — its **footprint**, the
//! union of the referenced columns' dimensions — and emit the result as a new nd
//! column on that footprint. [`NdBroadcastExec`] then broadcasts the (smaller)
//! result onto the full grid.
//!
//! Broadcasting commutes with element-wise evaluation, so the output is
//! identical to evaluating on the full grid — but a projection touching only a
//! coordinate axis (e.g. `lat * 2`) evaluates over `|lat|` elements instead of
//! the full `time·lat·lon` cross-product.

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use arrow::array::{ArrayRef, RecordBatch, RecordBatchOptions};
use arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::utils::{collect_columns, reassign_expr_columns};
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::metrics::{
    BaselineMetrics, Count, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet,
};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use futures::StreamExt;

use crate::nd::array::NdArrowArray;
use crate::nd::batch::NdRecordBatch;
use crate::nd::dimensions::Dimensions;

use super::{NdBroadcastExec, NdExecutionPlan, SendableNdBatchStream, as_nd_plan};

/// Per-partition metric counters recorded while projecting.
struct ProjectionMetrics {
    /// Total elements the expressions were evaluated over (∑ footprint sizes).
    elements_evaluated: Count,
    /// Elements avoided versus evaluating on the full grid (∑ target − footprint).
    elements_saved: Count,
    /// Implicit gathers to co-broadcast referenced columns onto their footprint.
    broadcasts: Count,
}

impl ProjectionMetrics {
    /// Record one output column: `evaluated` elements over its footprint (out of
    /// `target` on the full grid), and `broadcasts` implicit co-broadcasts.
    fn record(&self, evaluated: usize, target: usize, broadcasts: usize) {
        self.elements_evaluated.add(evaluated);
        self.elements_saved.add(target.saturating_sub(evaluated));
        self.broadcasts.add(broadcasts);
    }
}

/// One output column: an expression rewritten to reference only the columns it
/// uses, plus the indices of those columns in the child's schema.
#[derive(Debug, Clone)]
struct ProjectionColumn {
    /// The expression, with its `Column`s reindexed against `compact_schema`.
    expr: Arc<dyn PhysicalExpr>,
    /// Fields of the referenced input columns, in child-schema order.
    compact_schema: SchemaRef,
    /// Indices (into the child batch) of the referenced input columns, aligned
    /// with `compact_schema`.
    input_indices: Vec<usize>,
    /// True when `expr` is a bare column reference — the input nd column can be
    /// reused directly, skipping evaluation entirely.
    passthrough: bool,
}

impl ProjectionColumn {
    /// Analyze one expression against the child schema: collect the columns it
    /// references, build a compact schema of just those, and reindex the
    /// expression's `Column`s onto it so it can be evaluated against a batch of
    /// only the referenced columns.
    fn build(input_schema: &SchemaRef, expr: &Arc<dyn PhysicalExpr>) -> Result<Self> {
        let mut input_indices: Vec<usize> =
            collect_columns(expr).iter().map(|c| c.index()).collect();
        input_indices.sort_unstable();
        input_indices.dedup();
        let compact_schema = Arc::new(Schema::new(
            input_indices
                .iter()
                .map(|&i| input_schema.field(i).clone())
                .collect::<Vec<_>>(),
        ));
        let expr = reassign_expr_columns(expr.clone(), &compact_schema)?;
        let passthrough = expr.as_any().is::<Column>();
        Ok(Self {
            expr,
            compact_schema,
            input_indices,
            passthrough,
        })
    }

    /// Project this column from one nd batch onto the minimal footprint grid,
    /// returning an nd column and recording work into `metrics`.
    fn project(
        &self,
        batch: &NdRecordBatch,
        target: &Dimensions,
        metrics: &ProjectionMetrics,
    ) -> Result<NdArrowArray> {
        // Fast path: a single referenced column needs no co-broadcast — its
        // footprint is its own dimensions. Evaluate on the native (un-broadcast)
        // values and leave the one gather onto the full grid to NdBroadcastExec.
        if self.input_indices.len() == 1 {
            let source = batch.column(self.input_indices[0]);
            let n = source.values().len();
            metrics.record(n, target.num_elements(), 0);

            // A bare column reference reuses the nd array as-is.
            if self.passthrough {
                return Ok(source.clone());
            }
            let values = self.evaluate(vec![source.values().clone()], n)?;
            return NdArrowArray::try_new(values, source.dims().clone());
        }

        // General path: union the referenced columns' footprint (in target
        // order) and co-broadcast each input onto it before evaluating. A column
        // on fewer axes than the footprint needs a real gather (implicit
        // broadcast); one already spanning it passes through.
        let footprint = footprint_of(target, batch, &self.input_indices)?;
        let n = footprint.num_elements();
        let mut broadcasts = 0usize;
        let arrays: Vec<ArrayRef> = self
            .input_indices
            .iter()
            .map(|&i| {
                let source = batch.column(i);
                let map = source.broadcast_map(&footprint)?;
                if !map.is_identity() {
                    broadcasts += 1;
                }
                source.materialize_with_map(&map)
            })
            .collect::<Result<_>>()?;
        metrics.record(n, target.num_elements(), broadcasts);
        let values = self.evaluate(arrays, n)?;
        NdArrowArray::try_new(values, footprint)
    }

    /// Evaluate the (reindexed) expression on a compact batch of `arrays` with
    /// `rows` rows, returning the result array.
    fn evaluate(&self, arrays: Vec<ArrayRef>, rows: usize) -> Result<ArrayRef> {
        let options = RecordBatchOptions::new().with_row_count(Some(rows));
        let compact =
            RecordBatch::try_new_with_options(self.compact_schema.clone(), arrays, &options)
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
        self.expr.evaluate(&compact)?.into_array(rows)
    }
}

/// Projects a list of element-wise expressions over un-broadcast nd batches,
/// evaluating each on its footprint sub-grid. Requires an nd-aware child and is
/// itself nd-aware, so it slots between [`NdSourceExec`](super::NdSourceExec) and
/// [`NdBroadcastExec`].
#[derive(Debug, Clone)]
pub struct NdProjectionExec {
    /// nd-aware child producing the input nd batches.
    input: Arc<dyn ExecutionPlan>,
    /// Output expressions with their aliases, as given (drives display and
    /// `with_new_children`).
    exprs: Vec<(Arc<dyn PhysicalExpr>, String)>,
    /// Per-output-column evaluation plan (derived from `exprs`).
    columns: Vec<ProjectionColumn>,
    /// Output (projected) schema.
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl NdProjectionExec {
    /// Build a projection over an nd-aware `input`. `exprs` are `(expr, alias)`
    /// pairs; each expression must be evaluable against `input`'s schema. The
    /// output schema is derived from the expressions.
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        exprs: Vec<(Arc<dyn PhysicalExpr>, String)>,
    ) -> Result<Self> {
        Self::try_new_with_schema(input, exprs, None)
    }

    /// Like [`try_new`](Self::try_new), but adopts `output_schema` verbatim when
    /// provided. The pushdown rule passes the `ProjectionExec`'s exact schema so
    /// the rewrite preserves field metadata and the optimizer's schema check
    /// holds. When `None`, the schema is derived from the expressions.
    pub fn try_new_with_schema(
        input: Arc<dyn ExecutionPlan>,
        exprs: Vec<(Arc<dyn PhysicalExpr>, String)>,
        output_schema: Option<SchemaRef>,
    ) -> Result<Self> {
        if as_nd_plan(&input).is_none() {
            return Err(DataFusionError::Plan(format!(
                "NdProjectionExec requires an nd-aware input, got {}",
                input.name()
            )));
        }
        let input_schema = input.schema();

        let mut fields = Vec::with_capacity(exprs.len());
        let mut columns = Vec::with_capacity(exprs.len());
        for (expr, alias) in &exprs {
            fields.push(Field::new(
                alias,
                expr.data_type(&input_schema)?,
                expr.nullable(&input_schema)?,
            ));
            columns.push(ProjectionColumn::build(&input_schema, expr)?);
        }

        let derived = Arc::new(Schema::new(fields));
        let schema = match output_schema {
            Some(provided) => {
                // The provided schema must be type-compatible with the derived
                // one; only field metadata may differ.
                if provided.fields().len() != derived.fields().len()
                    || provided
                        .fields()
                        .iter()
                        .zip(derived.fields().iter())
                        .any(|(a, b)| a.data_type() != b.data_type())
                {
                    return Err(DataFusionError::Plan(format!(
                        "NdProjectionExec output schema {provided:?} is incompatible with the \
                         projected expressions {derived:?}"
                    )));
                }
                provided
            }
            None => derived,
        };
        let properties = Arc::new(
            input
                .properties()
                .as_ref()
                .clone()
                .with_eq_properties(EquivalenceProperties::new(schema.clone())),
        );
        Ok(Self {
            input,
            exprs,
            columns,
            schema,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    pub fn expressions(&self) -> &[(Arc<dyn PhysicalExpr>, String)] {
        &self.exprs
    }

    /// Project one nd batch: evaluate every output column on its footprint grid,
    /// recording work into `metrics`. Per-column logic lives in
    /// [`ProjectionColumn::project`].
    fn project_batch(
        &self,
        batch: &NdRecordBatch,
        metrics: &ProjectionMetrics,
    ) -> Result<NdRecordBatch> {
        let target = batch.target();
        let projected = self
            .columns
            .iter()
            .map(|column| column.project(batch, target, metrics))
            .collect::<Result<Vec<_>>>()?;
        NdRecordBatch::try_new(self.schema.clone(), projected, target.clone())
    }
}

/// The union of the dimensions of the columns at `input_indices`, ordered by
/// their position in `target`.
fn footprint_of(
    target: &Dimensions,
    batch: &NdRecordBatch,
    input_indices: &[usize],
) -> Result<Dimensions> {
    let referenced: std::collections::HashSet<&str> = input_indices
        .iter()
        .flat_map(|&i| batch.column(i).dims().iter().map(|d| d.name()))
        .collect();
    Dimensions::try_new(
        target
            .iter()
            .filter(|d| referenced.contains(d.name()))
            .cloned()
            .collect(),
    )
}

impl DisplayAs for NdProjectionExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let cols: Vec<String> = self.exprs.iter().map(|(_, alias)| alias.clone()).collect();
        write!(f, "NdProjectionExec: exprs=[{}]", cols.join(", "))
    }
}

impl ExecutionPlan for NdProjectionExec {
    fn name(&self) -> &str {
        "NdProjectionExec"
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
            DataFusionError::Internal("NdProjectionExec expects exactly one child".to_string())
        })?;
        Ok(Arc::new(Self::try_new(input, self.exprs.clone())?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // This node's real output is the un-broadcast `NdRecordBatch` stream from
        // `execute_nd`; the generic `ExecutionPlan::execute` must instead yield
        // flat Arrow `RecordBatch`es, and broadcasting is the only thing that
        // flattens them. So a standalone execution wraps this node in an
        // `NdBroadcastExec` to materialize.
        //
        // This is *not* structural coupling: it is only the fallback for when the
        // node is a plan root with nothing broadcasting above it. In a real plan
        // an `NdBroadcastExec` sits at the top and pulls this node's `execute_nd`
        // directly (through any nd operators in between), so this path is never
        // taken — the broadcast stays a separate, single terminal node.
        NdBroadcastExec::try_new(Arc::new(self.clone()))?.execute(partition, context)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

impl NdExecutionPlan for NdProjectionExec {
    fn execute_nd(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableNdBatchStream> {
        let baseline = BaselineMetrics::new(&self.metrics, partition);
        let projection_metrics = ProjectionMetrics {
            elements_evaluated: MetricBuilder::new(&self.metrics)
                .counter("elements_evaluated", partition),
            elements_saved: MetricBuilder::new(&self.metrics)
                .counter("elements_saved", partition),
            broadcasts: MetricBuilder::new(&self.metrics)
                .counter("implicit_broadcasts", partition),
        };
        let this = self.clone();
        let stream = as_nd_plan(&self.input)
            .expect("validated in try_new")
            .execute_nd(partition, context)?
            .map(move |item| {
                let _timer = baseline.elapsed_compute().timer();
                let batch = item?;
                let projected = this.project_batch(&batch, &projection_metrics)?;
                baseline.record_output(projected.num_rows());
                Ok(projected)
            });
        Ok(Box::pin(stream))
    }
}

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::sync::Arc;

    use arrow::array::{ArrayRef, AsArray, Int32Array};
    use arrow::datatypes::{DataType, Field, Int32Type, Schema, SchemaRef};
    use datafusion::common::config::ConfigOptions;
    use datafusion::logical_expr::{
        ColumnarValue, Operator, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
        Volatility,
    };
    use datafusion::physical_expr::ScalarFunctionExpr;
    use datafusion::physical_expr::expressions::{binary, col, lit};
    use datafusion::physical_plan::metrics::Count;

    use crate::nd::dimensions::Dimension;

    use super::*;

    /// A minimal element-wise scalar function that sums all its Int32 arguments
    /// (arrays and/or scalars) row-wise — enough to exercise projection over
    /// functions without pulling in datafusion-functions.
    #[derive(Debug, PartialEq, Eq, Hash)]
    struct SumUdf {
        signature: Signature,
    }

    impl SumUdf {
        fn new() -> Self {
            Self {
                signature: Signature::variadic_any(Volatility::Immutable),
            }
        }
    }

    impl ScalarUDFImpl for SumUdf {
        fn as_any(&self) -> &dyn Any {
            self
        }
        fn name(&self) -> &str {
            "test_sum"
        }
        fn signature(&self) -> &Signature {
            &self.signature
        }
        fn return_type(&self, _: &[DataType]) -> Result<DataType> {
            Ok(DataType::Int32)
        }
        fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
            let n = args.number_rows;
            let mut sums = vec![0i32; n];
            for arg in &args.args {
                let array = arg.clone().into_array(n)?;
                let column = array.as_primitive::<Int32Type>();
                for (i, s) in sums.iter_mut().enumerate() {
                    *s += column.value(i);
                }
            }
            Ok(ColumnarValue::Array(Arc::new(Int32Array::from(sums))))
        }
    }

    /// Build `test_sum(args...)` as a physical expression against `schema`.
    fn sum_fn(args: Vec<Arc<dyn PhysicalExpr>>, schema: &Schema) -> Arc<dyn PhysicalExpr> {
        let udf = Arc::new(ScalarUDF::new_from_impl(SumUdf::new()));
        Arc::new(
            ScalarFunctionExpr::try_new(udf, args, schema, Arc::new(ConfigOptions::default()))
                .unwrap(),
        )
    }

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

    fn metrics() -> ProjectionMetrics {
        ProjectionMetrics {
            elements_evaluated: Count::new(),
            elements_saved: Count::new(),
            broadcasts: Count::new(),
        }
    }

    fn ints(array: &ArrayRef) -> Vec<i32> {
        array.as_primitive::<Int32Type>().values().to_vec()
    }

    #[test]
    fn build_collects_columns_and_flags_passthrough() {
        let (schema, _) = test_batch();

        // `lat * 2` references only `lat` (index 0) and is not a bare column.
        let expr =
            binary(col("lat", &schema).unwrap(), Operator::Multiply, lit(2i32), &schema).unwrap();
        let column = ProjectionColumn::build(&schema, &expr).unwrap();
        assert_eq!(column.input_indices, vec![0]);
        assert!(!column.passthrough);

        // A bare `temp` reference (index 2) is a passthrough.
        let bare = col("temp", &schema).unwrap();
        let column = ProjectionColumn::build(&schema, &bare).unwrap();
        assert_eq!(column.input_indices, vec![2]);
        assert!(column.passthrough);
    }

    #[test]
    fn footprint_is_ordered_by_target() {
        let (_, batch) = test_batch();
        // Reference lon (idx 1) then lat (idx 0): footprint keeps target order.
        let footprint = footprint_of(batch.target(), &batch, &[0, 1]).unwrap();
        assert_eq!(footprint, dims(&[("lat", 3), ("lon", 2)]));
    }

    #[test]
    fn project_single_column_evaluates_on_native_footprint() {
        let (schema, batch) = test_batch();
        let expr =
            binary(col("lat", &schema).unwrap(), Operator::Multiply, lit(2i32), &schema).unwrap();
        let column = ProjectionColumn::build(&schema, &expr).unwrap();
        let m = metrics();

        let out = column.project(&batch, batch.target(), &m).unwrap();
        assert_eq!(out.dims(), &dims(&[("lat", 3)])); // footprint = lat's own dims
        assert_eq!(ints(out.values()), vec![20, 40, 60]);
        // No co-broadcast; evaluated over |lat|=3, saved 6-3.
        assert_eq!(m.broadcasts.value(), 0);
        assert_eq!(m.elements_evaluated.value(), 3);
        assert_eq!(m.elements_saved.value(), 3);
    }

    #[test]
    fn project_passthrough_reuses_the_nd_array() {
        let (schema, batch) = test_batch();
        let column = ProjectionColumn::build(&schema, &col("temp", &schema).unwrap()).unwrap();
        let m = metrics();

        let out = column.project(&batch, batch.target(), &m).unwrap();
        assert_eq!(out.dims(), &dims(&[("lat", 3), ("lon", 2)]));
        assert_eq!(ints(out.values()), vec![0, 1, 2, 3, 4, 5]);
        assert_eq!(m.broadcasts.value(), 0);
    }

    #[test]
    fn project_multi_column_co_broadcasts_onto_footprint() {
        let (schema, batch) = test_batch();
        // lat + lon → footprint {lat, lon}; each input gathered onto it.
        let expr = binary(
            col("lat", &schema).unwrap(),
            Operator::Plus,
            col("lon", &schema).unwrap(),
            &schema,
        )
        .unwrap();
        let column = ProjectionColumn::build(&schema, &expr).unwrap();
        let m = metrics();

        let out = column.project(&batch, batch.target(), &m).unwrap();
        assert_eq!(out.dims(), &dims(&[("lat", 3), ("lon", 2)]));
        // C-order over {lat, lon}: lat replicated over lon, lon tiled over lat.
        assert_eq!(ints(out.values()), vec![11, 12, 21, 22, 31, 32]);
        assert_eq!(m.broadcasts.value(), 2);
        assert_eq!(m.elements_evaluated.value(), 6);
        assert_eq!(m.elements_saved.value(), 0); // footprint == full grid
    }

    // ── scalar functions ────────────────────────────────────────────────

    /// A function over a single column takes the fast path: footprint is that
    /// column's own dims, no co-broadcast.
    #[test]
    fn function_of_single_column_does_not_broadcast() {
        let (schema, batch) = test_batch();
        let expr = sum_fn(vec![col("lat", &schema).unwrap()], &schema);
        let column = ProjectionColumn::build(&schema, &expr).unwrap();
        let m = metrics();

        let out = column.project(&batch, batch.target(), &m).unwrap();
        assert_eq!(out.dims(), &dims(&[("lat", 3)]));
        assert_eq!(ints(out.values()), vec![10, 20, 30]);
        assert_eq!(m.broadcasts.value(), 0);
        assert_eq!(m.elements_evaluated.value(), 3);
    }

    /// A function over two columns on different axes unions their footprint and
    /// co-broadcasts both onto it before evaluating.
    #[test]
    fn function_of_two_columns_co_broadcasts() {
        let (schema, batch) = test_batch();
        let expr = sum_fn(
            vec![col("lat", &schema).unwrap(), col("lon", &schema).unwrap()],
            &schema,
        );
        let column = ProjectionColumn::build(&schema, &expr).unwrap();
        let m = metrics();

        let out = column.project(&batch, batch.target(), &m).unwrap();
        assert_eq!(out.dims(), &dims(&[("lat", 3), ("lon", 2)]));
        assert_eq!(ints(out.values()), vec![11, 12, 21, 22, 31, 32]);
        assert_eq!(m.broadcasts.value(), 2);
        assert_eq!(m.elements_evaluated.value(), 6);
    }

    /// A function over one column plus a scalar literal references only that one
    /// column, so it stays on the fast path — the literal adds no dimensions and
    /// nothing is broadcast up front.
    #[test]
    fn function_of_column_and_scalar_does_not_broadcast() {
        let (schema, batch) = test_batch();
        let expr = sum_fn(vec![col("lat", &schema).unwrap(), lit(100i32)], &schema);
        let column = ProjectionColumn::build(&schema, &expr).unwrap();
        // Only `lat` is a referenced column; the literal is folded into the expr.
        assert_eq!(column.input_indices, vec![0]);
        let m = metrics();

        let out = column.project(&batch, batch.target(), &m).unwrap();
        assert_eq!(out.dims(), &dims(&[("lat", 3)])); // footprint = lat's dims
        assert_eq!(ints(out.values()), vec![110, 120, 130]);
        assert_eq!(m.broadcasts.value(), 0); // no up-front broadcast
        assert_eq!(m.elements_evaluated.value(), 3); // evaluated over |lat|, not the grid
    }

    /// A function over only literals references no columns: it evaluates once on
    /// a rank-0 scalar footprint and broadcasts to a constant column.
    #[test]
    fn function_of_only_scalars_is_rank_zero() {
        let (schema, batch) = test_batch();
        let expr = sum_fn(vec![lit(1i32), lit(2i32)], &schema);
        let column = ProjectionColumn::build(&schema, &expr).unwrap();
        assert!(column.input_indices.is_empty());
        let m = metrics();

        let out = column.project(&batch, batch.target(), &m).unwrap();
        assert_eq!(out.dims().rank(), 0); // scalar
        assert_eq!(ints(out.values()), vec![3]);
        assert_eq!(m.broadcasts.value(), 0);
        assert_eq!(m.elements_evaluated.value(), 1);
        assert_eq!(m.elements_saved.value(), 5); // 6-cell grid minus the 1 scalar
    }
}
