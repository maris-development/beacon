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

            // Referenced input columns, sorted by their child-schema index, form
            // a compact schema; reindex the expression's `Column`s onto it so it
            // can be evaluated against a batch of just those columns.
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
            columns.push(ProjectionColumn {
                expr,
                compact_schema,
                input_indices,
            });
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
    /// recording the work done (elements evaluated, elements saved versus the
    /// full grid, and implicit co-broadcasts) into `metrics`.
    fn project_batch(
        &self,
        batch: &NdRecordBatch,
        metrics: &ProjectionMetrics,
    ) -> Result<NdRecordBatch> {
        let target = batch.target();
        let mut projected = Vec::with_capacity(self.columns.len());

        for column in &self.columns {
            // Footprint = the target axes referenced by this expression's input
            // columns, kept in target order so the sub-grid is C-order-consistent
            // with the full grid.
            let footprint = footprint_of(target, batch, &column.input_indices)?;

            // The expression runs over the footprint instead of the full grid;
            // the difference is the work the pushdown avoided.
            metrics.elements_evaluated.add(footprint.num_elements());
            metrics
                .elements_saved
                .add(target.num_elements().saturating_sub(footprint.num_elements()));

            // Co-broadcast the referenced columns onto the footprint so they
            // align element-wise, then evaluate. A referenced column on fewer
            // axes than the footprint needs a real gather (an implicit
            // broadcast); one already spanning the footprint passes through.
            let arrays: Vec<ArrayRef> = column
                .input_indices
                .iter()
                .map(|&i| {
                    let source = batch.column(i);
                    let map = source.broadcast_map(&footprint)?;
                    if !map.is_identity() {
                        metrics.broadcasts.add(1);
                    }
                    source.materialize_with_map(&map)
                })
                .collect::<Result<_>>()?;
            let options =
                RecordBatchOptions::new().with_row_count(Some(footprint.num_elements()));
            let compact = RecordBatch::try_new_with_options(
                column.compact_schema.clone(),
                arrays,
                &options,
            )
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

            let values = column
                .expr
                .evaluate(&compact)?
                .into_array(footprint.num_elements())?;
            projected.push(NdArrowArray::try_new(values, footprint)?);
        }

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
