//! Element-wise expression evaluation on footprint sub-grids, shared by the nd
//! operators that push work below the broadcast.
//!
//! An element-wise expression's value at grid cell `(t, y, x)` depends only on
//! its input columns at `(t, y, x)`. So instead of broadcasting every input onto
//! the full grid and then evaluating, it can be evaluated on the *minimal*
//! sub-grid its inputs span — its **footprint**, the union of the referenced
//! columns' dimensions. [`NdProjectionExec`](super::NdProjectionExec) emits the
//! result as an output column; [`NdFilterExec`](super::NdFilterExec) evaluates a
//! boolean predicate the same way and turns it into a grid selection. The two
//! share [`NdExprColumn`] and [`ProjectMetrics`].

use std::collections::HashSet;
use std::sync::Arc;

use arrow::array::{ArrayRef, RecordBatch, RecordBatchOptions};
use arrow::datatypes::{Schema, SchemaRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::utils::{collect_columns, reassign_expr_columns};
use datafusion::physical_plan::metrics::Count;

use crate::nd::array::NdArrowArray;
use crate::nd::batch::NdRecordBatch;
use crate::nd::dimensions::Dimensions;

/// Per-partition counters shared by the footprint-evaluating nd operators.
pub(super) struct ProjectMetrics {
    /// Total elements the expressions were evaluated over (∑ footprint sizes).
    pub elements_evaluated: Count,
    /// Elements avoided versus evaluating on the full grid (∑ target − footprint).
    pub elements_saved: Count,
    /// Implicit gathers to co-broadcast referenced columns onto their footprint.
    pub broadcasts: Count,
}

impl ProjectMetrics {
    /// Record one evaluated expression: `evaluated` elements over its footprint
    /// (out of `target` on the full grid), and `broadcasts` implicit co-broadcasts.
    pub fn record(&self, evaluated: usize, target: usize, broadcasts: usize) {
        self.elements_evaluated.add(evaluated);
        self.elements_saved.add(target.saturating_sub(evaluated));
        self.broadcasts.add(broadcasts);
    }
}

/// One element-wise expression rewritten to reference only the columns it uses,
/// plus the indices of those columns in the child's schema.
#[derive(Debug, Clone)]
pub(super) struct NdExprColumn {
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

impl NdExprColumn {
    /// Analyze one expression against the child schema: collect the columns it
    /// references, build a compact schema of just those, and reindex the
    /// expression's `Column`s onto it so it can be evaluated against a batch of
    /// only the referenced columns.
    pub fn build(input_schema: &SchemaRef, expr: &Arc<dyn PhysicalExpr>) -> Result<Self> {
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

    /// Evaluate this expression over one nd batch on its minimal footprint grid,
    /// returning an nd column and recording work into `metrics`.
    pub fn project(
        &self,
        batch: &NdRecordBatch,
        target: &Dimensions,
        metrics: &ProjectMetrics,
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

/// The union of the dimensions of the columns at `input_indices`, ordered by
/// their position in `target`.
pub(super) fn footprint_of(
    target: &Dimensions,
    batch: &NdRecordBatch,
    input_indices: &[usize],
) -> Result<Dimensions> {
    let referenced: HashSet<&str> = input_indices
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
    /// (arrays and/or scalars) row-wise — enough to exercise evaluation over
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

    fn metrics() -> ProjectMetrics {
        ProjectMetrics {
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
        let column = NdExprColumn::build(&schema, &expr).unwrap();
        assert_eq!(column.input_indices, vec![0]);
        assert!(!column.passthrough);

        // A bare `temp` reference (index 2) is a passthrough.
        let bare = col("temp", &schema).unwrap();
        let column = NdExprColumn::build(&schema, &bare).unwrap();
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
        let column = NdExprColumn::build(&schema, &expr).unwrap();
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
        let column = NdExprColumn::build(&schema, &col("temp", &schema).unwrap()).unwrap();
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
        let column = NdExprColumn::build(&schema, &expr).unwrap();
        let m = metrics();

        let out = column.project(&batch, batch.target(), &m).unwrap();
        assert_eq!(out.dims(), &dims(&[("lat", 3), ("lon", 2)]));
        // C-order over {lat, lon}: lat replicated over lon, lon tiled over lat.
        assert_eq!(ints(out.values()), vec![11, 12, 21, 22, 31, 32]);
        assert_eq!(m.broadcasts.value(), 2);
        assert_eq!(m.elements_evaluated.value(), 6);
        assert_eq!(m.elements_saved.value(), 0); // footprint == full grid
    }

    /// A function over a single column takes the fast path: footprint is that
    /// column's own dims, no co-broadcast.
    #[test]
    fn function_of_single_column_does_not_broadcast() {
        let (schema, batch) = test_batch();
        let expr = sum_fn(vec![col("lat", &schema).unwrap()], &schema);
        let column = NdExprColumn::build(&schema, &expr).unwrap();
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
        let column = NdExprColumn::build(&schema, &expr).unwrap();
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
        let column = NdExprColumn::build(&schema, &expr).unwrap();
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
        let column = NdExprColumn::build(&schema, &expr).unwrap();
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
