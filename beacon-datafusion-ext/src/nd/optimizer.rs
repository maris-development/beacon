//! Physical optimizer rule: sink element-wise projections below the broadcast.
//!
//! DataFusion plans a `ProjectionExec` above [`NdBroadcastExec`], so projection
//! expressions run *after* the full grid is materialized. When every output
//! expression is element-wise (its value at a grid cell depends only on its
//! inputs at that cell), the projection can instead run on the un-broadcast nd
//! columns — evaluated on each expression's footprint sub-grid — and be
//! broadcast afterwards. This rule rewrites
//!
//! ```text
//! ProjectionExec[exprs]              NdBroadcastExec
//!   NdBroadcastExec           ->       NdProjectionExec[exprs]
//!     nd-child                           nd-child
//! ```
//!
//! Broadcasting commutes with element-wise evaluation, so the result is
//! identical — but a projection touching only a coordinate axis now evaluates
//! over that axis instead of the full cross-product.

use std::sync::Arc;

use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::error::Result;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::{
    BinaryExpr, CaseExpr, CastExpr, Column, IsNotNullExpr, IsNullExpr, Literal, NegativeExpr,
    NotExpr, TryCastExpr,
};
use datafusion::physical_expr::{ScalarFunctionExpr, conjunction, split_conjunction};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::logical_expr::Volatility;

use super::exec::{NdBroadcastExec, NdFilterExec, NdProjectionExec};

/// Sinks element-wise `ProjectionExec`s below an [`NdBroadcastExec`] into an
/// [`NdProjectionExec`], so they evaluate before broadcasting.
#[derive(Debug, Default)]
pub struct NdProjectionPushdown;

impl NdProjectionPushdown {
    pub fn new() -> Self {
        Self
    }
}

impl PhysicalOptimizerRule for NdProjectionPushdown {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_down(|node| {
            let Some(projection) = node.as_any().downcast_ref::<ProjectionExec>() else {
                return Ok(Transformed::no(node));
            };
            let Some(broadcast) = projection
                .input()
                .as_any()
                .downcast_ref::<NdBroadcastExec>()
            else {
                return Ok(Transformed::no(node));
            };

            // Only sink when every output expression is safe to evaluate before
            // broadcast; a mixed projection is left in place.
            if !projection
                .expr()
                .iter()
                .all(|pe| is_pushable_expr(&pe.expr))
            {
                return Ok(Transformed::no(node));
            }

            let exprs: Vec<(Arc<dyn PhysicalExpr>, String)> = projection
                .expr()
                .iter()
                .map(|pe| (pe.expr.clone(), pe.alias.clone()))
                .collect();

            // Preserve the projection's exact output schema so the rewrite is
            // schema-preserving for the optimizer's schema check.
            let nd_projection = Arc::new(NdProjectionExec::try_new_with_schema(
                broadcast.input().clone(),
                exprs,
                Some(projection.schema()),
            )?);
            let new_broadcast = Arc::new(NdBroadcastExec::try_new(nd_projection)?);
            Ok(Transformed::yes(new_broadcast as Arc<dyn ExecutionPlan>))
        })
        .map(|t| t.data)
    }

    fn name(&self) -> &str {
        "NdProjectionPushdown"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Sinks the element-wise conjuncts of a `FilterExec` below an
/// [`NdBroadcastExec`] into an [`NdFilterExec`], so the predicate selects rows on
/// the un-broadcast nd columns and the broadcast fuses with the selection.
///
/// DataFusion plans a `FilterExec` above [`NdBroadcastExec`], so the `WHERE`
/// predicate runs *after* the full grid is materialized. A predicate is a
/// conjunction; each element-wise conjunct (its value at a grid cell depends only
/// on its inputs there) can instead be evaluated before broadcast — on its
/// footprint sub-grid — and recorded as a grid selection the broadcast applies.
/// This rule rewrites
///
/// ```text
/// FilterExec[a AND b AND c]          FilterExec[c]            (residual, only if any)
///   NdBroadcastExec           ->       NdBroadcastExec
///     nd-child                           NdFilterExec[a, b]   (element-wise conjuncts)
///                                          nd-child
/// ```
///
/// where `a`, `b` are element-wise ([`is_pushable_expr`]) and `c` is not (e.g. a
/// volatile function or a subquery). If every conjunct is pushable, the residual
/// `FilterExec` is dropped entirely. The rewrite is schema-preserving: a filter
/// never changes columns.
#[derive(Debug, Default)]
pub struct NdFilterPushdown;

impl NdFilterPushdown {
    pub fn new() -> Self {
        Self
    }
}

impl PhysicalOptimizerRule for NdFilterPushdown {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_down(|node| {
            let Some(filter) = node.as_any().downcast_ref::<FilterExec>() else {
                return Ok(Transformed::no(node));
            };
            // A `FilterExec` carrying an embedded projection also changes the
            // schema; leave those in place so the rewrite stays a pure row
            // selection.
            if filter.projection().is_some() {
                return Ok(Transformed::no(node));
            }
            let Some(broadcast) = filter.input().as_any().downcast_ref::<NdBroadcastExec>() else {
                return Ok(Transformed::no(node));
            };

            // Split the predicate and route each conjunct: element-wise ones sink
            // into the nd filter, the rest stay in a residual filter above.
            let mut push: Vec<Arc<dyn PhysicalExpr>> = Vec::new();
            let mut keep: Vec<Arc<dyn PhysicalExpr>> = Vec::new();
            for conjunct in split_conjunction(filter.predicate()) {
                if is_pushable_expr(conjunct) {
                    push.push(conjunct.clone());
                } else {
                    keep.push(conjunct.clone());
                }
            }
            if push.is_empty() {
                return Ok(Transformed::no(node));
            }

            let nd_filter = Arc::new(NdFilterExec::try_new(broadcast.input().clone(), push)?);
            let new_broadcast = Arc::new(NdBroadcastExec::try_new(nd_filter)?);
            let rewritten: Arc<dyn ExecutionPlan> = if keep.is_empty() {
                new_broadcast
            } else {
                Arc::new(FilterExec::try_new(conjunction(keep), new_broadcast)?)
            };
            Ok(Transformed::yes(rewritten))
        })
        .map(|t| t.data)
    }

    fn name(&self) -> &str {
        "NdFilterPushdown"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Whether an expression can be evaluated before broadcast and give the same
/// result after broadcast — i.e. it is element-wise and deterministic.
///
/// Conservative: an expression built only from a whitelist of element-wise node
/// types (arithmetic/comparison/boolean, cast, negation, null checks, `CASE`)
/// and non-volatile scalar functions is pushable. Any node outside the
/// whitelist — a volatile function like `random()`, a window/subquery
/// expression, or anything unrecognized — makes the whole expression stay above
/// the broadcast.
pub fn is_pushable_expr(expr: &Arc<dyn PhysicalExpr>) -> bool {
    is_elementwise_node(expr) && expr.children().iter().all(|c| is_pushable_expr(c))
}

/// Whether a single node (ignoring its children) is a known element-wise,
/// deterministic operator.
fn is_elementwise_node(expr: &Arc<dyn PhysicalExpr>) -> bool {
    let any = expr.as_any();
    if any.is::<Column>()
        || any.is::<Literal>()
        || any.is::<BinaryExpr>()
        || any.is::<CastExpr>()
        || any.is::<TryCastExpr>()
        || any.is::<NegativeExpr>()
        || any.is::<NotExpr>()
        || any.is::<IsNullExpr>()
        || any.is::<IsNotNullExpr>()
        || any.is::<CaseExpr>()
    {
        return true;
    }
    // Scalar functions are row-wise, but a volatile one (e.g. `random()`) would
    // produce fewer distinct values if evaluated before broadcast.
    if let Some(func) = any.downcast_ref::<ScalarFunctionExpr>() {
        return func.fun().signature().volatility != Volatility::Volatile;
    }
    false
}

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::config::ConfigOptions;
    use datafusion::logical_expr::{
        ColumnarValue, Operator, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
        Volatility,
    };
    use datafusion::physical_expr::ScalarFunctionExpr;
    use datafusion::physical_expr::expressions::{binary, cast, col, lit};
    use datafusion::scalar::ScalarValue;

    use super::*;

    fn schema() -> Schema {
        Schema::new(vec![
            Field::new("lat", DataType::Int32, true),
            Field::new("sst", DataType::Float64, true),
        ])
    }

    #[test]
    fn arithmetic_cast_literal_are_pushable() {
        let s = schema();
        let lat = col("lat", &s).unwrap();
        // (lat * 2) + 1
        let arith = binary(
            binary(lat.clone(), Operator::Multiply, lit(2i32), &s).unwrap(),
            Operator::Plus,
            lit(1i32),
            &s,
        )
        .unwrap();
        assert!(is_pushable_expr(&arith));

        // CAST(lat AS Float64)
        let casted = cast(lat, &s, DataType::Float64).unwrap();
        assert!(is_pushable_expr(&casted));

        // a bare literal
        let seven: Arc<dyn PhysicalExpr> = lit(7i32);
        assert!(is_pushable_expr(&seven));
    }

    /// A minimal volatile scalar function, to prove the volatility guard.
    #[derive(Debug, PartialEq, Eq, Hash)]
    struct VolatileUdf {
        signature: Signature,
    }

    impl ScalarUDFImpl for VolatileUdf {
        fn as_any(&self) -> &dyn Any {
            self
        }
        fn name(&self) -> &str {
            "test_volatile"
        }
        fn signature(&self) -> &Signature {
            &self.signature
        }
        fn return_type(&self, _: &[DataType]) -> Result<DataType> {
            Ok(DataType::Float64)
        }
        fn invoke_with_args(&self, _: ScalarFunctionArgs) -> Result<ColumnarValue> {
            Ok(ColumnarValue::Scalar(ScalarValue::Float64(Some(0.0))))
        }
    }

    #[test]
    fn volatile_scalar_function_is_not_pushable() {
        let s = schema();
        let udf = Arc::new(ScalarUDF::new_from_impl(VolatileUdf {
            signature: Signature::exact(vec![], Volatility::Volatile),
        }));
        let func = ScalarFunctionExpr::try_new(
            udf,
            vec![],
            &s,
            Arc::new(ConfigOptions::default()),
        )
        .unwrap();
        let expr: Arc<dyn PhysicalExpr> = Arc::new(func);
        assert!(!is_pushable_expr(&expr));

        // …and a whitelisted node wrapping it is tainted too.
        let wrapped = binary(expr, Operator::Plus, lit(1.0f64), &s).unwrap();
        assert!(!is_pushable_expr(&wrapped));
    }
}
