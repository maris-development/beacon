//! Turns filters on functions with planner hooks into plain predicates.
//!
//! The rule runs once per query, after type coercion and before every optimizer rule.
//! So filter pushdown, file statistics pruning, table providers and remote sources
//! see plain column predicates and need no knowledge of the function.

use beacon_plugin_sdk::scalar_functions::{hooks_of, HookedUdf, Implied};
use datafusion::{
    arrow::datatypes::DataType,
    common::{
        config::ConfigOptions,
        tree_node::{Transformed, TreeNode},
        DFSchema, Result, ScalarValue,
    },
    logical_expr::{
        expr::ScalarFunction,
        interval_arithmetic::Interval,
        utils::{conjunction, split_conjunction_owned},
        BinaryExpr, Cast, Expr, ExprSchemable, Filter, LogicalPlan, Operator, TryCast,
    },
    optimizer::{analyzer::type_coercion::TypeCoercionRewriter, AnalyzerRule},
};

/// Applies the `implied` and `propagate` hooks to every filter.
#[derive(Debug, Default)]
pub(crate) struct FunctionHooksRule;

impl AnalyzerRule for FunctionHooksRule {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform_up_with_subqueries(|node| match node {
            LogicalPlan::Filter(filter) => rewrite_filter(filter),
            other => Ok(Transformed::no(other)),
        })
        .map(|transformed| transformed.data)
    }

    fn name(&self) -> &str {
        "beacon_function_hooks"
    }
}

fn rewrite_filter(filter: Filter) -> Result<Transformed<LogicalPlan>> {
    let schema = filter.input.schema().clone();
    let mut changed = false;
    let mut conjuncts = Vec::new();
    let mut extra = Vec::new();
    for conjunct in split_conjunction_owned(filter.predicate.clone()) {
        let Some((hooked, args)) = hooked_call(&conjunct) else {
            let rewritten = replace_exact(conjunct)?;
            changed |= rewritten.transformed;
            extra.extend(propagated(&rewritten.data, &schema));
            conjuncts.push(rewritten.data);
            continue;
        };
        match implied(hooked, args) {
            Some(Implied::Exact(expr)) => {
                changed = true;
                conjuncts.push(expr);
                continue;
            }
            Some(Implied::Necessary(expr)) => extra.push(expr),
            // The propagate hook is the fallback for a call without a pushdown rule.
            None => extra.extend(propagated(&conjunct, &schema)),
        }
        // Only the arguments: the call itself has had its rule.
        let rewritten = conjunct.map_children(replace_exact)?;
        changed |= rewritten.transformed;
        conjuncts.push(rewritten.data);
    }
    for expr in extra.into_iter().flat_map(split_conjunction_owned) {
        if !conjuncts.contains(&expr) {
            conjuncts.push(expr);
            changed = true;
        }
    }
    if !changed {
        return Ok(Transformed::no(LogicalPlan::Filter(filter)));
    }
    let Some(predicate) = conjunction(conjuncts) else {
        return Ok(Transformed::no(LogicalPlan::Filter(filter)));
    };
    // Type coercion ran before this rule, and a hook can give any literal type.
    let predicate = predicate
        .rewrite(&mut TypeCoercionRewriter::new(&schema))?
        .data;
    let filter = Filter::try_new(predicate, filter.input)?;
    Ok(Transformed::yes(LogicalPlan::Filter(filter)))
}

/// The hooked UDF and the arguments of a call to a function with hooks.
fn hooked_call(expr: &Expr) -> Option<(&HookedUdf, &[Expr])> {
    match expr {
        Expr::ScalarFunction(ScalarFunction { func, args }) => {
            hooks_of(func).map(|hooked| (hooked, args.as_slice()))
        }
        _ => None,
    }
}

/// The value of a constant expression, such as `CAST(2 AS DOUBLE)` or `-2`.
fn constant(expr: &Expr) -> Option<ScalarValue> {
    match expr {
        Expr::Literal(value, _) => Some(value.clone()),
        Expr::Cast(Cast { expr, field }) | Expr::TryCast(TryCast { expr, field }) => {
            constant(expr)?.cast_to(field.data_type()).ok()
        }
        Expr::Negative(expr) => constant(expr)?.arithmetic_negate().ok(),
        _ => None,
    }
}

/// The result of the `implied` hook. A hook error gives `None`.
fn implied(hooked: &HookedUdf, args: &[Expr]) -> Option<Implied> {
    let args: Vec<Expr> = args
        .iter()
        .map(|arg| constant(arg).map_or_else(|| arg.clone(), |value| Expr::Literal(value, None)))
        .collect();
    hooked.implied(&args).unwrap_or_else(|error| {
        tracing::warn!(
            "Ignoring the implied hook of '{}' for this query: {error}",
            hooked.inner().name()
        );
        None
    })
}

/// Replaces each call that has an `Exact` rule.
fn replace_exact(expr: Expr) -> Result<Transformed<Expr>> {
    expr.transform_up(|expr| {
        if let Some((hooked, args)) = hooked_call(&expr) {
            if let Some(Implied::Exact(replacement)) = implied(hooked, args) {
                return Ok(Transformed::yes(replacement));
            }
        }
        Ok(Transformed::no(expr))
    })
}

/// The column ranges that the `propagate` hook gives for a conjunct such as
/// `f(x)` or `f(x) > 5`.
fn propagated(conjunct: &Expr, schema: &DFSchema) -> Vec<Expr> {
    let Some((hooked, args, output)) = constrained_call(conjunct, schema) else {
        return Vec::new();
    };
    let Some(inputs) = args
        .iter()
        .map(|arg| arg_interval(arg, schema))
        .collect::<Option<Vec<_>>>()
    else {
        return Vec::new();
    };
    let inputs: Vec<&Interval> = inputs.iter().collect();
    let intervals = match hooked.propagate(&output, &inputs) {
        Ok(Some(intervals)) => intervals,
        Ok(None) => return Vec::new(),
        Err(error) => {
            tracing::warn!(
                "Ignoring the propagate hook of '{}' for this query: {error}",
                hooked.inner().name()
            );
            return Vec::new();
        }
    };
    args.iter()
        .zip(&intervals)
        .filter(|(arg, _)| matches!(arg, Expr::Column(_)))
        .flat_map(|(arg, interval)| range(arg, interval))
        .collect()
}

/// The hooked call in `expr` and the output interval that `expr` needs.
fn constrained_call<'a>(
    expr: &'a Expr,
    schema: &DFSchema,
) -> Option<(&'a HookedUdf, &'a [Expr], Interval)> {
    if let Some((hooked, args)) = hooked_call(expr) {
        let boolean = expr.get_type(schema).ok()? == DataType::Boolean;
        return boolean.then_some((hooked, args, Interval::TRUE));
    }
    let Expr::BinaryExpr(BinaryExpr { left, op, right }) = expr else {
        return None;
    };
    let ((hooked, args), op, value) = match (hooked_call(left), constant(right)) {
        (Some(call), Some(value)) => (call, *op, value),
        _ => (hooked_call(right)?, op.swap()?, constant(left)?),
    };
    if value.is_null() {
        return None;
    }
    let unbounded = ScalarValue::try_from(value.data_type()).ok()?;
    // Closed intervals also hold for `<` and `>`: a wider range is still necessary.
    let output = match op {
        Operator::Eq => Interval::try_new(value.clone(), value),
        Operator::Gt | Operator::GtEq => Interval::try_new(value, unbounded),
        Operator::Lt | Operator::LtEq => Interval::try_new(unbounded, value),
        _ => return None,
    }
    .ok()?;
    Some((hooked, args, output))
}

/// A point interval for a constant argument, else the unbounded interval of its type.
fn arg_interval(arg: &Expr, schema: &DFSchema) -> Option<Interval> {
    match constant(arg) {
        Some(value) => Interval::try_new(value.clone(), value).ok(),
        None => Interval::make_unbounded(&arg.get_type(schema).ok()?).ok(),
    }
}

/// `expr >= lower` and `expr <= upper` for the bounded sides of `interval`.
fn range(expr: &Expr, interval: &Interval) -> Vec<Expr> {
    let mut predicates = Vec::new();
    if !interval.lower().is_null() {
        predicates.push(
            expr.clone()
                .gt_eq(Expr::Literal(interval.lower().clone(), None)),
        );
    }
    if !interval.upper().is_null() {
        predicates.push(
            expr.clone()
                .lt_eq(Expr::Literal(interval.upper().clone(), None)),
        );
    }
    predicates
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Array, BooleanArray, Float64Array, Int64Array};
    use arrow::datatypes::{Field, Schema};
    use arrow::record_batch::RecordBatch;
    use beacon_plugin_sdk::scalar_functions::{function, FunctionBuilder};
    use datafusion::{
        execution::SessionStateBuilder,
        logical_expr::{lit, ScalarUDF},
        parquet::{arrow::ArrowWriter, file::properties::WriterProperties},
        prelude::{ParquetReadOptions, SessionContext},
    };

    use super::*;

    fn f64s(array: &Arc<dyn Array>) -> &Float64Array {
        array.as_any().downcast_ref::<Float64Array>().unwrap()
    }

    fn box_side(args: &[&Interval], low: usize, high: usize) -> Result<Interval> {
        Interval::try_new(args[low].lower().clone(), args[high].upper().clone())
    }

    /// Narrows `lon` and `lat` to the box when the output is true.
    fn box_propagate(output: &Interval, args: &[&Interval]) -> Result<Option<Vec<Interval>>> {
        if output != &Interval::TRUE {
            return Ok(None);
        }
        let (Some(lon), Some(lat)) = (
            args[0].intersect(box_side(args, 2, 4)?)?,
            args[1].intersect(box_side(args, 3, 5)?)?,
        ) else {
            return Ok(None);
        };
        let mut result = vec![lon, lat];
        result.extend(args[2..].iter().map(|a| (*a).clone()));
        Ok(Some(result))
    }

    /// The `in_box` body: `lon` and `lat` are in the box.
    fn box_invoke(arrays: &[Arc<dyn Array>]) -> Result<Arc<dyn Array>> {
        let value = |i: usize, row: usize| f64s(&arrays[i]).value(row);
        let rows = (0..arrays[0].len()).map(|row| {
            Some(
                value(0, row) >= value(2, row)
                    && value(0, row) <= value(4, row)
                    && value(1, row) >= value(3, row)
                    && value(1, row) <= value(5, row),
            )
        });
        Ok(Arc::new(rows.collect::<BooleanArray>()))
    }

    fn box_function(name: &str) -> FunctionBuilder {
        function(name)
            .args(vec![DataType::Float64; 6])
            .returns(DataType::Boolean)
    }

    /// `in_box(lon, lat, min_lon, min_lat, max_lon, max_lat)` with a `Necessary` rule.
    fn in_box() -> ScalarUDF {
        box_function("in_box")
            .implies(|args| {
                let lon = args[0].clone().between(args[2].clone(), args[4].clone());
                let lat = args[1].clone().between(args[3].clone(), args[5].clone());
                Ok(Some(Implied::Necessary(lon.and(lat))))
            })
            .propagate(box_propagate)
            .invoke_arrays(box_invoke)
    }

    /// `near_box(...)` is `in_box` with only the propagate hook.
    fn near_box() -> ScalarUDF {
        box_function("near_box")
            .propagate(box_propagate)
            .invoke_arrays(box_invoke)
    }

    /// `boom_box(...)` is `in_box`, but every hook panics.
    fn boom_box() -> ScalarUDF {
        box_function("boom_box")
            .implies(|_| panic!("implies exploded"))
            .propagate(|_, _| panic!("propagate exploded"))
            .invoke_arrays(box_invoke)
    }

    /// `is_east(lon)` is `lon > 0`. Its `Exact` rule replaces the call.
    fn is_east() -> ScalarUDF {
        function("is_east")
            .args([DataType::Float64])
            .returns(DataType::Boolean)
            .implies(|args| Ok(Some(Implied::Exact(args[0].clone().gt(lit(0.0))))))
            .invoke_arrays(|_| panic!("is_east must not run"))
    }

    /// A session with the rule, the test functions and table `points`: one Parquet
    /// file with two row groups, the first in the west and the second in the east.
    async fn session(dir: &std::path::Path) -> SessionContext {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("lon", DataType::Float64, false),
            Field::new("lat", DataType::Float64, false),
        ]));
        let path = dir.join("points.parquet");
        let properties = WriterProperties::builder()
            .set_max_row_group_row_count(Some(2))
            .build();
        let file = std::fs::File::create(&path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(properties)).unwrap();
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
                Arc::new(Float64Array::from(vec![-10.0, -9.0, 3.0, 4.0])),
                Arc::new(Float64Array::from(vec![10.0, 11.0, 52.0, 53.0])),
            ],
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_analyzer_rule(Arc::new(FunctionHooksRule))
            .build();
        let ctx = SessionContext::new_with_state(state);
        for udf in [in_box(), near_box(), boom_box(), is_east()] {
            ctx.register_udf(udf);
        }
        ctx.register_parquet(
            "points",
            path.to_str().unwrap(),
            ParquetReadOptions::default(),
        )
        .await
        .unwrap();
        ctx
    }

    async fn explain(ctx: &SessionContext, sql: &str) -> String {
        let batches = ctx
            .sql(&format!("EXPLAIN {sql}"))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        arrow::util::pretty::pretty_format_batches(&batches)
            .unwrap()
            .to_string()
    }

    async fn ids(ctx: &SessionContext, sql: &str) -> Vec<i64> {
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        let mut ids: Vec<i64> = batches
            .iter()
            .flat_map(|batch| {
                let ids = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                ids.values().to_vec()
            })
            .collect();
        ids.sort();
        ids
    }

    /// The `row_groups_pruned_statistics` metric of an `EXPLAIN ANALYZE` of `sql`.
    async fn pruned_row_groups(ctx: &SessionContext, sql: &str) -> String {
        let text = explain(ctx, &format!("ANALYZE {sql}")).await;
        let start = text
            .find("row_groups_pruned_statistics=")
            .unwrap_or_else(|| panic!("no pruning metric in {text}"));
        text[start..].split([',', ']']).next().unwrap().to_string()
    }

    const IN_BOX: &str = "SELECT id FROM points WHERE in_box(lon, lat, 2, 50, 8, 56)";

    #[tokio::test]
    async fn explain_shows_the_implied_predicate() {
        let dir = tempfile::tempdir().unwrap();
        let ctx = session(dir.path()).await;
        let plan = explain(&ctx, IN_BOX).await;
        assert!(plan.contains("in_box(points.lon, points.lat"), "{plan}");
        assert!(plan.contains("points.lon >= Float64(2)"), "{plan}");
        assert!(plan.contains("points.lat <= Float64(56)"), "{plan}");
        assert_eq!(ids(&ctx, IN_BOX).await, [3, 4]);
    }

    #[tokio::test]
    async fn the_implied_predicate_prunes_parquet_row_groups() {
        let dir = tempfile::tempdir().unwrap();
        let ctx = session(dir.path()).await;
        let metric = pruned_row_groups(&ctx, IN_BOX).await;
        assert!(metric.contains("2 total → 1 matched"), "{metric}");
    }

    #[tokio::test]
    async fn propagate_gives_column_ranges_without_a_pushdown_rule() {
        let dir = tempfile::tempdir().unwrap();
        let ctx = session(dir.path()).await;
        let sql = "SELECT id FROM points WHERE near_box(lon, lat, 2, 50, 8, 56)";
        let plan = explain(&ctx, sql).await;
        assert!(plan.contains("points.lon >= Float64(2)"), "{plan}");
        assert!(plan.contains("points.lat <= Float64(56)"), "{plan}");
        let metric = pruned_row_groups(&ctx, sql).await;
        assert!(metric.contains("2 total → 1 matched"), "{metric}");
        assert_eq!(ids(&ctx, sql).await, [3, 4]);
    }

    #[tokio::test]
    async fn propagate_reads_a_comparison_with_a_literal() {
        let dir = tempfile::tempdir().unwrap();
        let ctx = session(dir.path()).await;
        let sql = "SELECT id FROM points WHERE near_box(lon, lat, 2, 50, 8, 56) = true";
        let plan = explain(&ctx, sql).await;
        assert!(plan.contains("points.lon >= Float64(2)"), "{plan}");
        assert_eq!(ids(&ctx, sql).await, [3, 4]);
    }

    #[tokio::test]
    async fn an_exact_rule_replaces_the_call() {
        let dir = tempfile::tempdir().unwrap();
        let ctx = session(dir.path()).await;
        let sql = "SELECT id FROM points WHERE is_east(lon)";
        let plan = explain(&ctx, sql).await;
        assert!(!plan.contains("is_east("), "{plan}");
        assert_eq!(ids(&ctx, sql).await, [3, 4]);
    }

    #[tokio::test]
    async fn a_panicking_hook_is_ignored() {
        let dir = tempfile::tempdir().unwrap();
        let ctx = session(dir.path()).await;
        let sql = "SELECT id FROM points WHERE boom_box(lon, lat, 2, 50, 8, 56)";
        let plan = explain(&ctx, sql).await;
        assert!(!plan.contains("points.lon >="), "{plan}");
        let metric = pruned_row_groups(&ctx, sql).await;
        assert!(metric.contains("2 total → 2 matched"), "{metric}");
        assert_eq!(ids(&ctx, sql).await, [3, 4]);
    }

    #[tokio::test]
    async fn an_existing_conjunct_is_not_added_twice() {
        let dir = tempfile::tempdir().unwrap();
        let ctx = session(dir.path()).await;
        let sql = "SELECT id FROM points \
                   WHERE in_box(lon, lat, 2, 50, 8, 56) AND lon BETWEEN 2 AND 8";
        let plan = explain(&ctx, sql).await;
        assert_eq!(
            plan.matches("points.lon >= Float64(2)").count(),
            2,
            "{plan}"
        );
        assert_eq!(ids(&ctx, sql).await, [3, 4]);
    }
}
