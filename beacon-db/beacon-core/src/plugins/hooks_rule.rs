//! Turns filters on functions with planner hooks into plain predicates.
//!
//! The rule runs once per query, after type coercion and before every optimizer rule.
//! So filter pushdown, file statistics pruning, table providers and remote sources
//! see plain column predicates and need no knowledge of the function.

use std::collections::HashMap;
use std::sync::{Arc, Weak};

use beacon_plugin_sdk::scalar_functions::{hooks_of, HookedUdf, Implied};
use datafusion::{
    arrow::datatypes::DataType,
    common::{
        config::ConfigOptions,
        tree_node::{Transformed, TreeNode},
        Column, DFSchema, Result, ScalarValue,
    },
    execution::TaskContext,
    logical_expr::{
        expr::{Between, ScalarFunction},
        interval_arithmetic::Interval,
        utils::{conjunction, split_conjunction_owned},
        BinaryExpr, Cast, Expr, ExprSchemable, Filter, LogicalPlan, Operator, TryCast,
    },
    optimizer::{analyzer::type_coercion::TypeCoercionRewriter, AnalyzerRule},
    prelude::SessionContext,
};

use crate::statement_plan::SessionCell;

/// Applies the `implied` and `propagate` hooks to every filter.
#[derive(Debug)]
pub(crate) struct FunctionHooksRule {
    session: SessionCell,
}

impl FunctionHooksRule {
    /// A rule that decodes hook expressions with the functions of `session`.
    pub(crate) fn new(session: SessionCell) -> Self {
        Self { session }
    }

    fn task_ctx(&self) -> Arc<TaskContext> {
        match self.session.get().and_then(Weak::upgrade) {
            Some(session) => session.task_ctx(),
            // Without a session, hook expressions can use only the built-in functions.
            None => SessionContext::new().task_ctx(),
        }
    }
}

impl AnalyzerRule for FunctionHooksRule {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        let ctx = self.task_ctx();
        plan.transform_up_with_subqueries(|node| match node {
            LogicalPlan::Filter(filter) => rewrite_filter(filter, &ctx),
            other => Ok(Transformed::no(other)),
        })
        .map(|transformed| transformed.data)
    }

    fn name(&self) -> &str {
        "beacon_function_hooks"
    }
}

fn rewrite_filter(filter: Filter, ctx: &Arc<TaskContext>) -> Result<Transformed<LogicalPlan>> {
    let schema = filter.input.schema().clone();
    let mut changed = false;
    let mut conjuncts = Vec::new();
    let mut extra = Vec::new();
    let predicates = split_conjunction_owned(filter.predicate.clone());
    let intervals = Intervals::new(&schema, &predicates);
    for conjunct in predicates {
        if hooked_call(&conjunct).is_none() {
            let rewritten = replace_exact(conjunct, ctx)?;
            changed |= rewritten.transformed;
            extra.extend(intervals.propagated(&rewritten.data));
            conjuncts.push(rewritten.data);
            continue;
        }
        // The arguments first, so the hook of the call sees their `Exact` rules.
        let rewritten = conjunct.map_children(|arg| replace_exact(arg, ctx))?;
        changed |= rewritten.transformed;
        let conjunct = rewritten.data;
        let Some((hooked, args)) = hooked_call(&conjunct) else {
            conjuncts.push(conjunct);
            continue;
        };
        match implied(hooked, args, ctx) {
            Some(Implied::Exact(expr)) => {
                changed = true;
                conjuncts.push(expr);
                continue;
            }
            Some(Implied::Necessary(expr)) => extra.push(expr),
            // The propagate hook is the fallback for a call without a pushdown rule.
            None => extra.extend(intervals.propagated(&conjunct)),
        }
        conjuncts.push(conjunct);
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
fn implied(hooked: &HookedUdf, args: &[Expr], ctx: &Arc<TaskContext>) -> Option<Implied> {
    let args: Vec<Expr> = args
        .iter()
        .map(|arg| constant(arg).map_or_else(|| arg.clone(), |value| Expr::Literal(value, None)))
        .collect();
    hooked.implied(&args, ctx).unwrap_or_else(|error| {
        tracing::warn!(
            "Ignoring the implied hook of '{}' for this query: {error}",
            hooked.inner().name()
        );
        None
    })
}

/// Replaces each call that has an `Exact` rule.
fn replace_exact(expr: Expr, ctx: &Arc<TaskContext>) -> Result<Transformed<Expr>> {
    expr.transform_up(|expr| {
        if let Some((hooked, args)) = hooked_call(&expr) {
            if let Some(Implied::Exact(replacement)) = implied(hooked, args, ctx) {
                return Ok(Transformed::yes(replacement));
            }
        }
        Ok(Transformed::no(expr))
    })
}

/// The input intervals of the hooks in one filter.
struct Intervals<'a> {
    schema: &'a DFSchema,
    /// Column ranges from the plain conjuncts of the filter, such as `x >= 0`.
    known: HashMap<Column, Interval>,
}

impl<'a> Intervals<'a> {
    fn new(schema: &'a DFSchema, conjuncts: &[Expr]) -> Self {
        let mut known: HashMap<Column, Interval> = HashMap::new();
        for (column, interval) in conjuncts.iter().filter_map(column_range) {
            let merged = match known.get(&column) {
                Some(old) => match old.intersect(&interval) {
                    Ok(Some(merged)) => merged,
                    // An empty or mixed-type intersection keeps the old range.
                    _ => continue,
                },
                None => interval,
            };
            known.insert(column, merged);
        }
        Self { schema, known }
    }

    /// The column ranges that the `propagate` hook gives for a conjunct such as
    /// `f(x)` or `f(x) > 5`.
    fn propagated(&self, conjunct: &Expr) -> Vec<Expr> {
        let Some((hooked, args, output)) = constrained_call(conjunct, self.schema) else {
            return Vec::new();
        };
        let mut ranges = Vec::new();
        self.propagate_into(hooked, args, &output, &mut ranges);
        ranges
    }

    /// Adds the column ranges under `args` when the call output is in `output`.
    ///
    /// A nested hooked call gets its argument interval as its own output, so the
    /// ranges reach the columns below it.
    fn propagate_into(
        &self,
        hooked: &HookedUdf,
        args: &[Expr],
        output: &Interval,
        ranges: &mut Vec<Expr>,
    ) {
        let Some(inputs) = args
            .iter()
            .map(|arg| self.interval_of(arg))
            .collect::<Option<Vec<_>>>()
        else {
            return;
        };
        let refs: Vec<&Interval> = inputs.iter().collect();
        let intervals = match hooked.propagate(output, &refs) {
            Ok(Some(intervals)) => intervals,
            Ok(None) => return,
            Err(error) => {
                tracing::warn!(
                    "Ignoring the propagate hook of '{}' for this query: {error}",
                    hooked.inner().name()
                );
                return;
            }
        };
        for ((arg, interval), input) in args.iter().zip(&intervals).zip(&inputs) {
            // An interval that did not narrow gives no range and no reason to recurse.
            if interval != input {
                self.narrow(arg, interval, ranges);
            }
        }
    }

    /// Adds the column ranges that follow from `expr` in `interval`.
    ///
    /// Arithmetic with a constant is inverted, so `x + 1` in `[3, 9]` gives `x` in `[2, 8]`.
    fn narrow(&self, expr: &Expr, interval: &Interval, ranges: &mut Vec<Expr>) {
        match expr {
            Expr::Column(_) => ranges.extend(range(expr, interval)),
            // A cast can lose precision, so the range stays on the cast column.
            Expr::Cast(Cast { expr: inner, .. }) | Expr::TryCast(TryCast { expr: inner, .. }) => {
                if matches!(inner.as_ref(), Expr::Column(_)) {
                    ranges.extend(range(expr, interval));
                }
            }
            Expr::Negative(inner) => {
                if let Ok(negated) = interval.arithmetic_negate() {
                    self.narrow(inner, &negated, ranges);
                }
            }
            Expr::BinaryExpr(binary) => {
                if let Some((inner, interval)) = self.invert(binary, interval) {
                    self.narrow(inner, &interval, ranges);
                }
            }
            _ => {
                if let Some((hooked, args)) = hooked_call(expr) {
                    self.propagate_into(hooked, args, interval, ranges);
                }
            }
        }
    }

    /// The operand of `binary` that is not constant, and its interval when `binary`
    /// is in `interval`.
    fn invert<'e>(
        &self,
        binary: &'e BinaryExpr,
        interval: &Interval,
    ) -> Option<(&'e Expr, Interval)> {
        let BinaryExpr { left, op, right } = binary;
        let (inner, constant_on_right, value) = match (constant(left), constant(right)) {
            (None, Some(value)) => (left.as_ref(), true, value),
            (Some(value), None) => (right.as_ref(), false, value),
            _ => return None,
        };
        // Integer division truncates, and a zero factor loses the operand.
        let zero = ScalarValue::new_zero(&value.data_type()).ok();
        let scales = self.is_float(inner) && !value.is_null() && zero.as_ref() != Some(&value);
        let value = Interval::try_new(value.clone(), value).ok()?;
        let result = match (op, constant_on_right) {
            (Operator::Plus, _) => interval.sub(&value),
            (Operator::Minus, true) => interval.add(&value),
            (Operator::Minus, false) => value.sub(interval),
            (Operator::Multiply, _) if scales => interval.div(&value),
            (Operator::Divide, true) if scales => interval.mul(&value),
            _ => return None,
        };
        Some((inner, result.ok()?))
    }

    fn is_float(&self, expr: &Expr) -> bool {
        matches!(
            expr.get_type(self.schema),
            Ok(DataType::Float16 | DataType::Float32 | DataType::Float64)
        )
    }

    /// The interval of `expr`: a point for a constant, the range of a column from
    /// another conjunct, interval arithmetic, or the `bounds` hook of a hooked call.
    /// Else the unbounded interval of its type.
    fn interval_of(&self, expr: &Expr) -> Option<Interval> {
        if let Some(value) = constant(expr) {
            return Interval::try_new(value.clone(), value).ok();
        }
        let data_type = expr.get_type(self.schema).ok()?;
        let derived = match expr {
            Expr::Column(column) => self.known.get(column).cloned(),
            Expr::Negative(inner) => self
                .interval_of(inner)
                .and_then(|interval| interval.arithmetic_negate().ok()),
            Expr::BinaryExpr(binary) => self.evaluate(binary),
            _ => self.bounds_of(expr),
        };
        // An interval of another type, such as Int64 bounds on a Float64 column, is unusable.
        match derived {
            Some(interval) if interval.data_type() == data_type => Some(interval),
            _ => Interval::make_unbounded(&data_type).ok(),
        }
    }

    /// The interval of `left <op> right` for `+`, `-`, and for floats `*` and `/`.
    fn evaluate(&self, binary: &BinaryExpr) -> Option<Interval> {
        let BinaryExpr { left, op, right } = binary;
        let float = self.is_float(left) && self.is_float(right);
        let (left, right) = (self.interval_of(left)?, self.interval_of(right)?);
        match op {
            Operator::Plus => left.add(right),
            Operator::Minus => left.sub(right),
            Operator::Multiply if float => left.mul(right),
            Operator::Divide if float => left.div(right),
            _ => return None,
        }
        .ok()
    }

    /// The result of the `bounds` hook for a hooked call, with the intervals of its arguments.
    fn bounds_of(&self, expr: &Expr) -> Option<Interval> {
        let (hooked, args) = hooked_call(expr)?;
        let inputs = args
            .iter()
            .map(|arg| self.interval_of(arg))
            .collect::<Option<Vec<_>>>()?;
        let refs: Vec<&Interval> = inputs.iter().collect();
        hooked.bounds(&refs).unwrap_or_else(|error| {
            tracing::warn!(
                "Ignoring the bounds hook of '{}' for this query: {error}",
                hooked.inner().name()
            );
            None
        })
    }
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
    Some((hooked, args, comparison_interval(op, value)?))
}

/// The interval of `x` for `x <op> value`.
fn comparison_interval(op: Operator, value: ScalarValue) -> Option<Interval> {
    if value.is_null() {
        return None;
    }
    let unbounded = ScalarValue::try_from(value.data_type()).ok()?;
    // Closed intervals also hold for `<` and `>`: a wider range is still necessary.
    match op {
        Operator::Eq => Interval::try_new(value.clone(), value),
        Operator::Gt | Operator::GtEq => Interval::try_new(value, unbounded),
        Operator::Lt | Operator::LtEq => Interval::try_new(unbounded, value),
        _ => return None,
    }
    .ok()
}

/// The column and its interval for a conjunct such as `x >= 0` or `x BETWEEN 2 AND 8`.
fn column_range(conjunct: &Expr) -> Option<(Column, Interval)> {
    match conjunct {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            let (column, op, value) = match (left.as_ref(), right.as_ref()) {
                (Expr::Column(column), other) => (column, *op, constant(other)?),
                (other, Expr::Column(column)) => (column, op.swap()?, constant(other)?),
                _ => return None,
            };
            Some((column.clone(), comparison_interval(op, value)?))
        }
        Expr::Between(Between {
            expr,
            negated: false,
            low,
            high,
        }) => {
            let Expr::Column(column) = expr.as_ref() else {
                return None;
            };
            let interval = Interval::try_new(constant(low)?, constant(high)?).ok()?;
            Some((column.clone(), interval))
        }
        _ => None,
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

    /// `flip(x)` is `-x`. Its `Exact` rule replaces the call.
    fn flip() -> ScalarUDF {
        function("flip")
            .args([DataType::Float64])
            .returns(DataType::Float64)
            .implies(|args| {
                Ok(Some(Implied::Exact(Expr::Negative(Box::new(
                    args[0].clone(),
                )))))
            })
            .invoke_arrays(|_| panic!("flip must not run"))
    }

    /// `half(x)` is `x / 2`, with `bounds` and `propagate` hooks and no `implies` rule.
    fn half() -> ScalarUDF {
        let two = || ScalarValue::from(2.0);
        function("half")
            .args([DataType::Float64])
            .returns(DataType::Float64)
            .bounds(move |args| {
                let lower = args[0].lower().div(two())?;
                let upper = args[0].upper().div(two())?;
                Ok(Some(Interval::try_new(lower, upper)?))
            })
            .propagate(move |output, args| {
                let lower = output.lower().mul(two())?;
                let upper = output.upper().mul(two())?;
                let x = args[0].intersect(Interval::try_new(lower, upper)?)?;
                Ok(x.map(|x| vec![x]))
            })
            .invoke_arrays(|arrays| {
                let halves: Float64Array = f64s(&arrays[0])
                    .iter()
                    .map(|v| v.map(|v| v / 2.0))
                    .collect();
                Ok(Arc::new(halves))
            })
    }

    /// `bad_half(x)` is `half(x)`, but its `bounds` hook panics.
    fn bad_half() -> ScalarUDF {
        function("bad_half")
            .args([DataType::Float64])
            .returns(DataType::Float64)
            .bounds(|_| panic!("bounds exploded"))
            .invoke_arrays(|arrays| {
                let halves: Float64Array = f64s(&arrays[0])
                    .iter()
                    .map(|v| v.map(|v| v / 2.0))
                    .collect();
                Ok(Arc::new(halves))
            })
    }

    /// `gap(a, b)` is `a - b`. Its `propagate` hook uses the interval of each argument.
    fn gap() -> ScalarUDF {
        function("gap")
            .args([DataType::Float64, DataType::Float64])
            .returns(DataType::Float64)
            .propagate(|output, args| {
                let (Some(a), Some(b)) = (
                    args[0].intersect(args[1].add(output)?)?,
                    args[1].intersect(args[0].sub(output)?)?,
                ) else {
                    return Ok(None);
                };
                Ok(Some(vec![a, b]))
            })
            .invoke_arrays(|arrays| {
                let (a, b) = (f64s(&arrays[0]), f64s(&arrays[1]));
                let gaps: Float64Array = (0..a.len())
                    .map(|i| Some(a.value(i) - b.value(i)))
                    .collect();
                Ok(Arc::new(gaps))
            })
    }

    /// A session with the rule, the test functions and table `points`: one Parquet
    /// file with two row groups, the first in the west and the second in the east.
    async fn session(dir: &std::path::Path) -> Arc<SessionContext> {
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

        let cell = crate::statement_plan::new_session_cell();
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_analyzer_rule(Arc::new(FunctionHooksRule::new(cell.clone())))
            .build();
        let ctx = Arc::new(SessionContext::new_with_state(state));
        cell.set(Arc::downgrade(&ctx)).unwrap();
        for udf in [
            in_box(),
            near_box(),
            boom_box(),
            is_east(),
            flip(),
            half(),
            bad_half(),
            gap(),
        ] {
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
    async fn the_hook_of_a_call_sees_the_exact_rules_of_its_arguments() {
        let dir = tempfile::tempdir().unwrap();
        let ctx = session(dir.path()).await;
        let sql = "SELECT id FROM points WHERE in_box(flip(lon), lat, -8, 50, -2, 56)";
        let plan = explain(&ctx, sql).await;
        assert!(!plan.contains("flip("), "{plan}");
        assert!(plan.contains("(- points.lon) >= Float64(-8)"), "{plan}");
        assert_eq!(ids(&ctx, sql).await, [3, 4]);
    }

    #[tokio::test]
    async fn propagate_reaches_a_column_through_a_nested_call() {
        let dir = tempfile::tempdir().unwrap();
        let ctx = session(dir.path()).await;
        let sql = "SELECT id FROM points WHERE near_box(half(lon), lat, 1, 50, 4, 56)";
        let plan = explain(&ctx, sql).await;
        assert!(plan.contains("points.lon >= Float64(2)"), "{plan}");
        assert!(plan.contains("points.lon <= Float64(8)"), "{plan}");
        let metric = pruned_row_groups(&ctx, sql).await;
        assert!(metric.contains("2 total → 1 matched"), "{metric}");
        assert_eq!(ids(&ctx, sql).await, [3, 4]);
    }

    #[tokio::test]
    async fn bounds_gives_the_interval_of_a_nested_call() {
        let dir = tempfile::tempdir().unwrap();
        let ctx = session(dir.path()).await;
        let sql = "SELECT id FROM points WHERE near_box(lon, lat, half(4), 50, half(16), 56)";
        let plan = explain(&ctx, sql).await;
        assert!(plan.contains("points.lon >= Float64(2)"), "{plan}");
        assert!(plan.contains("points.lon <= Float64(8)"), "{plan}");
        assert_eq!(ids(&ctx, sql).await, [3, 4]);
    }

    #[tokio::test]
    async fn propagate_reaches_a_column_through_addition() {
        let dir = tempfile::tempdir().unwrap();
        let ctx = session(dir.path()).await;
        let sql = "SELECT id FROM points WHERE near_box(lon + 1, lat, 3, 50, 9, 56)";
        let plan = explain(&ctx, sql).await;
        // On Windows, DataFusion rounds each bound one ulp outward; elsewhere it is exact.
        assert!(
            plan.contains("points.lon >= Float64(2)")
                || plan.contains("points.lon >= Float64(1.99999"),
            "{plan}"
        );
        assert!(
            plan.contains("points.lon <= Float64(8)")
                || plan.contains("points.lon <= Float64(8.00000"),
            "{plan}"
        );
        let metric = pruned_row_groups(&ctx, sql).await;
        assert!(metric.contains("2 total → 1 matched"), "{metric}");
        assert_eq!(ids(&ctx, sql).await, [3, 4]);
    }

    #[tokio::test]
    async fn propagate_reaches_a_column_through_multiplication() {
        let dir = tempfile::tempdir().unwrap();
        let ctx = session(dir.path()).await;
        let sql = "SELECT id FROM points WHERE near_box(lon * 2, lat, 4, 50, 16, 56)";
        let plan = explain(&ctx, sql).await;
        assert!(plan.contains("points.lon >= Float64("), "{plan}");
        assert!(plan.contains("points.lon <= Float64("), "{plan}");
        let metric = pruned_row_groups(&ctx, sql).await;
        assert!(metric.contains("2 total → 1 matched"), "{metric}");
        assert_eq!(ids(&ctx, sql).await, [3, 4]);
    }

    #[tokio::test]
    async fn propagate_gives_a_range_on_a_cast_column() {
        let dir = tempfile::tempdir().unwrap();
        let ctx = session(dir.path()).await;
        let sql = "SELECT id FROM points WHERE near_box(CAST(id AS DOUBLE), lat, 3, 50, 9, 56)";
        let plan = explain(&ctx, sql).await;
        assert!(
            plan.contains("points.id >= Int64(3)")
                || plan.contains("CAST(points.id AS Float64) >= Float64(3)"),
            "{plan}"
        );
        assert_eq!(ids(&ctx, sql).await, [3, 4]);
    }

    #[tokio::test]
    async fn another_conjunct_gives_the_input_interval_of_a_column() {
        let dir = tempfile::tempdir().unwrap();
        let ctx = session(dir.path()).await;
        let alone = "SELECT id FROM points WHERE gap(lat, lon) > 45";
        let plan = explain(&ctx, alone).await;
        assert!(!plan.contains("points.lat >= "), "{plan}");
        let sql = "SELECT id FROM points WHERE gap(lat, lon) > 45 AND lon >= 0";
        let plan = explain(&ctx, sql).await;
        // On Windows, DataFusion rounds the bound one ulp outward; elsewhere it is exact.
        assert!(
            plan.contains("points.lat >= Float64(45)")
                || plan.contains("points.lat >= Float64(44.9999"),
            "{plan}"
        );
        let metric = pruned_row_groups(&ctx, sql).await;
        assert!(metric.contains("2 total → 1 matched"), "{metric}");
        assert_eq!(ids(&ctx, sql).await, [3, 4]);
    }

    #[tokio::test]
    async fn a_panicking_bounds_hook_gives_an_unbounded_interval() {
        let dir = tempfile::tempdir().unwrap();
        let ctx = session(dir.path()).await;
        let sql = "SELECT id FROM points WHERE near_box(lon, lat, bad_half(4), 50, 8, 56)";
        let plan = explain(&ctx, sql).await;
        assert!(!plan.contains("points.lon >= "), "{plan}");
        assert!(plan.contains("points.lon <= Float64(8)"), "{plan}");
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
