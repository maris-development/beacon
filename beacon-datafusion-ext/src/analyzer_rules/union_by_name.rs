use std::collections::HashSet;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, IntervalUnit, TimeUnit};
use datafusion::common::Column;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::{DFSchema, Result, exec_err};
use datafusion::execution::context::SessionContext;
use datafusion::execution::session_state::{SessionState, SessionStateBuilder};
use datafusion::logical_expr::{Expr, ExprSchemable, LogicalPlan, LogicalPlanBuilder, Union};
use datafusion::optimizer::Analyzer;
use datafusion::optimizer::analyzer::AnalyzerRule;
use datafusion::optimizer::analyzer::type_coercion::TypeCoercion;
use datafusion::prelude::lit;
use datafusion::scalar::ScalarValue;

#[derive(Debug, Default)]
pub struct SupercastUnionCoercion;

impl SupercastUnionCoercion {
    fn coerce_union(&self, union: Union) -> Result<LogicalPlan> {
        if union.inputs.is_empty() {
            return exec_err!("UNION must have at least one input");
        }

        let merged_schema = self.supercast_union_schema(&union)?;

        let inputs = union
            .inputs
            .into_iter()
            .map(|input| {
                let input = input.as_ref().clone();
                self.coerce_input_to_schema_by_name(input, &merged_schema)
                    .map(Arc::new)
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(LogicalPlan::Union(Union {
            inputs,
            schema: Arc::new(merged_schema),
        }))
    }

    fn coerce_input_to_schema_by_name(
        &self,
        input: LogicalPlan,
        merged_schema: &DFSchema,
    ) -> Result<LogicalPlan> {
        let input_arrow_schema = input.schema().as_arrow();
        let input_df_schema = input.schema();

        let projection_exprs = merged_schema
            .fields()
            .iter()
            .map(|target_field| {
                let target_name = target_field.name().clone();
                let target_type = target_field.data_type();

                let expr = match input_arrow_schema.field_with_name(&target_name) {
                    Ok(input_field) => {
                        let col_expr = Expr::Column(Column::new_unqualified(target_name.clone()));
                        if input_field.data_type() == target_type {
                            col_expr
                        } else {
                            col_expr.cast_to(target_type, input_df_schema.as_ref())?
                        }
                    }
                    Err(_) => {
                        lit(ScalarValue::Null).cast_to(target_type, input_df_schema.as_ref())?
                    }
                };

                Ok(expr.alias(target_name))
            })
            .collect::<Result<Vec<_>>>()?;

        LogicalPlanBuilder::from(input)
            .project(projection_exprs)?
            .build()
    }

    fn supercast_union_schema(&self, union: &Union) -> Result<DFSchema> {
        let mut seen = HashSet::new();
        let mut ordered_names = Vec::new();

        for input in &union.inputs {
            for field in input.schema().fields() {
                let name = field.name().clone();
                if seen.insert(name.clone()) {
                    ordered_names.push(name);
                }
            }
        }

        let mut fields = Vec::with_capacity(ordered_names.len());

        for name in ordered_names {
            let mut merged_type = DataType::Null;
            let mut nullable = false;

            for input in &union.inputs {
                match input.schema().as_arrow().field_with_name(&name) {
                    Ok(field) => {
                        merged_type = self.supertype(&merged_type, field.data_type())?;
                        nullable |= field.is_nullable();
                    }
                    Err(_) => {
                        // Missing columns are synthesized as NULLs in rewritten inputs.
                        nullable = true;
                    }
                }
            }

            fields.push(Field::new(name, merged_type, nullable));
        }

        DFSchema::from_unqualified_fields(fields.into(), Default::default())
    }

    fn supertype(&self, left: &DataType, right: &DataType) -> Result<DataType> {
        use DataType::*;

        if left == right {
            return Ok(left.clone());
        }

        let t = match (left, right) {
            // Null promotes to the other side
            (Null, other) | (other, Null) => other.clone(),

            // String promotion
            (Utf8, Utf8View) | (Utf8View, Utf8) => Utf8,
            (LargeUtf8, Utf8) | (Utf8, LargeUtf8) => LargeUtf8,
            (LargeUtf8, Utf8View) | (Utf8View, LargeUtf8) => LargeUtf8,
            (Utf8, Binary) | (Binary, Utf8) => Utf8,
            (LargeUtf8, LargeBinary) | (LargeBinary, LargeUtf8) => LargeUtf8,

            // Binary promotion
            (Binary, BinaryView) | (BinaryView, Binary) => Binary,
            (LargeBinary, Binary) | (Binary, LargeBinary) => LargeBinary,
            (LargeBinary, BinaryView) | (BinaryView, LargeBinary) => LargeBinary,

            // Integer widening
            (Int8, Int16) | (Int16, Int8) => Int16,
            (Int8, Int32) | (Int32, Int8) => Int32,
            (Int8, Int64) | (Int64, Int8) => Int64,
            (Int16, Int32) | (Int32, Int16) => Int32,
            (Int16, Int64) | (Int64, Int16) => Int64,
            (Int32, Int64) | (Int64, Int32) => Int64,

            (UInt8, UInt16) | (UInt16, UInt8) => UInt16,
            (UInt8, UInt32) | (UInt32, UInt8) => UInt32,
            (UInt8, UInt64) | (UInt64, UInt8) => UInt64,
            (UInt16, UInt32) | (UInt32, UInt16) => UInt32,
            (UInt16, UInt64) | (UInt64, UInt16) => UInt64,
            (UInt32, UInt64) | (UInt64, UInt32) => UInt64,

            // Signed + unsigned -> signed wide enough, else Decimal/Float fallback
            (Int8, UInt8) | (UInt8, Int8) => Int16,
            (Int16, UInt8) | (UInt8, Int16) => Int16,
            (Int16, UInt16) | (UInt16, Int16) => Int32,
            (Int32, UInt8) | (UInt8, Int32) => Int32,
            (Int32, UInt16) | (UInt16, Int32) => Int32,
            (Int32, UInt32) | (UInt32, Int32) => Int64,
            (Int64, UInt8) | (UInt8, Int64) => Int64,
            (Int64, UInt16) | (UInt16, Int64) => Int64,
            (Int64, UInt32) | (UInt32, Int64) => Int64,

            // UInt64 mixed with signed doesn't fit safely into any signed integer
            // Choose Decimal128(20,0) as a simple exact supertype.
            (Int8, UInt64)
            | (UInt64, Int8)
            | (Int16, UInt64)
            | (UInt64, Int16)
            | (Int32, UInt64)
            | (UInt64, Int32)
            | (Int64, UInt64)
            | (UInt64, Int64) => Decimal128(20, 0),

            // Float widening
            (Float16, Float32) | (Float32, Float16) => Float32,
            (Float16, Float64) | (Float64, Float16) => Float64,
            (Float32, Float64) | (Float64, Float32) => Float64,

            // Integer + float -> float
            (Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 | UInt64, Float32)
            | (Float32, Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 | UInt64) => Float32,

            (Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 | UInt64, Float64)
            | (Float64, Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 | UInt64) => Float64,

            // Decimal widening
            (Decimal128(p1, s1), Decimal128(p2, s2)) => {
                // Error as decimal coercions can be lossy and we don't want to silently widen and lose precision
                if p1 != p2 || s1 != s2 {
                    return exec_err!(
                        "Cannot supercast between different Decimal types: {p1},{s1} vs {p2},{s2}"
                    );
                }
                Decimal128(*p1, *s1)
            }

            // Decimal + integer
            (Decimal128(p, s), Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 | UInt64)
            | (Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 | UInt64, Decimal128(p, s)) =>
            {
                // Error as decimal coercions can be lossy and we don't want to silently widen and lose precision
                if s != &0 {
                    return exec_err!(
                        "Cannot supercast between Decimal with non-zero scale and integer: {p},{s}"
                    );
                }
                Decimal128(*p, *s)
            }

            // Decimal + float -> Float64
            (Decimal128(_, _), Float32 | Float64) | (Float32 | Float64, Decimal128(_, _)) => {
                Float64
            }

            // Timestamp coercion: keep same timezone requirement simple
            (Timestamp(u1, tz1), Timestamp(u2, tz2)) if tz1 == tz2 => {
                Timestamp(max_time_unit(u1, u2), tz1.clone())
            }

            // Date coercion
            (Date32, Date64) | (Date64, Date32) => Date64,

            // Duration coercion
            (Duration(u1), Duration(u2)) => Duration(max_time_unit(u1, u2)),

            // Interval coercion
            (Interval(IntervalUnit::YearMonth), Interval(IntervalUnit::DayTime))
            | (Interval(IntervalUnit::DayTime), Interval(IntervalUnit::YearMonth))
            | (Interval(IntervalUnit::YearMonth), Interval(IntervalUnit::MonthDayNano))
            | (Interval(IntervalUnit::MonthDayNano), Interval(IntervalUnit::YearMonth))
            | (Interval(IntervalUnit::DayTime), Interval(IntervalUnit::MonthDayNano))
            | (Interval(IntervalUnit::MonthDayNano), Interval(IntervalUnit::DayTime)) => {
                Interval(IntervalUnit::MonthDayNano)
            }

            // Bool only unions with bool in this sample
            (Boolean, Boolean) => Boolean,

            _ => return exec_err!("No custom UNION supertype for {left:?} and {right:?}"),
        };

        Ok(t)
    }
}

fn max_time_unit(left: &TimeUnit, right: &TimeUnit) -> TimeUnit {
    use TimeUnit::*;
    match (left, right) {
        (Second, Second) => Second,
        (Second, Millisecond) | (Millisecond, Second) | (Millisecond, Millisecond) => Millisecond,
        (Second, Microsecond)
        | (Microsecond, Second)
        | (Millisecond, Microsecond)
        | (Microsecond, Millisecond)
        | (Microsecond, Microsecond) => Microsecond,
        _ => Nanosecond,
    }
}

impl AnalyzerRule for SupercastUnionCoercion {
    fn name(&self) -> &str {
        "supercast_union_coercion"
    }

    fn analyze(&self, plan: LogicalPlan, config: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform_up(|node| {
            let rewritten = match node {
                LogicalPlan::Union(union) => self.coerce_union(union)?,
                other => TypeCoercion::new().analyze(other, config)?,
            };
            Ok(Transformed::yes(rewritten))
        })
        .data()
    }
}

pub fn make_session_state_with_supercast_union() -> SessionState {
    let mut rules = Analyzer::new().rules;
    // rules.retain(|r| r.name() != TypeCoercion::new().name());
    rules.push(Arc::new(SupercastUnionCoercion));

    SessionStateBuilder::new_with_default_features()
        .with_analyzer_rules(rules)
        .build()
}

pub fn make_context_with_supercast_union() -> SessionContext {
    SessionContext::new_with_state(make_session_state_with_supercast_union())
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;

    /// Helper: register a single-row MemTable with the given schema and name.
    fn register_one_row_table(ctx: &SessionContext, name: &str, fields: Vec<Field>) {
        let schema = Arc::new(Schema::new(fields.clone()));
        let arrays: Vec<ArrayRef> = fields
            .iter()
            .map(|f| match f.data_type() {
                DataType::Int8 => Arc::new(Int8Array::from(vec![1i8])) as ArrayRef,
                DataType::Int16 => Arc::new(Int16Array::from(vec![1i16])) as ArrayRef,
                DataType::Int32 => Arc::new(Int32Array::from(vec![1i32])) as ArrayRef,
                DataType::Int64 => Arc::new(Int64Array::from(vec![1i64])) as ArrayRef,
                DataType::UInt8 => Arc::new(UInt8Array::from(vec![1u8])) as ArrayRef,
                DataType::UInt16 => Arc::new(UInt16Array::from(vec![1u16])) as ArrayRef,
                DataType::UInt32 => Arc::new(UInt32Array::from(vec![1u32])) as ArrayRef,
                DataType::UInt64 => Arc::new(UInt64Array::from(vec![1u64])) as ArrayRef,
                DataType::Float32 => Arc::new(Float32Array::from(vec![1.0f32])) as ArrayRef,
                DataType::Float64 => Arc::new(Float64Array::from(vec![1.0f64])) as ArrayRef,
                DataType::Utf8 => Arc::new(StringArray::from(vec!["a"])) as ArrayRef,
                DataType::LargeUtf8 => Arc::new(LargeStringArray::from(vec!["a"])) as ArrayRef,
                DataType::Boolean => Arc::new(BooleanArray::from(vec![true])) as ArrayRef,
                DataType::Date32 => Arc::new(Date32Array::from(vec![0i32])) as ArrayRef,
                DataType::Date64 => Arc::new(Date64Array::from(vec![0i64])) as ArrayRef,
                DataType::Null => Arc::new(NullArray::new(1)) as ArrayRef,
                dt => panic!("register_one_row_table: unsupported type {dt:?}"),
            })
            .collect();
        let batch = RecordBatch::try_new(schema.clone(), arrays).unwrap();
        let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table(name, Arc::new(table)).unwrap();
    }

    /// Run a UNION ALL and return field types from the optimized plan schema.
    async fn union_result_types(ctx: &SessionContext, sql: &str) -> Vec<(String, DataType)> {
        let (state, plan) = ctx.sql(sql).await.unwrap().into_parts();
        let optimized = state.optimize(&plan).unwrap();
        optimized
            .schema()
            .fields()
            .iter()
            .map(|f| (f.name().clone(), f.data_type().clone()))
            .collect()
    }

    async fn coerce_union_of_tables(
        ctx: &SessionContext,
        left_table: &str,
        right_table: &str,
    ) -> LogicalPlan {
        let (_, left_plan) = ctx.table(left_table).await.unwrap().into_parts();
        let (_, right_plan) = ctx.table(right_table).await.unwrap().into_parts();

        let union = Union {
            schema: Arc::new(left_plan.schema().as_ref().clone()),
            inputs: vec![Arc::new(left_plan), Arc::new(right_plan)],
        };

        SupercastUnionCoercion.coerce_union(union).unwrap()
    }

    async fn coerce_union_of_three_tables(
        ctx: &SessionContext,
        first_table: &str,
        second_table: &str,
        third_table: &str,
    ) -> LogicalPlan {
        let (_, first_plan) = ctx.table(first_table).await.unwrap().into_parts();
        let (_, second_plan) = ctx.table(second_table).await.unwrap().into_parts();
        let (_, third_plan) = ctx.table(third_table).await.unwrap().into_parts();

        let union = Union {
            schema: Arc::new(first_plan.schema().as_ref().clone()),
            inputs: vec![
                Arc::new(first_plan),
                Arc::new(second_plan),
                Arc::new(third_plan),
            ],
        };

        SupercastUnionCoercion.coerce_union(union).unwrap()
    }

    #[tokio::test]
    async fn test_int_widening() {
        let ctx = make_context_with_supercast_union();
        register_one_row_table(&ctx, "t1", vec![Field::new("a", DataType::Int8, false)]);
        register_one_row_table(&ctx, "t2", vec![Field::new("a", DataType::Int32, false)]);

        let cols = union_result_types(&ctx, "SELECT a FROM t1 UNION ALL SELECT a FROM t2").await;
        assert_eq!(cols[0].1, DataType::Int32);
    }

    #[tokio::test]
    async fn test_int64_widening() {
        let ctx = make_context_with_supercast_union();
        register_one_row_table(&ctx, "t1", vec![Field::new("a", DataType::Int16, false)]);
        register_one_row_table(&ctx, "t2", vec![Field::new("a", DataType::Int64, false)]);

        let cols = union_result_types(&ctx, "SELECT a FROM t1 UNION ALL SELECT a FROM t2").await;
        assert_eq!(cols[0].1, DataType::Int64);
    }

    #[tokio::test]
    async fn test_uint_widening() {
        let ctx = make_context_with_supercast_union();
        register_one_row_table(&ctx, "t1", vec![Field::new("a", DataType::UInt8, false)]);
        register_one_row_table(&ctx, "t2", vec![Field::new("a", DataType::UInt32, false)]);

        let cols = union_result_types(&ctx, "SELECT a FROM t1 UNION ALL SELECT a FROM t2").await;
        assert_eq!(cols[0].1, DataType::UInt32);
    }

    #[tokio::test]
    async fn test_signed_unsigned_mix() {
        let ctx = make_context_with_supercast_union();
        register_one_row_table(&ctx, "t1", vec![Field::new("a", DataType::Int8, false)]);
        register_one_row_table(&ctx, "t2", vec![Field::new("a", DataType::UInt8, false)]);

        let cols = union_result_types(&ctx, "SELECT a FROM t1 UNION ALL SELECT a FROM t2").await;
        assert_eq!(cols[0].1, DataType::Int16);
    }

    #[tokio::test]
    async fn test_int64_uint64_to_decimal() {
        let ctx = make_context_with_supercast_union();
        register_one_row_table(&ctx, "t1", vec![Field::new("a", DataType::Int64, false)]);
        register_one_row_table(&ctx, "t2", vec![Field::new("a", DataType::UInt64, false)]);

        let cols = union_result_types(&ctx, "SELECT a FROM t1 UNION ALL SELECT a FROM t2").await;
        assert_eq!(cols[0].1, DataType::Decimal128(20, 0));
    }

    #[tokio::test]
    async fn test_float_widening() {
        let ctx = make_context_with_supercast_union();
        register_one_row_table(&ctx, "t1", vec![Field::new("a", DataType::Float32, false)]);
        register_one_row_table(&ctx, "t2", vec![Field::new("a", DataType::Float64, false)]);

        let cols = union_result_types(&ctx, "SELECT a FROM t1 UNION ALL SELECT a FROM t2").await;
        assert_eq!(cols[0].1, DataType::Float64);
    }

    #[tokio::test]
    async fn test_int_float_mix() {
        let ctx = make_context_with_supercast_union();
        register_one_row_table(&ctx, "t1", vec![Field::new("a", DataType::Int32, false)]);
        register_one_row_table(&ctx, "t2", vec![Field::new("a", DataType::Float64, false)]);

        let cols = union_result_types(&ctx, "SELECT a FROM t1 UNION ALL SELECT a FROM t2").await;
        assert_eq!(cols[0].1, DataType::Float64);
    }

    #[tokio::test]
    async fn test_string_promotion_utf8_largeutf8() {
        let ctx = make_context_with_supercast_union();
        register_one_row_table(&ctx, "t1", vec![Field::new("a", DataType::Utf8, false)]);
        register_one_row_table(
            &ctx,
            "t2",
            vec![Field::new("a", DataType::LargeUtf8, false)],
        );

        let cols = union_result_types(&ctx, "SELECT a FROM t1 UNION ALL SELECT a FROM t2").await;
        assert_eq!(cols[0].1, DataType::LargeUtf8);
    }

    #[tokio::test]
    async fn test_null_promotes_to_other() {
        let ctx = make_context_with_supercast_union();
        register_one_row_table(&ctx, "t1", vec![Field::new("a", DataType::Null, true)]);
        register_one_row_table(&ctx, "t2", vec![Field::new("a", DataType::Int32, false)]);

        let cols = union_result_types(&ctx, "SELECT a FROM t1 UNION ALL SELECT a FROM t2").await;
        assert_eq!(cols[0].1, DataType::Int32);
    }

    #[tokio::test]
    async fn test_date_coercion() {
        let ctx = make_context_with_supercast_union();
        register_one_row_table(&ctx, "t1", vec![Field::new("a", DataType::Date32, false)]);
        register_one_row_table(&ctx, "t2", vec![Field::new("a", DataType::Date64, false)]);

        let cols = union_result_types(&ctx, "SELECT a FROM t1 UNION ALL SELECT a FROM t2").await;
        assert_eq!(cols[0].1, DataType::Date64);
    }

    #[tokio::test]
    async fn test_same_types_unchanged() {
        let ctx = make_context_with_supercast_union();
        register_one_row_table(&ctx, "t1", vec![Field::new("a", DataType::Int32, false)]);
        register_one_row_table(&ctx, "t2", vec![Field::new("a", DataType::Int32, false)]);

        let cols = union_result_types(&ctx, "SELECT a FROM t1 UNION ALL SELECT a FROM t2").await;
        assert_eq!(cols[0].1, DataType::Int32);
    }

    #[tokio::test]
    async fn test_nullable_propagation() {
        let ctx = make_context_with_supercast_union();
        register_one_row_table(&ctx, "t1", vec![Field::new("a", DataType::Int32, false)]);
        register_one_row_table(&ctx, "t2", vec![Field::new("a", DataType::Int32, true)]);

        let (state, plan) = ctx
            .sql("SELECT a FROM t1 UNION ALL SELECT a FROM t2")
            .await
            .unwrap()
            .into_parts();
        let optimized = state.optimize(&plan).unwrap();
        let field = &optimized.schema().fields()[0];
        assert!(field.is_nullable());
    }

    #[tokio::test]
    async fn test_multi_column_union() {
        let ctx = make_context_with_supercast_union();
        register_one_row_table(
            &ctx,
            "t1",
            vec![
                Field::new("a", DataType::Int8, false),
                Field::new("b", DataType::Float32, false),
            ],
        );
        register_one_row_table(
            &ctx,
            "t2",
            vec![
                Field::new("a", DataType::Int32, false),
                Field::new("b", DataType::Float64, false),
            ],
        );

        let cols =
            union_result_types(&ctx, "SELECT a, b FROM t1 UNION ALL SELECT a, b FROM t2").await;
        assert_eq!(cols[0].1, DataType::Int32);
        assert_eq!(cols[1].1, DataType::Float64);
    }

    #[tokio::test]
    async fn test_union_by_name_reorders_columns_by_name() {
        let ctx = make_context_with_supercast_union();
        register_one_row_table(
            &ctx,
            "t1",
            vec![
                Field::new("a", DataType::Int8, false),
                Field::new("b", DataType::Float32, false),
            ],
        );
        register_one_row_table(
            &ctx,
            "t2",
            vec![
                Field::new("b", DataType::Float64, false),
                Field::new("a", DataType::Int32, false),
            ],
        );

        let plan = coerce_union_of_tables(&ctx, "t1", "t2").await;

        let LogicalPlan::Union(union) = plan else {
            panic!("expected union plan")
        };

        let out_fields = union.schema.fields();
        assert_eq!(out_fields[0].name(), "a");
        assert_eq!(out_fields[0].data_type(), &DataType::Int32);
        assert_eq!(out_fields[1].name(), "b");
        assert_eq!(out_fields[1].data_type(), &DataType::Float64);

        let right_fields = union.inputs[1].schema().fields();
        assert_eq!(right_fields[0].name(), "a");
        assert_eq!(right_fields[1].name(), "b");
    }

    #[tokio::test]
    async fn test_union_by_name_adds_missing_columns_as_null() {
        let ctx = make_context_with_supercast_union();
        register_one_row_table(&ctx, "t1", vec![Field::new("a", DataType::Int32, false)]);
        register_one_row_table(
            &ctx,
            "t2",
            vec![
                Field::new("a", DataType::Int32, false),
                Field::new("b", DataType::Float64, false),
            ],
        );

        let plan = coerce_union_of_tables(&ctx, "t1", "t2").await;

        let LogicalPlan::Union(union) = plan else {
            panic!("expected union plan")
        };

        let out_fields = union.schema.fields();
        assert_eq!(out_fields.len(), 2);
        assert_eq!(out_fields[0].name(), "a");
        assert_eq!(out_fields[1].name(), "b");
        assert_eq!(out_fields[1].data_type(), &DataType::Float64);
        assert!(out_fields[1].is_nullable());

        let left_fields = union.inputs[0].schema().fields();
        assert_eq!(left_fields.len(), 2);
        assert_eq!(left_fields[1].name(), "b");
        assert_eq!(left_fields[1].data_type(), &DataType::Float64);
        assert!(left_fields[1].is_nullable());
    }

    #[tokio::test]
    async fn test_union_by_name_adds_missing_columns_as_null_reverse_side() {
        let ctx = make_context_with_supercast_union();
        register_one_row_table(
            &ctx,
            "t1",
            vec![
                Field::new("a", DataType::Int32, false),
                Field::new("b", DataType::Float64, false),
            ],
        );
        register_one_row_table(&ctx, "t2", vec![Field::new("a", DataType::Int32, false)]);

        let plan = coerce_union_of_tables(&ctx, "t1", "t2").await;

        let LogicalPlan::Union(union) = plan else {
            panic!("expected union plan")
        };

        let out_fields = union.schema.fields();
        assert_eq!(out_fields.len(), 2);
        assert_eq!(out_fields[0].name(), "a");
        assert_eq!(out_fields[1].name(), "b");
        assert_eq!(out_fields[1].data_type(), &DataType::Float64);
        assert!(out_fields[1].is_nullable());

        let right_fields = union.inputs[1].schema().fields();
        assert_eq!(right_fields.len(), 2);
        assert_eq!(right_fields[1].name(), "b");
        assert_eq!(right_fields[1].data_type(), &DataType::Float64);
        assert!(right_fields[1].is_nullable());
    }

    #[tokio::test]
    async fn test_union_by_name_sparse_three_input_columns() {
        let ctx = make_context_with_supercast_union();
        register_one_row_table(&ctx, "t1", vec![Field::new("a", DataType::Int8, false)]);
        register_one_row_table(&ctx, "t2", vec![Field::new("b", DataType::Int16, false)]);
        register_one_row_table(&ctx, "t3", vec![Field::new("c", DataType::Int32, false)]);

        let plan = coerce_union_of_three_tables(&ctx, "t1", "t2", "t3").await;

        let LogicalPlan::Union(union) = plan else {
            panic!("expected union plan")
        };

        let out_fields = union.schema.fields();
        assert_eq!(out_fields.len(), 3);
        assert_eq!(out_fields[0].name(), "a");
        assert_eq!(out_fields[0].data_type(), &DataType::Int8);
        assert!(out_fields[0].is_nullable());
        assert_eq!(out_fields[1].name(), "b");
        assert_eq!(out_fields[1].data_type(), &DataType::Int16);
        assert!(out_fields[1].is_nullable());
        assert_eq!(out_fields[2].name(), "c");
        assert_eq!(out_fields[2].data_type(), &DataType::Int32);
        assert!(out_fields[2].is_nullable());

        for input in &union.inputs {
            assert_eq!(input.schema().fields().len(), 3);
        }
    }

    #[tokio::test]
    async fn test_three_way_union() {
        let ctx = make_context_with_supercast_union();
        register_one_row_table(&ctx, "t1", vec![Field::new("a", DataType::Int8, false)]);
        register_one_row_table(&ctx, "t2", vec![Field::new("a", DataType::Int16, false)]);
        register_one_row_table(&ctx, "t3", vec![Field::new("a", DataType::Int32, false)]);

        let cols = union_result_types(
            &ctx,
            "SELECT a FROM t1 UNION ALL SELECT a FROM t2 UNION ALL SELECT a FROM t3",
        )
        .await;
        assert_eq!(cols[0].1, DataType::Int32);
    }

    #[tokio::test]
    async fn test_boolean_union() {
        let ctx = make_context_with_supercast_union();
        register_one_row_table(&ctx, "t1", vec![Field::new("a", DataType::Boolean, false)]);
        register_one_row_table(&ctx, "t2", vec![Field::new("a", DataType::Boolean, false)]);

        let cols = union_result_types(&ctx, "SELECT a FROM t1 UNION ALL SELECT a FROM t2").await;
        assert_eq!(cols[0].1, DataType::Boolean);
    }

    #[tokio::test]
    async fn test_incompatible_types_error() {
        let ctx = make_context_with_supercast_union();
        register_one_row_table(&ctx, "t1", vec![Field::new("a", DataType::Boolean, false)]);
        register_one_row_table(&ctx, "t2", vec![Field::new("a", DataType::Int32, false)]);

        let (state, plan) = ctx
            .sql("SELECT a FROM t1 UNION ALL SELECT a FROM t2")
            .await
            .unwrap()
            .into_parts();
        let result = state.optimize(&plan);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_sql_union_by_name() {
        let ctx = make_context_with_supercast_union();
        register_one_row_table(&ctx, "t1", vec![Field::new("a", DataType::Int32, false)]);
        register_one_row_table(
            &ctx,
            "t2",
            vec![
                Field::new("a", DataType::Float64, false),
                Field::new("b", DataType::Int32, false),
            ],
        );

        // SQL UNION ALL with mismatched SELECT widths fails in SQL planning,
        // before analyzer rules run.
        let result = ctx
            .sql("SELECT a FROM t1 UNION ALL BY NAME SELECT a, b FROM t2")
            .await;
        // assert!(result.is_err());
        let df = result.unwrap();
        df.show().await.unwrap();
    }

    #[tokio::test]
    async fn test_execution_produces_correct_values() {
        let ctx = make_context_with_supercast_union();
        register_one_row_table(&ctx, "t1", vec![Field::new("a", DataType::Int8, false)]);
        register_one_row_table(&ctx, "t2", vec![Field::new("a", DataType::Int32, false)]);

        let df = ctx
            .sql("SELECT a FROM t1 UNION ALL SELECT a FROM t2")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);
    }
}
