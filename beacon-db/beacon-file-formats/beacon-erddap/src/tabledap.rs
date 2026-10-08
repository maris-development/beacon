//! tabledap: SQL filters as ERDDAP constraints, the request query, and the parquet decode.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, RecordBatch, RecordBatchOptions,
    TimestampNanosecondArray,
};
use arrow::compute::{CastOptions, cast_with_options, nullif};
use arrow::datatypes::{DataType, Float32Type, Float64Type, Schema, SchemaRef, TimeUnit};
use datafusion::common::ScalarValue;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::expr::InList;
use datafusion::logical_expr::{Between, BinaryExpr, Cast, Expr, Like, Operator};
use futures::stream::{BoxStream, StreamExt};

use crate::encode::{
    LiteralValue, erddap_string, iso_millis, java_regex_literal, like_to_regex, literal_value,
    query_part, timestamp_nanos,
};

/// The constraints `expr` becomes, or `None` when it cannot be pushed.
///
/// Each result is a superset filter: DataFusion applies `expr` again above the scan.
pub fn translate(expr: &Expr, schema: &Schema) -> Option<Vec<String>> {
    match expr {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            if let (Some(column), Expr::Literal(value, _)) =
                (column_name(left, schema), right.as_ref())
            {
                return comparison(schema, column, *op, value);
            }
            if let (Expr::Literal(value, _), Some(column)) =
                (left.as_ref(), column_name(right, schema))
            {
                return comparison(schema, column, op.swap()?, value);
            }
            None
        }
        Expr::Between(Between {
            expr,
            negated: false,
            low,
            high,
        }) => {
            let column = column_name(expr, schema)?;
            let (Expr::Literal(low, _), Expr::Literal(high, _)) = (low.as_ref(), high.as_ref())
            else {
                return None;
            };
            let mut constraints = comparison(schema, column, Operator::GtEq, low)?;
            constraints.extend(comparison(schema, column, Operator::LtEq, high)?);
            Some(constraints)
        }
        Expr::InList(InList {
            expr,
            list,
            negated: false,
        }) => in_list(schema, column_name(expr, schema)?, list),
        Expr::Like(Like {
            negated: false,
            expr,
            pattern,
            escape_char: None,
            case_insensitive: false,
        }) => {
            let column = column_name(expr, schema)?;
            if column_kind(schema, column)? != Kind::Text {
                return None;
            }
            let Expr::Literal(value, _) = pattern.as_ref() else {
                return None;
            };
            let Some(LiteralValue::Text(p)) = literal_value(value) else {
                return None;
            };
            Some(vec![format!(
                "{column}=~{}",
                erddap_string(&like_to_regex(&p)?)
            )])
        }
        Expr::IsNull(inner) => nan_test(schema, inner, "="),
        Expr::IsNotNull(inner) => nan_test(schema, inner, "!="),
        _ => None,
    }
}

/// `vars` and `constraints` as an encoded query, with no leading `?`.
pub fn request_query(vars: &[String], constraints: &[String]) -> String {
    let mut query = vars
        .iter()
        .map(|v| query_part(v))
        .collect::<Vec<_>>()
        .join(",");
    for constraint in constraints {
        query.push('&');
        query.push_str(&query_part(constraint));
    }
    query
}

/// How a column compares in ERDDAP.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    Text,
    Integer,
    Float,
    Time,
}

/// A column, also behind a lossless numeric widening cast that DataFusion adds in coercion.
fn column_name<'a>(expr: &'a Expr, schema: &Schema) -> Option<&'a str> {
    match expr {
        Expr::Column(column) => Some(column.name.as_str()),
        Expr::Cast(Cast { expr, data_type }) => {
            let Expr::Column(column) = expr.as_ref() else {
                return None;
            };
            let source = schema.field_with_name(&column.name).ok()?.data_type();
            is_lossless_widening(source, data_type).then_some(column.name.as_str())
        }
        _ => None,
    }
}

/// True when every value of `from` keeps its numeric value in `to`.
fn is_lossless_widening(from: &DataType, to: &DataType) -> bool {
    use DataType::*;
    fn bits(t: &DataType) -> Option<(bool, u32)> {
        match t {
            Int8 => Some((true, 8)),
            Int16 => Some((true, 16)),
            Int32 => Some((true, 32)),
            Int64 => Some((true, 64)),
            UInt8 => Some((false, 8)),
            UInt16 => Some((false, 16)),
            UInt32 => Some((false, 32)),
            UInt64 => Some((false, 64)),
            _ => None,
        }
    }
    match (bits(from), bits(to)) {
        (Some((from_signed, from_bits)), Some((to_signed, to_bits))) => {
            if from_signed == to_signed {
                to_bits >= from_bits
            } else {
                !from_signed && to_signed && to_bits > from_bits
            }
        }
        (Some((_, from_bits)), None) => match to {
            Float64 => from_bits <= 32,
            Float32 => from_bits <= 16,
            _ => false,
        },
        (None, _) => matches!(
            (from, to),
            (Float16, Float16 | Float32 | Float64)
                | (Float32, Float32 | Float64)
                | (Float64, Float64)
        ),
    }
}

/// The comparison kind of `column`, or `None` for an unsupported type.
fn column_kind(schema: &Schema, column: &str) -> Option<Kind> {
    match schema.field_with_name(column).ok()?.data_type() {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Some(Kind::Text),
        DataType::Timestamp(_, _) => Some(Kind::Time),
        DataType::Float16 | DataType::Float32 | DataType::Float64 => Some(Kind::Float),
        t if t.is_numeric() => Some(Kind::Integer),
        _ => None,
    }
}

fn comparison(
    schema: &Schema,
    column: &str,
    op: Operator,
    value: &ScalarValue,
) -> Option<Vec<String>> {
    let kind = column_kind(schema, column)?;
    // ERDDAP compares floats and times inexactly, so strict bounds become inclusive and `!=` stays local.
    let inexact = matches!(kind, Kind::Float | Kind::Time);
    let symbol = match op {
        Operator::Eq => "=",
        Operator::NotEq if !inexact => "!=",
        Operator::Lt if inexact => "<=",
        Operator::Lt => "<",
        Operator::LtEq => "<=",
        Operator::Gt if inexact => ">=",
        Operator::Gt => ">",
        Operator::GtEq => ">=",
        _ => return None,
    };
    match (kind, literal_value(value)?) {
        (Kind::Text, LiteralValue::Text(s)) if matches!(op, Operator::Eq | Operator::NotEq) => {
            Some(vec![format!("{column}{symbol}{}", erddap_string(&s))])
        }
        (Kind::Integer | Kind::Float, LiteralValue::Number(n)) => {
            Some(vec![format!("{column}{symbol}{n}")])
        }
        (Kind::Time, LiteralValue::Time(_)) => time_comparison(column, op, value),
        _ => None,
    }
}

/// A time comparison at the whole-millisecond precision ERDDAP uses, rounded to keep a superset.
fn time_comparison(column: &str, op: Operator, value: &ScalarValue) -> Option<Vec<String>> {
    let nanos = timestamp_nanos(value)?;
    let floor = nanos.div_euclid(1_000_000);
    let exact = nanos.rem_euclid(1_000_000) == 0;
    let ceil = if exact { floor } else { floor.checked_add(1)? };
    match op {
        Operator::Gt | Operator::GtEq => Some(vec![format!("{column}>={}", iso_millis(floor)?)]),
        Operator::Lt | Operator::LtEq => Some(vec![format!("{column}<={}", iso_millis(ceil)?)]),
        Operator::Eq if exact => Some(vec![format!("{column}={}", iso_millis(floor)?)]),
        Operator::Eq => Some(vec![
            format!("{column}>={}", iso_millis(floor)?),
            format!("{column}<={}", iso_millis(ceil)?),
        ]),
        _ => None,
    }
}

fn in_list(schema: &Schema, column: &str, list: &[Expr]) -> Option<Vec<String>> {
    let values: Vec<LiteralValue> = list
        .iter()
        .map(|e| match e {
            Expr::Literal(v, _) => literal_value(v),
            _ => None,
        })
        .collect::<Option<_>>()?;
    if values.is_empty() {
        return None;
    }
    if column_kind(schema, column)? == Kind::Text {
        let alternatives: Vec<String> = values
            .iter()
            .map(|v| match v {
                LiteralValue::Text(s) => Some(java_regex_literal(s)),
                _ => None,
            })
            .collect::<Option<_>>()?;
        return Some(vec![format!(
            "{column}=~{}",
            erddap_string(&alternatives.join("|"))
        )]);
    }
    // A numeric list becomes its min..max range, a superset of the list.
    let scalars: Vec<&ScalarValue> = list
        .iter()
        .filter_map(|e| match e {
            Expr::Literal(v, _) => Some(v),
            _ => None,
        })
        .collect();
    // Mixed types or unordered values give no safe range.
    let data_type = scalars.first()?.data_type();
    if scalars.iter().any(|s| s.data_type() != data_type) {
        return None;
    }
    let (mut min, mut max) = (scalars[0], scalars[0]);
    for &s in &scalars[1..] {
        if s.partial_cmp(min)?.is_lt() {
            min = s;
        }
        if s.partial_cmp(max)?.is_gt() {
            max = s;
        }
    }
    let mut constraints = comparison(schema, column, Operator::GtEq, min)?;
    constraints.extend(comparison(schema, column, Operator::LtEq, max)?);
    Some(constraints)
}

fn nan_test(schema: &Schema, inner: &Expr, op: &str) -> Option<Vec<String>> {
    let column = column_name(inner, schema)?;
    if column_kind(schema, column)? == Kind::Text {
        return None;
    }
    Some(vec![format!("{column}{op}NaN")])
}

/// Shape an ERDDAP parquet batch to the pinned table schema.
pub fn conform(batch: &RecordBatch, target: &SchemaRef) -> Result<RecordBatch> {
    // Out-of-range values fail instead of becoming null.
    let strict = CastOptions {
        safe: false,
        ..Default::default()
    };
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(target.fields().len());
    for field in target.fields() {
        let source = batch.column_by_name(field.name()).ok_or_else(|| {
            DataFusionError::Execution(format!(
                "ERDDAP response has no column '{}'; the dataset changed, re-create the table",
                field.name()
            ))
        })?;
        let column = match (source.data_type(), field.data_type()) {
            (s, DataType::Timestamp(TimeUnit::Nanosecond, None)) if s.is_numeric() => {
                let seconds = cast_with_options(source, &DataType::Float64, &strict)?;
                let nanos: TimestampNanosecondArray = seconds
                    .as_primitive::<Float64Type>()
                    .iter()
                    .map(|v| {
                        v.filter(|v| v.is_finite())
                            .map(|v| (v * 1e9).round() as i64)
                    })
                    .collect();
                Arc::new(nanos) as ArrayRef
            }
            (s, t) if s == t => source.clone(),
            _ => cast_with_options(source, field.data_type(), &strict).map_err(|e| {
                DataFusionError::Execution(format!(
                    "ERDDAP column '{}' cannot be read as {}: {e}; re-create the table",
                    field.name(),
                    field.data_type()
                ))
            })?,
        };
        columns.push(nan_to_null(column)?);
    }
    let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
    Ok(RecordBatch::try_new_with_options(
        target.clone(),
        columns,
        &options,
    )?)
}

/// ERDDAP writes missing floats as NaN; SQL needs null.
fn nan_to_null(column: ArrayRef) -> Result<ArrayRef> {
    let mask: BooleanArray = match column.data_type() {
        DataType::Float32 => column
            .as_primitive::<Float32Type>()
            .iter()
            .map(|v| Some(v.is_some_and(f32::is_nan)))
            .collect(),
        DataType::Float64 => column
            .as_primitive::<Float64Type>()
            .iter()
            .map(|v| Some(v.is_some_and(f64::is_nan)))
            .collect(),
        _ => return Ok(column),
    };
    Ok(nullif(&column, &mask)?)
}

/// Stream the parquet file, conformed to `target`. The file lives until the stream ends.
pub async fn decode_parquet(
    file: tempfile::NamedTempFile,
    target: SchemaRef,
    batch_size: usize,
) -> Result<BoxStream<'static, Result<RecordBatch>>> {
    let handle = tokio::fs::File::open(file.path()).await?;
    let stream = parquet::arrow::ParquetRecordBatchStreamBuilder::new(handle)
        .await?
        .with_batch_size(batch_size)
        .build()?;
    Ok(stream
        .map(move |batch| {
            let _keep = &file;
            conform(&batch?, &target)
        })
        .boxed())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use datafusion::common::ScalarValue;
    use datafusion::logical_expr::{cast, col, lit};

    fn schema() -> Schema {
        Schema::new(vec![
            Field::new("temp", DataType::Float32, true),
            Field::new("depth", DataType::Int32, true),
            Field::new("ship", DataType::Utf8, true),
            Field::new(
                "time",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ),
        ])
    }

    fn t(expr: Expr) -> Option<Vec<String>> {
        translate(&expr, &schema())
    }

    #[test]
    fn numeric_comparisons_both_sides() {
        assert_eq!(t(col("depth").gt_eq(lit(5))), Some(vec!["depth>=5".into()]));
        assert_eq!(t(lit(5).lt(col("depth"))), Some(vec!["depth>5".into()]));
        assert_eq!(
            t(col("depth").not_eq(lit(5))),
            Some(vec!["depth!=5".into()])
        );
        assert_eq!(
            t(cast(col("temp"), DataType::Float64).gt(lit(1.5))),
            Some(vec!["temp>=1.5".into()])
        );
    }

    #[test]
    fn time_comparison_uses_iso() {
        let ts = lit(ScalarValue::TimestampNanosecond(
            Some(1_577_836_800_000_000_000),
            None,
        ));
        assert_eq!(
            t(col("time").lt_eq(ts)),
            Some(vec!["time<=2020-01-01T00:00:00Z".into()])
        );
    }

    #[test]
    fn string_equality_only() {
        assert_eq!(
            t(col("ship").eq(lit("New \"Horizon\""))),
            Some(vec![r#"ship="New \"Horizon\"""#.into()])
        );
        assert_eq!(
            t(col("ship").not_eq(lit("x"))),
            Some(vec![r#"ship!="x""#.into()])
        );
        assert_eq!(t(col("ship").gt(lit("x"))), None);
    }

    #[test]
    fn in_lists() {
        assert_eq!(
            t(col("ship").in_list(vec![lit("a.b"), lit("c")], false)),
            Some(vec![r#"ship=~"a\\.b|c""#.into()])
        );
        assert_eq!(
            t(col("depth").in_list(vec![lit(9), lit(1), lit(5)], false)),
            Some(vec!["depth>=1".into(), "depth<=9".into()])
        );
        assert_eq!(t(col("depth").in_list(vec![lit(1)], true)), None);
    }

    #[test]
    fn like_and_nulls_and_between() {
        assert_eq!(
            t(col("ship").like(lit("Ne%"))),
            Some(vec![r#"ship=~"(?s)Ne.*""#.into()])
        );
        assert_eq!(t(col("ship").ilike(lit("ne%"))), None);
        assert_eq!(t(col("temp").is_null()), Some(vec!["temp=NaN".into()]));
        assert_eq!(t(col("temp").is_not_null()), Some(vec!["temp!=NaN".into()]));
        assert_eq!(t(col("ship").is_null()), None);
        assert_eq!(
            t(col("depth").between(lit(1), lit(3))),
            Some(vec!["depth>=1".into(), "depth<=3".into()])
        );
    }

    fn ts_ns(nanos: i64) -> Expr {
        lit(ScalarValue::TimestampNanosecond(Some(nanos), None))
    }

    #[test]
    fn narrowing_and_non_numeric_casts_are_not_pushed() {
        assert_eq!(t(cast(col("temp"), DataType::Int32).eq(lit(7))), None);
        assert_eq!(t(cast(col("depth"), DataType::Int16).gt(lit(1))), None);
        assert_eq!(
            t(cast(col("time"), DataType::Int64).gt(lit(1_577_836_800_000_000_000_i64))),
            None
        );
        assert_eq!(
            t(cast(col("depth"), DataType::Int64).gt(lit(5_i64))),
            Some(vec!["depth>5".into()])
        );
        assert_eq!(
            t(cast(col("depth"), DataType::Float64).lt(lit(2.5))),
            Some(vec!["depth<2.5".into()])
        );
    }

    #[test]
    fn literal_kind_must_match_column_kind() {
        assert_eq!(t(col("depth").gt(ts_ns(0))), None);
        assert_eq!(t(col("time").gt(lit(5))), None);
        assert_eq!(t(col("ship").eq(lit(5))), None);
    }

    #[test]
    fn float_and_time_bounds_stay_inclusive() {
        assert_eq!(t(col("temp").gt(lit(1.5))), Some(vec!["temp>=1.5".into()]));
        assert_eq!(t(col("temp").lt(lit(1.5))), Some(vec!["temp<=1.5".into()]));
        assert_eq!(t(col("temp").eq(lit(1.5))), Some(vec!["temp=1.5".into()]));
        assert_eq!(t(col("temp").not_eq(lit(1.5))), None);
        assert_eq!(
            t(col("time").lt(ts_ns(1_577_836_800_000_000_000))),
            Some(vec!["time<=2020-01-01T00:00:00Z".into()])
        );
        assert_eq!(t(col("time").not_eq(ts_ns(0))), None);
    }

    #[test]
    fn sub_millisecond_times_round_outwards() {
        let base = 1_577_836_800_000_000_000;
        // 00:00:00.0005
        let half_ms = base + 500_000;
        assert_eq!(
            t(col("time").gt_eq(ts_ns(half_ms))),
            Some(vec!["time>=2020-01-01T00:00:00Z".into()])
        );
        assert_eq!(
            t(col("time").lt(ts_ns(half_ms))),
            Some(vec!["time<=2020-01-01T00:00:00.001Z".into()])
        );
        assert_eq!(
            t(col("time").eq(ts_ns(half_ms))),
            Some(vec![
                "time>=2020-01-01T00:00:00Z".into(),
                "time<=2020-01-01T00:00:00.001Z".into()
            ])
        );
        assert_eq!(
            t(col("time").eq(ts_ns(base + 2_000_000))),
            Some(vec!["time=2020-01-01T00:00:00.002Z".into()])
        );
        // One nanosecond before 1970: floor is -1 ms, ceil is 0.
        assert_eq!(
            t(col("time").gt_eq(ts_ns(-1))),
            Some(vec!["time>=1969-12-31T23:59:59.999Z".into()])
        );
        assert_eq!(
            t(col("time").lt_eq(ts_ns(-1))),
            Some(vec!["time<=1970-01-01T00:00:00Z".into()])
        );
    }

    #[test]
    fn mixed_type_in_lists_are_not_pushed() {
        assert_eq!(
            t(col("depth").in_list(vec![lit(1_i32), lit(9_i64)], false)),
            None
        );
        assert_eq!(
            t(col("depth").in_list(vec![lit(1_i64), lit(9_i64)], false)),
            Some(vec!["depth>=1".into(), "depth<=9".into()])
        );
    }

    #[test]
    fn unsupported_shapes() {
        assert_eq!(t(col("depth").gt(lit(1)).or(col("depth").lt(lit(0)))), None);
        assert_eq!(t(col("depth").gt(col("temp"))), None);
        assert_eq!(t(col("nope").gt(lit(1))), None);
        assert_eq!(t(col("temp").gt(lit(f64::NAN))), None);
    }

    #[test]
    fn request_query_encodes_vars_and_constraints() {
        let q = request_query(
            &["ship".into(), "time".into()],
            &["time>=2020-01-01T00:00:00Z".into()],
        );
        assert_eq!(q, "ship,time&time%3E%3D2020-01-01T00%3A00%3A00Z");
    }
}

#[cfg(test)]
mod decode_tests {
    use super::*;
    use arrow::array::{Float32Array, Float64Array, Int32Array};
    use arrow::datatypes::Field;
    use futures::TryStreamExt;

    #[test]
    fn conform_converts_epoch_seconds_and_nan() {
        let source = RecordBatch::try_from_iter(vec![
            (
                "time",
                Arc::new(Float64Array::from(vec![Some(1.5), None, Some(f64::NAN)])) as _,
            ),
            (
                "temp",
                Arc::new(Float32Array::from(vec![1.0, f32::NAN, 2.0])) as _,
            ),
        ])
        .unwrap();
        let target = Arc::new(Schema::new(vec![
            Field::new("temp", DataType::Float32, true),
            Field::new(
                "time",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ),
        ]));
        let out = conform(&source, &target).unwrap();
        let time = out
            .column(1)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap();
        assert_eq!(time.value(0), 1_500_000_000);
        assert!(time.is_null(1) && time.is_null(2));
        assert!(out.column(0).is_null(1), "NaN is missing in ERDDAP");
    }

    #[test]
    fn conform_names_a_missing_column() {
        let source =
            RecordBatch::try_from_iter(vec![("a", Arc::new(Float64Array::from(vec![1.0])) as _)])
                .unwrap();
        let target = Arc::new(Schema::new(vec![Field::new("b", DataType::Float64, true)]));
        let err = conform(&source, &target).unwrap_err().to_string();
        assert!(err.contains("'b'") && err.contains("re-create"), "{err}");
    }

    #[test]
    fn conform_keeps_the_row_count_for_no_columns() {
        let source = RecordBatch::try_from_iter(vec![(
            "a",
            Arc::new(Float64Array::from(vec![1.0, 2.0])) as _,
        )])
        .unwrap();
        let out = conform(&source, &Arc::new(Schema::empty())).unwrap();
        assert_eq!(out.num_rows(), 2);
    }

    #[test]
    fn conform_errors_on_integer_overflow() {
        let source = RecordBatch::try_from_iter(vec![(
            "n",
            Arc::new(Int32Array::from(vec![1, 70_000])) as _,
        )])
        .unwrap();
        let target = Arc::new(Schema::new(vec![Field::new("n", DataType::Int16, true)]));
        let err = conform(&source, &target).unwrap_err().to_string();
        assert!(err.contains("'n'") && err.contains("re-create"), "{err}");
    }

    #[tokio::test]
    async fn decodes_the_recorded_parquet() {
        let info =
            crate::DatasetInfo::parse(&crate::fixture::test_file("tabledap_info.json")).unwrap();
        let schema = info.tabledap_schema().unwrap();
        // Only the columns the fixture request asked for.
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(file.path(), crate::fixture::test_file("tabledap.parquet")).unwrap();
        let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
            std::fs::File::open(file.path()).unwrap(),
        )
        .unwrap();
        let names: Vec<String> = reader
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        // The first recorded time value in milliseconds, read from the file itself.
        let first_ms = {
            let mut batches = reader.build().unwrap();
            let batch = batches.next().unwrap().unwrap();
            batch
                .column_by_name("time")
                .unwrap()
                .as_primitive::<arrow::datatypes::TimestampMillisecondType>()
                .value(0)
        };
        let target = Arc::new(
            schema
                .project(
                    &names
                        .iter()
                        .map(|n| schema.index_of(n).unwrap())
                        .collect::<Vec<_>>(),
                )
                .unwrap(),
        );
        let batches: Vec<RecordBatch> = decode_parquet(file, target.clone(), 8192)
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert!(rows > 0);
        assert!(batches.iter().all(|b| b.schema() == target));
        // The cast keeps the UTC instant.
        let time = batches[0]
            .column_by_name("time")
            .unwrap()
            .as_primitive::<arrow::datatypes::TimestampNanosecondType>();
        assert_eq!(time.value(0), first_ms * 1_000_000);
    }
}
