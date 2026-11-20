//! The physical expression adapter of a merge strategy.
//!
//! A reader gives back the columns of the file it read, and those columns are
//! not the merged schema. DataFusion's `PhysicalExprAdapter` closes that gap:
//! it matches columns by name, casts a column the merge widened, and reads a
//! column the file lacks as null. Every format reads the factory from its
//! `FileScanConfig`. See [`expr_adapter`](crate::expr_adapter).
//!
//! # A column the merge could not join
//!
//! [`TypeConflict::KeepFirst`] lets the merge settle a column that two files
//! type in two families. The table then reports the type of the first file, and
//! a file of the other family holds values that type cannot hold. The adapter
//! asks the strategy that merged the schema, through [`casts_leniently`], which
//! casts may read such a value as null:
//!
//! - A value the type cannot hold reads as null. `Utf8` "abc" to `Float64`
//!   gives null, not an error.
//! - A type no cast reaches reads as null for the whole file. A list beside a
//!   number is one such pair.
//!
//! The strategy answers `true` for a pair its rules do not widen, because only
//! the setting lets such a pair reach a scan. A pair the rules widen, such as
//! `Int32` into `Int64`, keeps a strict cast under either setting.
//!
//! # An nd column
//!
//! An nd column reads leniently under every strategy. Its cast lands on the
//! `values` list inside the `beacon.nd` struct, and one collection of a million
//! datasets may store an array as text where another stores numbers. One cell
//! that does not parse would otherwise fail the whole scan, and no single
//! dataset is worth a collection.
//!
//! Every other cast stays strict, and a value it cannot hold is an error.
//!
//! [`TypeConflict::KeepFirst`]: super::TypeConflict::KeepFirst
//! [`casts_leniently`]: super::ArrowTypeWideningStrategy::casts_leniently

use std::sync::Arc;

use arrow::compute::{CastOptions, can_cast_types};
use arrow::datatypes::{DataType, Field, SchemaRef};
use arrow::util::display::FormatOptions;
use datafusion::common::ScalarValue;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::error::Result;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::{CastExpr, Column, lit};
use datafusion::physical_expr_adapter::{
    DefaultPhysicalExprAdapterFactory, PhysicalExprAdapter, PhysicalExprAdapterFactory,
};

use super::ArrowTypeWideningStrategy;
use crate::nd::is_nd_encoded;

/// The cast for a column the merge could not join, where a null is the answer.
/// The files state two families, so no value of the other family is a value of
/// this one. See the [module docs](self).
const LENIENT_CAST_OPTIONS: CastOptions<'static> = CastOptions {
    safe: true,
    format_options: FormatOptions::new(),
};

/// Whether a cast of a file column of `source` onto `target` may read a value
/// the type cannot hold as null.
///
/// Two cases qualify: a pair the strategy did not widen, and an nd column. See
/// the [module docs](self).
fn casts_leniently(
    target: &Field,
    source: &DataType,
    strategy: &dyn ArrowTypeWideningStrategy,
) -> bool {
    is_nd_encoded(target) || strategy.casts_leniently(source, target.data_type())
}

/// The [`PhysicalExprAdapterFactory`] that adapts one file with `strategy`.
///
/// A cast that `strategy` reads leniently gives null for a value the type
/// cannot hold. Every other cast is strict. See the [module docs](self).
pub fn lenient_cast_adapter_factory(
    strategy: Arc<dyn ArrowTypeWideningStrategy>,
) -> Arc<dyn PhysicalExprAdapterFactory> {
    Arc::new(LenientCastAdapterFactory { strategy })
}

/// Builds [`LenientCastAdapter`] for one file.
#[derive(Debug)]
struct LenientCastAdapterFactory {
    strategy: Arc<dyn ArrowTypeWideningStrategy>,
}

impl PhysicalExprAdapterFactory for LenientCastAdapterFactory {
    fn create(
        &self,
        logical_file_schema: SchemaRef,
        physical_file_schema: SchemaRef,
    ) -> Result<Arc<dyn PhysicalExprAdapter>> {
        Ok(Arc::new(LenientCastAdapter {
            inner: DefaultPhysicalExprAdapterFactory.create(
                Arc::clone(&logical_file_schema),
                Arc::clone(&physical_file_schema),
            )?,
            logical_file_schema,
            physical_file_schema,
            strategy: Arc::clone(&self.strategy),
        }))
    }
}

/// DataFusion's rule, with a null for a column the merge could not join.
///
/// Two passes wrap the inner rewrite, because the inner rule refuses a pair no
/// cast reaches before it builds any cast at all:
///
/// 1. Before: a lenient column that no cast reaches becomes a null literal. The
///    inner rule then sees a literal and builds no cast.
/// 2. After: the cast of a lenient column takes [`LENIENT_CAST_OPTIONS`].
#[derive(Debug)]
struct LenientCastAdapter {
    inner: Arc<dyn PhysicalExprAdapter>,
    /// The schema the table reports.
    logical_file_schema: SchemaRef,
    /// The schema of the file being read.
    physical_file_schema: SchemaRef,
    /// The rule that merged the table schema. It decides which casts read null.
    strategy: Arc<dyn ArrowTypeWideningStrategy>,
}

impl LenientCastAdapter {
    /// The table field of `column` and the type the file states for it, when
    /// the cast between them may read null. `None` for a column the file lacks:
    /// the inner rule fills that null.
    fn lenient_pair(&self, column: &Column) -> Option<(&Field, &DataType)> {
        let target = self
            .logical_file_schema
            .field(self.logical_file_schema.index_of(column.name()).ok()?);
        let source = self
            .physical_file_schema
            .field(self.physical_file_schema.index_of(column.name()).ok()?)
            .data_type();
        casts_leniently(target, source, self.strategy.as_ref()).then_some((target, source))
    }
}

impl PhysicalExprAdapter for LenientCastAdapter {
    fn rewrite(&self, expr: Arc<dyn PhysicalExpr>) -> Result<Arc<dyn PhysicalExpr>> {
        let expr = expr
            .transform_down(|expr| {
                let Some(column) = expr.downcast_ref::<Column>() else {
                    return Ok(Transformed::no(expr));
                };
                let Some((target, source)) = self.lenient_pair(column) else {
                    return Ok(Transformed::no(expr));
                };
                if can_cast_types(source, target.data_type()) {
                    return Ok(Transformed::no(expr));
                }
                // No cast reaches this type. The file reads null for the column.
                Ok(Transformed::yes(lit(ScalarValue::try_new_null(
                    target.data_type(),
                )?)))
            })
            .data()?;

        self.inner
            .rewrite(expr)?
            .transform_down(|expr| {
                let Some(cast) = expr.downcast_ref::<CastExpr>() else {
                    return Ok(Transformed::no(expr));
                };
                // The inner rule casts the file column straight to the table type.
                // Other casts come from the query and keep their options.
                let Some(column) = cast.expr().downcast_ref::<Column>() else {
                    return Ok(Transformed::no(expr));
                };
                let Some((target, _)) = self.lenient_pair(column) else {
                    return Ok(Transformed::no(expr));
                };
                if cast.cast_type() != target.data_type() {
                    return Ok(Transformed::no(expr));
                }
                Ok(Transformed::yes(Arc::new(CastExpr::new_with_target_field(
                    Arc::clone(cast.expr()),
                    Arc::clone(cast.target_field()),
                    Some(LENIENT_CAST_OPTIONS),
                ))
                    as Arc<dyn PhysicalExpr>))
            })
            .data()
    }
}

/// Reads the one-column file `file` as a table column of `target` through the
/// adapter of `strategy`.
#[cfg(test)]
pub(crate) fn adapt_column(
    strategy: &dyn ArrowTypeWideningStrategy,
    file: arrow::array::ArrayRef,
    target: DataType,
) -> Result<arrow::array::ArrayRef> {
    use arrow::array::RecordBatch;
    use arrow::datatypes::Schema;

    let physical = Arc::new(Schema::new(vec![Field::new(
        "v",
        file.data_type().clone(),
        true,
    )]));
    let logical = Arc::new(Schema::new(vec![Field::new("v", target, true)]));
    let adapter = strategy
        .physical_expr_adapter_factory()
        .create(logical, Arc::clone(&physical))?;
    let expr = adapter.rewrite(Arc::new(Column::new("v", 0)))?;
    let rows = file.len();
    let batch = RecordBatch::try_new(physical, vec![file])?;
    expr.evaluate(&batch)?.into_array(rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr_adapter::BatchAdapter;
    use crate::type_widening::DefaultArrowTypeWidening;
    use arrow::array::{
        Array, ArrayRef, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray,
        TimestampSecondArray,
    };
    use arrow::datatypes::{Schema, TimeUnit};

    fn batch(fields: Vec<Field>, columns: Vec<ArrayRef>) -> RecordBatch {
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).expect("valid batch")
    }

    /// The rule that refuses a column no type holds. Every cast is strict.
    fn strict() -> Arc<dyn ArrowTypeWideningStrategy> {
        Arc::new(DefaultArrowTypeWidening::new())
    }

    /// The rule that keeps the first type of such a column.
    fn keeping_first() -> Arc<dyn ArrowTypeWideningStrategy> {
        Arc::new(DefaultArrowTypeWidening::keeping_first_type())
    }

    /// `source`, mapped onto `target` with the adapter of `strategy`.
    fn adapt(
        target: SchemaRef,
        source: &RecordBatch,
        strategy: Arc<dyn ArrowTypeWideningStrategy>,
    ) -> Result<RecordBatch> {
        let factory = strategy.physical_expr_adapter_factory();
        BatchAdapter::try_new(target, &source.schema(), factory.as_ref())?.adapt_batch(source)
    }

    /// A batch of one `Timestamp(Second)` column whose value overflows a
    /// nanosecond timestamp. The unit widens, so the pair is no conflict, and
    /// only a strict cast reports the overflow.
    fn far_future_seconds() -> RecordBatch {
        batch(
            vec![Field::new(
                "t",
                DataType::Timestamp(TimeUnit::Second, None),
                true,
            )],
            vec![Arc::new(TimestampSecondArray::from(vec![i64::MAX / 1_000]))],
        )
    }

    fn nanoseconds() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new(
            "t",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        )]))
    }

    fn list_column() -> RecordBatch {
        let list = DataType::List(Arc::new(Field::new("item", DataType::Int32, true)));
        batch(
            vec![Field::new("v", list, true)],
            vec![Arc::new(arrow::array::ListArray::from_iter_primitive::<
                arrow::datatypes::Int32Type,
                _,
                _,
            >(vec![
                Some(vec![Some(1)]),
                Some(vec![Some(2)]),
            ]))],
        )
    }

    /// A column the merge widened is cast to the merged type.
    #[test]
    fn a_widened_column_is_cast() {
        let target = Arc::new(Schema::new(vec![Field::new("v", DataType::Float64, true)]));
        let source = batch(
            vec![Field::new("v", DataType::Int32, true)],
            vec![Arc::new(Int32Array::from(vec![1, 2]))],
        );

        let adapted = adapt(target, &source, strict()).expect("cast");
        let values = adapted
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("Float64");
        assert_eq!(values.values(), &[1.0, 2.0]);
    }

    /// A column the file lacks reads nulls, and keeps the row count of the file.
    #[test]
    fn a_missing_column_reads_nulls() {
        let target = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Utf8, true),
        ]));
        let source = batch(
            vec![Field::new("a", DataType::Int64, true)],
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        );

        let adapted = adapt(target, &source, strict()).expect("fill");
        assert_eq!(adapted.num_rows(), 3);
        assert_eq!(adapted.column(1).null_count(), 3);
    }

    /// A column the schema does not hold is dropped, whatever its place in the
    /// file. The remaining columns follow the target, not the file.
    #[test]
    fn an_unheld_column_is_dropped_and_the_order_follows_the_target() {
        let target = Arc::new(Schema::new(vec![
            Field::new("b", DataType::Int64, true),
            Field::new("a", DataType::Int64, true),
        ]));
        let source = batch(
            vec![
                Field::new("a", DataType::Int64, true),
                Field::new("extra", DataType::Utf8, true),
                Field::new("b", DataType::Int64, true),
            ],
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(StringArray::from(vec!["drop me"])),
                Arc::new(Int64Array::from(vec![2])),
            ],
        );

        let adapted = adapt(target, &source, strict()).expect("project");
        assert_eq!(adapted.num_columns(), 2);
        let b = adapted
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64");
        assert_eq!(b.values(), &[2], "the target names `b` first");
    }

    /// A target that holds nothing keeps the row count. `SELECT count(*)` reads
    /// such a batch.
    #[test]
    fn a_target_of_no_column_keeps_the_row_count() {
        let source = batch(
            vec![Field::new("a", DataType::Int64, true)],
            vec![Arc::new(Int64Array::from(vec![1, 2, 3, 4]))],
        );

        let adapted = adapt(Arc::new(Schema::empty()), &source, strict()).expect("count");
        assert_eq!(adapted.num_columns(), 0);
        assert_eq!(adapted.num_rows(), 4);
    }

    /// A column that may hold no null, and that the file lacks, is an error. A
    /// scan cannot fill it, and a silent null would break what the schema states.
    #[test]
    fn a_missing_column_that_may_not_be_null_is_an_error() {
        let target = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let source = batch(
            vec![Field::new("b", DataType::Int64, true)],
            vec![Arc::new(Int64Array::from(vec![1]))],
        );

        let error = adapt(target, &source, strict())
            .expect_err("a non-nullable column cannot be filled")
            .to_string();
        assert!(error.contains("Non-nullable column 'a'"), "{error}");
    }

    /// A value the target type cannot hold is an error, not a null.
    #[test]
    fn a_value_the_target_cannot_hold_is_an_error() {
        let target = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, true)]));
        let source = batch(
            vec![Field::new("v", DataType::Int64, true)],
            vec![Arc::new(Int64Array::from(vec![i64::MAX]))],
        );

        assert!(
            adapt(target, &source, strict()).is_err(),
            "an overflow must be told"
        );
    }

    // ── a column the merge could not join ──────────────────────────────

    /// Under the default rule a pair no rule widens casts strictly. Such a
    /// pair reaches a scan through a declared schema alone.
    #[test]
    fn the_default_rule_reads_no_null_for_a_pair_it_did_not_widen() {
        let target = Arc::new(Schema::new(vec![Field::new("v", DataType::Float64, true)]));
        let source = batch(
            vec![Field::new("v", DataType::Utf8, true)],
            vec![Arc::new(StringArray::from(vec!["1.5", "abc"]))],
        );

        assert!(
            adapt(target, &source, strict()).is_err(),
            "\"abc\" must be told"
        );
    }

    /// A value the target type cannot hold reads as null, not as an error,
    /// because the files disagree on what the column holds.
    #[test]
    fn a_settled_column_reads_a_value_it_cannot_hold_as_null() {
        let target = Arc::new(Schema::new(vec![Field::new("v", DataType::Float64, true)]));
        let source = batch(
            vec![Field::new("v", DataType::Utf8, true)],
            vec![Arc::new(StringArray::from(vec!["1.5", "abc"]))],
        );

        let adapted =
            adapt(target, &source, keeping_first()).expect("a settled column may not fail");
        let values = adapted
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("Float64");
        assert_eq!(values.value(0), 1.5, "a value the type holds is read");
        assert!(values.is_null(1), "a value it cannot hold reads null");
    }

    /// A type no cast reaches reads null for the whole file, rather than
    /// failing the scan. DataFusion's rule refuses such a pair before it builds
    /// a cast, so the null comes from the pass above it.
    #[test]
    fn a_settled_column_no_cast_reaches_reads_null() {
        let target = Arc::new(Schema::new(vec![Field::new("v", DataType::Float64, true)]));
        let source = list_column();

        let adapted = adapt(target, &source, keeping_first()).expect("no cast reaches this type");
        assert_eq!(adapted.num_rows(), 2);
        assert_eq!(adapted.column(0).null_count(), 2);
    }

    /// The setting reaches only a pair the rule did not widen. A pair it
    /// widened keeps a strict cast, so an overflow is still an error.
    #[test]
    fn the_setting_leaves_a_widened_column_strict() {
        let source = far_future_seconds();
        for strategy in [strict(), keeping_first()] {
            assert!(
                adapt(nanoseconds(), &source, strategy).is_err(),
                "an overflow must be told"
            );
        }
    }
}
