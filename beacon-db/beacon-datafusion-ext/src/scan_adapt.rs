//! Adapting the batches of one file to the schema a scan reports.
//!
//! A collection holds files that do not agree. One file types a column `Int32`
//! and another `Float32`. One file holds a column that another lacks. The merge
//! rule of the session settles one schema for the collection, and the table
//! reports it. See [`type_widening`](crate::type_widening) for that rule.
//!
//! A reader gives back the columns of the file it read, and those columns are
//! not the merged schema. [`AdaptingOpener`] closes that gap. It wraps the
//! opener of a format and maps each batch onto the schema the scan reports: it
//! matches columns by name, casts a column the merge widened, fills a column
//! the file lacks with nulls, and drops a column the schema does not hold.
//!
//! A scan that skips this step reports a schema it cannot produce. The reader
//! then fails on the first batch, and `LIMIT 0` still succeeds, because it reads
//! no batch.
//!
//! # A column the merge could not join
//!
//! [`TypeConflict::KeepFirst`] lets the merge settle a column that two files
//! type in two families. The table then reports the type of the first file, and
//! a file of the other family holds values that type cannot hold. The scan asks
//! the strategy that merged the schema, through [`casts_leniently`], which
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
//! Every format captures the strategy of the session when it plans, because
//! DataFusion hands a `FileSource` no session when it opens a file. A source
//! built without a session takes the strict default rule.
//!
//! # An nd column
//!
//! An nd column reads leniently under every strategy. Its
//! cast lands on the `values` list inside the `beacon.nd` struct, and one
//! collection of a million datasets may store an array as text where another
//! stores numbers. One cell that does not parse would otherwise fail the whole
//! scan, and no single dataset is worth a collection.
//!
//! Every other cast stays strict, and a value it cannot hold is an error.
//!
//! [`TypeConflict::KeepFirst`]: crate::type_widening::TypeConflict::KeepFirst
//! [`casts_leniently`]: crate::type_widening::ArrowTypeWideningStrategy::casts_leniently

use std::sync::Arc;

use arrow::array::{ArrayRef, RecordBatch, RecordBatchOptions, new_null_array};
use arrow::compute::{CastOptions, can_cast_types, cast_with_options};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::util::display::FormatOptions;
use datafusion::common::ScalarValue;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{FileOpenFuture, FileOpener};
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::{CastColumnExpr, Column, lit};
use datafusion::physical_expr_adapter::{
    BatchAdapterFactory, DefaultPhysicalExprAdapterFactory, PhysicalExprAdapter,
    PhysicalExprAdapterFactory,
};
use futures::StreamExt;

use crate::nd::is_nd_encoded;
use crate::type_widening::ArrowTypeWideningStrategy;

/// Whether a cast of a file column of `source` onto `target` may read a value
/// the type cannot hold as null.
///
/// Two cases qualify.
///
/// A pair the strategy did not widen. The sources state two families, so no
/// value of the other family is a value of this one, and only
/// `TypeConflict::KeepFirst` let the pair reach the scan. The strategy answers.
///
/// An nd column. Its cast lands on the `values` list inside the `beacon.nd`
/// struct, and a collection of a million datasets may store one array as text
/// where another stores numbers. One cell that does not parse must not fail the
/// whole scan, because no single dataset is worth the collection.
fn casts_leniently(
    target: &Field,
    source: &DataType,
    strategy: &dyn ArrowTypeWideningStrategy,
) -> bool {
    is_nd_encoded(target) || strategy.casts_leniently(source, target.data_type())
}

/// Where one column of the target schema comes from.
#[derive(Debug, Clone)]
enum Source {
    /// The file holds this column, with this type. Take it as it is.
    Column(usize),
    /// The file holds this column with another type. Cast it. `lenient` reads a
    /// value the type cannot hold as null; see the [module docs](self).
    Cast {
        at: usize,
        data_type: DataType,
        lenient: bool,
    },
    /// The file lacks this column, or no cast reaches its type. Read nulls of
    /// this type.
    Missing(DataType),
}

/// Maps the batches of one file onto one schema.
///
/// The map is settled once, from the schema of the file, and then serves every
/// batch of it.
#[derive(Debug)]
pub struct BatchAdapter {
    target: SchemaRef,
    sources: Vec<Source>,
}

/// A cast may not turn a value it cannot hold into a null. A collection that
/// reads is worth less than one that says why it cannot.
const CAST_OPTIONS: CastOptions<'static> = CastOptions {
    safe: false,
    format_options: FormatOptions::new(),
};

/// The cast for a column the merge could not join, where a null is the answer.
/// The files state two families, so no value of the other family is a value of
/// this one. See the [module docs](self).
const LENIENT_CAST_OPTIONS: CastOptions<'static> = CastOptions {
    safe: true,
    format_options: FormatOptions::new(),
};

impl BatchAdapter {
    /// The map from `source` onto `target`. `strategy` decides which casts
    /// read null; see the [module docs](self).
    ///
    /// A column of `target` that `source` lacks reads nulls, so such a column
    /// has to be nullable. The merge rule already makes a column that some file
    /// lacks nullable; a schema that a statement declares may not, and this
    /// reports that.
    pub fn try_new(
        target: SchemaRef,
        source: &Schema,
        strategy: &dyn ArrowTypeWideningStrategy,
    ) -> Result<Self> {
        let sources = target
            .fields()
            .iter()
            .map(|field| match source.index_of(field.name()) {
                Ok(at) if source.field(at).data_type() == field.data_type() => {
                    Ok(Source::Column(at))
                }
                // A column the merge could not join reads null where the cast
                // cannot answer, and null for the whole file where no cast
                // reaches its type.
                Ok(at) if casts_leniently(field, source.field(at).data_type(), strategy) => {
                    let data_type = field.data_type().clone();
                    Ok(
                        if can_cast_types(source.field(at).data_type(), &data_type) {
                            Source::Cast {
                                at,
                                data_type,
                                lenient: true,
                            }
                        } else {
                            Source::Missing(data_type)
                        },
                    )
                }
                Ok(at) => Ok(Source::Cast {
                    at,
                    data_type: field.data_type().clone(),
                    lenient: false,
                }),
                Err(_) if field.is_nullable() => Ok(Source::Missing(field.data_type().clone())),
                Err(_) => Err(DataFusionError::Execution(format!(
                    "Non-nullable column '{}' is missing from a file of this collection",
                    field.name()
                ))),
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self { target, sources })
    }

    /// `batch`, as one batch of the target schema.
    pub fn adapt(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        let rows = batch.num_rows();
        let columns = self
            .sources
            .iter()
            .map(|source| -> Result<ArrayRef> {
                Ok(match source {
                    Source::Column(at) => Arc::clone(batch.column(*at)),
                    Source::Cast {
                        at,
                        data_type,
                        lenient,
                    } => {
                        let options = if *lenient {
                            &LENIENT_CAST_OPTIONS
                        } else {
                            &CAST_OPTIONS
                        };
                        cast_with_options(batch.column(*at), data_type, options)?
                    }
                    Source::Missing(data_type) => new_null_array(data_type, rows),
                })
            })
            .collect::<Result<Vec<_>>>()?;

        // The row count is stated, because a batch of no column states it
        // nowhere else. `SELECT count(*)` plans such a batch.
        let options = RecordBatchOptions::new().with_row_count(Some(rows));
        Ok(RecordBatch::try_new_with_options(
            Arc::clone(&self.target),
            columns,
            &options,
        )?)
    }
}

/// A [`FileOpener`] that maps the batches of `inner` onto one target schema.
///
/// The target is the file schema of the table, narrowed to the columns the scan
/// reads. A `FileSource` that hands this opener to `ProjectionOpener` derives
/// that schema the same way, so the two always agree. The strategy that merged
/// the schema decides which casts read null.
pub struct AdaptingOpener {
    inner: Arc<dyn FileOpener>,
    target: SchemaRef,
    strategy: Arc<dyn ArrowTypeWideningStrategy>,
}

impl AdaptingOpener {
    /// Wrap `inner` so that every batch it produces carries `target`.
    pub fn new(
        inner: Arc<dyn FileOpener>,
        target: SchemaRef,
        strategy: Arc<dyn ArrowTypeWideningStrategy>,
    ) -> Self {
        Self {
            inner,
            target,
            strategy,
        }
    }

    /// The same, as a `FileOpener` to hand on.
    pub fn wrap(
        inner: Arc<dyn FileOpener>,
        target: SchemaRef,
        strategy: Arc<dyn ArrowTypeWideningStrategy>,
    ) -> Arc<dyn FileOpener> {
        Arc::new(Self::new(inner, target, strategy))
    }
}

impl FileOpener for AdaptingOpener {
    fn open(&self, partitioned_file: PartitionedFile) -> Result<FileOpenFuture> {
        let target = Arc::clone(&self.target);
        let strategy = Arc::clone(&self.strategy);
        let inner = self.inner.open(partitioned_file)?;

        Ok(Box::pin(async move {
            let stream = inner.await?;
            // One map serves every batch of one file, because a reader keeps one
            // schema for a file. The batch states that schema, so the first batch
            // settles the map. A reader that changes schema settles a second one.
            let mut adapter: Option<(SchemaRef, BatchAdapter)> = None;
            let stream = stream.map(move |batch| {
                let batch = batch?;
                let source = batch.schema();
                let settled = match &adapter {
                    Some((held, _)) => held == &source,
                    None => false,
                };
                if !settled {
                    let built =
                        BatchAdapter::try_new(Arc::clone(&target), &source, strategy.as_ref())?;
                    adapter = Some((source, built));
                }
                let (_, adapter) = adapter.as_ref().expect("just settled");
                adapter.adapt(&batch)
            });
            Ok(stream.boxed())
        }))
    }
}

/// A [`BatchAdapterFactory`] that reads a column the merge could not join as
/// null.
///
/// Every format that maps its batches with DataFusion's factory builds it here.
/// `strategy` decides which casts read null, so a scan whose files agree with
/// the table gets what `BatchAdapterFactory::new` gives: a strict cast, and an
/// error for a value the type cannot hold. See the [module docs](self).
pub fn batch_adapter_factory(
    target: SchemaRef,
    strategy: Arc<dyn ArrowTypeWideningStrategy>,
) -> BatchAdapterFactory {
    BatchAdapterFactory::new(target)
        .with_adapter_factory(Arc::new(LenientCastAdapterFactory { strategy }))
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
                let Some(column) = expr.as_any().downcast_ref::<Column>() else {
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
                let Some(cast) = expr.as_any().downcast_ref::<CastColumnExpr>() else {
                    return Ok(Transformed::no(expr));
                };
                if !casts_leniently(
                    cast.target_field(),
                    cast.input_field().data_type(),
                    self.strategy.as_ref(),
                ) {
                    return Ok(Transformed::no(expr));
                }
                Ok(Transformed::yes(Arc::new(CastColumnExpr::new(
                    Arc::clone(cast.expr()),
                    Arc::clone(cast.input_field()),
                    Arc::clone(cast.target_field()),
                    Some(LENIENT_CAST_OPTIONS),
                )) as Arc<dyn PhysicalExpr>))
            })
            .data()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::type_widening::DefaultArrowTypeWidening;
    use arrow::array::{
        Array, Float64Array, Int32Array, Int64Array, StringArray, TimestampSecondArray,
    };
    use arrow::datatypes::TimeUnit;

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

    /// The map from the schema of `source` onto `target` under `strategy`.
    fn adapter(
        target: SchemaRef,
        source: &RecordBatch,
        strategy: &Arc<dyn ArrowTypeWideningStrategy>,
    ) -> Result<BatchAdapter> {
        BatchAdapter::try_new(target, source.schema().as_ref(), strategy.as_ref())
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

    /// A column the merge widened is cast to the merged type.
    #[test]
    fn a_widened_column_is_cast() {
        let target = Arc::new(Schema::new(vec![Field::new("v", DataType::Float64, true)]));
        let source = batch(
            vec![Field::new("v", DataType::Int32, true)],
            vec![Arc::new(Int32Array::from(vec![1, 2]))],
        );

        let adapter = adapter(target, &source, &strict()).expect("map");
        let adapted = adapter.adapt(&source).expect("cast");
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

        let adapter = adapter(target, &source, &strict()).expect("map");
        let adapted = adapter.adapt(&source).expect("fill");
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

        let adapter = adapter(target, &source, &strict()).expect("map");
        let adapted = adapter.adapt(&source).expect("project");
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
        let target = Arc::new(Schema::empty());
        let source = batch(
            vec![Field::new("a", DataType::Int64, true)],
            vec![Arc::new(Int64Array::from(vec![1, 2, 3, 4]))],
        );

        let adapter = adapter(target, &source, &strict()).expect("map");
        let adapted = adapter.adapt(&source).expect("count");
        assert_eq!(adapted.num_columns(), 0);
        assert_eq!(adapted.num_rows(), 4);
    }

    /// A column that may hold no null, and that the file lacks, is an error. A
    /// scan cannot fill it, and a silent null would break what the schema states.
    #[test]
    fn a_missing_column_that_may_not_be_null_is_an_error() {
        let target = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let source = Schema::new(vec![Field::new("b", DataType::Int64, true)]);

        let error = BatchAdapter::try_new(target, &source, strict().as_ref())
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

        let adapter = adapter(target, &source, &strict()).expect("map");
        assert!(adapter.adapt(&source).is_err(), "an overflow must be told");
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

        let adapter = adapter(target, &source, &strict()).expect("map");
        assert!(adapter.adapt(&source).is_err(), "\"abc\" must be told");
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

        let adapter = adapter(target, &source, &keeping_first()).expect("map");
        let adapted = adapter
            .adapt(&source)
            .expect("a settled column may not fail");
        let values = adapted
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("Float64");
        assert_eq!(values.value(0), 1.5, "a value the type holds is read");
        assert!(values.is_null(1), "a value it cannot hold reads null");
    }

    /// A type no cast reaches reads null for the whole file, rather than
    /// failing the scan.
    #[test]
    fn a_settled_column_no_cast_reaches_reads_null() {
        let list = DataType::List(Arc::new(Field::new("item", DataType::Int32, true)));
        let target = Arc::new(Schema::new(vec![Field::new("v", DataType::Float64, true)]));
        let source = batch(
            vec![Field::new("v", list, true)],
            vec![Arc::new(
                arrow::array::ListArray::from_iter_primitive::<arrow::datatypes::Int32Type, _, _>(
                    vec![Some(vec![Some(1)]), Some(vec![Some(2)])],
                ),
            )],
        );

        let adapter = adapter(target, &source, &keeping_first()).expect("map");
        let adapted = adapter.adapt(&source).expect("no cast reaches this type");
        assert_eq!(adapted.num_rows(), 2);
        assert_eq!(adapted.column(0).null_count(), 2);
    }

    /// The setting reaches only a pair the rule did not widen. A pair it
    /// widened keeps a strict cast, so an overflow is still an error.
    #[test]
    fn the_setting_leaves_a_widened_column_strict() {
        let source = far_future_seconds();
        let strict_adapter = adapter(nanoseconds(), &source, &strict()).expect("map");
        assert!(
            strict_adapter.adapt(&source).is_err(),
            "an overflow is told"
        );

        let lenient_adapter = adapter(nanoseconds(), &source, &keeping_first()).expect("map");
        assert!(
            lenient_adapter.adapt(&source).is_err(),
            "the setting reads no null for a unit that widens"
        );
    }

    /// DataFusion's own adapter asks the strategy the same way. Every format
    /// that maps its batches with `batch_adapter_factory` gets this.
    #[test]
    fn the_datafusion_adapter_asks_the_strategy() {
        let target = Arc::new(Schema::new(vec![Field::new("v", DataType::Float64, true)]));
        let source = batch(
            vec![Field::new("v", DataType::Utf8, true)],
            vec![Arc::new(StringArray::from(vec!["1.5", "abc"]))],
        );

        let adapted = batch_adapter_factory(target, keeping_first())
            .make_adapter(&source.schema())
            .expect("map")
            .adapt_batch(&source)
            .expect("a settled column may not fail");
        let values = adapted
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("Float64");
        assert_eq!(values.value(0), 1.5);
        assert!(values.is_null(1), "a value it cannot hold reads null");
    }

    /// The same, for a type no cast reaches. DataFusion's rule refuses such a
    /// pair before it builds a cast, so the null comes from the pass above it.
    #[test]
    fn the_datafusion_adapter_reads_an_unreachable_type_as_null() {
        let list = DataType::List(Arc::new(Field::new("item", DataType::Int32, true)));
        let target = Arc::new(Schema::new(vec![Field::new("v", DataType::Float64, true)]));
        let source = batch(
            vec![Field::new("v", list, true)],
            vec![Arc::new(
                arrow::array::ListArray::from_iter_primitive::<arrow::datatypes::Int32Type, _, _>(
                    vec![Some(vec![Some(1)]), Some(vec![Some(2)])],
                ),
            )],
        );

        let adapted = batch_adapter_factory(target, keeping_first())
            .make_adapter(&source.schema())
            .expect("map")
            .adapt_batch(&source)
            .expect("no cast reaches this type");
        assert_eq!(adapted.num_rows(), 2);
        assert_eq!(adapted.column(0).null_count(), 2);
    }

    /// A pair the rule widens keeps DataFusion's strict cast under either
    /// setting, and every pair does under the default rule.
    #[test]
    fn the_datafusion_adapter_leaves_a_widened_column_strict() {
        let source = far_future_seconds();
        for strategy in [strict(), keeping_first()] {
            let adapted = batch_adapter_factory(nanoseconds(), strategy)
                .make_adapter(&source.schema())
                .expect("map")
                .adapt_batch(&source);
            assert!(adapted.is_err(), "an overflow must be told");
        }

        let target = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, true)]));
        let source = batch(
            vec![Field::new("v", DataType::Int64, true)],
            vec![Arc::new(Int64Array::from(vec![i64::MAX]))],
        );
        let adapted = batch_adapter_factory(target, strict())
            .make_adapter(&source.schema())
            .expect("map")
            .adapt_batch(&source);
        assert!(adapted.is_err(), "an overflow must be told");
    }
}
