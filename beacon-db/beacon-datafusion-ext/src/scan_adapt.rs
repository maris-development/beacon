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
//! the merge marks the column with
//! [`TYPE_CONFLICT_KEY`](crate::type_widening::TYPE_CONFLICT_KEY). The files
//! disagree on what such a column holds, so its cast may not fail:
//!
//! - A value the type cannot hold reads as null. `Utf8` "abc" to `Float64`
//!   gives null, not an error.
//! - A type no cast reaches reads as null for the whole file. A list beside a
//!   number is one such pair.
//!
//! Every other cast stays strict, and a value it cannot hold is an error. The
//! merged schema carries the mark, so no scan reads the setting itself.
//!
//! [`TypeConflict::KeepFirst`]: crate::type_widening::TypeConflict::KeepFirst

use std::sync::Arc;

use arrow::array::{ArrayRef, RecordBatch, RecordBatchOptions, new_null_array};
use arrow::compute::{CastOptions, can_cast_types, cast_with_options};
use arrow::datatypes::{DataType, Schema, SchemaRef};
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

use crate::type_widening::is_type_conflict;

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
    /// The map from `source` onto `target`.
    ///
    /// A column of `target` that `source` lacks reads nulls, so such a column
    /// has to be nullable. The merge rule already makes a column that some file
    /// lacks nullable; a schema that a statement declares may not, and this
    /// reports that.
    pub fn try_new(target: SchemaRef, source: &Schema) -> Result<Self> {
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
                Ok(at) if is_type_conflict(field) => {
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
/// that schema the same way, so the two always agree.
pub struct AdaptingOpener {
    inner: Arc<dyn FileOpener>,
    target: SchemaRef,
}

impl AdaptingOpener {
    /// Wrap `inner` so that every batch it produces carries `target`.
    pub fn new(inner: Arc<dyn FileOpener>, target: SchemaRef) -> Self {
        Self { inner, target }
    }

    /// The same, as a `FileOpener` to hand on.
    pub fn wrap(inner: Arc<dyn FileOpener>, target: SchemaRef) -> Arc<dyn FileOpener> {
        Arc::new(Self::new(inner, target))
    }
}

impl FileOpener for AdaptingOpener {
    fn open(&self, partitioned_file: PartitionedFile) -> Result<FileOpenFuture> {
        let target = Arc::clone(&self.target);
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
                    let built = BatchAdapter::try_new(Arc::clone(&target), &source)?;
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
/// The rule reads the mark on the target field, so a schema without a marked
/// column gets what `BatchAdapterFactory::new` gives: a strict cast, and an
/// error for a value the type cannot hold. See the [module docs](self).
pub fn batch_adapter_factory(target: SchemaRef) -> BatchAdapterFactory {
    BatchAdapterFactory::new(target).with_adapter_factory(Arc::new(LenientCastAdapterFactory))
}

/// Builds [`LenientCastAdapter`] for one file.
#[derive(Debug)]
struct LenientCastAdapterFactory;

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
        }))
    }
}

/// DataFusion's rule, with a null for a column the merge could not join.
///
/// Two passes wrap the inner rewrite, because the inner rule refuses a pair no
/// cast reaches before it builds any cast at all:
///
/// 1. Before: a marked column that no cast reaches becomes a null literal. The
///    inner rule then sees a literal and builds no cast.
/// 2. After: the cast of a marked column takes [`LENIENT_CAST_OPTIONS`].
#[derive(Debug)]
struct LenientCastAdapter {
    inner: Arc<dyn PhysicalExprAdapter>,
    /// The schema the table reports, which carries the mark.
    logical_file_schema: SchemaRef,
    /// The schema of the file being read.
    physical_file_schema: SchemaRef,
}

impl LenientCastAdapter {
    /// The target field of `column`, when the merge could not join it.
    fn conflicted(&self, column: &Column) -> Option<&arrow::datatypes::Field> {
        let at = self.logical_file_schema.index_of(column.name()).ok()?;
        let field = self.logical_file_schema.field(at);
        is_type_conflict(field).then_some(field)
    }
}

impl PhysicalExprAdapter for LenientCastAdapter {
    fn rewrite(&self, expr: Arc<dyn PhysicalExpr>) -> Result<Arc<dyn PhysicalExpr>> {
        let expr = expr
            .transform_down(|expr| {
                let Some(column) = expr.as_any().downcast_ref::<Column>() else {
                    return Ok(Transformed::no(expr));
                };
                let Some(field) = self.conflicted(column) else {
                    return Ok(Transformed::no(expr));
                };
                let Ok(at) = self.physical_file_schema.index_of(column.name()) else {
                    // The file lacks the column. The inner rule fills the null.
                    return Ok(Transformed::no(expr));
                };
                if can_cast_types(
                    self.physical_file_schema.field(at).data_type(),
                    field.data_type(),
                ) {
                    return Ok(Transformed::no(expr));
                }
                // No cast reaches this type. The file reads null for the column.
                Ok(Transformed::yes(lit(ScalarValue::try_new_null(
                    field.data_type(),
                )?)))
            })
            .data()?;

        self.inner
            .rewrite(expr)?
            .transform_down(|expr| {
                let Some(cast) = expr.as_any().downcast_ref::<CastColumnExpr>() else {
                    return Ok(Transformed::no(expr));
                };
                if !is_type_conflict(cast.target_field()) {
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
    use crate::type_widening::{TYPE_CONFLICT_FIRST_TYPE, TYPE_CONFLICT_KEY};
    use arrow::array::{Array, Float64Array, Int32Array, Int64Array, StringArray};
    use arrow::datatypes::Field;

    fn batch(fields: Vec<Field>, columns: Vec<ArrayRef>) -> RecordBatch {
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).expect("valid batch")
    }

    /// A column the merge widened is cast to the merged type.
    #[test]
    fn a_widened_column_is_cast() {
        let target = Arc::new(Schema::new(vec![Field::new("v", DataType::Float64, true)]));
        let source = batch(
            vec![Field::new("v", DataType::Int32, true)],
            vec![Arc::new(Int32Array::from(vec![1, 2]))],
        );

        let adapter = BatchAdapter::try_new(target, source.schema().as_ref()).expect("map");
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

        let adapter = BatchAdapter::try_new(target, source.schema().as_ref()).expect("map");
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

        let adapter = BatchAdapter::try_new(target, source.schema().as_ref()).expect("map");
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

        let adapter = BatchAdapter::try_new(target, source.schema().as_ref()).expect("map");
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

        let error = BatchAdapter::try_new(target, &source)
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

        let adapter = BatchAdapter::try_new(target, source.schema().as_ref()).expect("map");
        assert!(adapter.adapt(&source).is_err(), "an overflow must be told");
    }

    // ── a column the merge could not join ──────────────────────────────

    /// `field`, marked as `TypeConflict::KeepFirst` marks it.
    fn conflicted(name: &str, data_type: DataType) -> Field {
        Field::new(name, data_type, true).with_metadata(
            [(
                TYPE_CONFLICT_KEY.to_string(),
                TYPE_CONFLICT_FIRST_TYPE.to_string(),
            )]
            .into_iter()
            .collect(),
        )
    }

    /// A value the target type cannot hold reads as null, not as an error,
    /// because the files disagree on what the column holds.
    #[test]
    fn a_kept_column_reads_a_value_it_cannot_hold_as_null() {
        let target = Arc::new(Schema::new(vec![conflicted("v", DataType::Float64)]));
        let source = batch(
            vec![Field::new("v", DataType::Utf8, true)],
            vec![Arc::new(StringArray::from(vec!["1.5", "abc"]))],
        );

        let adapter = BatchAdapter::try_new(target, source.schema().as_ref()).expect("map");
        let adapted = adapter.adapt(&source).expect("a marked column may not fail");
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
    fn a_kept_column_no_cast_reaches_reads_null() {
        let list = DataType::List(Arc::new(Field::new("item", DataType::Int32, true)));
        let target = Arc::new(Schema::new(vec![conflicted("v", DataType::Float64)]));
        let source = batch(
            vec![Field::new("v", list, true)],
            vec![Arc::new(
                arrow::array::ListArray::from_iter_primitive::<arrow::datatypes::Int32Type, _, _>(
                    vec![Some(vec![Some(1)]), Some(vec![Some(2)])],
                ),
            )],
        );

        let adapter = BatchAdapter::try_new(target, source.schema().as_ref()).expect("map");
        let adapted = adapter.adapt(&source).expect("no cast reaches this type");
        assert_eq!(adapted.num_rows(), 2);
        assert_eq!(adapted.column(0).null_count(), 2);
    }

    /// The mark reaches only the column that carries it. Every other cast stays
    /// strict, so an overflow is still an error.
    #[test]
    fn the_mark_leaves_every_other_column_strict() {
        let target = Arc::new(Schema::new(vec![
            conflicted("kept", DataType::Float64),
            Field::new("plain", DataType::Int32, true),
        ]));
        let source = batch(
            vec![
                Field::new("kept", DataType::Utf8, true),
                Field::new("plain", DataType::Int64, true),
            ],
            vec![
                Arc::new(StringArray::from(vec!["abc"])),
                Arc::new(Int64Array::from(vec![i64::MAX])),
            ],
        );

        let adapter = BatchAdapter::try_new(target, source.schema().as_ref()).expect("map");
        assert!(
            adapter.adapt(&source).is_err(),
            "the unmarked column still reports its overflow"
        );
    }

    /// DataFusion's own adapter reads the mark the same way. Every format that
    /// maps its batches with `batch_adapter_factory` gets this.
    #[test]
    fn the_datafusion_adapter_reads_the_mark() {
        let target = Arc::new(Schema::new(vec![conflicted("v", DataType::Float64)]));
        let source = batch(
            vec![Field::new("v", DataType::Utf8, true)],
            vec![Arc::new(StringArray::from(vec!["1.5", "abc"]))],
        );

        let adapted = batch_adapter_factory(target)
            .make_adapter(&source.schema())
            .expect("map")
            .adapt_batch(&source)
            .expect("a marked column may not fail");
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
        let target = Arc::new(Schema::new(vec![conflicted("v", DataType::Float64)]));
        let source = batch(
            vec![Field::new("v", list, true)],
            vec![Arc::new(
                arrow::array::ListArray::from_iter_primitive::<arrow::datatypes::Int32Type, _, _>(
                    vec![Some(vec![Some(1)]), Some(vec![Some(2)])],
                ),
            )],
        );

        let adapted = batch_adapter_factory(target)
            .make_adapter(&source.schema())
            .expect("map")
            .adapt_batch(&source)
            .expect("no cast reaches this type");
        assert_eq!(adapted.num_rows(), 2);
        assert_eq!(adapted.column(0).null_count(), 2);
    }

    /// An unmarked column keeps DataFusion's strict cast.
    #[test]
    fn the_datafusion_adapter_leaves_an_unmarked_column_strict() {
        let target = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, true)]));
        let source = batch(
            vec![Field::new("v", DataType::Int64, true)],
            vec![Arc::new(Int64Array::from(vec![i64::MAX]))],
        );

        let adapted = batch_adapter_factory(target)
            .make_adapter(&source.schema())
            .expect("map")
            .adapt_batch(&source);
        assert!(adapted.is_err(), "an overflow must be told");
    }
}
