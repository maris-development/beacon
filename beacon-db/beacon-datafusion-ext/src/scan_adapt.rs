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

use std::sync::Arc;

use arrow::array::{ArrayRef, RecordBatch, RecordBatchOptions, new_null_array};
use arrow::compute::{CastOptions, cast_with_options};
use arrow::datatypes::{DataType, Schema, SchemaRef};
use arrow::util::display::FormatOptions;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{FileOpenFuture, FileOpener};
use datafusion::error::{DataFusionError, Result};
use futures::StreamExt;

/// Where one column of the target schema comes from.
#[derive(Debug, Clone)]
enum Source {
    /// The file holds this column, with this type. Take it as it is.
    Column(usize),
    /// The file holds this column with another type. Cast it.
    Cast(usize, DataType),
    /// The file lacks this column. Read nulls of this type.
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
                Ok(at) => Ok(Source::Cast(at, field.data_type().clone())),
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
                    Source::Cast(at, data_type) => {
                        cast_with_options(batch.column(*at), data_type, &CAST_OPTIONS)?
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int32Array, Int64Array, StringArray};
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
}
