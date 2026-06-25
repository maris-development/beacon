//! Low-level Lance dataset writes, shared by create / insert / replace.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::error::ArrowError;
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::StreamExt;
use lance::dataset::write::InsertBuilder;
use lance::dataset::{WriteMode, WriteParams};
use lance::session::Session;

/// Map an Arrow data type to one Lance can store. Lance 7.x does not support the
/// Arrow "view" types (`Utf8View`/`BinaryView`) that DataFusion 53 produces for
/// SQL string/binary columns, so they are widened to their non-view equivalents.
fn lance_compatible_type(data_type: &DataType) -> DataType {
    match data_type {
        DataType::Utf8View => DataType::Utf8,
        DataType::BinaryView => DataType::Binary,
        other => other.clone(),
    }
}

/// A Lance-writable version of `schema` (view types widened to non-view).
pub(crate) fn lance_compatible_schema(schema: &Schema) -> SchemaRef {
    let fields = schema
        .fields()
        .iter()
        .map(|f| {
            Arc::new(Field::new(
                f.name(),
                lance_compatible_type(f.data_type()),
                f.is_nullable(),
            ))
        })
        .collect::<Vec<_>>();
    Arc::new(Schema::new(fields))
}

/// Cast a single batch to `target` (only the differing columns are cast).
fn coerce_batch(batch: &RecordBatch, target: &SchemaRef) -> Result<RecordBatch, ArrowError> {
    let columns = batch
        .columns()
        .iter()
        .zip(target.fields())
        .map(|(column, field)| {
            if column.data_type() == field.data_type() {
                Ok(column.clone())
            } else {
                cast(column, field.data_type())
            }
        })
        .collect::<Result<Vec<_>, _>>()?;
    RecordBatch::try_new(target.clone(), columns)
}

/// An empty (zero-row) stream carrying `schema` — used to create an empty
/// dataset that only establishes the schema.
pub fn empty_stream(schema: SchemaRef) -> SendableRecordBatchStream {
    Box::pin(RecordBatchStreamAdapter::new(
        schema,
        futures::stream::empty::<Result<RecordBatch, DataFusionError>>(),
    ))
}

/// How rows should be applied to a dataset.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteKind {
    /// Create a new dataset (errors if one already exists at the location).
    Create,
    /// Append rows to an existing dataset.
    Append,
    /// Replace all rows: a new dataset version containing only the streamed rows.
    Overwrite,
}

impl From<WriteKind> for WriteMode {
    fn from(kind: WriteKind) -> Self {
        match kind {
            WriteKind::Create => WriteMode::Create,
            WriteKind::Append => WriteMode::Append,
            WriteKind::Overwrite => WriteMode::Overwrite,
        }
    }
}

/// Stream `rows` into the Lance dataset at `uri` (a `beacon-tables://` URI),
/// resolved through `session`'s object-store registry. The input stream is fed
/// directly into Lance's [`InsertBuilder`] (no full-table buffering); each batch
/// is coerced to a Lance-writable schema (Arrow view types widened) on the fly.
/// Returns the number of rows written.
///
/// Callers hold the dataset's [`LanceWarehouse`](crate::warehouse::LanceWarehouse)
/// write lock across the call.
pub async fn write_stream(
    uri: &str,
    session: Arc<Session>,
    rows: SendableRecordBatchStream,
    kind: WriteKind,
) -> anyhow::Result<u64> {
    let target = lance_compatible_schema(&rows.schema());

    // Count rows + coerce view types as batches stream past, without collecting.
    let written = Arc::new(AtomicU64::new(0));
    let counter = written.clone();
    let coerce_target = target.clone();
    let coerced = rows.map(move |batch| {
        let batch = batch?;
        counter.fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
        coerce_batch(&batch, &coerce_target).map_err(DataFusionError::from)
    });
    let source: SendableRecordBatchStream =
        Box::pin(RecordBatchStreamAdapter::new(target, coerced));

    let params = WriteParams {
        mode: kind.into(),
        session: Some(session),
        ..Default::default()
    };
    InsertBuilder::new(uri)
        .with_params(&params)
        .execute_stream(source)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to write Lance dataset '{uri}': {e}"))?;

    Ok(written.load(Ordering::Relaxed))
}
