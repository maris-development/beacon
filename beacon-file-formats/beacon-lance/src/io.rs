//! Low-level Lance dataset writes, shared by create / insert / replace.

use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatchIterator;
use lance::dataset::builder::DatasetBuilder;
use lance::dataset::{Dataset, WriteMode, WriteParams};
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

/// Cast `batches` to `target` (only the differing columns are cast).
fn coerce_batches(
    batches: Vec<RecordBatch>,
    target: &SchemaRef,
) -> anyhow::Result<Vec<RecordBatch>> {
    batches
        .into_iter()
        .map(|batch| {
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
            Ok(RecordBatch::try_new(target.clone(), columns)?)
        })
        .collect()
}

/// How a batch of rows should be applied to a dataset.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteKind {
    /// Create a new dataset (errors if one already exists at the location).
    Create,
    /// Append rows to an existing dataset.
    Append,
    /// Replace all rows: a new dataset version containing only `batches`.
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

/// Write `batches` to the Lance dataset at `uri` (a `beacon-tables://` URI),
/// resolved through `session`'s object-store registry. Returns the rows written.
///
/// `Create`/`Overwrite` go through `Dataset::write`; `Append` opens the existing
/// dataset and appends. Callers hold the dataset's
/// [`LanceWarehouse`](crate::warehouse::LanceWarehouse) write lock across the call.
pub async fn write_batches(
    uri: &str,
    session: Arc<Session>,
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
    kind: WriteKind,
) -> anyhow::Result<u64> {
    let num_rows: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();

    // Lance can't store Arrow view types; coerce the schema and data to a
    // Lance-writable form before writing.
    let target = lance_compatible_schema(&schema);
    let batches = coerce_batches(batches, &target)?;

    match kind {
        WriteKind::Append => {
            let mut dataset = DatasetBuilder::from_uri(uri)
                .with_session(session)
                .load()
                .await
                .map_err(|e| anyhow::anyhow!("Failed to open Lance dataset '{uri}': {e}"))?;
            let reader = RecordBatchIterator::new(batches.into_iter().map(Ok), target);
            dataset
                .append(reader, None)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to append to Lance dataset '{uri}': {e}"))?;
        }
        WriteKind::Create | WriteKind::Overwrite => {
            let params = WriteParams {
                mode: kind.into(),
                session: Some(session),
                ..Default::default()
            };
            let reader = RecordBatchIterator::new(batches.into_iter().map(Ok), target);
            Dataset::write(reader, uri, Some(params))
                .await
                .map_err(|e| anyhow::anyhow!("Failed to write Lance dataset '{uri}': {e}"))?;
        }
    }

    Ok(num_rows)
}
