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

use crate::config::LanceConfig;

/// Map an Arrow data type to one Lance can store. Lance 7.x does not support the
/// Arrow "view" types (`Utf8View`/`BinaryView`) that DataFusion 53 produces for
/// SQL string/binary columns, so they are widened to their non-view equivalents.
pub(crate) fn lance_compatible_type(data_type: &DataType) -> DataType {
    match data_type {
        DataType::Utf8View => DataType::Utf8,
        DataType::BinaryView => DataType::Binary,
        other => other.clone(),
    }
}

/// A Lance-writable version of `schema` (view types widened to non-view), with
/// `config`'s encoding hints attached as Arrow field metadata.
pub(crate) fn lance_compatible_schema(schema: &Schema, config: &LanceConfig) -> SchemaRef {
    // Lance takes per-field encoding hints from Arrow field metadata.
    //
    // Lance used to store this data about 2x larger than parquet (27GB vs 14GB on
    // ClickBench). Writing file version 2.2 (see `lance_storage_version`) closes
    // most of that on its own, for free. Every *explicit* encoding lever was
    // measured on a 20M-row subset and all of them cost more than the disk they
    // save, so none is enabled by default:
    //   zstd/lz4 on strings  -14%  but ~11x slower string scans
    //   fsst on strings      +13%  (larger)
    //   dictionary encoding   +5%  and 31x slower GROUP BY
    //   max_rows_per_group      0%  (legacy-format knob, inert on v2.1)
    //   minichunk 64KB          0%  and slightly slower numeric scans
    // Note `compression = none` on a numeric column is a trap: it does not merely
    // skip block compression, it short-circuits Lance's whole encoder chain and
    // disables bitpacking and RLE too. That measured *larger* than the default
    // (6.61GB vs 3.80GB on the 20M subset) and no faster.
    //
    // Note this also preserves any metadata already on the field: rebuilding with
    // `Field::new` alone silently dropped it.
    let compression = config.string_compression();
    let numeric_compression = config.numeric_compression();
    let minichunk = config.minichunk_size();
    let fields = schema
        .fields()
        .iter()
        .map(|f| {
            let mut metadata = f.metadata().clone();
            if let Some(scheme) = compression {
                if is_string_like(f.data_type()) {
                    metadata
                        .entry(COMPRESSION_META_KEY.to_string())
                        .or_insert_with(|| scheme.to_string());
                }
            }
            if let Some(scheme) = numeric_compression {
                if !is_string_like(f.data_type()) {
                    metadata
                        .entry(COMPRESSION_META_KEY.to_string())
                        .or_insert_with(|| scheme.to_string());
                }
            }
            if let Some(size) = minichunk.as_deref() {
                if !is_string_like(f.data_type()) {
                    metadata
                        .entry(MINICHUNK_SIZE_META_KEY.to_string())
                        .or_insert_with(|| size.to_string());
                }
            }
            Arc::new(
                Field::new(
                    f.name(),
                    lance_compatible_type(f.data_type()),
                    f.is_nullable(),
                )
                .with_metadata(metadata),
            )
        })
        .collect::<Vec<_>>();
    Arc::new(Schema::new(fields))
}

/// Arrow field-metadata key Lance reads for a per-column compression scheme.
const COMPRESSION_META_KEY: &str = "lance-encoding:compression";

/// Arrow field-metadata key for Lance's minichunk size (its decode unit for
/// fixed-width columns). Sizes of 32KB and up need file version 2.2 or later;
/// on 2.1 Lance logs a warning and ignores them.
const MINICHUNK_SIZE_META_KEY: &str = "lance-encoding:minichunk-size";

/// True for the types where block compression can pay for itself.
fn is_string_like(dt: &arrow::datatypes::DataType) -> bool {
    use arrow::datatypes::DataType::*;
    matches!(
        dt,
        Utf8 | LargeUtf8 | Utf8View | Binary | LargeBinary | BinaryView
    )
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

/// Stream `rows` into the Lance dataset at `uri` (a `db://` URI),
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
    config: &LanceConfig,
) -> anyhow::Result<u64> {
    let target = lance_compatible_schema(&rows.schema(), config);

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
        data_storage_version: config.storage_version(),
        ..Default::default()
    };
    InsertBuilder::new(uri)
        .with_params(&params)
        .execute_stream(source)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to write Lance dataset '{uri}': {e}"))?;

    Ok(written.load(Ordering::Relaxed))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};

    /// Lance 7.x can't store Arrow "view" types that DataFusion 53 produces, so
    /// they must be widened; every other type passes through untouched.
    #[test]
    fn view_types_are_widened_others_untouched() {
        assert_eq!(lance_compatible_type(&DataType::Utf8View), DataType::Utf8);
        assert_eq!(
            lance_compatible_type(&DataType::BinaryView),
            DataType::Binary
        );
        assert_eq!(lance_compatible_type(&DataType::Int64), DataType::Int64);
        assert_eq!(lance_compatible_type(&DataType::Utf8), DataType::Utf8);
        // Nested/dictionary types are not "view" types and pass through.
        assert_eq!(
            lance_compatible_type(&DataType::List(Arc::new(Field::new(
                "item",
                DataType::Int32,
                true
            )))),
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true)))
        );
    }

    /// The whole schema is rewritten field-by-field, preserving names and
    /// nullability while widening view types (as a `VARCHAR`/`Utf8View` CTAS
    /// column would need).
    #[test]
    fn schema_widening_preserves_names_and_nullability() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8View, true),
            Field::new("blob", DataType::BinaryView, true),
        ]);
        let widened = lance_compatible_schema(&schema, &LanceConfig::default());
        assert_eq!(widened.field(0).data_type(), &DataType::Int64);
        assert_eq!(widened.field(1).data_type(), &DataType::Utf8);
        assert!(widened.field(1).is_nullable());
        assert_eq!(widened.field(1).name(), "name");
        assert_eq!(widened.field(2).data_type(), &DataType::Binary);
        assert!(!widened.field(0).is_nullable());
    }

    /// The encoding hints reach Lance as field metadata, and only on the column
    /// kinds each applies to — string compression on strings, numeric compression
    /// and minichunking on everything else.
    #[test]
    fn config_encoding_hints_land_on_the_right_columns() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]);
        let config = LanceConfig {
            compression: "zstd".to_string(),
            numeric_compression: "lz4".to_string(),
            minichunk: "65536".to_string(),
            ..Default::default()
        };

        let hinted = lance_compatible_schema(&schema, &config);
        let numeric = hinted.field(0).metadata().clone();
        let string = hinted.field(1).metadata().clone();

        assert_eq!(numeric.get(COMPRESSION_META_KEY).map(String::as_str), Some("lz4"));
        assert_eq!(
            numeric.get(MINICHUNK_SIZE_META_KEY).map(String::as_str),
            Some("65536")
        );
        assert_eq!(string.get(COMPRESSION_META_KEY).map(String::as_str), Some("zstd"));
        assert_eq!(string.get(MINICHUNK_SIZE_META_KEY), None);

        // The default config attaches nothing at all.
        let plain = lance_compatible_schema(&schema, &LanceConfig::default());
        assert!(plain.field(0).metadata().is_empty());
        assert!(plain.field(1).metadata().is_empty());
    }

    #[test]
    fn write_kind_maps_to_lance_write_mode() {
        assert!(matches!(WriteMode::from(WriteKind::Create), WriteMode::Create));
        assert!(matches!(WriteMode::from(WriteKind::Append), WriteMode::Append));
        assert!(matches!(
            WriteMode::from(WriteKind::Overwrite),
            WriteMode::Overwrite
        ));
    }

    /// `coerce_batch` casts only the columns whose type differs from the target,
    /// leaving already-matching columns as the exact same array.
    #[test]
    fn coerce_batch_casts_only_differing_columns() {
        let source_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8View, true),
        ]));
        let batch = RecordBatch::try_new(
            source_schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                // Build a Utf8View array by casting from Utf8.
                arrow::compute::cast(
                    &(Arc::new(StringArray::from(vec!["a", "b"])) as arrow::array::ArrayRef),
                    &DataType::Utf8View,
                )
                .unwrap(),
            ],
        )
        .unwrap();

        let target = lance_compatible_schema(&source_schema, &LanceConfig::default());
        let coerced = coerce_batch(&batch, &target).unwrap();
        assert_eq!(coerced.schema().field(1).data_type(), &DataType::Utf8);
        assert_eq!(coerced.num_rows(), 2);
        // The unchanged Int64 column keeps its values through the coercion.
        let ids = coerced
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ids.values(), &[1, 2]);
        let names = coerced
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "a");
    }

    #[test]
    fn empty_stream_carries_schema_and_no_rows() {
        use futures::StreamExt as _;

        let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
            "id",
            DataType::Int64,
            false,
        )]));
        let mut stream = empty_stream(schema.clone());
        assert_eq!(stream.schema(), schema);
        // No batches are produced.
        let rt = tokio::runtime::Runtime::new().unwrap();
        let first = rt.block_on(async { stream.next().await });
        assert!(first.is_none());
    }
}
