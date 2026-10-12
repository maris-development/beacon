//! Writing Arrow record batches as a flat Zarr v3 store.
//!
//! A zarr store is a *directory* of metadata and chunk files, not a single
//! file. [`ZarrTableWriter`] builds that directory on the local filesystem. Each
//! column becomes a 1-D array over one shared [`OBS_DIMENSION`], so the store is
//! a 2-D table: rows along `obs`, one array per column.
//!
//! [`archive_store`] packs a finished store into one zip file, for callers such
//! as the HTTP query API that can only hand back a single file.
//!
//! Nulls are written as a `_FillValue` sentinel, the maximum of the column's
//! type, as the NetCDF writer does. Beacon's zarr reader and xarray both mask
//! that value back to missing. Temporal columns carry CF `units` so both read
//! them back as times.

use std::io::{Seek, Write};
use std::path::Path;
use std::sync::Arc;

use anyhow::Context;
use arrow::array::{Array, ArrayRef, AsArray, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use zarrs::array::{ArrayBuilder, FillValueMetadata};
use zarrs::filesystem::FilesystemStore;
use zarrs::group::GroupBuilder;

/// The one dimension of a flat store. Every array is indexed by it.
pub const OBS_DIMENSION: &str = "obs";

/// Rows per chunk when no other size is configured.
pub const DEFAULT_CHUNK_SIZE: usize = 64 * 1024;

/// Zstandard level for chunk compression.
const ZSTD_LEVEL: i32 = 3;

/// How a flat zarr store is written.
#[derive(Debug, Clone)]
pub struct ZarrWriteOptions {
    /// Rows per chunk. A chunk is one file, so this trades file count against
    /// read granularity.
    pub chunk_size: usize,
    /// Pack the finished store into one zip file at the output path. The HTTP
    /// query API sets this; `COPY TO` writes a plain directory store.
    pub zip_output: bool,
}

impl Default for ZarrWriteOptions {
    fn default() -> Self {
        Self {
            chunk_size: DEFAULT_CHUNK_SIZE,
            zip_output: false,
        }
    }
}

// ─── Arrow → zarr type mapping ───────────────────────────────────────────────

/// How one Arrow column is stored in zarr.
struct ColumnEncoding {
    /// Zarr v3 data type name, such as `"float64"`.
    data_type: &'static str,
    /// The value nulls are written as. Also the array's `fill_value`.
    fill_value: FillValueMetadata,
    /// The `_FillValue` attribute that marks `fill_value` as missing, if the
    /// type has a sentinel.
    fill_attribute: Option<serde_json::Value>,
    /// Extra attributes, such as CF `units` for temporal columns.
    attributes: Vec<(&'static str, serde_json::Value)>,
}

impl ColumnEncoding {
    fn plain(data_type: &'static str, fill: serde_json::Number) -> Self {
        Self {
            data_type,
            fill_value: FillValueMetadata::Number(fill.clone()),
            fill_attribute: Some(serde_json::Value::Number(fill)),
            attributes: Vec::new(),
        }
    }

    fn time(data_type: &'static str, fill: serde_json::Number, units: String) -> Self {
        let mut encoding = Self::plain(data_type, fill);
        encoding.attributes = vec![("units", units.into()), ("calendar", "gregorian".into())];
        encoding
    }
}

/// The type a column is stored as. A dictionary column is stored as its values.
fn storage_type(data_type: &DataType) -> &DataType {
    match data_type {
        DataType::Dictionary(_, values) => values.as_ref(),
        other => other,
    }
}

fn float_number(value: f64) -> serde_json::Number {
    serde_json::Number::from_f64(value).expect("a finite float is a JSON number")
}

/// Maps an Arrow type onto its zarr encoding, or `None` if zarr cannot hold it.
fn encoding_for(data_type: &DataType) -> Option<ColumnEncoding> {
    use serde_json::Number;

    let since_epoch = |unit: &str| format!("{unit} since 1970-01-01T00:00:00Z");

    Some(match storage_type(data_type) {
        DataType::Boolean => ColumnEncoding {
            data_type: "bool",
            fill_value: FillValueMetadata::Bool(false),
            // A boolean has no spare value, so its nulls read back as false.
            fill_attribute: None,
            attributes: Vec::new(),
        },
        DataType::Int8 => ColumnEncoding::plain("int8", Number::from(i8::MAX)),
        DataType::Int16 => ColumnEncoding::plain("int16", Number::from(i16::MAX)),
        DataType::Int32 => ColumnEncoding::plain("int32", Number::from(i32::MAX)),
        DataType::Int64 => ColumnEncoding::plain("int64", Number::from(i64::MAX)),
        DataType::UInt8 => ColumnEncoding::plain("uint8", Number::from(u8::MAX)),
        DataType::UInt16 => ColumnEncoding::plain("uint16", Number::from(u16::MAX)),
        DataType::UInt32 => ColumnEncoding::plain("uint32", Number::from(u32::MAX)),
        DataType::UInt64 => ColumnEncoding::plain("uint64", Number::from(u64::MAX)),
        DataType::Float32 => ColumnEncoding::plain("float32", float_number(f32::MAX as f64)),
        DataType::Float64 => ColumnEncoding::plain("float64", float_number(f64::MAX)),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => ColumnEncoding {
            data_type: "string",
            fill_value: FillValueMetadata::String(String::new()),
            fill_attribute: Some(serde_json::Value::String(String::new())),
            attributes: Vec::new(),
        },
        DataType::Timestamp(unit, _) => {
            let unit = match unit {
                TimeUnit::Second => "seconds",
                TimeUnit::Millisecond => "milliseconds",
                TimeUnit::Microsecond => "microseconds",
                TimeUnit::Nanosecond => "nanoseconds",
            };
            ColumnEncoding::time("int64", Number::from(i64::MAX), since_epoch(unit))
        }
        DataType::Date32 => {
            ColumnEncoding::time("int32", Number::from(i32::MAX), since_epoch("days"))
        }
        DataType::Date64 => {
            ColumnEncoding::time("int64", Number::from(i64::MAX), since_epoch("milliseconds"))
        }
        _ => return None,
    })
}

/// The columns of `schema` that zarr cannot hold, as `name (type)`.
pub fn unsupported_columns(schema: &Schema) -> Vec<String> {
    schema
        .fields()
        .iter()
        .filter(|field| encoding_for(field.data_type()).is_none())
        .map(|field| format!("{} ({})", field.name(), field.data_type()))
        .collect()
}

// ─── Store writer ────────────────────────────────────────────────────────────

/// Builds a flat Zarr v3 store on the local filesystem from record batches.
///
/// Rows are buffered until a whole chunk is ready, so each chunk file is
/// encoded and written once. Memory stays at about one chunk of rows.
pub struct ZarrTableWriter {
    schema: SchemaRef,
    /// One array per column, in schema order.
    arrays: Vec<zarrs::array::Array<FilesystemStore>>,
    chunk_size: usize,
    /// Rows not yet written, always fewer than one chunk after a write.
    pending: Vec<RecordBatch>,
    pending_rows: usize,
    rows_written: u64,
}

impl ZarrTableWriter {
    /// Creates the store at `root` with one empty array per column of `schema`.
    ///
    /// Fails before it writes anything if a column has a type zarr cannot hold.
    pub fn try_new(root: &Path, schema: SchemaRef, chunk_size: usize) -> anyhow::Result<Self> {
        let unsupported = unsupported_columns(&schema);
        if !unsupported.is_empty() {
            anyhow::bail!(
                "Zarr output does not support these columns: {}",
                unsupported.join(", ")
            );
        }
        let chunk_size = chunk_size.max(1);

        std::fs::create_dir_all(root)
            .with_context(|| format!("create zarr store directory {}", root.display()))?;
        let store = Arc::new(FilesystemStore::new(root)?);
        GroupBuilder::new()
            .build(store.clone(), "/")?
            .store_metadata()?;

        let mut arrays = Vec::with_capacity(schema.fields().len());
        for field in schema.fields() {
            arrays.push(create_array(&store, field, chunk_size)?);
        }

        Ok(Self {
            schema,
            arrays,
            chunk_size,
            pending: Vec::new(),
            pending_rows: 0,
            rows_written: 0,
        })
    }

    /// Buffers `batch` and writes every chunk that is now full.
    pub fn write_batch(&mut self, batch: RecordBatch) -> anyhow::Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        self.pending_rows += batch.num_rows();
        self.pending.push(batch);
        if self.pending_rows >= self.chunk_size {
            self.flush(false)?;
        }
        Ok(())
    }

    /// Writes the remaining rows and the array metadata. Returns the row count.
    pub fn finish(mut self) -> anyhow::Result<u64> {
        self.flush(true)?;
        for array in &self.arrays {
            array.store_metadata()?;
        }
        Ok(self.rows_written)
    }

    /// Writes the buffered rows: only whole chunks, or all of them when `all`.
    fn flush(&mut self, all: bool) -> anyhow::Result<()> {
        if self.pending_rows == 0 {
            return Ok(());
        }
        let rows = arrow::compute::concat_batches(&self.schema, &self.pending)?;
        let count = if all {
            rows.num_rows()
        } else {
            rows.num_rows() / self.chunk_size * self.chunk_size
        };

        let start = self.rows_written;
        let end = start + count as u64;
        let subset =
            zarrs::array::ArraySubset::new_with_ranges(std::slice::from_ref(&(start..end)));
        for (index, array) in self.arrays.iter_mut().enumerate() {
            array.set_shape(vec![end])?;
            let field = self.schema.field(index);
            let column = rows.column(index).slice(0, count);
            store_column(array, &subset, field, &column)
                .with_context(|| format!("write zarr column '{}'", field.name()))?;
        }
        self.rows_written = end;

        let rest = rows.slice(count, rows.num_rows() - count);
        self.pending_rows = rest.num_rows();
        self.pending = if rest.num_rows() == 0 {
            Vec::new()
        } else {
            vec![rest]
        };
        Ok(())
    }
}

/// Creates the empty, growable array for `field` along [`OBS_DIMENSION`].
fn create_array(
    store: &Arc<FilesystemStore>,
    field: &Field,
    chunk_size: usize,
) -> anyhow::Result<zarrs::array::Array<FilesystemStore>> {
    let encoding = encoding_for(field.data_type())
        .with_context(|| format!("column '{}' has no zarr type", field.name()))?;

    let mut builder = ArrayBuilder::new(
        vec![0u64],
        vec![chunk_size as u64],
        encoding.data_type,
        encoding.fill_value,
    );
    builder.dimension_names(Some([OBS_DIMENSION.to_string()]));
    builder.bytes_to_bytes_codecs(vec![Arc::new(
        zarrs::array::codec::bytes_to_bytes::zstd::ZstdCodec::new(ZSTD_LEVEL, false),
    )]);
    let attributes = builder.attributes_mut();
    if let Some(fill) = encoding.fill_attribute {
        attributes.insert("_FillValue".to_string(), fill);
    }
    for (key, value) in encoding.attributes {
        attributes.insert(key.to_string(), value);
    }

    Ok(builder.build(store.clone(), &format!("/{}", field.name()))?)
}

/// Encodes one Arrow column into `subset` of `array`, with nulls as the fill.
fn store_column(
    array: &zarrs::array::Array<FilesystemStore>,
    subset: &zarrs::array::ArraySubset,
    field: &Field,
    values: &ArrayRef,
) -> anyhow::Result<()> {
    use arrow::datatypes::*;

    // A dictionary column is stored as its values.
    let values = match values.data_type() {
        DataType::Dictionary(_, value_type) => arrow::compute::cast(values, value_type)?,
        _ => values.clone(),
    };

    macro_rules! store_primitive {
        ($arrow_type:ty, $rust_type:ty, $fill:expr) => {{
            let typed = values.as_primitive::<$arrow_type>();
            let elements: Vec<$rust_type> =
                typed.iter().map(|value| value.unwrap_or($fill)).collect();
            array.store_array_subset(subset, &elements)?
        }};
    }

    match values.data_type() {
        DataType::Boolean => {
            let elements: Vec<bool> = values
                .as_boolean()
                .iter()
                .map(|value| value.unwrap_or(false))
                .collect();
            array.store_array_subset(subset, &elements)?
        }
        DataType::Int8 => store_primitive!(Int8Type, i8, i8::MAX),
        DataType::Int16 => store_primitive!(Int16Type, i16, i16::MAX),
        DataType::Int32 => store_primitive!(Int32Type, i32, i32::MAX),
        DataType::Int64 => store_primitive!(Int64Type, i64, i64::MAX),
        DataType::UInt8 => store_primitive!(UInt8Type, u8, u8::MAX),
        DataType::UInt16 => store_primitive!(UInt16Type, u16, u16::MAX),
        DataType::UInt32 => store_primitive!(UInt32Type, u32, u32::MAX),
        DataType::UInt64 => store_primitive!(UInt64Type, u64, u64::MAX),
        DataType::Float32 => store_primitive!(Float32Type, f32, f32::MAX),
        DataType::Float64 => store_primitive!(Float64Type, f64, f64::MAX),
        DataType::Date32 => store_primitive!(Date32Type, i32, i32::MAX),
        DataType::Date64 => store_primitive!(Date64Type, i64, i64::MAX),
        DataType::Timestamp(TimeUnit::Second, _) => {
            store_primitive!(TimestampSecondType, i64, i64::MAX)
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            store_primitive!(TimestampMillisecondType, i64, i64::MAX)
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            store_primitive!(TimestampMicrosecondType, i64, i64::MAX)
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            store_primitive!(TimestampNanosecondType, i64, i64::MAX)
        }
        DataType::Utf8 => store_strings(array, subset, values.as_string::<i32>().iter())?,
        DataType::LargeUtf8 => store_strings(array, subset, values.as_string::<i64>().iter())?,
        DataType::Utf8View => store_strings(array, subset, values.as_string_view().iter())?,
        other => anyhow::bail!(
            "column '{}' has type {other}, which zarr output does not support",
            field.name()
        ),
    }
    Ok(())
}

fn store_strings<'a>(
    array: &zarrs::array::Array<FilesystemStore>,
    subset: &zarrs::array::ArraySubset,
    values: impl Iterator<Item = Option<&'a str>>,
) -> anyhow::Result<()> {
    let elements: Vec<String> = values
        .map(|value| value.unwrap_or_default().to_string())
        .collect();
    array.store_array_subset(subset, &elements)?;
    Ok(())
}

// ─── Zip packaging ───────────────────────────────────────────────────────────

/// Packs the zarr store at `store_root` into a zip archive.
///
/// Entries are `Stored`, not deflated: the chunks are already compressed, and
/// stored entries let `zarr-python`'s `ZipStore` read a chunk in place.
pub fn archive_store<W: Write + Seek>(store_root: &Path, writer: W) -> anyhow::Result<()> {
    let mut zip = zip::ZipWriter::new(writer);
    let options: zip::write::FileOptions<'_, ()> =
        zip::write::FileOptions::default().compression_method(zip::CompressionMethod::Stored);

    let mut directories = vec![store_root.to_path_buf()];
    while let Some(directory) = directories.pop() {
        for entry in std::fs::read_dir(&directory)? {
            let path = entry?.path();
            if path.is_dir() {
                directories.push(path);
                continue;
            }
            // Store-relative and `/`-separated, so the archive unpacks into a store.
            let name = path
                .strip_prefix(store_root)?
                .components()
                .map(|component| component.as_os_str().to_string_lossy())
                .collect::<Vec<_>>()
                .join("/");
            zip.start_file(name, options)?;
            let mut file = std::fs::File::open(&path)?;
            std::io::copy(&mut file, &mut zip)?;
        }
    }

    zip.finish()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        DictionaryArray, Float64Array, Int64Array, StringArray, TimestampSecondArray,
    };
    use arrow::datatypes::Int32Type;
    use datafusion::datasource::listing::{
        ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
    };
    use datafusion::prelude::SessionContext;

    use crate::datafusion::ZarrFormat;

    fn sample_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("temp", DataType::Float64, true),
            Field::new("label", DataType::Utf8, true),
            Field::new("time", DataType::Timestamp(TimeUnit::Second, None), true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![Some(10), None, Some(30)])),
                Arc::new(Float64Array::from(vec![Some(1.5), None, Some(3.5)])),
                Arc::new(StringArray::from(vec![Some("a"), Some("b"), None])),
                Arc::new(TimestampSecondArray::from(vec![Some(0), Some(60), None])),
            ],
        )
        .unwrap()
    }

    /// Reads a store back through the crate's own reader as table `store`.
    async fn read_back(root: &Path, sql: &str) -> RecordBatch {
        let ctx = SessionContext::new();
        // The trailing separator marks the path as a directory.
        let dir = format!("{}{}", root.display(), std::path::MAIN_SEPARATOR);
        let table_path = ListingTableUrl::parse(dir).unwrap();
        let listing_options =
            ListingOptions::new(Arc::new(ZarrFormat::default())).with_file_extension("zarr.json");
        let config = ListingTableConfig::new(table_path)
            .with_listing_options(listing_options)
            .infer_schema(&ctx.state())
            .await
            .unwrap();
        ctx.register_table("store", Arc::new(ListingTable::try_new(config).unwrap()))
            .unwrap();
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        arrow::compute::concat_batches(&batches[0].schema(), batches.iter()).unwrap()
    }

    /// What the writer emits must open in Beacon's own reader, with every null
    /// read back as null.
    #[tokio::test]
    async fn flat_store_round_trips_values_and_nulls() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("flat.zarr");
        let batch = sample_batch();

        let mut writer = ZarrTableWriter::try_new(&root, batch.schema(), 1024).unwrap();
        writer.write_batch(batch).unwrap();
        assert_eq!(writer.finish().unwrap(), 3);

        let meta: serde_json::Value =
            serde_json::from_slice(&std::fs::read(root.join("temp/zarr.json")).unwrap()).unwrap();
        assert_eq!(meta["shape"], serde_json::json!([3]));
        assert_eq!(meta["dimension_names"], serde_json::json!(["obs"]));

        let rows = read_back(
            &root,
            "SELECT id, temp, label, time FROM store ORDER BY label",
        )
        .await;
        assert_eq!(rows.num_rows(), 3);

        let id = rows.column(0).as_primitive::<arrow::datatypes::Int64Type>();
        let temp = rows
            .column(1)
            .as_primitive::<arrow::datatypes::Float64Type>();
        let label = arrow::compute::cast(rows.column(2), &DataType::Utf8).unwrap();
        // ORDER BY label puts the null label last.
        assert_eq!(
            label.as_string::<i32>().iter().collect::<Vec<_>>(),
            vec![Some("a"), Some("b"), None]
        );
        assert_eq!(
            id.iter().collect::<Vec<_>>(),
            vec![Some(10), None, Some(30)]
        );
        assert_eq!(
            temp.iter().collect::<Vec<_>>(),
            vec![Some(1.5), None, Some(3.5)]
        );

        let time =
            arrow::compute::cast(rows.column(3), &DataType::Timestamp(TimeUnit::Second, None))
                .unwrap();
        let time = time.as_primitive::<arrow::datatypes::TimestampSecondType>();
        assert_eq!(
            time.iter().collect::<Vec<_>>(),
            vec![Some(0), Some(60), None]
        );
    }

    /// Batches that do not line up with chunks must still land in order, and
    /// every chunk is written as a whole.
    #[test]
    fn batches_across_chunk_boundaries_keep_row_order() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("multi.zarr");
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));

        let mut writer = ZarrTableWriter::try_new(&root, schema.clone(), 2).unwrap();
        for values in [vec![1i64, 2, 3], vec![4, 5], vec![6]] {
            let batch =
                RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(values))])
                    .unwrap();
            writer.write_batch(batch).unwrap();
        }
        assert_eq!(writer.finish().unwrap(), 6);

        let store = Arc::new(FilesystemStore::new(&root).unwrap());
        let array = zarrs::array::Array::open(store, "/a").unwrap();
        assert_eq!(array.shape(), &[6]);
        let elements: Vec<i64> = array
            .retrieve_array_subset(&zarrs::array::ArraySubset::new_with_shape(vec![6]))
            .unwrap();
        assert_eq!(elements, vec![1, 2, 3, 4, 5, 6]);
        for chunk in 0..3 {
            assert!(root.join(format!("a/c/{chunk}")).is_file(), "chunk {chunk}");
        }
    }

    /// A dictionary column is stored as its values.
    #[test]
    fn dictionary_columns_are_stored_as_their_values() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("dict.zarr");
        let keys: DictionaryArray<Int32Type> = vec!["x", "y", "x"].into_iter().collect();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "site",
            keys.data_type().clone(),
            false,
        )]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(keys)]).unwrap();

        let mut writer = ZarrTableWriter::try_new(&root, schema, 16).unwrap();
        writer.write_batch(batch).unwrap();
        writer.finish().unwrap();

        let store = Arc::new(FilesystemStore::new(&root).unwrap());
        let array = zarrs::array::Array::open(store, "/site").unwrap();
        let elements: Vec<String> = array
            .retrieve_array_subset(&zarrs::array::ArraySubset::new_with_shape(vec![3]))
            .unwrap();
        assert_eq!(elements, vec!["x", "y", "x"]);
    }

    /// An empty result is a valid store with zero-length arrays.
    #[test]
    fn an_empty_result_writes_zero_length_arrays() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("empty.zarr");
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));

        let mut writer = ZarrTableWriter::try_new(&root, schema.clone(), 16).unwrap();
        writer.write_batch(RecordBatch::new_empty(schema)).unwrap();
        assert_eq!(writer.finish().unwrap(), 0);

        let meta: serde_json::Value =
            serde_json::from_slice(&std::fs::read(root.join("a/zarr.json")).unwrap()).unwrap();
        assert_eq!(meta["shape"], serde_json::json!([0]));
    }

    /// Columns zarr cannot hold are all named before anything is written.
    #[test]
    fn unsupported_columns_are_rejected_up_front() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("bad.zarr");
        let schema = Arc::new(Schema::new(vec![
            Field::new("ok", DataType::Int32, true),
            Field::new(
                "nested",
                DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                true,
            ),
        ]));

        let err = ZarrTableWriter::try_new(&root, schema, 16).err().unwrap();
        assert!(err.to_string().contains("nested"), "{err}");
        assert!(!err.to_string().contains("ok ("), "{err}");
        assert!(!root.exists(), "nothing is written for a rejected schema");
    }

    /// The archive holds store-relative, `/`-separated paths.
    #[test]
    fn archive_uses_store_relative_paths() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("z.zarr");
        std::fs::create_dir_all(root.join("temp/c")).unwrap();
        std::fs::write(root.join("zarr.json"), b"{}").unwrap();
        std::fs::write(root.join("temp/zarr.json"), b"{}").unwrap();
        std::fs::write(root.join("temp/c/0"), b"chunk").unwrap();

        let mut buffer = std::io::Cursor::new(Vec::new());
        archive_store(&root, &mut buffer).unwrap();

        let mut archive = zip::ZipArchive::new(buffer).unwrap();
        let mut names: Vec<String> = (0..archive.len())
            .map(|i| archive.by_index(i).unwrap().name().to_string())
            .collect();
        names.sort();
        assert_eq!(names, vec!["temp/c/0", "temp/zarr.json", "zarr.json"]);
    }
}
