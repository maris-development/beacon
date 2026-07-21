//! Writing Arrow data out as a Zarr v3 store.
//!
//! A zarr store is a *directory* of metadata and chunk files, not a single
//! file. [`ZarrStoreWriter`] builds that directory on the local filesystem, and
//! [`archive_store`] optionally packs it into one zip file for callers (such as
//! the HTTP query API) that can only hand back a single artifact.
//!
//! Two shapes are supported, mirroring the NetCDF sinks:
//!
//! * **Flat** — every column becomes a 1-D array over a shared `obs` dimension.
//!   Rows stream in and are appended, so memory stays bounded.
//! * **Gridded** — named dimension columns become coordinate arrays and the
//!   remaining columns become N-D arrays indexed by them. See
//!   [`crate::datafusion::sink::ZarrNdSink`].
//!
//! Arrays carry CF attributes (`units`/`calendar` for temporal columns) so that
//! Beacon's own zarr reader — and xarray — decode what we wrote back to the
//! original Arrow types. See [`fill_value_for`] for how nulls are represented.

use std::collections::HashMap;
use std::io::{Seek, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{Array, AsArray};
use arrow::datatypes::{DataType, Field, TimeUnit};
use zarrs::array::{ArrayBuilder, DimensionName};
use zarrs::filesystem::FilesystemStore;
use zarrs::group::GroupBuilder;

use crate::datafusion::options::{ZarrCompression, ZarrOptions, ZarrVersion};

/// Dimension name used by flat (record-oriented) output.
pub const OBS_DIMENSION: &str = "obs";

/// Errors raised while writing a zarr store.
#[derive(Debug, thiserror::Error)]
pub enum ZarrWriteError {
    #[error("unsupported Arrow data type {data_type} in column '{column}'")]
    UnsupportedDataType { column: String, data_type: DataType },
    #[error("zarr v2 output is not supported yet; only v3 stores can be written")]
    UnsupportedVersion,
    #[error("zarr error: {0}")]
    Zarr(String),
    #[error(transparent)]
    Arrow(#[from] arrow::error::ArrowError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

type Result<T> = std::result::Result<T, ZarrWriteError>;

fn zarr_err(e: impl std::fmt::Display) -> ZarrWriteError {
    ZarrWriteError::Zarr(e.to_string())
}

// ─── Arrow → zarr type mapping ───────────────────────────────────────────────

/// The zarr v3 data type name a column is stored as, plus the CF attributes
/// needed to decode it back into the original Arrow type.
struct ZarrColumnType {
    /// Zarr v3 data type name (e.g. `"float64"`).
    name: &'static str,
    /// Attributes written alongside the array (CF `units`, `calendar`, …).
    attributes: Vec<(&'static str, serde_json::Value)>,
}

/// Maps an Arrow field onto a zarr v3 data type.
///
/// Temporal types are stored as their underlying integer with CF `units`, which
/// is what [`crate::compat::array_to_nd_array`]'s `CfTimeBackend` reads back.
fn zarr_type_for(field: &Field) -> Result<ZarrColumnType> {
    let plain = |name| ZarrColumnType {
        name,
        attributes: Vec::new(),
    };

    Ok(match field.data_type() {
        DataType::Boolean => plain("bool"),
        DataType::Int8 => plain("int8"),
        DataType::Int16 => plain("int16"),
        DataType::Int32 => plain("int32"),
        DataType::Int64 => plain("int64"),
        DataType::UInt8 => plain("uint8"),
        DataType::UInt16 => plain("uint16"),
        DataType::UInt32 => plain("uint32"),
        DataType::UInt64 => plain("uint64"),
        DataType::Float32 => plain("float32"),
        DataType::Float64 => plain("float64"),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => plain("string"),
        DataType::Timestamp(unit, _) => {
            let unit = match unit {
                TimeUnit::Second => "seconds",
                TimeUnit::Millisecond => "milliseconds",
                TimeUnit::Microsecond => "microseconds",
                TimeUnit::Nanosecond => "nanoseconds",
            };
            ZarrColumnType {
                name: "int64",
                attributes: vec![
                    (
                        "units",
                        format!("{unit} since 1970-01-01T00:00:00Z").into(),
                    ),
                    ("calendar", "gregorian".into()),
                    ("standard_name", "time".into()),
                ],
            }
        }
        DataType::Date32 => ZarrColumnType {
            name: "int32",
            attributes: vec![
                ("units", "days since 1970-01-01T00:00:00Z".into()),
                ("calendar", "gregorian".into()),
            ],
        },
        DataType::Date64 => ZarrColumnType {
            name: "int64",
            attributes: vec![
                ("units", "milliseconds since 1970-01-01T00:00:00Z".into()),
                ("calendar", "gregorian".into()),
            ],
        },
        other => {
            return Err(ZarrWriteError::UnsupportedDataType {
                column: field.name().clone(),
                data_type: other.clone(),
            });
        }
    })
}

/// Whether a field can be written at all.
pub fn is_supported_field(field: &Field) -> bool {
    zarr_type_for(field).is_ok()
}

// ─── Store writer ────────────────────────────────────────────────────────────

/// Builds a Zarr v3 store on the local filesystem from Arrow data.
///
/// Arrays are created lazily on first write and their metadata is only flushed
/// in [`finish`](Self::finish), which lets the flat path grow the `obs`
/// dimension as batches arrive without knowing the row count up front.
pub struct ZarrStoreWriter {
    store: Arc<FilesystemStore>,
    root: PathBuf,
    options: ZarrOptions,
    arrays: HashMap<String, zarrs::array::Array<FilesystemStore>>,
    /// Insertion order of `arrays`, so metadata is written deterministically.
    order: Vec<String>,
}

impl ZarrStoreWriter {
    /// Create a store rooted at `root`, which must not already exist as a file.
    pub fn new(root: PathBuf, options: ZarrOptions) -> Result<Self> {
        if options.zarr_version != ZarrVersion::V3 {
            return Err(ZarrWriteError::UnsupportedVersion);
        }
        std::fs::create_dir_all(&root)?;
        let store = Arc::new(FilesystemStore::new(&root).map_err(zarr_err)?);

        GroupBuilder::new()
            .build(store.clone(), "/")
            .map_err(zarr_err)?
            .store_metadata()
            .map_err(zarr_err)?;

        Ok(Self {
            store,
            root,
            options,
            arrays: HashMap::new(),
            order: Vec::new(),
        })
    }

    /// The directory the store is being written into.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Declare an array of `shape` indexed by `dimensions`.
    ///
    /// The array is created with its full shape; the flat path passes a shape of
    /// zero and grows it via [`append`](Self::append).
    pub fn create_array(
        &mut self,
        field: &Field,
        shape: Vec<u64>,
        dimensions: &[String],
    ) -> Result<()> {
        let column_type = zarr_type_for(field)?;
        let chunk_shape = self.chunk_shape(&shape);

        let mut builder = ArrayBuilder::new(
            shape,
            chunk_shape,
            column_type.name,
            fill_value_for(column_type.name),
        );
        builder.dimension_names(Some(
            dimensions
                .iter()
                .map(|d| DimensionName::Some(d.clone()))
                .collect::<Vec<_>>(),
        ));

        for (key, value) in column_type.attributes {
            builder.attributes_mut().insert(key.to_string(), value);
        }
        match self.options.compression {
            ZarrCompression::None => {}
            ZarrCompression::Zstd => {
                builder.bytes_to_bytes_codecs(vec![Arc::new(
                    zarrs::array::codec::bytes_to_bytes::zstd::ZstdCodec::new(3, false),
                )]);
            }
            ZarrCompression::Gzip => {
                builder.bytes_to_bytes_codecs(vec![Arc::new(
                    zarrs::array::codec::bytes_to_bytes::gzip::GzipCodec::new(5).map_err(zarr_err)?,
                )]);
            }
        }

        let array = builder
            .build(self.store.clone(), &format!("/{}", field.name()))
            .map_err(zarr_err)?;

        self.order.push(field.name().clone());
        self.arrays.insert(field.name().clone(), array);
        Ok(())
    }

    /// Chunk shape for an array: the configured chunk size along the first
    /// (record) dimension, whole extents along the rest.
    ///
    /// Gridded arrays are typically small along the trailing coordinate axes, so
    /// chunking those too would produce a large number of tiny files.
    fn chunk_shape(&self, shape: &[u64]) -> Vec<u64> {
        let chunk = self.options.chunk_size.max(1) as u64;
        shape
            .iter()
            .enumerate()
            .map(|(i, &extent)| {
                if i == 0 {
                    // A zero extent means the array grows as rows stream in, so
                    // the chunk size cannot be clamped to it yet.
                    if extent == 0 {
                        chunk
                    } else {
                        chunk.min(extent)
                    }
                } else {
                    extent.max(1)
                }
            })
            .collect()
    }

    /// Write `values` into `array_name` over `offset..offset + len` of the first
    /// dimension, growing the array if needed.
    ///
    /// Used by the flat path, where the total row count is unknown until the
    /// stream ends.
    pub fn append(&mut self, field: &Field, offset: u64, values: &dyn Array) -> Result<()> {
        let array = self
            .arrays
            .get_mut(field.name())
            .ok_or_else(|| zarr_err(format!("array '{}' was not created", field.name())))?;

        let len = values.len() as u64;
        if len == 0 {
            return Ok(());
        }

        let end = offset + len;
        if array.shape()[0] < end {
            let mut shape = array.shape().to_vec();
            shape[0] = end;
            array.set_shape(shape).map_err(zarr_err)?;
        }

        let subset = zarrs::array::ArraySubset::new_with_ranges(&[offset..end]);
        store_subset(array, &subset, field, values)
    }

    /// Write a fully-formed slab covering the whole extent of `field`'s array.
    ///
    /// Used by the gridded path, which knows the complete shape up front.
    pub fn write_full(&mut self, field: &Field, values: &dyn Array) -> Result<()> {
        let array = self
            .arrays
            .get_mut(field.name())
            .ok_or_else(|| zarr_err(format!("array '{}' was not created", field.name())))?;

        let subset = zarrs::array::ArraySubset::new_with_shape(array.shape().to_vec());
        if subset.num_elements() != values.len() as u64 {
            return Err(zarr_err(format!(
                "column '{}' has {} values but its zarr array holds {}",
                field.name(),
                values.len(),
                subset.num_elements()
            )));
        }
        store_subset(array, &subset, field, values)
    }

    /// Flush every array's metadata, completing the store.
    pub fn finish(self) -> Result<()> {
        for name in &self.order {
            self.arrays[name].store_metadata().map_err(zarr_err)?;
        }
        Ok(())
    }
}

/// Encodes an Arrow array into the zarr subset, substituting the array's fill
/// value for nulls.
fn store_subset(
    array: &zarrs::array::Array<FilesystemStore>,
    subset: &zarrs::array::ArraySubset,
    field: &Field,
    values: &dyn Array,
) -> Result<()> {
    /// Downcast a primitive Arrow column and store it, mapping nulls to `$fill`.
    macro_rules! store_primitive {
        ($arrow_type:ty, $rust_type:ty, $fill:expr) => {{
            let typed = values.as_primitive::<$arrow_type>();
            let elements: Vec<$rust_type> = (0..typed.len())
                .map(|i| {
                    if typed.is_null(i) {
                        $fill
                    } else {
                        typed.value(i) as $rust_type
                    }
                })
                .collect();
            array
                .store_array_subset(subset, elements)
                .map_err(zarr_err)
        }};
    }

    use arrow::datatypes::*;

    match field.data_type() {
        DataType::Boolean => {
            let typed = values.as_boolean();
            let elements: Vec<bool> = (0..typed.len())
                .map(|i| !typed.is_null(i) && typed.value(i))
                .collect();
            array
                .store_array_subset(subset, elements)
                .map_err(zarr_err)
        }
        DataType::Int8 => store_primitive!(Int8Type, i8, 0),
        DataType::Int16 => store_primitive!(Int16Type, i16, 0),
        DataType::Int32 => store_primitive!(Int32Type, i32, 0),
        DataType::Int64 => store_primitive!(Int64Type, i64, 0),
        DataType::UInt8 => store_primitive!(UInt8Type, u8, 0),
        DataType::UInt16 => store_primitive!(UInt16Type, u16, 0),
        DataType::UInt32 => store_primitive!(UInt32Type, u32, 0),
        DataType::UInt64 => store_primitive!(UInt64Type, u64, 0),
        DataType::Float32 => store_primitive!(Float32Type, f32, f32::NAN),
        DataType::Float64 => store_primitive!(Float64Type, f64, f64::NAN),
        DataType::Date32 => store_primitive!(Date32Type, i32, 0),
        DataType::Date64 => store_primitive!(Date64Type, i64, 0),
        DataType::Timestamp(unit, _) => match unit {
            TimeUnit::Second => store_primitive!(TimestampSecondType, i64, 0),
            TimeUnit::Millisecond => store_primitive!(TimestampMillisecondType, i64, 0),
            TimeUnit::Microsecond => store_primitive!(TimestampMicrosecondType, i64, 0),
            TimeUnit::Nanosecond => store_primitive!(TimestampNanosecondType, i64, 0),
        },
        DataType::Utf8 => store_strings(array, subset, values.as_string::<i32>().iter()),
        DataType::LargeUtf8 => store_strings(array, subset, values.as_string::<i64>().iter()),
        DataType::Utf8View => store_strings(array, subset, values.as_string_view().iter()),
        other => Err(ZarrWriteError::UnsupportedDataType {
            column: field.name().clone(),
            data_type: other.clone(),
        }),
    }
}

fn store_strings<'a>(
    array: &zarrs::array::Array<FilesystemStore>,
    subset: &zarrs::array::ArraySubset,
    values: impl Iterator<Item = Option<&'a str>>,
) -> Result<()> {
    let elements: Vec<String> = values.map(|v| v.unwrap_or_default().to_string()).collect();
    array
        .store_array_subset(subset, elements)
        .map_err(zarr_err)
}

/// The zarr fill value for a data type — what Arrow nulls are encoded as.
///
/// Floats use NaN, the conventional missing-value marker that xarray and CF
/// tooling already treat as missing. Other types have no sentinel that cannot
/// collide with real data, so their nulls become zero / `false` / `""` and are
/// indistinguishable from those values on read. No `_FillValue` attribute is
/// written: it cannot express NaN as a JSON number, and advertising a colliding
/// sentinel for the other types would mask real values.
fn fill_value_for(zarr_type: &str) -> zarrs::metadata::FillValueMetadata {
    use zarrs::metadata::FillValueMetadata;
    match zarr_type {
        "float32" | "float64" => FillValueMetadata::String("NaN".to_string()),
        "bool" => FillValueMetadata::Bool(false),
        "string" => FillValueMetadata::String(String::new()),
        _ => FillValueMetadata::Number(serde_json::Number::from(0)),
    }
}

// ─── Zip packaging ───────────────────────────────────────────────────────────

/// Pack a zarr store directory into a zip archive.
///
/// The entries are `Stored` (uncompressed) because chunk data is already
/// compressed by its codec, and stored entries let `zarr-python`'s `ZipStore`
/// and fsspec read chunks without inflating the whole archive.
pub fn archive_store<W: Write + Seek>(store_root: &Path, writer: W) -> Result<()> {
    let mut zip = zip::ZipWriter::new(writer);
    let options: zip::write::FileOptions<'_, ()> =
        zip::write::FileOptions::default().compression_method(zip::CompressionMethod::Stored);

    let mut stack = vec![store_root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(&dir)? {
            let path = entry?.path();
            // Zip entries are `/`-separated paths relative to the store root, so
            // the archive unpacks straight into a usable store.
            let name = path
                .strip_prefix(store_root)
                .map_err(|e| zarr_err(e))?
                .components()
                .map(|c| c.as_os_str().to_string_lossy())
                .collect::<Vec<_>>()
                .join("/");

            if path.is_dir() {
                stack.push(path);
            } else {
                zip.start_file(name, options).map_err(zarr_err)?;
                let mut file = std::fs::File::open(&path)?;
                std::io::copy(&mut file, &mut zip)?;
            }
        }
    }

    zip.finish().map_err(zarr_err)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int64Array, RecordBatch, StringArray};
    use arrow::datatypes::Schema;
    use datafusion::datasource::listing::{
        ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
    };
    use datafusion::prelude::SessionContext;

    use crate::datafusion::ZarrFormat;

    fn sample_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("time", DataType::Int64, false),
            Field::new("temp", DataType::Float64, true),
            Field::new("label", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![10, 20, 30])),
                Arc::new(Float64Array::from(vec![Some(1.5), None, Some(3.5)])),
                Arc::new(StringArray::from(vec![Some("a"), Some("b"), None])),
            ],
        )
        .unwrap()
    }

    /// Write a flat store, then read it back through the crate's own reader.
    /// This is the round-trip that matters: what we emit must be a store Beacon
    /// (and any other zarr v3 reader) can open.
    #[tokio::test]
    async fn flat_store_round_trips_through_the_reader() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("flat.zarr");
        let batch = sample_batch();
        let schema = batch.schema();

        let mut writer = ZarrStoreWriter::new(root.clone(), ZarrOptions::default()).unwrap();
        let dimensions = vec![OBS_DIMENSION.to_string()];
        for field in schema.fields() {
            writer.create_array(field, vec![0], &dimensions).unwrap();
        }
        for (index, field) in schema.fields().iter().enumerate() {
            writer.append(field, 0, batch.column(index).as_ref()).unwrap();
        }
        writer.finish().unwrap();

        assert!(root.join("zarr.json").exists(), "root group metadata");
        assert!(root.join("temp/zarr.json").exists(), "array metadata");

        let ctx = SessionContext::new();
        let table_path =
            ListingTableUrl::parse(format!("file://{}/", root.display())).unwrap();
        let listing_options = ListingOptions::new(Arc::new(ZarrFormat::default()))
            .with_file_extension("zarr.json");
        let config = ListingTableConfig::new(table_path)
            .with_listing_options(listing_options)
            .infer_schema(&ctx.state())
            .await
            .unwrap();
        ctx.register_table("flat", Arc::new(ListingTable::try_new(config).unwrap()))
            .unwrap();

        let batches = ctx
            .sql("SELECT time, temp, label FROM flat ORDER BY time")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let read_back = arrow::compute::concat_batches(&batches[0].schema(), batches.iter()).unwrap();
        assert_eq!(read_back.num_rows(), 3);
        assert_eq!(
            read_back
                .column(0)
                .as_primitive::<arrow::datatypes::Int64Type>()
                .values(),
            &[10, 20, 30]
        );
        // Arrow nulls in a float column are written as NaN — the conventional
        // zarr/CF missing-value marker — so they read back as NaN, not null.
        let temp = read_back
            .column(1)
            .as_primitive::<arrow::datatypes::Float64Type>();
        assert!(temp.value(1).is_nan(), "null should be encoded as NaN");
        assert_eq!(temp.value(0), 1.5);
    }

    /// The flat path grows the `obs` dimension as batches arrive, so a store
    /// written across several batches must hold every row in order — including
    /// across a chunk boundary, where a partial chunk is re-read and extended.
    #[test]
    fn appending_multiple_batches_grows_the_obs_dimension() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("multi.zarr");
        let field = Field::new("a", DataType::Int64, false);

        // A chunk size of 2 forces the second and third batches to land in
        // chunks that are already partially written.
        let options = ZarrOptions {
            chunk_size: 2,
            ..Default::default()
        };
        let mut writer = ZarrStoreWriter::new(root.clone(), options).unwrap();
        writer
            .create_array(&field, vec![0], &[OBS_DIMENSION.to_string()])
            .unwrap();

        let mut offset = 0u64;
        for values in [vec![1i64, 2, 3], vec![4, 5], vec![6]] {
            let array = Int64Array::from(values);
            writer.append(&field, offset, &array).unwrap();
            offset += array.len() as u64;
        }
        writer.finish().unwrap();

        let store = Arc::new(FilesystemStore::new(&root).unwrap());
        let array = zarrs::array::Array::open(store, "/a").unwrap();
        assert_eq!(array.shape(), &[6]);
        let elements = array
            .retrieve_array_subset::<Vec<i64>>(&zarrs::array::ArraySubset::new_with_shape(vec![6]))
            .unwrap();
        assert_eq!(elements, vec![1, 2, 3, 4, 5, 6]);
    }

    /// Unsupported Arrow types are rejected up front rather than producing a
    /// half-written store.
    #[test]
    fn unsupported_types_are_rejected() {
        let field = Field::new(
            "nested",
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            true,
        );
        assert!(!is_supported_field(&field));
        assert!(is_supported_field(&Field::new("x", DataType::Float32, true)));
    }

    /// The zip archive must contain store-relative, `/`-separated paths so it
    /// unpacks straight into a usable store.
    #[test]
    fn archive_uses_store_relative_paths() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("z.zarr");
        std::fs::create_dir_all(root.join("temp/c/0")).unwrap();
        std::fs::write(root.join("zarr.json"), b"{}").unwrap();
        std::fs::write(root.join("temp/zarr.json"), b"{}").unwrap();
        std::fs::write(root.join("temp/c/0/0"), b"chunk").unwrap();

        let mut buffer = std::io::Cursor::new(Vec::new());
        archive_store(&root, &mut buffer).unwrap();

        let mut archive = zip::ZipArchive::new(buffer).unwrap();
        let mut names: Vec<String> = (0..archive.len())
            .map(|i| archive.by_index(i).unwrap().name().to_string())
            .collect();
        names.sort();
        assert_eq!(names, vec!["temp/c/0/0", "temp/zarr.json", "zarr.json"]);
    }
}
