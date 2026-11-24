//! Helpers that the NetCDF tests share.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{ArrayRef, AsArray, Float64Array};
use arrow::datatypes::{
    DataType, Field, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, Schema,
    UInt16Type, UInt32Type, UInt64Type, UInt8Type,
};
use arrow::record_batch::RecordBatch;
use beacon_datafusion_ext::fast_object::FastObjectTable;
use beacon_datafusion_ext::listing_factory::ListingFactory;
use beacon_nd_array::dataset::Dataset;
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use object_store::{local::LocalFileSystem, ObjectMeta, ObjectStore, ObjectStoreExt};
use oxcdf::{AttributeValue, DType, Extent, Extents};

use super::options::NetcdfOptions;
use super::NetcdfFormat;
use crate::encoders::default::DefaultEncoder;
use crate::writer::ArrowRecordBatchWriter;

/// A regular gridded file: `analysed_sst` on `[time, lat, lon]`, with `lat` in 2 native chunks.
pub(crate) const GRIDDED_FILE: &str = "gridded-example.nc";
/// A ragged file, which the reader refuses.
pub(crate) const WOD_FILE: &str = "wod_ctd_1964.nc";

/// The absolute path of a bundled test file.
pub(crate) fn test_file(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("test_files")
        .join(name)
}

/// The local store and the object of a file on disk.
pub(crate) async fn local_object(path: &Path) -> (Arc<dyn ObjectStore>, ObjectMeta) {
    let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new());
    let location = object_store::path::Path::from_absolute_path(path).unwrap();
    let object = store.head(&location).await.unwrap();
    (store, object)
}

/// Open a bundled regular test file.
pub(crate) async fn open_test_file(name: &str) -> Dataset {
    let (store, object) = local_object(&test_file(name)).await;
    super::open::open_whole(&store, &object).await.unwrap()
}

/// Write the `Float64` columns of `columns` to a new NetCDF file at `path`.
pub(crate) fn write_netcdf(path: &Path, columns: Vec<(String, Vec<f64>)>) {
    let schema = Arc::new(Schema::new(
        columns
            .iter()
            .map(|(name, _)| Field::new(name, DataType::Float64, false))
            .collect::<Vec<_>>(),
    ));
    let arrays: Vec<ArrayRef> = columns
        .into_iter()
        .map(|(_, values)| Arc::new(Float64Array::from(values)) as ArrayRef)
        .collect();
    let batch = RecordBatch::try_new(schema.clone(), arrays).unwrap();

    let mut writer =
        ArrowRecordBatchWriter::<DefaultEncoder>::new(path, schema).expect("a netCDF writer");
    writer.write_record_batch(batch).expect("write the batch");
    writer.finish().expect("finish the file");
}

/// One variable for [`write_cf_netcdf`].
///
/// A binary `values` array makes a `char` variable that holds its bytes in order.
pub(crate) struct TestVariable {
    pub name: &'static str,
    pub dimensions: Vec<&'static str>,
    pub values: ArrayRef,
    pub attributes: Vec<(&'static str, AttributeValue)>,
}

/// Write `variables` to a new NetCDF file at `path`, with the `oxcdf` writer.
///
/// A dimension size of `None` marks an unlimited dimension. Its length for a
/// variable is the value count over the product of the other sizes. A
/// `_FillValue` attribute becomes the fill value of the variable.
pub(crate) fn write_cf_netcdf(
    path: &Path,
    dimensions: &[(&str, Option<usize>)],
    variables: Vec<TestVariable>,
) {
    let mut file = oxcdf::create(path).expect("create the file");
    for (name, size) in dimensions {
        match size {
            Some(size) => file.add_dimension(name, *size),
            None => file.add_unlimited_dimension(name),
        }
        .expect("add the dimension");
    }
    for variable in variables {
        let values = variable.values.as_ref();
        let bytes: Option<Vec<u8>> = match values.data_type() {
            DataType::Binary => Some(
                values
                    .as_binary::<i32>()
                    .iter()
                    .flatten()
                    .flatten()
                    .copied()
                    .collect(),
            ),
            DataType::FixedSizeBinary(_) => Some(
                values
                    .as_fixed_size_binary()
                    .iter()
                    .flatten()
                    .flatten()
                    .copied()
                    .collect(),
            ),
            _ => None,
        };
        let dtype = match values.data_type() {
            DataType::Binary | DataType::FixedSizeBinary(_) => DType::Char,
            DataType::Utf8 => DType::String,
            DataType::Int8 => DType::Int(1),
            DataType::Int16 => DType::Int(2),
            DataType::Int32 => DType::Int(4),
            DataType::Int64 => DType::Int(8),
            DataType::UInt8 => DType::Uint(1),
            DataType::UInt16 => DType::Uint(2),
            DataType::UInt32 => DType::Uint(4),
            DataType::UInt64 => DType::Uint(8),
            DataType::Float32 => DType::Float(4),
            DataType::Float64 => DType::Float(8),
            other => panic!("no NetCDF type for {other}"),
        };
        let count = bytes.as_ref().map_or(values.len(), Vec::len);
        let fixed: usize = variable
            .dimensions
            .iter()
            .filter_map(|name| dimension_size(dimensions, name))
            .product();
        let extents: Vec<Extent> = variable
            .dimensions
            .iter()
            .map(|name| Extent::SliceCount {
                start: 0,
                count: dimension_size(dimensions, name).unwrap_or(count / fixed),
                stride: 1,
            })
            .collect();
        let extents = Extents::Extent(extents);

        let mut writer = file
            .add_variable_with_type(variable.name, &variable.dimensions, &dtype)
            .expect("add the variable");
        for (name, value) in variable.attributes {
            if name == "_FillValue" {
                match value {
                    AttributeValue::Str(fill) => writer.set_fill_value(fill.as_bytes()[0]),
                    other => writer.set_fill_value(other.as_f64().expect("a numeric fill")),
                }
                .expect("set the fill value");
            } else {
                writer
                    .put_attribute(name, value)
                    .expect("put the attribute");
            }
        }

        macro_rules! put {
            ($($arrow:ty => $data:pat),*) => {
                match values.data_type() {
                    $($data => writer.put_values(values.as_primitive::<$arrow>().values(), extents),)*
                    DataType::Utf8 => {
                        let strings: Vec<&str> = values.as_string::<i32>().iter().map(|v| v.unwrap_or("")).collect();
                        writer.put_strings(&strings, extents)
                    }
                    _ => writer.put_values(bytes.as_deref().expect("char bytes"), extents),
                }
            };
        }
        put!(
            Int8Type => DataType::Int8,
            Int16Type => DataType::Int16,
            Int32Type => DataType::Int32,
            Int64Type => DataType::Int64,
            UInt8Type => DataType::UInt8,
            UInt16Type => DataType::UInt16,
            UInt32Type => DataType::UInt32,
            UInt64Type => DataType::UInt64,
            Float32Type => DataType::Float32,
            Float64Type => DataType::Float64
        )
        .expect("write the values");
    }
    file.close().expect("close the file");
}

/// The size of a fixed dimension. `None` for an unlimited dimension.
fn dimension_size(dimensions: &[(&str, Option<usize>)], name: &str) -> Option<usize> {
    dimensions
        .iter()
        .find(|(dimension, _)| *dimension == name)
        .and_then(|(_, size)| *size)
}

/// Write one small regular file: column `V0` holds `0.0, 1.0, ..` for `rows` rows.
pub(crate) fn write_regular_netcdf(dir: &Path, name: &str, rows: usize) -> PathBuf {
    let path = dir.join(name);
    write_netcdf(
        &path,
        vec![("V0".to_string(), (0..rows).map(|row| row as f64).collect())],
    );
    path
}

/// A format with the default options.
pub(crate) fn format() -> NetcdfFormat {
    NetcdfFormat::new(
        Arc::new(ListingFactory::dynamic()),
        NetcdfOptions::default(),
    )
}

/// A session with `target_partitions` partitions and `batch_size` rows in a batch.
pub(crate) fn session(target_partitions: usize, batch_size: usize) -> SessionContext {
    let config = SessionConfig::new()
        .with_target_partitions(target_partitions)
        .with_batch_size(batch_size)
        // `FastObjectTable` merges its schemas through this.
        .with_extension(
            beacon_datafusion_ext::type_widening::ArrowTypeWidening::default_extension(),
        );
    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .build();
    SessionContext::new_with_state(state)
}

/// A one-partition session, so a scan gives its rows in a stable order.
pub(crate) fn one_partition_session() -> SessionContext {
    session(1, 8192)
}

/// Register the file or directory at `path` as `table`, with `format`.
pub(crate) async fn register_with(
    ctx: &SessionContext,
    table: &str,
    path: &Path,
    format: NetcdfFormat,
) -> datafusion::error::Result<()> {
    let url = ListingTableUrl::parse(path.to_string_lossy()).unwrap();
    let listing = FastObjectTable::try_new(&ctx.state(), Arc::new(format), vec![url]).await?;
    ctx.register_table(table, Arc::new(listing))?;
    Ok(())
}

/// Register the file or directory at `path` as `table`, with the default format.
pub(crate) async fn register(ctx: &SessionContext, table: &str, path: &Path) {
    register_with(ctx, table, path, format())
        .await
        .unwrap_or_else(|e| panic!("register {}: {e}", path.display()));
}
