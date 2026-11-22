//! Helpers that the NetCDF tests share.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use beacon_datafusion_ext::fast_object::FastObjectTable;
use beacon_datafusion_ext::listing_factory::ListingFactory;
use beacon_nd_array::dataset::Dataset;
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use object_store::{local::LocalFileSystem, ObjectMeta, ObjectStore, ObjectStoreExt};

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
