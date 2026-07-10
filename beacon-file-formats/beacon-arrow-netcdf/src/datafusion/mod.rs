use std::any::Any;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use beacon_common::super_typing::super_type_schema;
use beacon_datafusion_ext::format_ext::{DatasetMetadata, FileFormatFactoryExt};
use beacon_datafusion_ext::unique_values::UniqueValuesExec;
use beacon_object_storage::DatasetsStore;
use datafusion::{
    catalog::{memory::DataSourceExec, Session},
    common::{exec_datafusion_err, GetExt, Statistics},
    datasource::{
        file_format::{file_compression_type::FileCompressionType, FileFormat, FileFormatFactory},
        physical_plan::{FileScanConfig, FileScanConfigBuilder, FileSinkConfig, FileSource},
        sink::DataSinkExec,
    },
    physical_expr::{LexOrdering, LexRequirement, PhysicalSortExpr},
    physical_plan::{
        sorts::{sort::SortExec, sort_preserving_merge::SortPreservingMergeExec},
        ExecutionPlan,
    },
};
use object_store::{ObjectMeta, ObjectStore};

use crate::datafusion::{
    options::NetcdfOptions,
    sink::{NetCDFNdSink, NetCDFSink},
    source::NetCDFSource,
};

const NETCDF_EXTENSION: &str = "nc";

pub mod options;
pub mod reader;
pub mod sink;
pub mod source;
pub mod statistics;
pub mod table_function;

pub use reader::NetcdfReaderCache;
pub use table_function::ReadNetCDFFunc;

/// Runtime configuration for the NetCDF format.
///
/// Plain data with sensible defaults; the caller populates it (there is no
/// environment parsing here, so the crate stays reusable and the host decides
/// where the values come from). These are the *defaults* for a runtime — some
/// can be overridden per table via `CREATE EXTERNAL TABLE ... OPTIONS (...)`.
#[derive(Debug, Clone)]
pub struct NetcdfConfig {
    /// Whether reads consult the shared reader cache by default.
    pub use_reader_cache: bool,
    /// Capacity (number of opened datasets) of the shared reader cache.
    pub reader_cache_size: usize,
    /// Whether to generate per-file statistics during planning.
    pub enable_statistics: bool,
}

impl Default for NetcdfConfig {
    fn default() -> Self {
        Self {
            use_reader_cache: true,
            reader_cache_size: 128,
            enable_statistics: true,
        }
    }
}

/// Parse a boolean value supplied through a `CREATE EXTERNAL TABLE` option.
fn parse_bool_option(key: &str, value: &str) -> datafusion::error::Result<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" | "on" => Ok(true),
        "false" | "0" | "no" | "off" => Ok(false),
        other => Err(exec_datafusion_err!(
            "invalid boolean for NetCDF option '{key}': '{other}'"
        )),
    }
}

#[derive(Debug, Clone)]
pub struct NetCDFFormatFactory {
    pub datasets_object_store: Arc<DatasetsStore>,
    pub options: NetcdfOptions,
    pub config: NetcdfConfig,
    /// Shared reader cache for this runtime, sized from `config`.
    cache: NetcdfReaderCache,
}

impl NetCDFFormatFactory {
    pub fn new(
        datasets_object_store: Arc<DatasetsStore>,
        options: NetcdfOptions,
        config: NetcdfConfig,
    ) -> Self {
        let cache = NetcdfReaderCache::new(config.reader_cache_size);
        Self {
            datasets_object_store,
            options,
            config,
            cache,
        }
    }

    /// Build a [`NetcdfFormat`] with the given per-table effective settings,
    /// wiring in the shared reader cache when caching is enabled.
    fn build_format(
        &self,
        options: NetcdfOptions,
        use_reader_cache: bool,
        enable_statistics: bool,
    ) -> NetcdfFormat {
        let cache = use_reader_cache.then(|| self.cache.clone());
        NetcdfFormat::new(self.datasets_object_store.clone(), options)
            .with_cache(cache)
            .with_enable_statistics(enable_statistics)
    }
}

impl FileFormatFactory for NetCDFFormatFactory {
    fn create(
        &self,
        _state: &dyn Session,
        format_options: &std::collections::HashMap<String, String>,
    ) -> datafusion::error::Result<Arc<dyn FileFormat>> {
        // Per-table overrides from `CREATE EXTERNAL TABLE ... OPTIONS (...)`,
        // defaulting to the runtime config.
        let mut options = self.options.clone();
        let mut use_reader_cache = self.config.use_reader_cache;
        let mut enable_statistics = self.config.enable_statistics;

        if let Some(value) = format_options.get("read_dimensions") {
            options.read_dimensions = Some(
                value
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect(),
            );
        }
        if let Some(value) = format_options.get("use_reader_cache") {
            use_reader_cache = parse_bool_option("use_reader_cache", value)?;
        }
        if let Some(value) = format_options.get("enable_statistics") {
            enable_statistics = parse_bool_option("enable_statistics", value)?;
        }

        Ok(Arc::new(self.build_format(
            options,
            use_reader_cache,
            enable_statistics,
        )))
    }

    fn default(&self) -> Arc<dyn FileFormat> {
        Arc::new(self.build_format(
            self.options.clone(),
            self.config.use_reader_cache,
            self.config.enable_statistics,
        ))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl GetExt for NetCDFFormatFactory {
    fn get_ext(&self) -> String {
        NETCDF_EXTENSION.to_string()
    }
}

impl FileFormatFactoryExt for NetCDFFormatFactory {
    fn discover_datasets(
        &self,
        objects: &[ObjectMeta],
    ) -> datafusion::error::Result<Vec<DatasetMetadata>> {
        let datasets = objects
            .iter()
            .filter(|obj| {
                obj.location
                    .extension()
                    .map(|ext| ext == NETCDF_EXTENSION)
                    .unwrap_or(false)
            })
            .map(|obj| DatasetMetadata::new(obj.location.to_string(), self.get_ext()))
            .collect();
        Ok(datasets)
    }

    fn file_format_name(&self) -> String {
        self.get_ext()
    }
}

#[derive(Debug, Clone)]
pub struct NetcdfFormat {
    pub datasets_object_store: Arc<DatasetsStore>,
    pub options: NetcdfOptions,
    /// Reader cache to consult, or `None` to bypass caching for this format.
    cache: Option<NetcdfReaderCache>,
    /// Whether to generate per-file statistics during planning.
    enable_statistics: bool,
}

impl NetcdfFormat {
    pub fn new(datasets_object_store: Arc<DatasetsStore>, options: NetcdfOptions) -> Self {
        Self {
            datasets_object_store,
            options,
            cache: None,
            enable_statistics: false,
        }
    }

    /// Wire in a reader cache (`Some`) or disable caching (`None`).
    pub fn with_cache(mut self, cache: Option<NetcdfReaderCache>) -> Self {
        self.cache = cache;
        self
    }

    /// Set whether per-file statistics are generated during planning.
    pub fn with_enable_statistics(mut self, enable_statistics: bool) -> Self {
        self.enable_statistics = enable_statistics;
        self
    }
}

#[async_trait::async_trait]
impl FileFormat for NetcdfFormat {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn compression_type(&self) -> Option<FileCompressionType> {
        None
    }

    fn get_ext(&self) -> String {
        NETCDF_EXTENSION.to_string()
    }

    fn get_ext_with_compression(
        &self,
        _file_compression_type: &FileCompressionType,
    ) -> datafusion::error::Result<String> {
        Ok(NETCDF_EXTENSION.to_string())
    }

    async fn infer_schema(
        &self,
        _state: &dyn Session,
        _store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> datafusion::error::Result<SchemaRef> {
        let mut tasks = vec![];
        for object in objects {
            let task = reader::fetch_schema(
                self.datasets_object_store.clone(),
                object.clone(),
                self.options.read_dimensions.clone(),
            );
            tasks.push(task);
        }
        let schemas = futures::future::try_join_all(tasks).await?;
        if schemas.is_empty() {
            // Return a default empty schema
            return Ok(Arc::new(arrow::datatypes::Schema::empty()));
        }
        let schema = super_type_schema(&schemas).map_err(|e| {
            exec_datafusion_err!(
                "Failed to compute super type schema for NetCDF datasets: {}",
                e
            )
        })?;
        Ok(schema.into())
    }

    async fn infer_stats(
        &self,
        _state: &dyn Session,
        _store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        object: &ObjectMeta,
    ) -> datafusion::error::Result<Statistics> {
        if self.enable_statistics {
            Ok(statistics::generate_statistics(
                self.datasets_object_store.clone(),
                object,
                &table_schema,
            )
            .await
            .unwrap_or_else(|e| {
                tracing::warn!(
                    "Failed to generate statistics for object {}: {}",
                    object.location,
                    e
                );
                Statistics::new_unknown(&table_schema)
            }))
        } else {
            Ok(Statistics::new_unknown(&table_schema))
        }
    }

    async fn create_physical_plan(
        &self,
        _state: &dyn Session,
        conf: FileScanConfig,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let table_schema = datafusion::datasource::table_schema::TableSchema::new(
            conf.file_schema().clone(),
            conf.table_partition_cols().clone(),
        );
        // Preserve a projection that the scan pushed down into the incoming
        // source — rebuilding the source below would otherwise drop it.
        let projection = conf.file_source().projection().cloned();
        let source = NetCDFSource::new(
            self.datasets_object_store.clone(),
            self.options.read_dimensions.clone(),
            table_schema,
        )
        .with_cache(self.cache.clone())
        .with_projection(projection);
        let conf = FileScanConfigBuilder::from(conf)
            .with_source(Arc::new(source))
            .build();
        Ok(DataSourceExec::from_data_source(conf))
    }

    async fn create_writer_physical_plan(
        &self,
        input: Arc<dyn ExecutionPlan>,
        _state: &dyn Session,
        conf: FileSinkConfig,
        order_requirements: Option<LexRequirement>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        // NetCDF needs a real local path (the netcdf-c writer cannot stream to an
        // object store). Write into the configured tmp store root, threaded in via
        // the datasets store's `StorageConfig`, rather than the OS temp dir.
        let output_dir = self.datasets_object_store.storage().tmp_dir.clone();
        match &self.options.write_dimensions {
            Some(dim_columns) if !dim_columns.is_empty() => {
                let unique_columns = dim_columns.clone();

                let (unique_exec, collection_handle) =
                    UniqueValuesExec::new(input, unique_columns.clone())?;

                // Create lex order requirements based on the unique columns
                let schema = unique_exec.schema();
                let mut sort_exprs = vec![];
                for col in &unique_columns {
                    sort_exprs.push(PhysicalSortExpr::new_default(
                        datafusion::physical_expr::expressions::col(col, &schema)?,
                    ));
                }
                let lex_order = LexOrdering::new(sort_exprs).ok_or(exec_datafusion_err!(
                    "Failed to create LexOrdering for NetCDF dimension columns"
                ))?;

                let sort_exec = SortExec::new(lex_order.clone(), Arc::new(unique_exec));
                let sort_preserving_merge_exec =
                    SortPreservingMergeExec::new(lex_order, Arc::new(sort_exec));

                let netcdf_sink = Arc::new(NetCDFNdSink::new(
                    conf,
                    unique_columns.len(),
                    collection_handle,
                    output_dir.clone(),
                )?);

                Ok(Arc::new(DataSinkExec::new(
                    Arc::new(sort_preserving_merge_exec),
                    netcdf_sink,
                    order_requirements,
                )))
            }
            _ => {
                let netcdf_sink = Arc::new(NetCDFSink::new(conf, output_dir));
                Ok(Arc::new(DataSinkExec::new(
                    input,
                    netcdf_sink,
                    order_requirements,
                )))
            }
        }
    }

    fn file_source(
        &self,
        table_schema: datafusion::datasource::table_schema::TableSchema,
    ) -> Arc<dyn FileSource> {
        Arc::new(
            NetCDFSource::new(
                self.datasets_object_store.clone(),
                self.options.read_dimensions.clone(),
                table_schema,
            )
            .with_cache(self.cache.clone()),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::datasource::listing::PartitionedFile;
    use datafusion::execution::object_store::ObjectStoreUrl;
    use futures::StreamExt;
    use object_store::path::Path;
    use std::path::PathBuf;
    use std::sync::Once;

    static TEST_FIXTURES: Once = Once::new();

    fn ensure_test_fixtures() {
        TEST_FIXTURES.call_once(|| {
            let dst_dir: PathBuf =
                beacon_config::DATASETS_DIR_PATH.join("beacon-arrow-netcdf-tests");
            std::fs::create_dir_all(&dst_dir).expect("create test dir");

            for name in ["wod_ctd_1964.nc", "gridded-example.nc"] {
                let src = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                    .join("test_files")
                    .join(name);
                let dst = dst_dir.join(name);
                if !dst.exists() {
                    std::fs::copy(&src, &dst)
                        .unwrap_or_else(|e| panic!("copy {name} into datasets dir: {e}"));
                }
            }
        });
    }

    fn wod_object_meta() -> ObjectMeta {
        ObjectMeta {
            location: Path::from("beacon-arrow-netcdf-tests/wod_ctd_1964.nc"),
            last_modified: chrono::Utc::now(),
            size: 0,
            e_tag: None,
            version: None,
        }
    }

    fn gridded_object_meta() -> ObjectMeta {
        ObjectMeta {
            location: Path::from("beacon-arrow-netcdf-tests/gridded-example.nc"),
            last_modified: chrono::Utc::now(),
            size: 0,
            e_tag: None,
            version: None,
        }
    }

    async fn test_store() -> Arc<DatasetsStore> {
        ensure_test_fixtures();
        Arc::new(
            beacon_object_storage::local_datasets_store(
                beacon_config::DATASETS_DIR_PATH.to_path_buf(),
            )
            .await
            .expect("local datasets store"),
        )
    }

    fn test_format(store: Arc<DatasetsStore>) -> NetcdfFormat {
        NetcdfFormat::new(store, NetcdfOptions::default())
    }

    // ── fetch_schema ───────────────────────────────────────────────────

    #[tokio::test]
    async fn fetch_schema_returns_fields_for_ragged_file() {
        let store = test_store().await;
        let schema = reader::fetch_schema(store, wod_object_meta(), None)
            .await
            .expect("schema");
        assert!(!schema.fields().is_empty());
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert!(names.contains(&"lat"), "expected lat in {names:?}");
        assert!(names.contains(&"lon"), "expected lon in {names:?}");
        assert!(names.contains(&"z"), "expected z in {names:?}");
        assert!(
            names.contains(&"Temperature"),
            "expected Temperature in {names:?}"
        );
    }

    #[tokio::test]
    async fn fetch_schema_returns_fields_for_gridded_file() {
        let store = test_store().await;
        let schema = reader::fetch_schema(store, gridded_object_meta(), None)
            .await
            .expect("schema");
        assert!(!schema.fields().is_empty());
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert!(
            names.contains(&"analysed_sst"),
            "expected analysed_sst in {names:?}"
        );
    }

    #[tokio::test]
    async fn fetch_schema_with_dimensions_limits_fields() {
        let store = test_store().await;
        let full = reader::fetch_schema(store.clone(), gridded_object_meta(), None)
            .await
            .expect("full schema");
        let projected =
            reader::fetch_schema(store, gridded_object_meta(), Some(vec!["time".to_string()]))
                .await
                .expect("projected schema");
        assert!(
            projected.fields().len() < full.fields().len(),
            "dimension projection should reduce fields: projected {} vs full {}",
            projected.fields().len(),
            full.fields().len(),
        );
    }

    // ── infer_schema ───────────────────────────────────────────────────

    #[tokio::test]
    async fn infer_schema_single_file() {
        let store = test_store().await;
        let format = test_format(store.clone());
        let dummy_store: Arc<dyn ObjectStore> =
            Arc::new(object_store::local::LocalFileSystem::new());
        let ctx = datafusion::prelude::SessionContext::new();

        let schema = format
            .infer_schema(&ctx.state(), &dummy_store, &[gridded_object_meta()])
            .await
            .expect("infer schema");
        assert!(!schema.fields().is_empty());
    }

    #[tokio::test]
    async fn infer_schema_multiple_files_merges() {
        let store = test_store().await;
        let format = test_format(store.clone());
        let dummy_store: Arc<dyn ObjectStore> =
            Arc::new(object_store::local::LocalFileSystem::new());
        let ctx = datafusion::prelude::SessionContext::new();

        let schema = format
            .infer_schema(
                &ctx.state(),
                &dummy_store,
                &[gridded_object_meta(), gridded_object_meta()],
            )
            .await
            .expect("merged schema");
        assert!(!schema.fields().is_empty());
    }

    #[tokio::test]
    async fn infer_schema_empty_objects_returns_empty_schema() {
        let store = test_store().await;
        let format = test_format(store.clone());
        let dummy_store: Arc<dyn ObjectStore> =
            Arc::new(object_store::local::LocalFileSystem::new());
        let ctx = datafusion::prelude::SessionContext::new();

        // With no objects there is nothing to infer, so the format yields an
        // empty schema — consistent with the zarr/tiff N-D formats.
        let schema = format
            .infer_schema(&ctx.state(), &dummy_store, &[])
            .await
            .expect("empty object list should infer an empty schema");
        assert_eq!(schema.fields().len(), 0);
    }

    // ── file_source ────────────────────────────────────────────────────

    #[tokio::test]
    async fn file_source_returns_netcdf_type() {
        let store = test_store().await;
        let format = test_format(store);
        let source = format.file_source(
            datafusion::datasource::table_schema::TableSchema::from_file_schema(Arc::new(
                arrow::datatypes::Schema::empty(),
            )),
        );
        assert_eq!(source.file_type(), "netcdf");
    }

    /// `CREATE EXTERNAL TABLE ... OPTIONS (...)` per-table overrides are parsed by
    /// the factory: known keys are accepted (defaulting to the runtime config) and
    /// a malformed boolean is rejected.
    #[tokio::test]
    async fn create_parses_per_table_options() {
        use datafusion::datasource::file_format::FileFormatFactory;
        use std::collections::HashMap;

        let store = test_store().await;
        let factory =
            NetCDFFormatFactory::new(store, NetcdfOptions::default(), NetcdfConfig::default());
        let ctx = datafusion::prelude::SessionContext::new();

        let mut options = HashMap::new();
        options.insert("use_reader_cache".to_string(), "false".to_string());
        options.insert("enable_statistics".to_string(), "false".to_string());
        options.insert("read_dimensions".to_string(), "time, lat".to_string());
        let format = factory
            .create(&ctx.state(), &options)
            .expect("valid options");
        let netcdf = format
            .as_any()
            .downcast_ref::<NetcdfFormat>()
            .expect("netcdf format");
        assert_eq!(
            netcdf.options.read_dimensions.as_deref(),
            Some(["time".to_string(), "lat".to_string()].as_slice())
        );

        // A malformed boolean for a known option is a hard error.
        let mut bad = HashMap::new();
        bad.insert("use_reader_cache".to_string(), "notabool".to_string());
        assert!(factory.create(&ctx.state(), &bad).is_err());
    }

    // ── FileOpener produces batches ────────────────────────────────────

    #[tokio::test]
    async fn opener_streams_batches_for_ragged_file() {
        let store = test_store().await;
        let table_schema = reader::fetch_schema(store.clone(), wod_object_meta(), None)
            .await
            .expect("schema");

        let ts = datafusion::datasource::table_schema::TableSchema::from_file_schema(
            table_schema.clone(),
        );
        let opener = source::NetCDFSource::new(store, None, ts);
        let file_opener = {
            let conf = FileScanConfigBuilder::new(
                ObjectStoreUrl::local_filesystem(),
                Arc::new(opener.clone()) as Arc<dyn FileSource>,
            )
            .build();
            opener
                .create_file_opener(
                    Arc::new(object_store::local::LocalFileSystem::new()),
                    &conf,
                    0,
                )
                .expect("file opener")
        };

        let stream = file_opener
            .open(PartitionedFile::from(wod_object_meta()))
            .expect("open")
            .await
            .expect("stream future");

        let batches: Vec<_> = stream.collect().await;
        assert!(!batches.is_empty(), "should produce at least one batch");

        let first = batches[0].as_ref().expect("first batch ok");
        assert!(first.num_columns() > 0);
        assert!(first.num_rows() > 0);
    }

    #[tokio::test]
    async fn opener_streams_batches_for_gridded_file() {
        let store = test_store().await;
        let table_schema = reader::fetch_schema(store.clone(), gridded_object_meta(), None)
            .await
            .expect("schema");

        let ts = datafusion::datasource::table_schema::TableSchema::from_file_schema(
            table_schema.clone(),
        );
        let opener = source::NetCDFSource::new(store, None, ts);
        let file_opener = {
            let conf = FileScanConfigBuilder::new(
                ObjectStoreUrl::local_filesystem(),
                Arc::new(opener.clone()) as Arc<dyn FileSource>,
            )
            .build();
            opener
                .create_file_opener(
                    Arc::new(object_store::local::LocalFileSystem::new()),
                    &conf,
                    0,
                )
                .expect("file opener")
        };

        let stream = file_opener
            .open(PartitionedFile::from(gridded_object_meta()))
            .expect("open")
            .await
            .expect("stream future");

        let batches: Vec<_> = stream.collect().await;
        assert!(!batches.is_empty(), "should produce at least one batch");

        let first = batches[0].as_ref().expect("first batch ok");
        assert!(first.num_columns() > 0);
        assert!(first.num_rows() > 0);
    }

    #[tokio::test]
    async fn opener_with_projection_selects_columns() {
        let store = test_store().await;
        let table_schema = reader::fetch_schema(store.clone(), gridded_object_meta(), None)
            .await
            .expect("schema");

        // Project to only the first column.
        let projected_schema: SchemaRef = Arc::new(table_schema.project(&[0]).expect("project"));

        let ts = datafusion::datasource::table_schema::TableSchema::from_file_schema(
            table_schema.clone(),
        );
        let opener = source::NetCDFSource::new(store, None, ts);
        let file_opener = {
            let conf = FileScanConfigBuilder::new(
                ObjectStoreUrl::local_filesystem(),
                Arc::new(opener.clone()) as Arc<dyn FileSource>,
            )
            .with_projection_indices(Some(vec![0]))
            .unwrap()
            .build();
            opener
                .create_file_opener(
                    Arc::new(object_store::local::LocalFileSystem::new()),
                    &conf,
                    0,
                )
                .expect("file opener")
        };

        let stream = file_opener
            .open(PartitionedFile::from(gridded_object_meta()))
            .expect("open")
            .await
            .expect("stream future");

        let batches: Vec<_> = stream.collect().await;
        assert!(!batches.is_empty());

        let first = batches[0].as_ref().expect("first batch ok");
        assert_eq!(
            first.num_columns(),
            projected_schema.fields().len(),
            "batch should have only the projected columns"
        );
    }

    /// When a file is scanned under a merged (super-typed) schema that includes
    /// columns it does not have, the `BatchAdapterFactory` must null-fill those
    /// columns. We merge the gridded + ragged schemas, read the ragged file, and
    /// assert a gridded-only column comes back all-null at the merged width.
    #[tokio::test]
    async fn opener_null_fills_columns_missing_from_a_file() {
        use arrow::array::Array;

        let store = test_store().await;
        let format = test_format(store.clone());
        let dummy_store: Arc<dyn ObjectStore> =
            Arc::new(object_store::local::LocalFileSystem::new());
        let ctx = datafusion::prelude::SessionContext::new();

        // Merged (super-typed) schema across the ragged + gridded files.
        let merged: SchemaRef = format
            .infer_schema(
                &ctx.state(),
                &dummy_store,
                &[wod_object_meta(), gridded_object_meta()],
            )
            .await
            .expect("merged schema");

        // Pick a merged column the ragged (wod) file does not provide.
        let wod_schema = reader::fetch_schema(store.clone(), wod_object_meta(), None)
            .await
            .expect("wod schema");
        let missing = merged
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .find(|name| wod_schema.index_of(name).is_err())
            .expect("merged schema should contain a column the wod file lacks");
        let missing_idx = merged.index_of(&missing).unwrap();

        // No projection pushed → the opener reads under the full merged schema.
        let ts =
            datafusion::datasource::table_schema::TableSchema::from_file_schema(merged.clone());
        let opener = source::NetCDFSource::new(store, None, ts);
        let conf = FileScanConfigBuilder::new(
            ObjectStoreUrl::local_filesystem(),
            Arc::new(opener.clone()) as Arc<dyn FileSource>,
        )
        .build();
        let file_opener = opener
            .create_file_opener(
                Arc::new(object_store::local::LocalFileSystem::new()),
                &conf,
                0,
            )
            .expect("file opener");

        let stream = file_opener
            .open(PartitionedFile::from(wod_object_meta()))
            .expect("open")
            .await
            .expect("stream future");
        let batches: Vec<_> = stream.collect().await;
        assert!(!batches.is_empty(), "ragged file should produce batches");

        for batch in &batches {
            let batch = batch.as_ref().expect("batch ok");
            assert_eq!(
                batch.schema().fields().len(),
                merged.fields().len(),
                "batch must conform to the merged schema width"
            );
            let col = batch.column(missing_idx);
            assert_eq!(
                col.null_count(),
                col.len(),
                "column `{missing}` (absent from the wod file) must be all-null",
            );
        }
    }

    #[tokio::test]
    async fn opener_with_read_dimensions_limits_columns() {
        let store = test_store().await;
        // Full schema without dimension filter.
        let full_schema = reader::fetch_schema(store.clone(), gridded_object_meta(), None)
            .await
            .expect("full schema");

        // Schema with dimension filter.
        let dim_schema = reader::fetch_schema(
            store.clone(),
            gridded_object_meta(),
            Some(vec!["time".to_string()]),
        )
        .await
        .expect("dim schema");

        let ts =
            datafusion::datasource::table_schema::TableSchema::from_file_schema(dim_schema.clone());
        let opener = source::NetCDFSource::new(store, Some(vec!["time".to_string()]), ts);
        let file_opener = {
            let conf = FileScanConfigBuilder::new(
                ObjectStoreUrl::local_filesystem(),
                Arc::new(opener.clone()) as Arc<dyn FileSource>,
            )
            .build();
            opener
                .create_file_opener(
                    Arc::new(object_store::local::LocalFileSystem::new()),
                    &conf,
                    0,
                )
                .expect("file opener")
        };

        let stream = file_opener
            .open(PartitionedFile::from(gridded_object_meta()))
            .expect("open")
            .await
            .expect("stream future");

        let batches: Vec<_> = stream.collect().await;
        assert!(!batches.is_empty());

        let first = batches[0].as_ref().expect("first batch ok");
        assert!(
            first.num_columns() < full_schema.fields().len(),
            "dimension-filtered batch should have fewer columns ({}) than full ({})",
            first.num_columns(),
            full_schema.fields().len(),
        );
    }

    // ── End-to-end via SessionContext (projection + predicate pushdown) ──

    /// Register `gridded-example.nc` as a DataFusion table backed by
    /// [`NetcdfFormat`] over the `datasets://` object store.
    async fn register_example(
        ctx: &datafusion::prelude::SessionContext,
        store: Arc<beacon_object_storage::DatasetsStore>,
    ) {
        use beacon_common::super_table::SuperListingTable;
        use datafusion::datasource::file_format::FileFormat;
        use datafusion::datasource::listing::ListingTableUrl;

        let store_url = ObjectStoreUrl::parse("datasets://").unwrap();
        ctx.register_object_store(store_url.as_ref(), store.clone());

        let format: Arc<dyn FileFormat> =
            Arc::new(NetcdfFormat::new(store, NetcdfOptions::default()));
        let url =
            ListingTableUrl::parse("datasets:///beacon-arrow-netcdf-tests/gridded-example.nc")
                .unwrap();
        let table = SuperListingTable::new(&ctx.state(), format, vec![url])
            .await
            .unwrap();
        ctx.register_table("gridded_nc", Arc::new(table)).unwrap();
    }

    #[tokio::test]
    async fn projection_pushdown_through_datafusion() {
        let store = test_store().await;
        let ctx = datafusion::prelude::SessionContext::new();
        register_example(&ctx, store).await;

        let df = ctx
            .sql("SELECT analysed_sst, lat FROM gridded_nc")
            .await
            .unwrap();
        let names: Vec<String> = df
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        assert_eq!(names, vec!["analysed_sst".to_string(), "lat".to_string()]);

        let batches = df.collect().await.unwrap();
        assert_eq!(batches[0].num_columns(), 2);
        assert!(batches.iter().map(|b| b.num_rows()).sum::<usize>() > 0);
    }

    #[tokio::test]
    async fn predicate_pushdown_prunes_through_datafusion() {
        let store = test_store().await;
        let ctx = datafusion::prelude::SessionContext::new();
        register_example(&ctx, store).await;

        // Latitude is geographic (≤ 90°), so this excludes every row.
        let rows: usize = ctx
            .sql("SELECT lat FROM gridded_nc WHERE lat > 100000")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap()
            .iter()
            .map(|b| b.num_rows())
            .sum();
        assert_eq!(
            rows, 0,
            "impossible latitude predicate should yield no rows"
        );
    }

    #[tokio::test]
    async fn predicate_pushdown_selects_subset_through_datafusion() {
        use arrow::array::{Float64Array, Int64Array};

        let store = test_store().await;
        let ctx = datafusion::prelude::SessionContext::new();
        register_example(&ctx, store).await;

        // Filter on the midpoint of the latitude range so the predicate keeps
        // some — but not all — rows. Cast to f64 so the test is type-agnostic.
        let stats = ctx
            .sql(
                "SELECT min(CAST(lat AS DOUBLE)) AS mn, max(CAST(lat AS DOUBLE)) AS mx, \
                 count(*) AS n FROM gridded_nc",
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let row = &stats[0];
        let d = |i: usize| {
            row.column(i)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0)
        };
        let (mn, mx) = (d(0), d(1));
        let total = row
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert!(mx > mn, "lat must span a range");
        let mid = mn + (mx - mn) / 2.0;

        let batches = ctx
            .sql(&format!(
                "SELECT CAST(lat AS DOUBLE) AS latd FROM gridded_nc \
                 WHERE CAST(lat AS DOUBLE) > {mid}"
            ))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let mut kept = 0i64;
        for b in &batches {
            let col = b.column(0).as_any().downcast_ref::<Float64Array>().unwrap();
            for i in 0..col.len() {
                assert!(
                    col.value(i) > mid,
                    "every returned lat must satisfy the predicate"
                );
            }
            kept += b.num_rows() as i64;
        }
        assert!(kept > 0, "midpoint predicate should keep some rows");
        assert!(kept < total, "midpoint predicate should drop some rows");
    }
}
