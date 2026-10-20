use std::path::PathBuf;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use beacon_datafusion_ext::file_stream_adapters::OnError;
use beacon_datafusion_ext::format_ext::{DatasetMetadata, FileFormatFactoryExt, SchemaOptions};
use beacon_datafusion_ext::format_options::format_option;
use beacon_datafusion_ext::listing_factory::ListingFactory;
use beacon_datafusion_ext::type_widening::{
    label_by_object, session_widening, ArrowTypeWidening, ArrowTypeWideningStrategy,
    DefaultArrowTypeWidening,
};
use beacon_datafusion_ext::unique_values::UniqueValuesExec;
use beacon_nd_array::nd_file_opener::NdFileOpener;
use beacon_nd_array::nd_morsel_file_source::NdMorselFileSource;
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

use crate::datafusion::config::NetCDFConfig;
use crate::datafusion::native::opener::NativeOpener;
use crate::datafusion::options::{NetCDFReaderOptions, NetCDFWriterOptions};
use crate::datafusion::oxcdf::opener::OxcdfOpener;
use crate::datafusion::{
    options::NetCDFOptions,
    sink::{NetCDFNdSink, NetCDFSink},
};
use crate::oxcdf_reader::open_dataset_v2;

pub const NETCDF_EXTENSION: &str = "nc";

/// Every filename extension that a netCDF file uses, canonical first.
///
/// `cdf` is not here: it is also the NASA Common Data Format, which this reader cannot open.
pub const NETCDF_EXTENSIONS: [&str; 4] = [NETCDF_EXTENSION, "nc3", "nc4", "netcdf"];

pub mod config;
pub mod native;
pub mod object_meta_resolver;
pub mod options;
pub mod oxcdf;
#[cfg(test)]
mod partition_coverage_tests;
pub mod reader;
pub mod sink;
pub mod statistics;
pub mod table_function;
pub use reader::{FileAccess, NetcdfInput, ReaderBackend};
pub use table_function::ReadNetCDFFunc;

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
    pub listing_factory: Arc<ListingFactory>,
    pub output_dir: PathBuf,
    pub config: NetCDFConfig,
}

impl NetCDFFormatFactory {
    pub fn new(
        listing_factory: Arc<ListingFactory>,
        output_dir: PathBuf,
        config: NetCDFConfig,
    ) -> Self {
        Self {
            listing_factory,
            output_dir,
            config,
        }
    }

    /// A format with the runtime config defaults and no per-table options.
    fn base_format(&self) -> NetcdfFormat {
        let reader_options = NetCDFReaderOptions {
            enable_statistics: self.config.enable_statistics,
            ..NetCDFReaderOptions::default()
        };
        NetcdfFormat::new(
            self.listing_factory.clone(),
            reader_options,
            NetCDFWriterOptions::default(),
        )
        .with_output_dir(self.output_dir.clone())
    }
}

impl FileFormatFactory for NetCDFFormatFactory {
    fn create(
        &self,
        _state: &dyn Session,
        format_options: &std::collections::HashMap<String, String>,
    ) -> datafusion::error::Result<Arc<dyn FileFormat>> {
        // Per-table options from `CREATE EXTERNAL TABLE` or `COPY` override the runtime config.
        let mut format = self.base_format();
        format.reader_options.apply_format_options(format_options)?;
        format.writer_options.apply_format_options(format_options)?;
        Ok(Arc::new(format))
    }

    fn default(&self) -> Arc<dyn FileFormat> {
        Arc::new(self.base_format())
    }
}

impl GetExt for NetCDFFormatFactory {
    fn get_ext(&self) -> String {
        NETCDF_EXTENSION.to_string()
    }
}

impl FileFormatFactoryExt for NetCDFFormatFactory {
    fn create_for_analysis(
        &self,
        state: &dyn Session,
        format_options: &std::collections::HashMap<String, String>,
        url: &datafusion::datasource::listing::ListingTableUrl,
        listing: &ListingFactory,
    ) -> datafusion::error::Result<Arc<dyn FileFormat>> {
        todo!()
    }

    fn create_with_native_root(
        &self,
        state: &dyn Session,
        format_options: &std::collections::HashMap<String, String>,
        _url: &datafusion::datasource::listing::ListingTableUrl,
        _listing: &ListingFactory,
    ) -> datafusion::error::Result<Arc<dyn FileFormat>> {
        todo!()
    }

    fn discover_datasets(
        &self,
        objects: &[ObjectMeta],
    ) -> datafusion::error::Result<Vec<DatasetMetadata>> {
        let datasets = objects
            .iter()
            .filter(|obj| {
                obj.location.extension().is_some_and(|ext| {
                    NETCDF_EXTENSIONS
                        .iter()
                        .any(|known| ext.eq_ignore_ascii_case(known))
                })
            })
            .map(|obj| DatasetMetadata::new(obj.location.to_string(), self.get_ext()))
            .collect();
        Ok(datasets)
    }

    fn file_format_name(&self) -> String {
        self.get_ext()
    }

    fn file_extensions(&self) -> Vec<String> {
        NETCDF_EXTENSIONS.map(String::from).to_vec()
    }

    /// Every spelling reads the same way, so the crawler can build a table on each.
    fn crawlable_extensions(&self) -> Vec<String> {
        self.file_extensions()
    }

    fn schema_options_fingerprint(&self, _format: &dyn FileFormat) -> Option<u64> {
        Some(SchemaOptions::new(NETCDF_EXTENSION).finish())
    }
}

#[derive(Debug, Clone)]
pub struct NetcdfFormat {
    pub listing_factory: Arc<ListingFactory>,
    pub type_widening: Arc<dyn ArrowTypeWideningStrategy>,

    pub reader_options: NetCDFReaderOptions,
    pub writer_options: NetCDFWriterOptions,

    /// Whether to generate per-file statistics during planning.
    enable_statistics: bool,
    /// Local directory the netcdf-c writer emits output files into.
    output_dir: PathBuf,
}

impl NetcdfFormat {
    pub fn new(
        listing_factory: Arc<ListingFactory>,
        reader_options: NetCDFReaderOptions,
        writer_options: NetCDFWriterOptions,
    ) -> Self {
        Self {
            listing_factory,
            reader_options,
            writer_options,
            enable_statistics: false,
            output_dir: std::env::temp_dir(),
            type_widening: Arc::new(DefaultArrowTypeWidening::default()),
        }
    }

    /// Set the local directory the writer emits output files into (defaults to the
    /// OS temp dir).
    pub fn with_output_dir(mut self, output_dir: PathBuf) -> Self {
        self.output_dir = output_dir;
        self
    }

    pub fn with_type_widening(mut self, type_widening: Arc<dyn ArrowTypeWideningStrategy>) -> Self {
        self.type_widening = type_widening;
        self
    }
}

#[async_trait::async_trait]
impl FileFormat for NetcdfFormat {
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
        state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> datafusion::error::Result<SchemaRef> {
        use futures::{StreamExt, TryStreamExt};

        let locations: Vec<_> = objects.iter().map(|o| o.location.clone()).collect();
        let schemas: Vec<SchemaRef> = futures::stream::iter(locations)
            .map(|location| {
                let store = Arc::clone(store);
                async move {
                    let dataset = open_dataset_v2(store, location.clone())
                        .await
                        .map_err(|e| {
                            exec_datafusion_err!("Failed to open dataset '{}': {}", location, e)
                        })?;
                    Ok::<SchemaRef, datafusion::error::DataFusionError>(Arc::new(
                        dataset.arrow_schema(),
                    ))
                }
            })
            .buffered(16)
            .try_collect()
            .await?;

        if schemas.is_empty() {
            // Return a default empty schema
            return Ok(Arc::new(arrow::datatypes::Schema::empty()));
        }

        // Merge the schemas of all input NetCDF files, using the session's type widening rules.
        session_widening(state)
            .merge_schemas(&label_by_object(objects, &schemas))
            .map_err(|e| {
                exec_datafusion_err!("Failed to merge the schemas of the NetCDF datasets: {}", e)
            })
    }

    async fn infer_stats(
        &self,
        _state: &dyn Session,
        _store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        _object: &ObjectMeta,
    ) -> datafusion::error::Result<Statistics> {
        // ToDo: Implement statistics generation for NetCDF files.
        // Currently, we return unknown statistics for all files.
        return Ok(Statistics::new_unknown(&table_schema));
    }

    async fn create_physical_plan(
        &self,
        _state: &dyn Session,
        conf: FileScanConfig,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let encoded_file_schema = Arc::new(beacon_datafusion_ext::nd::encoded_schema(
            conf.file_schema(),
        ));
        let table_schema = datafusion::datasource::table_schema::TableSchema::new(
            encoded_file_schema,
            beacon_nd_array::arrow::partition::encoded_partition_cols(conf.table_partition_cols()),
        );
        // Preserve a projection that the scan pushed down into the incoming
        // source — rebuilding the source below would otherwise drop it.
        let projection = conf.file_source().projection().cloned();
        let mut source = self.file_source(table_schema);
        if let Some(projection) = projection.as_ref() {
            if let Some(projected_source) = source.try_pushdown_projection(projection)? {
                source = projected_source;
            }
        }

        let conf = FileScanConfigBuilder::from(conf)
            .with_source(source)
            .build();

        let data_source: Arc<dyn ExecutionPlan> = DataSourceExec::from_data_source(conf);
        let nd_source = Arc::new(beacon_datafusion_ext::nd::exec::NdSourceExec::try_new(
            data_source,
        )?);
        let broadcast = beacon_datafusion_ext::nd::exec::NdBroadcastExec::try_new(nd_source)?;
        Ok(Arc::new(broadcast))
    }

    async fn create_writer_physical_plan(
        &self,
        input: Arc<dyn ExecutionPlan>,
        _state: &dyn Session,
        conf: FileSinkConfig,
        order_requirements: Option<LexRequirement>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        // NetCDF needs a real local path (the netcdf-c writer cannot stream to an
        // object store). Write into the configured output directory (the tmp store
        // root), threaded in by the runtime, rather than the OS temp dir.
        let output_dir = self.output_dir.clone();
        match &self.writer_options.write_dimensions {
            Some(dim_columns) if !dim_columns.is_empty() => {
                let flat_keys = self.writer_options.set_keys();
                if !flat_keys.is_empty() {
                    return Err(exec_datafusion_err!(
                        "{} applies only to the flat NetCDF output",
                        flat_keys.join(", ")
                    ));
                }
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
                let netcdf_sink = Arc::new(NetCDFSink::new(
                    conf,
                    output_dir,
                    self.writer_options.clone(),
                ));
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
        let opener = Arc::new(OxcdfOpener) as Arc<dyn NdFileOpener>;
        let mut morsel_file_source = NdMorselFileSource::new(opener, self.get_ext(), table_schema);

        if self.reader_options.skip_on_error {
            morsel_file_source = morsel_file_source.with_on_error(OnError::Skip);
        }

        Arc::new(morsel_file_source)
    }
}

/// The reader-backend flag: how it is set, and that both readers agree.
#[cfg(test)]
mod reader_backend_tests {
    use std::collections::HashMap;
    use std::path::PathBuf;

    use beacon_datafusion_ext::fast_object::FastObjectTable;
    use beacon_datafusion_ext::listing_factory::RootStore;
    use datafusion::datasource::listing::ListingTableUrl;
    use datafusion::execution::session_state::SessionStateBuilder;
    use datafusion::prelude::{SessionConfig, SessionContext};

    use super::*;

    const GRIDDED_FILE: &str = "gridded-example.nc";
    const WOD_FILE: &str = "wod_ctd_1964.nc";

    /// The absolute path of a bundled test file.
    fn test_file(name: &str) -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("test_files")
            .join(name)
    }

    fn factory() -> NetCDFFormatFactory {
        NetCDFFormatFactory::new(
            Arc::new(ListingFactory::dynamic()),
            std::env::temp_dir(),
            NetCDFOptions::default(),
            NetcdfConfig::default(),
        )
    }

    /// The backend of a format the factory built from `format_options`.
    fn backend_of(
        factory: &NetCDFFormatFactory,
        state: &dyn Session,
        format_options: &HashMap<String, String>,
    ) -> datafusion::error::Result<ReaderBackend> {
        let format = factory.create(state, format_options)?;
        Ok(format
            .downcast_ref::<NetcdfFormat>()
            .expect("the factory builds a NetcdfFormat")
            .reader_backend())
    }

    /// The access that reads the bundled test files on `backend`.
    ///
    /// netcdf-c opens a path itself, so it takes a resolver over the filesystem
    /// root. `oxcdf` reads through the object store and takes nothing.
    fn access_on(backend: ReaderBackend) -> FileAccess {
        match backend {
            ReaderBackend::NetcdfC => {
                FileAccess::netcdf_c(object_meta_resolver::create_object_resolver(
                    &RootStore::FileSystem(PathBuf::from("/")),
                ))
            }
            ReaderBackend::Oxcdf => FileAccess::Oxcdf,
        }
    }

    /// A format on `backend`, ready to read the bundled test files.
    fn format_on(backend: ReaderBackend) -> NetcdfFormat {
        NetcdfFormat::new(
            Arc::new(ListingFactory::dynamic()),
            NetCDFOptions::default(),
        )
        .with_access(access_on(backend))
    }

    /// A single-partition session, so a scan yields rows in a stable order.
    fn session() -> SessionContext {
        let state = SessionStateBuilder::new()
            .with_config(
                SessionConfig::new()
                    .with_target_partitions(1)
                    // `FastObjectTable` merges its schemas through this. A
                    // session that skips `RuntimeBuilder` registers it itself.
                    .with_extension(
                        beacon_datafusion_ext::type_widening::ArrowTypeWidening::default_extension(
                        ),
                    ),
            )
            .with_default_features()
            .build();
        SessionContext::new_with_state(state)
    }

    /// Register one bundled test file as a table read on `backend`.
    async fn register(ctx: &SessionContext, table: &str, backend: ReaderBackend, file: &str) {
        register_path(ctx, table, backend, &test_file(file)).await
    }

    /// The same, for a file that is not bundled — one a test wrote itself.
    async fn register_path(
        ctx: &SessionContext,
        table: &str,
        backend: ReaderBackend,
        path: &std::path::Path,
    ) {
        let url = ListingTableUrl::parse(path.to_string_lossy()).unwrap();
        let listing =
            FastObjectTable::try_new(&ctx.state(), Arc::new(format_on(backend)), vec![url])
                .await
                .unwrap_or_else(|e| panic!("register {} on {backend:?}: {e}", path.display()));
        ctx.register_table(table, Arc::new(listing)).unwrap();
    }

    /// The reader opens a file by its content, so an alias extension reads the same rows.
    #[tokio::test]
    async fn a_file_with_an_alias_extension_reads_the_same_rows() {
        let dir = tempfile::tempdir().unwrap();
        let alias = dir.path().join("wod.nc4");
        std::fs::copy(test_file(WOD_FILE), &alias).unwrap();

        let ctx = session();
        register(&ctx, "canonical", ReaderBackend::Oxcdf, WOD_FILE).await;
        register_path(&ctx, "alias", ReaderBackend::Oxcdf, &alias).await;

        let count = |table: &'static str| {
            let ctx = ctx.clone();
            async move {
                let batches = ctx
                    .sql(&format!("SELECT COUNT(*) FROM {table}"))
                    .await
                    .unwrap()
                    .collect()
                    .await
                    .unwrap();
                arrow::util::pretty::pretty_format_batches(&batches)
                    .unwrap()
                    .to_string()
            }
        };
        assert_eq!(count("alias").await, count("canonical").await);
    }

    /// A projection out of table-schema order reads its own columns.
    ///
    /// The table schema is sorted, so `Temperature` comes before `lat`. The scan
    /// must emit only the projected columns, because the projection above it
    /// picks them by index.
    #[tokio::test]
    async fn a_projection_out_of_schema_order_reads_its_own_columns() {
        use arrow::datatypes::DataType;

        let ctx = session();
        register(&ctx, "wod", ReaderBackend::Oxcdf, WOD_FILE).await;

        let batches = ctx
            .sql(r#"SELECT wod_unique_cast, "Temperature", lon, lat, z FROM wod LIMIT 10"#)
            .await
            .unwrap()
            .collect()
            .await
            .expect("the scan emits the projected columns");

        let rows: usize = batches.iter().map(|batch| batch.num_rows()).sum();
        assert_eq!(rows, 10);
        let types: Vec<DataType> = batches[0]
            .schema()
            .fields()
            .iter()
            .map(|field| field.data_type().clone())
            .collect();
        assert_eq!(
            types,
            [
                DataType::Int32,
                DataType::Float32,
                DataType::Float32,
                DataType::Float32,
                DataType::Float32
            ]
        );
    }

    // ── statistics follow the reader ───────────────────────────────────

    /// The object metadata for a bundled test file, read through a bare
    /// `LocalFileSystem`.
    fn local_object(file: &str) -> (Arc<dyn ObjectStore>, ObjectMeta) {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::local::LocalFileSystem::new());
        let path = test_file(file);
        let location = object_store::path::Path::from_absolute_path(&path)
            .unwrap_or_else(|e| panic!("{} is not an absolute object path: {e}", path.display()));
        let file_meta = std::fs::metadata(&path).expect("the bundled test file exists");
        let object = ObjectMeta {
            location,
            last_modified: file_meta.modified().map(Into::into).unwrap_or_default(),
            size: file_meta.len(),
            e_tag: None,
            version: None,
        };
        (store, object)
    }

    /// Statistics are a capability of the Rust reader, not of the format.
    ///
    /// Every netcdf-c call serialises on a process-global mutex, and the read is
    /// synchronous, so computing statistics under it is serial and parks a tokio
    /// worker. `oxcdf` has neither problem and produces the same ranges, so the
    /// capability follows the reader.
    #[tokio::test]
    async fn statistics_come_from_the_rust_reader_only() {
        let ctx = session();
        let state = ctx.state();
        let (store, object) = local_object(WOD_FILE);

        let oxcdf = format_on(ReaderBackend::Oxcdf).with_enable_statistics(true);
        let schema = oxcdf
            .infer_schema(&state, &store, std::slice::from_ref(&object))
            .await
            .expect("oxcdf infers a schema");

        let with_rust_reader = oxcdf
            .infer_stats(&state, &store, schema.clone(), &object)
            .await
            .expect("statistics are never an error");
        assert!(
            with_rust_reader
                .column_statistics
                .iter()
                .any(|column| column.min_value.get_value().is_some()),
            "the Rust reader must produce real ranges for the coordinate variables"
        );

        // netcdf-c reports unknown rather than erroring, so a deployment on it
        // keeps working and simply prunes nothing.
        let netcdf_c = format_on(ReaderBackend::NetcdfC).with_enable_statistics(true);
        let without_rust_reader = netcdf_c
            .infer_stats(&state, &store, schema.clone(), &object)
            .await
            .expect("netcdf-c reports unknown, it does not fail");
        assert!(
            without_rust_reader
                .column_statistics
                .iter()
                .all(|column| column.min_value.get_value().is_none()
                    && column.max_value.get_value().is_none()),
            "netcdf-c must report unknown rather than compute"
        );
        assert_eq!(
            without_rust_reader.column_statistics.len(),
            schema.fields().len(),
            "unknown statistics still cover every column, as DataFusion requires"
        );
    }

    /// The switch is still honoured on top of the reader gate.
    #[tokio::test]
    async fn disabling_statistics_wins_over_the_reader() {
        let ctx = session();
        let state = ctx.state();
        let (store, object) = local_object(WOD_FILE);

        let oxcdf = format_on(ReaderBackend::Oxcdf).with_enable_statistics(true);
        let schema = oxcdf
            .infer_schema(&state, &store, std::slice::from_ref(&object))
            .await
            .unwrap();

        let off = format_on(ReaderBackend::Oxcdf).with_enable_statistics(false);
        let statistics = off
            .infer_stats(&state, &store, schema, &object)
            .await
            .unwrap();
        assert!(
            statistics
                .column_statistics
                .iter()
                .all(|column| column.min_value.get_value().is_none()),
            "enable_statistics=false must still mean no statistics"
        );
    }

    /// What a statistics backfill actually costs per file.
    ///
    /// Run it with:
    ///
    /// ```text
    /// cargo test --release -p beacon-arrow-netcdf --lib \
    ///     statistics_backfill_cost -- --ignored --nocapture
    /// ```
    ///
    /// Ignored because it is a measurement, not an assertion. It reads the same
    /// file repeatedly, so the bytes are in the page cache: this is the parse
    /// and range-scan cost, and a **lower bound**. A cold local disk adds seek
    /// time, and object storage adds a round trip per file that will usually
    /// dominate everything here.
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    #[ignore = "measurement, not an assertion"]
    async fn statistics_backfill_cost() {
        use futures::StreamExt;
        use std::time::Instant;

        const ITERATIONS: usize = 200;
        const CONCURRENCY: usize = 8;

        for file in [WOD_FILE, GRIDDED_FILE] {
            let ctx = session();
            let state = ctx.state();
            let (store, object) = local_object(file);
            let format = Arc::new(format_on(ReaderBackend::Oxcdf).with_enable_statistics(true));

            let schema = format
                .infer_schema(&state, &store, std::slice::from_ref(&object))
                .await
                .unwrap();
            let bytes = object.size;

            // Serial: schema + stats, which is what the analyzer does per file.
            let start = Instant::now();
            for _ in 0..ITERATIONS {
                let schema = format
                    .infer_schema(&state, &store, std::slice::from_ref(&object))
                    .await
                    .unwrap();
                let _ = format
                    .infer_stats(&state, &store, schema, &object)
                    .await
                    .unwrap();
            }
            let serial = start.elapsed().as_secs_f64() / ITERATIONS as f64;

            // Concurrent: the shape a collector runs in.
            let start = Instant::now();
            futures::stream::iter(0..ITERATIONS)
                .map(|_| {
                    let format = format.clone();
                    let state = state.clone();
                    let store = store.clone();
                    let object = object.clone();
                    async move {
                        let schema = format
                            .infer_schema(&state, &store, std::slice::from_ref(&object))
                            .await
                            .unwrap();
                        format
                            .infer_stats(&state, &store, schema, &object)
                            .await
                            .unwrap()
                    }
                })
                .buffer_unordered(CONCURRENCY)
                .collect::<Vec<_>>()
                .await;
            let concurrent_total = start.elapsed().as_secs_f64();
            let rate = ITERATIONS as f64 / concurrent_total;

            // The same work spawned onto the runtime rather than polled from one
            // task. `buffer_unordered` gives concurrency, not parallelism: if the
            // work is CPU bound between await points, every future runs on the
            // single task polling them.
            let start = Instant::now();
            let mut set = tokio::task::JoinSet::new();
            for _ in 0..ITERATIONS {
                let format = format.clone();
                let state = state.clone();
                let store = store.clone();
                let object = object.clone();
                set.spawn(async move {
                    let schema = format
                        .infer_schema(&state, &store, std::slice::from_ref(&object))
                        .await
                        .unwrap();
                    format
                        .infer_stats(&state, &store, schema, &object)
                        .await
                        .unwrap()
                });
            }
            while set.join_next().await.is_some() {}
            let spawned_rate = ITERATIONS as f64 / start.elapsed().as_secs_f64();

            let ranged = {
                let statistics = format
                    .infer_stats(&state, &store, schema.clone(), &object)
                    .await
                    .unwrap();
                statistics
                    .column_statistics
                    .iter()
                    .filter(|c| c.min_value.get_value().is_some())
                    .count()
            };

            println!(
                "\n{file}  ({} KiB, {} columns, {ranged} with ranges)\n  \
                 serial               : {:.1} ms/file ({:.0} files/s)\n  \
                 buffer_unordered({CONCURRENCY})  : {:.0} files/s   -> {:.1} h for 1M\n  \
                 spawned              : {:.0} files/s   -> {:.1} h for 1M",
                bytes / 1024,
                schema.fields().len(),
                serial * 1e3,
                1.0 / serial,
                rate,
                1_000_000.0 / rate / 3600.0,
                spawned_rate,
                1_000_000.0 / spawned_rate / 3600.0,
            );
        }
    }

    // ── FileAccess ─────────────────────────────────────────────────────

    /// Each variant carries what its own reader needs, so the two cannot be
    /// mismatched: `Oxcdf` reads through the store it is handed, and `NetcdfC`
    /// cannot be built without a resolver.
    #[test]
    fn each_access_resolves_an_object_its_own_way() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::local::LocalFileSystem::new());
        let object = ObjectMeta {
            location: object_store::path::Path::from("dir/file.nc"),
            last_modified: chrono::DateTime::from_timestamp(0, 0).unwrap(),
            size: 0,
            e_tag: None,
            version: None,
        };

        // oxcdf keeps the store and the object path as they are.
        let input = FileAccess::Oxcdf.input_for(&store, &object).unwrap();
        assert!(matches!(input, NetcdfInput::Oxcdf { .. }));
        assert_eq!(input.backend(), ReaderBackend::Oxcdf);
        assert_eq!(input.location(), "dir/file.nc");

        // netcdf-c turns it into a native path through its resolver.
        let access = FileAccess::netcdf_c(object_meta_resolver::create_object_resolver(
            &RootStore::HttpsStore("https://example.org/data".to_string()),
        ));
        let input = access.input_for(&store, &object).unwrap();
        assert_eq!(input.backend(), ReaderBackend::NetcdfC);
        assert_eq!(
            input.location(),
            "https://example.org/data/dir/file.nc#mode=bytes"
        );
    }

    /// The default is netcdf-c with no location supplied yet. It fails per
    /// object, naming the path, rather than being silently unusable.
    #[test]
    fn the_default_access_cannot_resolve_and_says_so() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::local::LocalFileSystem::new());
        let object = ObjectMeta {
            location: object_store::path::Path::from("dir/file.nc"),
            last_modified: chrono::DateTime::from_timestamp(0, 0).unwrap(),
            size: 0,
            e_tag: None,
            version: None,
        };

        assert_eq!(FileAccess::default().backend(), ReaderBackend::NetcdfC);
        let err = FileAccess::default()
            .input_for(&store, &object)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("dir/file.nc"),
            "error should name the path: {err}"
        );
    }

    // ── The flag ───────────────────────────────────────────────────────

    /// The Rust reader reads a table that sets no option.
    #[test]
    fn the_default_config_takes_the_rust_reader() {
        assert!(NetcdfConfig::default().use_rust_reader);
        assert_eq!(ReaderBackend::default(), ReaderBackend::Oxcdf);
    }

    #[tokio::test]
    async fn a_table_option_selects_the_fallback() {
        let factory = factory();
        let ctx = session();
        let state = ctx.state();

        assert_eq!(
            backend_of(&factory, &state, &HashMap::new()).unwrap(),
            ReaderBackend::Oxcdf,
            "no option means the runtime default"
        );

        let options = HashMap::from([("use_rust_reader".to_string(), "false".to_string())]);
        assert_eq!(
            backend_of(&factory, &state, &options).unwrap(),
            ReaderBackend::NetcdfC
        );

        let options = HashMap::from([("use_rust_reader".to_string(), "on".to_string())]);
        assert_eq!(
            backend_of(&factory, &state, &options).unwrap(),
            ReaderBackend::Oxcdf
        );

        let options = HashMap::from([("use_rust_reader".to_string(), "maybe".to_string())]);
        assert!(
            backend_of(&factory, &state, &options).is_err(),
            "a malformed boolean is a hard error"
        );
    }

    /// The runtime config sets the default that a table inherits.
    #[tokio::test]
    async fn the_runtime_config_sets_the_default() {
        let factory = NetCDFFormatFactory::new(
            Arc::new(ListingFactory::dynamic()),
            std::env::temp_dir(),
            NetCDFOptions::default(),
            NetcdfConfig {
                use_rust_reader: false,
                ..NetcdfConfig::default()
            },
        );
        let ctx = session();

        assert_eq!(
            backend_of(&factory, &ctx.state(), &HashMap::new()).unwrap(),
            ReaderBackend::NetcdfC
        );

        // And one table can still ask for the other reader.
        let options = HashMap::from([("use_rust_reader".to_string(), "true".to_string())]);
        assert_eq!(
            backend_of(&factory, &ctx.state(), &options).unwrap(),
            ReaderBackend::Oxcdf
        );
    }

    /// The `combine_attrs` of a format the factory built from `format_options`.
    fn combine_attrs_of(
        format_options: &HashMap<String, String>,
    ) -> datafusion::error::Result<Option<crate::attributes::CombineAttrs>> {
        let format = factory().create(&session().state(), format_options)?;
        Ok(format
            .downcast_ref::<NetcdfFormat>()
            .expect("the factory builds a NetcdfFormat")
            .options
            .writer
            .combine_attrs)
    }

    #[tokio::test]
    async fn combine_attrs_is_off_by_default() {
        assert_eq!(combine_attrs_of(&HashMap::new()).unwrap(), None);
    }

    #[tokio::test]
    async fn combine_attrs_reads_the_bare_and_the_prefixed_key() {
        use crate::attributes::CombineAttrs;
        let bare = HashMap::from([("combine_attrs".to_string(), "identical".to_string())]);
        assert_eq!(
            combine_attrs_of(&bare).unwrap(),
            Some(CombineAttrs::Identical)
        );
        let prefixed = HashMap::from([(
            "format.combine_attrs".to_string(),
            "drop_conflicts".to_string(),
        )]);
        assert_eq!(
            combine_attrs_of(&prefixed).unwrap(),
            Some(CombineAttrs::DropConflicts)
        );
    }

    #[tokio::test]
    async fn a_bad_combine_attrs_is_an_error() {
        let options = HashMap::from([("combine_attrs".to_string(), "union".to_string())]);
        let error = combine_attrs_of(&options).unwrap_err().to_string();
        assert!(error.contains("combine_attrs"), "{error}");
    }

    #[tokio::test]
    async fn combine_attrs_with_write_dimensions_is_an_error() {
        use datafusion::datasource::physical_plan::{FileGroup, FileOutputMode};
        use datafusion::execution::object_store::ObjectStoreUrl;
        use datafusion::logical_expr::dml::InsertOp;
        use datafusion::physical_plan::empty::EmptyExec;

        let factory = NetCDFFormatFactory::new(
            Arc::new(ListingFactory::dynamic()),
            std::env::temp_dir(),
            NetCDFOptions {
                write_dimensions: Some(vec!["lat".to_string()]),
                writer: crate::datafusion::options::NetCDFWriterOptions {
                    combine_attrs: Some(crate::attributes::CombineAttrs::Identical),
                    ..Default::default()
                },
                ..NetCDFOptions::default()
            },
            NetcdfConfig::default(),
        );
        let ctx = session();
        let format = factory.create(&ctx.state(), &HashMap::new()).unwrap();
        let schema: SchemaRef = Arc::new(arrow::datatypes::Schema::empty());
        let conf = FileSinkConfig {
            original_url: String::new(),
            object_store_url: ObjectStoreUrl::local_filesystem(),
            file_group: FileGroup::default(),
            table_paths: vec![],
            output_schema: schema.clone(),
            table_partition_cols: vec![],
            insert_op: InsertOp::Append,
            keep_partition_by_columns: false,
            file_extension: "nc".to_string(),
            file_output_mode: FileOutputMode::SingleFile,
        };
        let error = format
            .create_writer_physical_plan(Arc::new(EmptyExec::new(schema)), &ctx.state(), conf, None)
            .await
            .err()
            .expect("combine_attrs with write_dimensions is an error")
            .to_string();
        assert!(error.contains("combine_attrs"), "{error}");
    }

    /// `FileFormatFactory::default` also carries the configured reader.
    #[test]
    fn the_default_format_carries_the_configured_reader() {
        let factory = NetCDFFormatFactory::new(
            Arc::new(ListingFactory::dynamic()),
            std::env::temp_dir(),
            NetCDFOptions::default(),
            NetcdfConfig {
                use_rust_reader: false,
                ..NetcdfConfig::default()
            },
        );
        let format = FileFormatFactory::default(&factory);
        assert_eq!(
            format
                .downcast_ref::<NetcdfFormat>()
                .unwrap()
                .reader_backend(),
            ReaderBackend::NetcdfC
        );
    }

    // ── Both readers agree ─────────────────────────────────────────────

    #[tokio::test]
    async fn both_readers_infer_the_same_schema() {
        for file in [GRIDDED_FILE, WOD_FILE] {
            let ctx = session();
            register(&ctx, "netcdf_c", ReaderBackend::NetcdfC, file).await;
            register(&ctx, "rust", ReaderBackend::Oxcdf, file).await;

            // Compare the Arrow schemas: a `DFSchema` also carries the table
            // name, which differs here by construction.
            let c = ctx
                .table("netcdf_c")
                .await
                .unwrap()
                .schema()
                .as_arrow()
                .clone();
            let rust = ctx.table("rust").await.unwrap().schema().as_arrow().clone();
            assert_eq!(rust, c, "schemas differ for {file}");
        }
    }

    // ── Partitioning ───────────────────────────────────────────────────

    /// `OPTIONS ('skip_unbroadcastable' 'true')` reaches the format.
    #[tokio::test]
    async fn the_factory_reads_the_skip_option() {
        let factory = factory();
        let ctx = SessionContext::new();
        let options = HashMap::from([("skip_unbroadcastable".to_string(), "true".to_string())]);

        let format = factory.create(&ctx.state(), &options).unwrap();
        let format = format.downcast_ref::<NetcdfFormat>().unwrap();
        assert!(format.options.skip_unbroadcastable);

        let default = factory.create(&ctx.state(), &HashMap::new()).unwrap();
        let default = default.downcast_ref::<NetcdfFormat>().unwrap();
        assert!(!default.options.skip_unbroadcastable);
    }

    /// A session with `target_partitions` partitions and the given batch size.
    fn splitting_session(target_partitions: usize, batch_size: usize) -> SessionContext {
        let config = SessionConfig::new()
            .with_target_partitions(target_partitions)
            .with_batch_size(batch_size)
            .with_extension(
                beacon_datafusion_ext::type_widening::ArrowTypeWidening::default_extension(),
            );

        let state = SessionStateBuilder::new()
            .with_config(config)
            .with_default_features()
            .build();
        SessionContext::new_with_state(state)
    }

    /// How many columns [`write_large_netcdf`] writes. They are named `V0`..,
    /// and the tests read `V0`.
    const LARGE_COLUMNS: usize = 20;
    /// How many rows it writes in each.
    const LARGE_ROWS: usize = 100_000;

    /// Write a netCDF-4 file of about 16 MB, and return its path.
    ///
    /// The morsels of a file stay in one partition below 1 MiB. Every bundled
    /// fixture is under that size, so a test that shares one file makes its own.
    ///
    /// It is written **wide** rather than long, and that is not a free choice.
    /// `oxcdf` cannot read this writer's output once a single variable grows
    /// past roughly 200k values: it fails with "chunk at […] was neither cached
    /// nor fetched" while netcdf-c reads the same bytes. The limit follows one
    /// variable's chunk count, not the file's size, so 20 columns of 100k values
    /// is large enough and still reads back. Measured:
    /// 20x100k is 16.3 MB and reads; 12x200k is 19.4 MB and does not.
    ///
    /// The caller holds the [`TempDir`](tempfile::TempDir) for as long as the
    /// table is registered.
    fn write_large_netcdf() -> (tempfile::TempDir, PathBuf) {
        use arrow::array::{ArrayRef, Float64Array};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;

        use crate::encoders::default::DefaultEncoder;
        use crate::writer::ArrowRecordBatchWriter;

        let schema = Arc::new(Schema::new(
            (0..LARGE_COLUMNS)
                .map(|column| Field::new(format!("V{column}"), DataType::Float64, false))
                .collect::<Vec<_>>(),
        ));
        let columns: Vec<ArrayRef> = (0..LARGE_COLUMNS)
            .map(|column| {
                Arc::new(Float64Array::from_iter_values(
                    (0..LARGE_ROWS).map(|row| (row + column) as f64 * 0.25),
                )) as ArrayRef
            })
            .collect();
        let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();

        let dir = tempfile::tempdir().expect("a temp directory");
        let path = dir.path().join("large.nc");
        let mut writer =
            ArrowRecordBatchWriter::<DefaultEncoder>::new(&path, schema).expect("a netCDF writer");
        writer.write_record_batch(batch).expect("write the batch");
        writer.finish().expect("finish the file");

        (dir, path)
    }

    /// `count` netCDF files in one directory, each holding `rows` rows of a
    /// single column, numbered so that no two files hold the same values.
    ///
    /// Uneven on purpose: a scan of equal files divides the same way however it
    /// is divided, and would hide a queue that handed one file out twice.
    fn write_netcdf_collection(count: usize, rows: usize) -> (tempfile::TempDir, PathBuf) {
        use arrow::array::{ArrayRef, Float64Array};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;

        use crate::encoders::default::DefaultEncoder;
        use crate::writer::ArrowRecordBatchWriter;

        let dir = tempfile::tempdir().expect("a temp directory");
        let schema = Arc::new(Schema::new(vec![Field::new(
            "V0",
            DataType::Float64,
            false,
        )]));

        for file in 0..count {
            // Each file is a different length and a different range of values.
            let held = rows + file % 7;
            let base = (file * 10_000) as f64;
            let values: ArrayRef = Arc::new(Float64Array::from_iter_values(
                (0..held).map(|row| base + row as f64),
            ));
            let batch = RecordBatch::try_new(schema.clone(), vec![values]).unwrap();

            let path = dir.path().join(format!("part-{file:04}.nc"));
            let mut writer = ArrowRecordBatchWriter::<DefaultEncoder>::new(&path, schema.clone())
                .expect("a netCDF writer");
            writer.write_record_batch(batch).expect("write the batch");
            writer.finish().expect("finish the file");
        }

        let path = dir.path().to_path_buf();
        (dir, path)
    }

    /// A collection scanned on many partitions returns exactly what one
    /// partition returns.
    ///
    /// This is the shape the morsel queue exists for, and the guard on it. The
    /// partitions take files from one queue, so nothing at plan time says which
    /// partition reads what — and nothing but the answer says whether a file was
    /// handed out twice or not at all. `count(*)` catches both. `sum` catches a
    /// file read from the wrong place, and `min`/`max` catch a partial read.
    /// None of those raise an error on their own.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_collection_scanned_on_many_partitions_returns_the_same_rows() {
        const FILES: usize = 40;
        const ROWS: usize = 64;
        const QUERY: &str =
            r#"SELECT count(*), count("V0"), min("V0"), max("V0"), sum("V0") FROM many"#;

        let (_dir, dir) = write_netcdf_collection(FILES, ROWS);

        let whole = session();
        register_path(&whole, "many", ReaderBackend::Oxcdf, &dir).await;

        let split = splitting_session(8, 1024);
        register_path(&split, "many", ReaderBackend::Oxcdf, &dir).await;

        let plan = split
            .sql(r#"SELECT "V0" FROM many"#)
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        assert_eq!(
            scan_partitions(&plan),
            8,
            "a collection scans on every partition:\n{}",
            datafusion::physical_plan::displayable(plan.as_ref()).indent(false)
        );

        let summary = async |ctx: &SessionContext| {
            let batches = ctx.sql(QUERY).await.unwrap().collect().await.unwrap();
            format!("{:?}", batches[0].columns())
        };

        let whole_summary = summary(&whole).await;
        assert_eq!(
            summary(&split).await,
            whole_summary,
            "eight partitions over one queue read the collection exactly once"
        );
    }

    /// A collection where only some files carry the column the query selects.
    ///
    /// Real collections are not uniform: of one CORA year, 2% of the files hold
    /// no `TEMP` and 10% no `DEPH`. `SELECT "TEMP"` therefore meets files that
    /// have none of what it asked for, and those must be skipped, not fail the
    /// scan.
    ///
    /// The failure this guards is not a wrong number, it is a dead query:
    /// planning such a file as a `COUNT(*)` — its projection resolves empty
    /// either way — built a batch of no columns against a one-field schema and
    /// returned "number of columns(0) must match number of fields(1)".
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_collection_where_only_some_files_have_the_column_still_reads() {
        use arrow::array::{ArrayRef, Float64Array};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;

        use crate::encoders::default::DefaultEncoder;
        use crate::writer::ArrowRecordBatchWriter;

        const ROWS: usize = 32;
        const WITH: usize = 3;
        const WITHOUT: usize = 2;

        let dir = tempfile::tempdir().expect("a temp directory");

        // Files that hold "V0", and files that hold only "OTHER".
        for (count, column, base) in [(WITH, "V0", 0.0), (WITHOUT, "OTHER", 1000.0)] {
            let schema = Arc::new(Schema::new(vec![Field::new(
                column,
                DataType::Float64,
                false,
            )]));
            for file in 0..count {
                let values: ArrayRef = Arc::new(Float64Array::from_iter_values(
                    (0..ROWS).map(|row| base + (file * ROWS + row) as f64),
                ));
                let batch = RecordBatch::try_new(schema.clone(), vec![values]).unwrap();
                let path = dir.path().join(format!("{column}-{file}.nc"));
                let mut writer =
                    ArrowRecordBatchWriter::<DefaultEncoder>::new(&path, schema.clone())
                        .expect("a netCDF writer");
                writer.write_record_batch(batch).expect("write the batch");
                writer.finish().expect("finish the file");
            }
        }

        let ctx = splitting_session(4, 1024);
        register_path(&ctx, "mixed", ReaderBackend::Oxcdf, dir.path()).await;

        // Every file lacking "V0" is skipped; the ones holding it are read whole.
        let batches = ctx
            .sql(r#"SELECT count("V0"), min("V0"), max("V0") FROM mixed"#)
            .await
            .expect("the query plans")
            .collect()
            .await
            .expect("a file without the column does not fail the scan");

        let summary = format!("{:?}", batches[0].columns());
        assert!(
            summary.contains(&format!("{}", WITH * ROWS)),
            "every row of the {WITH} files that have the column: {summary}"
        );
        assert!(
            summary.contains(&format!("{}", (WITH * ROWS - 1) as f64)),
            "and its largest value, so nothing was truncated: {summary}"
        );
    }

    /// The partition count of the scan at the bottom of `plan`.
    ///
    /// The root can carry more partitions than the scan does: DataFusion adds a
    /// round-robin repartition above a single-partition scan, which hides
    /// whether the scan itself was split. This looks at the scan, which is the
    /// thing splitting changes.
    fn scan_partitions(plan: &Arc<dyn datafusion::physical_plan::ExecutionPlan>) -> usize {
        use datafusion::physical_plan::ExecutionPlanProperties;

        let mut node = plan.clone();
        while let Some(child) = node.children().first() {
            node = Arc::clone(child);
        }
        node.output_partitioning().partition_count()
    }

    /// A large file scans in several partitions, and returns the same rows it
    /// returns in one.
    ///
    /// The partition count is the point of the feature. The row check is the
    /// guard on it: `count(*)` catches a morsel read twice or not at all, and
    /// `min`/`max` catch a morsel that read the wrong region. None of those
    /// raise an error on their own.
    #[tokio::test]
    async fn a_large_file_splits_and_returns_the_same_rows() {
        const QUERY: &str = r#"SELECT count(*), count("V0"), min("V0"), max("V0") FROM one"#;

        let (_dir, file) = write_large_netcdf();

        let whole = session();
        register_path(&whole, "one", ReaderBackend::Oxcdf, &file).await;

        let split = splitting_session(4, 8192);
        register_path(&split, "one", ReaderBackend::Oxcdf, &file).await;

        let plan = split
            .sql(r#"SELECT "V0" FROM one"#)
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        assert_eq!(
            scan_partitions(&plan),
            4,
            "a large file should scan in 4 partitions:\n{}",
            datafusion::physical_plan::displayable(plan.as_ref()).indent(false)
        );

        let summary = async |ctx: &SessionContext| {
            let batches = ctx.sql(QUERY).await.unwrap().collect().await.unwrap();
            format!("{:?}", batches[0].columns())
        };

        let whole_summary = summary(&whole).await;
        assert_eq!(summary(&split).await, whole_summary);
    }

    /// A bare `count(*)` over a shared file counts every row once.
    ///
    /// It projects no column, so the planner cuts the full grid of the file into
    /// morsels. A partition that read the file whole would count it once per
    /// partition, and the answer would grow with `target_partitions`. Nothing
    /// about that is an error.
    ///
    /// The whole-file session answers in one partition, so it is the count to
    /// match.
    #[tokio::test]
    async fn a_shared_count_star_counts_every_row_once() {
        let (_dir, file) = write_large_netcdf();

        let whole = session();
        register_path(&whole, "one", ReaderBackend::Oxcdf, &file).await;

        let count = async |ctx: &SessionContext| {
            let batches = ctx
                .sql("SELECT count(*) FROM one")
                .await
                .unwrap()
                .collect()
                .await
                .unwrap();
            batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .expect("int64 count")
                .value(0)
        };

        let expected = count(&whole).await;
        assert_eq!(
            expected, LARGE_ROWS as i64,
            "the fixture holds one row per value of each column"
        );

        // Several partition counts, because a file read once per partition
        // returns a multiple of the truth and 2 is easy to mistake for a
        // coincidence.
        for target_partitions in [2_usize, 4, 7] {
            let split = splitting_session(target_partitions, 8192);
            register_path(&split, "one", ReaderBackend::Oxcdf, &file).await;

            // Guard the guard: the count below proves nothing if the scan under
            // it was never shared in the first place.
            let plan = split
                .sql("SELECT count(*) FROM one")
                .await
                .unwrap()
                .create_physical_plan()
                .await
                .unwrap();
            assert_eq!(
                scan_partitions(&plan),
                target_partitions,
                "the count(*) scan must be shared:\n{}",
                datafusion::physical_plan::displayable(plan.as_ref()).indent(false)
            );

            assert_eq!(
                count(&split).await,
                expected,
                "target_partitions={target_partitions}: the shared file is counted once"
            );
        }
    }

    /// A filtered scan counts what the coordinate itself says it should.
    ///
    /// A scan that skips chunks on the predicate loses rows when a bound is too
    /// tight, and no error shows it. So the expected answer comes from the
    /// coordinate column itself, read with no predicate in the plan.
    ///
    /// The fixture's `lat` runs from about 38.8 to 48.8, so the thresholds below
    /// keep everything, some, and nothing in turn. The middle one is the one
    /// that matters: a chunk half inside the bound has to be read whole.
    #[tokio::test]
    async fn a_pruned_scan_counts_what_the_coordinate_says_it_should() {
        use arrow::array::{Float32Array, Int64Array};

        let ctx = session();
        register(&ctx, "gridded", ReaderBackend::Oxcdf, GRIDDED_FILE).await;

        // Every lat the file holds, with no predicate in the plan.
        let batches = ctx
            .sql("SELECT lat FROM gridded")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let lats: Vec<f32> = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .expect("lat is f32")
                    .iter()
                    .flatten()
                    .collect::<Vec<f32>>()
            })
            .collect();
        assert!(!lats.is_empty(), "the fixture must hold rows");

        let count = async |sql: &str| {
            let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
            batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("int64 count")
                .value(0) as usize
        };

        for threshold in [0.0_f32, 44.0, 60.0] {
            let expected = lats.iter().filter(|lat| **lat > threshold).count();
            assert_eq!(
                count(&format!(
                    "SELECT count(*) FROM gridded WHERE lat > {threshold}"
                ))
                .await,
                expected,
                "lat > {threshold}: the pruned scan does not match the coordinate"
            );
        }

        // The middle threshold has to be a partial prune, or the assertion above
        // it never exercised a chunk that straddles the bound.
        let partial = lats.iter().filter(|lat| **lat > 44.0).count();
        assert!(
            partial > 0 && partial < lats.len(),
            "44.0 must cut the fixture in two, it kept {partial} of {}",
            lats.len()
        );

        // A disjunction bounds nothing, so it must prune nothing and still
        // answer. `lat > 60` is inside `lat > 0`, so the two agree. The bounds
        // used to be intersected as if the `OR` were an `AND`, which pruned
        // every chunk under 60 — the whole file, since nothing reaches it.
        assert_eq!(
            count("SELECT count(*) FROM gridded WHERE lat > 0 OR lat > 60").await,
            lats.iter().filter(|lat| **lat > 0.0).count(),
            "a disjunction must not prune on one of its branches"
        );

        // The same for a negation, which means the opposite of its child.
        assert_eq!(
            count("SELECT count(*) FROM gridded WHERE NOT (lat <= 44)").await,
            partial,
            "a negation must not prune on the child it negates"
        );
    }

    /// A gridded scan must respect the batch size. `gridded-example.nc` has the
    /// shape `[time=1, lat=1208, lon=1920]`. The engine cut only the first axis,
    /// so the short `time` axis gave one chunk that held the whole array. See
    /// issue #338.
    ///
    /// `sea_ice_fraction` is contiguous in the file, so it reports its full
    /// shape as its chunk shape and the engine computes the chunk itself.
    #[tokio::test]
    async fn a_short_first_axis_still_splits_into_batches() {
        for batch_size in [4096usize, 8192] {
            let state = SessionStateBuilder::new()
                    .with_config(
                        SessionConfig::new()
                            .with_target_partitions(1)
                            .with_batch_size(batch_size)
                            .with_extension(
                                beacon_datafusion_ext::type_widening::ArrowTypeWidening::default_extension(),
                            ),
                    )
                    .with_default_features()
                    .build();
            let ctx = SessionContext::new_with_state(state);
            register(&ctx, "gridded", ReaderBackend::Oxcdf, GRIDDED_FILE).await;

            let batches = ctx
                .sql("SELECT sea_ice_fraction FROM gridded")
                .await
                .unwrap()
                .collect()
                .await
                .unwrap();

            let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            let largest = batches.iter().map(|b| b.num_rows()).max().unwrap_or(0);
            assert_eq!(rows, 1208 * 1920, "the scan must read every row");
            assert!(
                batches.len() > 1,
                "at batch size {batch_size} gave {} batch(es)",
                batches.len()
            );
            // The chunk fills from the last axis, so it holds at most
            // `batch_size` elements. Here one row of `lon` is 1920 ≤ 4096.
            assert!(
                largest <= batch_size,
                "at batch size {batch_size} gave a batch of {largest} rows"
            );
        }
    }
}

/// The filename extensions that netCDF files use.
#[cfg(test)]
mod extension_tests {
    use object_store::path::Path;

    use super::*;

    fn factory() -> NetCDFFormatFactory {
        NetCDFFormatFactory::new(
            Arc::new(ListingFactory::dynamic()),
            std::env::temp_dir(),
            NetCDFOptions::default(),
            NetcdfConfig::default(),
        )
    }

    fn object_meta(path: &str) -> ObjectMeta {
        ObjectMeta {
            location: Path::from(path),
            last_modified: Default::default(),
            size: 0,
            e_tag: None,
            version: None,
        }
    }

    /// The registry keys a format by these, so each spelling resolves to netCDF.
    #[test]
    fn factory_advertises_every_netcdf_extension() {
        let factory = factory();
        assert_eq!(factory.get_ext(), "nc");
        assert_eq!(
            factory.file_extensions(),
            vec!["nc", "nc3", "nc4", "netcdf"]
        );
    }

    /// Discovery keeps every spelling, in any case, and labels it `nc`.
    #[test]
    fn discovery_keeps_every_netcdf_extension() {
        let objects: Vec<ObjectMeta> = [
            "a.nc",
            "b.nc3",
            "c.nc4",
            "d.netcdf",
            "e.cdf",
            "f.NC4",
            "g.h5",
            "h.ncml",
            "i.parquet",
        ]
        .into_iter()
        .map(object_meta)
        .collect();

        let datasets = factory().discover_datasets(&objects).unwrap();

        let paths: Vec<&str> = datasets.iter().map(|d| d.file_path.as_str()).collect();
        assert_eq!(paths, ["a.nc", "b.nc3", "c.nc4", "d.netcdf", "f.NC4"]);
        assert!(datasets.iter().all(|d| d.format == "nc"));
    }
}
