use std::any::Any;
use std::path::PathBuf;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use beacon_common::super_typing::super_type_schema;
use beacon_datafusion_ext::format_ext::{DatasetMetadata, FileFormatFactoryExt};
use beacon_datafusion_ext::listing_factory::ListingFactory;
use beacon_datafusion_ext::unique_values::UniqueValuesExec;
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

pub const NETCDF_EXTENSION: &str = "nc";

pub mod object_meta_resolver;
pub mod options;
#[cfg(test)]
mod partition_coverage_tests;
pub mod reader;
pub mod sink;
pub mod source;
pub mod statistics;
pub mod table_function;

pub use reader::{FileAccess, NetcdfInput, NetcdfReaderCache, ReaderBackend};
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
    /// Whether reads go through the pure-Rust [`oxcdf`] reader instead of
    /// netcdf-c.
    ///
    /// Off by default, so the netcdf-c path stays the one a runtime uses until
    /// an operator opts in. Turn it on to get parallel reads and native object
    /// store access; see [`crate::oxcdf_reader`]. Writes always use netcdf-c.
    pub use_rust_reader: bool,
}

impl Default for NetcdfConfig {
    fn default() -> Self {
        Self {
            use_reader_cache: true,
            reader_cache_size: 128,
            enable_statistics: true,
            use_rust_reader: false,
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
    pub listing_factory: Arc<ListingFactory>,
    pub output_dir: PathBuf,
    pub options: NetcdfOptions,
    pub config: NetcdfConfig,
    /// Shared reader cache for this runtime, sized from `config`.
    cache: NetcdfReaderCache,
}

impl NetCDFFormatFactory {
    pub fn new(
        listing_factory: Arc<ListingFactory>,
        output_dir: PathBuf,
        options: NetcdfOptions,
        config: NetcdfConfig,
    ) -> Self {
        let cache = NetcdfReaderCache::new(config.reader_cache_size);
        Self {
            listing_factory,
            output_dir,
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
        access: FileAccess,
    ) -> NetcdfFormat {
        let cache = use_reader_cache.then(|| self.cache.clone());
        NetcdfFormat::new(self.listing_factory.clone(), options)
            .with_cache(cache)
            .with_enable_statistics(enable_statistics)
            .with_access(access)
            .with_output_dir(self.output_dir.clone())
    }
}

/// The access a `use_rust_reader` setting selects, for a format built without a
/// location.
///
/// `oxcdf` is complete as it stands: it reads through the scan's object store.
/// netcdf-c needs a resolver it cannot have yet, so it gets the unresolvable
/// default; `create_with_native_root` is where it gets a real one.
fn access_for(use_rust_reader: bool) -> FileAccess {
    if use_rust_reader {
        FileAccess::Oxcdf
    } else {
        FileAccess::default()
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
        let mut use_rust_reader = self.config.use_rust_reader;

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
        if let Some(value) = format_options.get("use_rust_reader") {
            use_rust_reader = parse_bool_option("use_rust_reader", value)?;
        }

        Ok(Arc::new(self.build_format(
            options,
            use_reader_cache,
            enable_statistics,
            access_for(use_rust_reader),
        )))
    }

    fn default(&self) -> Arc<dyn FileFormat> {
        Arc::new(self.build_format(
            self.options.clone(),
            self.config.use_reader_cache,
            self.config.enable_statistics,
            access_for(self.config.use_rust_reader),
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
    /// This is where a [`FileAccess::NetcdfC`] gets its resolver.
    ///
    /// netcdf-c opens a local path or an http(s) URL and never the object store,
    /// so it needs a resolver built from the root store `url` resolves against.
    /// Plain [`FileFormatFactory::create`] has no location to work from, so it
    /// leaves [`FileAccess::default`] in place, which resolves nothing.
    ///
    /// `native_read_root` rejects a scheme netcdf-c cannot open (s3/gs/az) here,
    /// naming the location, rather than failing later per listed object.
    ///
    /// [`FileAccess::Oxcdf`] reads through the object store, so it needs no
    /// resolver and no native root. It is returned as it is, which is what lets
    /// a table live in s3, gs or az.
    fn create_with_native_root(
        &self,
        state: &dyn Session,
        format_options: &std::collections::HashMap<String, String>,
        url: &datafusion::datasource::listing::ListingTableUrl,
        listing: &ListingFactory,
    ) -> datafusion::error::Result<Arc<dyn FileFormat>> {
        let format = self.create(state, format_options)?;
        let netcdf = format
            .as_any()
            .downcast_ref::<NetcdfFormat>()
            .ok_or_else(|| {
                exec_datafusion_err!("the NetCDF factory did not produce a NetcdfFormat")
            })?
            .clone();
        match netcdf.access {
            // Already complete: it reads through the scan's object store.
            FileAccess::Oxcdf => Ok(Arc::new(netcdf)),
            // Supply the resolver `create` had no location to build.
            FileAccess::NetcdfC { .. } => {
                let root = listing.native_read_root(url)?;
                Ok(Arc::new(netcdf.with_access(FileAccess::netcdf_c(
                    object_meta_resolver::create_object_resolver(&root),
                ))))
            }
        }
    }

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
    pub listing_factory: Arc<ListingFactory>,
    pub options: NetcdfOptions,
    /// Reader cache to consult, or `None` to bypass caching for this format.
    cache: Option<NetcdfReaderCache>,
    /// Whether to generate per-file statistics during planning.
    enable_statistics: bool,
    /// Local directory the netcdf-c writer emits output files into.
    output_dir: PathBuf,
    /// How this table reaches its files, and which reader opens them.
    pub access: FileAccess,
}

impl NetcdfFormat {
    pub fn new(listing_factory: Arc<ListingFactory>, options: NetcdfOptions) -> Self {
        Self {
            listing_factory,
            options,
            cache: None,
            enable_statistics: false,
            output_dir: std::env::temp_dir(),
            access: FileAccess::default(),
        }
    }

    /// Set how this format reaches its files.
    pub fn with_access(mut self, access: FileAccess) -> Self {
        self.access = access;
        self
    }

    /// The reader this format opens files with.
    pub fn reader_backend(&self) -> ReaderBackend {
        self.access.backend()
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

    /// Set the local directory the writer emits output files into (defaults to the
    /// OS temp dir).
    pub fn with_output_dir(mut self, output_dir: PathBuf) -> Self {
        self.output_dir = output_dir;
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
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> datafusion::error::Result<SchemaRef> {
        let mut tasks = vec![];
        let cache = self.cache.as_ref();
        for object in objects {
            let task = reader::fetch_schema(
                cache,
                self.access.input_for(store, object)?,
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
        store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        object: &ObjectMeta,
    ) -> datafusion::error::Result<Statistics> {
        if self.enable_statistics {
            // Built the same way the reader builds it, so statistics and scans
            // can never disagree about where a file is or which reader opens it.
            let input = self.access.input_for(store, object)?;
            Ok(statistics::generate_statistics(input, &table_schema)
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
        // The scan carries nd data as `beacon.nd`-encoded struct columns, so
        // the file source's schema is the encoded form of the logical table
        // schema. `NdSourceExec` decodes it and `NdBroadcastExec` broadcasts it
        // back to the logical schema above the scan.
        let encoded_file_schema = Arc::new(beacon_datafusion_ext::nd::encoded_schema(
            conf.file_schema(),
        ));
        let table_schema = datafusion::datasource::table_schema::TableSchema::new(
            encoded_file_schema,
            conf.table_partition_cols().clone(),
        );
        // Preserve a projection that the scan pushed down into the incoming
        // source — rebuilding the source below would otherwise drop it.
        let projection = conf.file_source().projection().cloned();
        let source = NetCDFSource::new(
            self.access.clone(),
            self.options.read_dimensions.clone(),
            table_schema,
        )
        .with_cache(self.cache.clone())
        .with_projection(projection);
        let conf = FileScanConfigBuilder::from(conf)
            .with_source(Arc::new(source))
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
                self.access.clone(),
                self.options.read_dimensions.clone(),
                table_schema,
            )
            .with_cache(self.cache.clone()),
        )
    }
}

/// The reader-backend flag: how it is set, and that both readers agree.
#[cfg(test)]
mod reader_backend_tests {
    use std::collections::HashMap;
    use std::path::PathBuf;

    use beacon_common::super_table::SuperListingTable;
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
            NetcdfOptions::default(),
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
            .as_any()
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
            NetcdfOptions::default(),
        )
        .with_access(access_on(backend))
    }

    /// A single-partition session, so a scan yields rows in a stable order.
    fn session() -> SessionContext {
        let state = SessionStateBuilder::new()
            .with_config(SessionConfig::new().with_target_partitions(1))
            .with_default_features()
            .build();
        SessionContext::new_with_state(state)
    }

    /// Register one test file as a table read on `backend`.
    async fn register(ctx: &SessionContext, table: &str, backend: ReaderBackend, file: &str) {
        let url = ListingTableUrl::parse(test_file(file).to_string_lossy()).unwrap();
        let listing = SuperListingTable::new(&ctx.state(), Arc::new(format_on(backend)), vec![url])
            .await
            .unwrap_or_else(|e| panic!("register {file} on {backend:?}: {e}"));
        ctx.register_table(table, Arc::new(listing)).unwrap();
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

    /// netcdf-c stays the reader until an operator opts in.
    #[test]
    fn the_default_config_keeps_netcdf_c() {
        assert!(!NetcdfConfig::default().use_rust_reader);
        assert_eq!(ReaderBackend::default(), ReaderBackend::NetcdfC);
    }

    #[tokio::test]
    async fn a_table_option_selects_the_rust_reader() {
        let factory = factory();
        let ctx = session();
        let state = ctx.state();

        assert_eq!(
            backend_of(&factory, &state, &HashMap::new()).unwrap(),
            ReaderBackend::NetcdfC,
            "no option means the runtime default"
        );

        let options = HashMap::from([("use_rust_reader".to_string(), "true".to_string())]);
        assert_eq!(
            backend_of(&factory, &state, &options).unwrap(),
            ReaderBackend::Oxcdf
        );

        let options = HashMap::from([("use_rust_reader".to_string(), "off".to_string())]);
        assert_eq!(
            backend_of(&factory, &state, &options).unwrap(),
            ReaderBackend::NetcdfC
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
            NetcdfOptions::default(),
            NetcdfConfig {
                use_rust_reader: true,
                ..NetcdfConfig::default()
            },
        );
        let ctx = session();

        assert_eq!(
            backend_of(&factory, &ctx.state(), &HashMap::new()).unwrap(),
            ReaderBackend::Oxcdf
        );

        // And one table can still opt back out.
        let options = HashMap::from([("use_rust_reader".to_string(), "false".to_string())]);
        assert_eq!(
            backend_of(&factory, &ctx.state(), &options).unwrap(),
            ReaderBackend::NetcdfC
        );
    }

    /// `FileFormatFactory::default` also carries the configured reader.
    #[test]
    fn the_default_format_carries_the_configured_reader() {
        let factory = NetCDFFormatFactory::new(
            Arc::new(ListingFactory::dynamic()),
            std::env::temp_dir(),
            NetcdfOptions::default(),
            NetcdfConfig {
                use_rust_reader: true,
                ..NetcdfConfig::default()
            },
        );
        let format = FileFormatFactory::default(&factory);
        assert_eq!(
            format
                .as_any()
                .downcast_ref::<NetcdfFormat>()
                .unwrap()
                .reader_backend(),
            ReaderBackend::Oxcdf
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

    /// A full scan through the object store gives the same rows as netcdf-c.
    #[tokio::test]
    async fn both_readers_return_the_same_rows() {
        use arrow::compute::concat_batches;

        let ctx = session();
        register(&ctx, "netcdf_c", ReaderBackend::NetcdfC, GRIDDED_FILE).await;
        register(&ctx, "rust", ReaderBackend::Oxcdf, GRIDDED_FILE).await;

        let query = "SELECT analysed_sst, lat, lon FROM {table}";
        let collect = async |table: &str| {
            let batches = ctx
                .sql(&query.replace("{table}", table))
                .await
                .unwrap()
                .collect()
                .await
                .unwrap();
            let schema = batches[0].schema();
            concat_batches(&schema, &batches).unwrap()
        };

        let rust = collect("rust").await;
        assert!(rust.num_rows() > 0, "the scan must return rows");
        assert_eq!(rust, collect("netcdf_c").await);
    }

    // ── Partitioning ───────────────────────────────────────────────────

    /// A scan of many files already runs one file for each partition, on either
    /// reader: `ListingTable` splits the file groups by `target_partitions`
    /// before `FileSource::repartitioned` is ever consulted. So neither reader
    /// needs a repartition rule to read files in parallel. What the Rust reader
    /// adds is that those partitions then run at the same time, because it
    /// holds no global lock.
    #[tokio::test]
    async fn a_multi_file_scan_gets_one_partition_for_each_file() {
        use datafusion::physical_plan::ExecutionPlanProperties;

        let files = 4;
        let dir = tempfile::tempdir().unwrap();
        for i in 0..files {
            std::fs::copy(test_file(GRIDDED_FILE), dir.path().join(format!("f{i}.nc"))).unwrap();
        }

        for backend in [ReaderBackend::NetcdfC, ReaderBackend::Oxcdf] {
            let state = SessionStateBuilder::new()
                .with_config(SessionConfig::new().with_target_partitions(files))
                .with_default_features()
                .build();
            let ctx = SessionContext::new_with_state(state);
            let url = ListingTableUrl::parse(dir.path().to_string_lossy()).unwrap();
            let table =
                SuperListingTable::new(&ctx.state(), Arc::new(format_on(backend)), vec![url])
                    .await
                    .unwrap();
            ctx.register_table("many", Arc::new(table)).unwrap();

            let plan = ctx
                .sql("SELECT analysed_sst FROM many")
                .await
                .unwrap()
                .create_physical_plan()
                .await
                .unwrap();
            assert_eq!(
                plan.output_partitioning().partition_count(),
                files,
                "{backend:?} should scan {files} files in {files} partitions"
            );
        }
    }

    /// [`NetCDFSource::repartitioned`] must keep returning `None`, on either
    /// reader. DataFusion's file-group partitioner splits a file by **byte
    /// range**, and a byte range of a NetCDF file is not a NetCDF file. The
    /// opener ignores [`PartitionedFile::range`] and opens the whole dataset, so
    /// a range split would return every row once for each range. File-level
    /// parallelism does not go through here (see the test above).
    ///
    /// [`PartitionedFile::range`]: datafusion::datasource::listing::PartitionedFile::range
    #[test]
    fn the_source_never_splits_a_file_by_byte_range() {
        use datafusion::datasource::table_schema::TableSchema;
        use datafusion::execution::object_store::ObjectStoreUrl;

        for backend in [ReaderBackend::NetcdfC, ReaderBackend::Oxcdf] {
            let table_schema =
                TableSchema::from_file_schema(Arc::new(arrow::datatypes::Schema::empty()));
            let source = NetCDFSource::new(access_on(backend), None, table_schema);
            let config = FileScanConfigBuilder::new(
                ObjectStoreUrl::local_filesystem(),
                Arc::new(source.clone()) as Arc<dyn FileSource>,
            )
            .build();

            assert!(
                source.repartitioned(8, 1, None, &config).unwrap().is_none(),
                "{backend:?} must not accept a byte-range repartition"
            );
        }
    }

    /// A projection and a predicate push down the same way on either reader.
    #[tokio::test]
    async fn a_pushed_down_predicate_gives_the_same_answer() {
        let ctx = session();
        register(&ctx, "netcdf_c", ReaderBackend::NetcdfC, GRIDDED_FILE).await;
        register(&ctx, "rust", ReaderBackend::Oxcdf, GRIDDED_FILE).await;

        let count = async |table: &str| {
            let sql = format!("SELECT count(*) FROM {table} WHERE lat > 0");
            let batches = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
            batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap()
                .value(0)
        };

        let rust = count("rust").await;
        assert!(rust > 0, "the predicate must keep some rows");
        assert_eq!(rust, count("netcdf_c").await);
    }
}

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use datafusion::datasource::listing::PartitionedFile;
//     use datafusion::execution::object_store::ObjectStoreUrl;
//     use futures::StreamExt;
//     use object_store::path::Path;
//     use std::path::PathBuf;
//     use std::sync::Once;

//     static TEST_FIXTURES: Once = Once::new();

//     fn ensure_test_fixtures() {
//         TEST_FIXTURES.call_once(|| {
//             let dst_dir: PathBuf =
//                 beacon_config::DATASETS_DIR_PATH.join("beacon-arrow-netcdf-tests");
//             std::fs::create_dir_all(&dst_dir).expect("create test dir");

//             for name in ["wod_ctd_1964.nc", "gridded-example.nc"] {
//                 let src = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
//                     .join("test_files")
//                     .join(name);
//                 let dst = dst_dir.join(name);
//                 if !dst.exists() {
//                     std::fs::copy(&src, &dst)
//                         .unwrap_or_else(|e| panic!("copy {name} into datasets dir: {e}"));
//                 }
//             }
//         });
//     }

//     fn wod_object_meta() -> ObjectMeta {
//         ObjectMeta {
//             location: Path::from("beacon-arrow-netcdf-tests/wod_ctd_1964.nc"),
//             last_modified: chrono::Utc::now(),
//             size: 0,
//             e_tag: None,
//             version: None,
//         }
//     }

//     fn gridded_object_meta() -> ObjectMeta {
//         ObjectMeta {
//             location: Path::from("beacon-arrow-netcdf-tests/gridded-example.nc"),
//             last_modified: chrono::Utc::now(),
//             size: 0,
//             e_tag: None,
//             version: None,
//         }
//     }

//     /// The datasets local root the readers translate object paths under. Named
//     /// `store` at call sites for brevity, but it is a plain filesystem root now
//     /// that NetCDF opens files natively rather than through `object_store`.
//     async fn test_store() -> PathBuf {
//         ensure_test_fixtures();
//         beacon_config::DATASETS_DIR_PATH.to_path_buf()
//     }

//     fn test_format(datasets_root: PathBuf) -> NetcdfFormat {
//         NetcdfFormat::new(datasets_root, NetcdfOptions::default())
//     }

//     // ── fetch_schema ───────────────────────────────────────────────────

//     #[tokio::test]
//     async fn fetch_schema_returns_fields_for_ragged_file() {
//         let store = test_store().await;
//         let schema = reader::fetch_schema(store, wod_object_meta(), None)
//             .await
//             .expect("schema");
//         assert!(!schema.fields().is_empty());
//         let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
//         assert!(names.contains(&"lat"), "expected lat in {names:?}");
//         assert!(names.contains(&"lon"), "expected lon in {names:?}");
//         assert!(names.contains(&"z"), "expected z in {names:?}");
//         assert!(
//             names.contains(&"Temperature"),
//             "expected Temperature in {names:?}"
//         );
//     }

//     #[tokio::test]
//     async fn fetch_schema_returns_fields_for_gridded_file() {
//         let store = test_store().await;
//         let schema = reader::fetch_schema(store, gridded_object_meta(), None)
//             .await
//             .expect("schema");
//         assert!(!schema.fields().is_empty());
//         let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
//         assert!(
//             names.contains(&"analysed_sst"),
//             "expected analysed_sst in {names:?}"
//         );
//     }

//     #[tokio::test]
//     async fn fetch_schema_with_dimensions_limits_fields() {
//         let store = test_store().await;
//         let full = reader::fetch_schema(store.clone(), gridded_object_meta(), None)
//             .await
//             .expect("full schema");
//         let projected =
//             reader::fetch_schema(store, gridded_object_meta(), Some(vec!["time".to_string()]))
//                 .await
//                 .expect("projected schema");
//         assert!(
//             projected.fields().len() < full.fields().len(),
//             "dimension projection should reduce fields: projected {} vs full {}",
//             projected.fields().len(),
//             full.fields().len(),
//         );
//     }

//     // ── infer_schema ───────────────────────────────────────────────────

//     #[tokio::test]
//     async fn infer_schema_single_file() {
//         let store = test_store().await;
//         let format = test_format(store.clone());
//         let dummy_store: Arc<dyn ObjectStore> =
//             Arc::new(object_store::local::LocalFileSystem::new());
//         let ctx = datafusion::prelude::SessionContext::new();

//         let schema = format
//             .infer_schema(&ctx.state(), &dummy_store, &[gridded_object_meta()])
//             .await
//             .expect("infer schema");
//         assert!(!schema.fields().is_empty());
//     }

//     #[tokio::test]
//     async fn infer_schema_multiple_files_merges() {
//         let store = test_store().await;
//         let format = test_format(store.clone());
//         let dummy_store: Arc<dyn ObjectStore> =
//             Arc::new(object_store::local::LocalFileSystem::new());
//         let ctx = datafusion::prelude::SessionContext::new();

//         let schema = format
//             .infer_schema(
//                 &ctx.state(),
//                 &dummy_store,
//                 &[gridded_object_meta(), gridded_object_meta()],
//             )
//             .await
//             .expect("merged schema");
//         assert!(!schema.fields().is_empty());
//     }

//     #[tokio::test]
//     async fn infer_schema_empty_objects_returns_empty_schema() {
//         let store = test_store().await;
//         let format = test_format(store.clone());
//         let dummy_store: Arc<dyn ObjectStore> =
//             Arc::new(object_store::local::LocalFileSystem::new());
//         let ctx = datafusion::prelude::SessionContext::new();

//         // With no objects there is nothing to infer, so the format yields an
//         // empty schema — consistent with the zarr/tiff N-D formats.
//         let schema = format
//             .infer_schema(&ctx.state(), &dummy_store, &[])
//             .await
//             .expect("empty object list should infer an empty schema");
//         assert_eq!(schema.fields().len(), 0);
//     }

//     // ── file_source ────────────────────────────────────────────────────

//     #[tokio::test]
//     async fn file_source_returns_netcdf_type() {
//         let store = test_store().await;
//         let format = test_format(store);
//         let source = format.file_source(
//             datafusion::datasource::table_schema::TableSchema::from_file_schema(Arc::new(
//                 arrow::datatypes::Schema::empty(),
//             )),
//         );
//         assert_eq!(source.file_type(), "netcdf");
//     }

//     /// `CREATE EXTERNAL TABLE ... OPTIONS (...)` per-table overrides are parsed by
//     /// the factory: known keys are accepted (defaulting to the runtime config) and
//     /// a malformed boolean is rejected.
//     #[tokio::test]
//     async fn create_parses_per_table_options() {
//         use datafusion::datasource::file_format::FileFormatFactory;
//         use std::collections::HashMap;

//         let store = test_store().await;
//         let factory = NetCDFFormatFactory::new(
//             store,
//             std::env::temp_dir(),
//             NetcdfOptions::default(),
//             NetcdfConfig::default(),
//         );
//         let ctx = datafusion::prelude::SessionContext::new();

//         let mut options = HashMap::new();
//         options.insert("use_reader_cache".to_string(), "false".to_string());
//         options.insert("enable_statistics".to_string(), "false".to_string());
//         options.insert("read_dimensions".to_string(), "time, lat".to_string());
//         let format = factory.create(&ctx.state(), &options).expect("valid options");
//         let netcdf = format
//             .as_any()
//             .downcast_ref::<NetcdfFormat>()
//             .expect("netcdf format");
//         assert_eq!(
//             netcdf.options.read_dimensions.as_deref(),
//             Some(["time".to_string(), "lat".to_string()].as_slice())
//         );

//         // A malformed boolean for a known option is a hard error.
//         let mut bad = HashMap::new();
//         bad.insert("use_reader_cache".to_string(), "notabool".to_string());
//         assert!(factory.create(&ctx.state(), &bad).is_err());
//     }

//     // ── FileOpener produces batches ────────────────────────────────────

//     #[tokio::test]
//     async fn opener_streams_batches_for_ragged_file() {
//         let store = test_store().await;
//         let table_schema = reader::fetch_schema(store.clone(), wod_object_meta(), None)
//             .await
//             .expect("schema");

//         // The opener emits nd-encoded batches, so its source schema is encoded.
//         let ts = datafusion::datasource::table_schema::TableSchema::from_file_schema(Arc::new(
//             beacon_datafusion_ext::nd::encoded_schema(&table_schema),
//         ));
//         let opener = source::NetCDFSource::new(store, None, ts);
//         let file_opener = {
//             let conf = FileScanConfigBuilder::new(
//                 ObjectStoreUrl::local_filesystem(),
//                 Arc::new(opener.clone()) as Arc<dyn FileSource>,
//             )
//             .build();
//             opener
//                 .create_file_opener(
//                     Arc::new(object_store::local::LocalFileSystem::new()),
//                     &conf,
//                     0,
//                 )
//                 .expect("file opener")
//         };

//         let stream = file_opener
//             .open(PartitionedFile::from(wod_object_meta()))
//             .expect("open")
//             .await
//             .expect("stream future");

//         let batches: Vec<_> = stream.collect().await;
//         assert!(!batches.is_empty(), "should produce at least one batch");

//         let first = batches[0].as_ref().expect("first batch ok");
//         assert!(first.num_columns() > 0);
//         assert!(first.num_rows() > 0);
//     }

//     #[tokio::test]
//     async fn opener_streams_batches_for_gridded_file() {
//         let store = test_store().await;
//         let table_schema = reader::fetch_schema(store.clone(), gridded_object_meta(), None)
//             .await
//             .expect("schema");

//         let ts = datafusion::datasource::table_schema::TableSchema::from_file_schema(Arc::new(
//             beacon_datafusion_ext::nd::encoded_schema(&table_schema),
//         ));
//         let opener = source::NetCDFSource::new(store, None, ts);
//         let file_opener = {
//             let conf = FileScanConfigBuilder::new(
//                 ObjectStoreUrl::local_filesystem(),
//                 Arc::new(opener.clone()) as Arc<dyn FileSource>,
//             )
//             .build();
//             opener
//                 .create_file_opener(
//                     Arc::new(object_store::local::LocalFileSystem::new()),
//                     &conf,
//                     0,
//                 )
//                 .expect("file opener")
//         };

//         let stream = file_opener
//             .open(PartitionedFile::from(gridded_object_meta()))
//             .expect("open")
//             .await
//             .expect("stream future");

//         let batches: Vec<_> = stream.collect().await;
//         assert!(!batches.is_empty(), "should produce at least one batch");

//         let first = batches[0].as_ref().expect("first batch ok");
//         assert!(first.num_columns() > 0);
//         assert!(first.num_rows() > 0);
//     }

//     #[tokio::test]
//     async fn opener_with_projection_selects_columns() {
//         let store = test_store().await;
//         let table_schema = reader::fetch_schema(store.clone(), gridded_object_meta(), None)
//             .await
//             .expect("schema");

//         // Project to only the first column.
//         let projected_schema: SchemaRef = Arc::new(table_schema.project(&[0]).expect("project"));

//         let ts = datafusion::datasource::table_schema::TableSchema::from_file_schema(Arc::new(
//             beacon_datafusion_ext::nd::encoded_schema(&table_schema),
//         ));
//         let opener = source::NetCDFSource::new(store, None, ts);
//         let file_opener = {
//             let conf = FileScanConfigBuilder::new(
//                 ObjectStoreUrl::local_filesystem(),
//                 Arc::new(opener.clone()) as Arc<dyn FileSource>,
//             )
//             .with_projection_indices(Some(vec![0]))
//             .unwrap()
//             .build();
//             opener
//                 .create_file_opener(
//                     Arc::new(object_store::local::LocalFileSystem::new()),
//                     &conf,
//                     0,
//                 )
//                 .expect("file opener")
//         };

//         let stream = file_opener
//             .open(PartitionedFile::from(gridded_object_meta()))
//             .expect("open")
//             .await
//             .expect("stream future");

//         let batches: Vec<_> = stream.collect().await;
//         assert!(!batches.is_empty());

//         let first = batches[0].as_ref().expect("first batch ok");
//         assert_eq!(
//             first.num_columns(),
//             projected_schema.fields().len(),
//             "batch should have only the projected columns"
//         );
//     }

//     /// When a file is scanned under a merged (super-typed) schema that includes
//     /// columns it does not have, the `BatchAdapterFactory` must null-fill those
//     /// columns. We merge the gridded + ragged schemas, read the ragged file, and
//     /// assert a gridded-only column comes back all-null at the merged width.
//     #[tokio::test]
//     async fn opener_null_fills_columns_missing_from_a_file() {
//         use arrow::array::Array;

//         let store = test_store().await;
//         let format = test_format(store.clone());
//         let dummy_store: Arc<dyn ObjectStore> =
//             Arc::new(object_store::local::LocalFileSystem::new());
//         let ctx = datafusion::prelude::SessionContext::new();

//         // Merged (super-typed) schema across the ragged + gridded files.
//         let merged: SchemaRef = format
//             .infer_schema(
//                 &ctx.state(),
//                 &dummy_store,
//                 &[wod_object_meta(), gridded_object_meta()],
//             )
//             .await
//             .expect("merged schema");

//         // Pick a merged column the ragged (wod) file does not provide.
//         let wod_schema = reader::fetch_schema(store.clone(), wod_object_meta(), None)
//             .await
//             .expect("wod schema");
//         let missing = merged
//             .fields()
//             .iter()
//             .map(|f| f.name().clone())
//             .find(|name| wod_schema.index_of(name).is_err())
//             .expect("merged schema should contain a column the wod file lacks");
//         let missing_idx = merged.index_of(&missing).unwrap();

//         // No projection pushed → the opener reads under the full merged schema.
//         let ts = datafusion::datasource::table_schema::TableSchema::from_file_schema(Arc::new(
//             beacon_datafusion_ext::nd::encoded_schema(&merged),
//         ));
//         let opener = source::NetCDFSource::new(store, None, ts);
//         let conf = FileScanConfigBuilder::new(
//             ObjectStoreUrl::local_filesystem(),
//             Arc::new(opener.clone()) as Arc<dyn FileSource>,
//         )
//         .build();
//         let file_opener = opener
//             .create_file_opener(
//                 Arc::new(object_store::local::LocalFileSystem::new()),
//                 &conf,
//                 0,
//             )
//             .expect("file opener");

//         let stream = file_opener
//             .open(PartitionedFile::from(wod_object_meta()))
//             .expect("open")
//             .await
//             .expect("stream future");
//         let batches: Vec<_> = stream.collect().await;
//         assert!(!batches.is_empty(), "ragged file should produce batches");

//         for batch in &batches {
//             let batch = batch.as_ref().expect("batch ok");
//             assert_eq!(
//                 batch.schema().fields().len(),
//                 merged.fields().len(),
//                 "batch must conform to the merged schema width"
//             );
//             let col = batch.column(missing_idx);
//             assert_eq!(
//                 col.null_count(),
//                 col.len(),
//                 "column `{missing}` (absent from the wod file) must be all-null",
//             );
//         }
//     }

//     #[tokio::test]
//     async fn opener_with_read_dimensions_limits_columns() {
//         let store = test_store().await;
//         // Full schema without dimension filter.
//         let full_schema = reader::fetch_schema(store.clone(), gridded_object_meta(), None)
//             .await
//             .expect("full schema");

//         // Schema with dimension filter.
//         let dim_schema = reader::fetch_schema(
//             store.clone(),
//             gridded_object_meta(),
//             Some(vec!["time".to_string()]),
//         )
//         .await
//         .expect("dim schema");

//         let ts = datafusion::datasource::table_schema::TableSchema::from_file_schema(Arc::new(
//             beacon_datafusion_ext::nd::encoded_schema(&dim_schema),
//         ));
//         let opener = source::NetCDFSource::new(store, Some(vec!["time".to_string()]), ts);
//         let file_opener = {
//             let conf = FileScanConfigBuilder::new(
//                 ObjectStoreUrl::local_filesystem(),
//                 Arc::new(opener.clone()) as Arc<dyn FileSource>,
//             )
//             .build();
//             opener
//                 .create_file_opener(
//                     Arc::new(object_store::local::LocalFileSystem::new()),
//                     &conf,
//                     0,
//                 )
//                 .expect("file opener")
//         };

//         let stream = file_opener
//             .open(PartitionedFile::from(gridded_object_meta()))
//             .expect("open")
//             .await
//             .expect("stream future");

//         let batches: Vec<_> = stream.collect().await;
//         assert!(!batches.is_empty());

//         let first = batches[0].as_ref().expect("first batch ok");
//         assert!(
//             first.num_columns() < full_schema.fields().len(),
//             "dimension-filtered batch should have fewer columns ({}) than full ({})",
//             first.num_columns(),
//             full_schema.fields().len(),
//         );
//     }

//     // ── End-to-end via SessionContext (projection + predicate pushdown) ──

//     /// Register `gridded-example.nc` as a DataFusion table backed by
//     /// [`NetcdfFormat`] over the `datasets://` object store.
//     async fn register_example(ctx: &datafusion::prelude::SessionContext, datasets_root: PathBuf) {
//         use beacon_common::super_table::SuperListingTable;
//         use datafusion::datasource::file_format::FileFormat;
//         use datafusion::datasource::listing::ListingTableUrl;

//         // The `datasets://` store lists the files; the format opens them natively
//         // under `datasets_root`.
//         let store = beacon_object_storage::local_datasets_store(datasets_root.clone())
//             .await
//             .expect("local datasets store");
//         let store_url = ObjectStoreUrl::parse("datasets://").unwrap();
//         ctx.register_object_store(store_url.as_ref(), store);

//         let format: Arc<dyn FileFormat> =
//             Arc::new(NetcdfFormat::new(datasets_root, NetcdfOptions::default()));
//         let url =
//             ListingTableUrl::parse("datasets:///beacon-arrow-netcdf-tests/gridded-example.nc")
//                 .unwrap();
//         let table = SuperListingTable::new(&ctx.state(), format, vec![url])
//             .await
//             .unwrap();
//         ctx.register_table("gridded_nc", Arc::new(table)).unwrap();
//     }

//     #[tokio::test]
//     async fn projection_pushdown_through_datafusion() {
//         let store = test_store().await;
//         let ctx = datafusion::prelude::SessionContext::new();
//         register_example(&ctx, store).await;

//         let df = ctx
//             .sql("SELECT analysed_sst, lat FROM gridded_nc")
//             .await
//             .unwrap();
//         let names: Vec<String> = df
//             .schema()
//             .fields()
//             .iter()
//             .map(|f| f.name().clone())
//             .collect();
//         assert_eq!(names, vec!["analysed_sst".to_string(), "lat".to_string()]);

//         let batches = df.collect().await.unwrap();
//         assert_eq!(batches[0].num_columns(), 2);
//         assert!(batches.iter().map(|b| b.num_rows()).sum::<usize>() > 0);
//     }

//     #[tokio::test]
//     async fn predicate_pushdown_prunes_through_datafusion() {
//         let store = test_store().await;
//         let ctx = datafusion::prelude::SessionContext::new();
//         register_example(&ctx, store).await;

//         // Latitude is geographic (≤ 90°), so this excludes every row.
//         let rows: usize = ctx
//             .sql("SELECT lat FROM gridded_nc WHERE lat > 100000")
//             .await
//             .unwrap()
//             .collect()
//             .await
//             .unwrap()
//             .iter()
//             .map(|b| b.num_rows())
//             .sum();
//         assert_eq!(rows, 0, "impossible latitude predicate should yield no rows");
//     }

//     #[tokio::test]
//     async fn predicate_pushdown_selects_subset_through_datafusion() {
//         use arrow::array::{Float64Array, Int64Array};

//         let store = test_store().await;
//         let ctx = datafusion::prelude::SessionContext::new();
//         register_example(&ctx, store).await;

//         // Filter on the midpoint of the latitude range so the predicate keeps
//         // some — but not all — rows. Cast to f64 so the test is type-agnostic.
//         let stats = ctx
//             .sql(
//                 "SELECT min(CAST(lat AS DOUBLE)) AS mn, max(CAST(lat AS DOUBLE)) AS mx, \
//                  count(*) AS n FROM gridded_nc",
//             )
//             .await
//             .unwrap()
//             .collect()
//             .await
//             .unwrap();
//         let row = &stats[0];
//         let d = |i: usize| {
//             row.column(i)
//                 .as_any()
//                 .downcast_ref::<Float64Array>()
//                 .unwrap()
//                 .value(0)
//         };
//         let (mn, mx) = (d(0), d(1));
//         let total = row
//             .column(2)
//             .as_any()
//             .downcast_ref::<Int64Array>()
//             .unwrap()
//             .value(0);
//         assert!(mx > mn, "lat must span a range");
//         let mid = mn + (mx - mn) / 2.0;

//         let batches = ctx
//             .sql(&format!(
//                 "SELECT CAST(lat AS DOUBLE) AS latd FROM gridded_nc \
//                  WHERE CAST(lat AS DOUBLE) > {mid}"
//             ))
//             .await
//             .unwrap()
//             .collect()
//             .await
//             .unwrap();
//         let mut kept = 0i64;
//         for b in &batches {
//             let col = b
//                 .column(0)
//                 .as_any()
//                 .downcast_ref::<Float64Array>()
//                 .unwrap();
//             for i in 0..col.len() {
//                 assert!(col.value(i) > mid, "every returned lat must satisfy the predicate");
//             }
//             kept += b.num_rows() as i64;
//         }
//         assert!(kept > 0, "midpoint predicate should keep some rows");
//         assert!(kept < total, "midpoint predicate should drop some rows");
//     }

//     /// `scale_factor`/`add_offset` are actually applied: the decoded
//     /// `analysed_sst` (packed int16 with scale 0.01, offset 273.15 kelvin) lands
//     /// in a physical sea-surface-temperature range, which raw packed values
//     /// never would.
//     #[tokio::test]
//     async fn scale_offset_decodes_to_physical_range() {
//         use arrow::array::Float64Array;

//         let store = test_store().await;
//         let ctx = datafusion::prelude::SessionContext::new();
//         register_example(&ctx, store).await;

//         let batches = ctx
//             .sql("SELECT min(analysed_sst) AS mn, max(analysed_sst) AS mx FROM gridded_nc")
//             .await
//             .unwrap()
//             .collect()
//             .await
//             .unwrap();

//         let f = |i: usize| {
//             batches[0]
//                 .column(i)
//                 .as_any()
//                 .downcast_ref::<Float64Array>()
//                 .unwrap()
//                 .value(0)
//         };
//         let (mn, mx) = (f(0), f(1));
//         assert!(
//             (250.0..350.0).contains(&mn) && (250.0..350.0).contains(&mx),
//             "decoded SST must be in a physical kelvin range, got [{mn}, {mx}]"
//         );
//         assert!(mx > mn, "SST must span a range");
//     }

//     /// End-to-end: with the nd projection-pushdown rule registered (as
//     /// beacon-core does), `SELECT lat * 2` plans with an `NdProjectionExec`
//     /// *below* the `NdBroadcastExec`, and yields the same values as a plain
//     /// session.
//     #[tokio::test]
//     async fn projection_pushdown_fires_end_to_end() {
//         use arrow::compute::concat_batches;
//         use datafusion::execution::session_state::SessionStateBuilder;
//         use datafusion::physical_plan::displayable;

//         let store = test_store().await;

//         let state = SessionStateBuilder::new()
//             .with_default_features()
//             .with_physical_optimizer_rule(Arc::new(
//                 beacon_datafusion_ext::nd::NdProjectionPushdown::new(),
//             ))
//             .build();
//         let ctx = datafusion::prelude::SessionContext::new_with_state(state);
//         register_example(&ctx, store.clone()).await;

//         let df = ctx
//             .sql("SELECT lat * 2 AS lat2 FROM gridded_nc")
//             .await
//             .unwrap();
//         let plan = df.clone().create_physical_plan().await.unwrap();
//         let rendered = displayable(plan.as_ref()).indent(true).to_string();

//         let broadcast = rendered.find("NdBroadcastExec");
//         let projection = rendered.find("NdProjectionExec");
//         let source = rendered.find("NdSourceExec");
//         assert!(
//             broadcast < projection && projection < source,
//             "projection must be pushed below the broadcast:\n{rendered}"
//         );

//         let bare = datafusion::prelude::SessionContext::new();
//         register_example(&bare, store).await;
//         let expected = bare
//             .sql("SELECT lat * 2 AS lat2 FROM gridded_nc")
//             .await
//             .unwrap()
//             .collect()
//             .await
//             .unwrap();
//         let actual = df.collect().await.unwrap();

//         let schema = actual[0].schema();
//         assert_eq!(
//             concat_batches(&schema, &actual).unwrap(),
//             concat_batches(&schema, &expected).unwrap(),
//         );
//     }

//     // ── nd pipeline: plan shape + variables & attributes end-to-end ──────

//     /// The physical plan for an nd scan is the nd spine on top of the standard
//     /// file scan: `NdBroadcastExec` → `NdSourceExec` → `DataSourceExec`, in that
//     /// nesting order (parent above child in the indented render).
//     #[tokio::test]
//     async fn physical_plan_is_nd_spine_over_scan() {
//         use datafusion::physical_plan::displayable;

//         let store = test_store().await;
//         let ctx = datafusion::prelude::SessionContext::new();
//         register_example(&ctx, store).await;

//         let plan = ctx
//             .sql("SELECT analysed_sst FROM gridded_nc")
//             .await
//             .unwrap()
//             .create_physical_plan()
//             .await
//             .unwrap();
//         let rendered = displayable(plan.as_ref()).indent(true).to_string();

//         let broadcast = rendered.find("NdBroadcastExec");
//         let source = rendered.find("NdSourceExec");
//         let scan = rendered.find("DataSourceExec");
//         assert!(
//             broadcast.is_some() && source.is_some() && scan.is_some(),
//             "plan must contain the nd spine over a DataSourceExec:\n{rendered}"
//         );
//         assert!(
//             broadcast < source && source < scan,
//             "expected NdBroadcastExec → NdSourceExec → DataSourceExec nesting:\n{rendered}"
//         );
//     }

//     /// End-to-end through DataFusion: a gridded data variable comes back decoded
//     /// (scale/offset applied → Float64), and its rank-0 attributes — a variable
//     /// attribute (`analysed_sst.units`) and a global attribute (`.Conventions`) —
//     /// ride the `beacon.nd` encoding as constant columns on every row.
//     #[tokio::test]
//     async fn end_to_end_reads_variable_with_attributes() {
//         use arrow::array::StringArray;
//         use arrow::datatypes::DataType;

//         let store = test_store().await;
//         let ctx = datafusion::prelude::SessionContext::new();
//         register_example(&ctx, store).await;

//         let batches = ctx
//             .sql(
//                 r#"SELECT analysed_sst,
//                           "analysed_sst.units" AS units,
//                           ".Conventions"       AS conventions
//                    FROM gridded_nc LIMIT 4"#,
//             )
//             .await
//             .unwrap()
//             .collect()
//             .await
//             .unwrap();

//         let total: usize = batches.iter().map(|b| b.num_rows()).sum();
//         assert_eq!(total, 4, "LIMIT 4 should yield exactly 4 rows");

//         let batch = &batches[0];
//         // The data variable is decoded via scale_factor/add_offset → Float64,
//         // consistent with the zarr reader.
//         assert_eq!(
//             batch.column_by_name("analysed_sst").unwrap().data_type(),
//             &DataType::Float64
//         );

//         let units = batch
//             .column_by_name("units")
//             .unwrap()
//             .as_any()
//             .downcast_ref::<StringArray>()
//             .unwrap();
//         let conventions = batch
//             .column_by_name("conventions")
//             .unwrap()
//             .as_any()
//             .downcast_ref::<StringArray>()
//             .unwrap();
//         for i in 0..batch.num_rows() {
//             assert_eq!(units.value(i), "kelvin", "variable attribute must be constant");
//             assert_eq!(conventions.value(i), "CF-1.4", "global attribute must be constant");
//         }
//     }

//     /// The strongest constant-column check: co-selected with a gridded variable
//     /// (`lat`, which establishes the broadcast target), a rank-0 attribute is
//     /// present on *every* grid row and has exactly one distinct value across all
//     /// of them. Referencing a gridded variable matters — projecting to only the
//     /// scalar attribute would collapse the grid to a single row.
//     #[tokio::test]
//     async fn attribute_is_single_distinct_value_across_grid() {
//         use arrow::array::Int64Array;

//         let store = test_store().await;
//         let ctx = datafusion::prelude::SessionContext::new();
//         register_example(&ctx, store).await;

//         let batches = ctx
//             .sql(
//                 r#"SELECT COUNT(DISTINCT "analysed_sst.units") AS distinct_units,
//                           COUNT("analysed_sst.units")          AS attr_rows,
//                           COUNT(lat)                           AS grid_rows
//                    FROM gridded_nc"#,
//             )
//             .await
//             .unwrap()
//             .collect()
//             .await
//             .unwrap();

//         let int = |name: &str| {
//             batches[0]
//                 .column_by_name(name)
//                 .unwrap()
//                 .as_any()
//                 .downcast_ref::<Int64Array>()
//                 .unwrap()
//                 .value(0)
//         };
//         assert_eq!(int("distinct_units"), 1, "attribute must be a single constant");
//         assert!(int("grid_rows") > 1, "gridded variable must define a multi-row grid");
//         assert_eq!(
//             int("attr_rows"),
//             int("grid_rows"),
//             "attribute must be broadcast (non-null) onto every grid row"
//         );
//     }
// }
