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
        state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> datafusion::error::Result<SchemaRef> {
        use futures::{StreamExt, TryStreamExt};

        let cache = self.cache.as_ref();

        // Resolving an input can fail, and doing it up front keeps the stream
        // below infallible in its setup.
        let mut inputs = Vec::with_capacity(objects.len());
        for object in objects {
            inputs.push((self.access.input_for(store, object)?, object.clone()));
        }

        // Bounded, because each open holds a file descriptor until its schema is
        // read. `try_join_all` polls every future at once, so a table over a
        // hundred thousand objects opened a hundred thousand files before the
        // first one closed and died with `Too many open files` — the crash in
        // issue #361. `buffered` keeps the width to the session's
        // `meta_fetch_concurrency`, the same knob Parquet's inference uses, and
        // preserves order so the merged schema does not depend on which file
        // finished first.
        let width = state
            .config_options()
            .execution
            .meta_fetch_concurrency
            .max(1);
        let schemas: Vec<SchemaRef> = futures::stream::iter(inputs)
            .map(|(input, object)| {
                reader::fetch_schema(cache, input, object, self.options.read_dimensions.clone())
            })
            .buffered(width)
            .try_collect()
            .await?;
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
        // Statistics need the Rust reader. Every netcdf-c call serialises on a
        // process-global mutex (`netcdf_sys::libnetcdf_lock`, which the crate
        // documents as the difference between working and segfaulting on a
        // non-threadsafe hdf5 build), and `read_arrays` is synchronous, so
        // generating statistics under netcdf-c parks a tokio worker while
        // queued behind every other netCDF call in the process. One cold query
        // over a large collection is then serial *and* blocks query serving.
        //
        // `oxcdf` reads byte ranges through the object store: async, no global
        // lock, and it produces the same ranges for the same file. So the
        // capability follows the reader.
        //
        // Reporting unknown rather than erroring is deliberate. Absent
        // statistics are always a legal answer -- DataFusion prunes nothing and
        // scans everything, which is correct, just slower -- so a netcdf-c
        // deployment keeps working untouched.
        if !self.enable_statistics {
            return Ok(Statistics::new_unknown(&table_schema));
        }
        if !matches!(self.access, FileAccess::Oxcdf) {
            tracing::debug!(
                object = %object.location,
                "netCDF statistics need the Rust reader; set use_rust_reader to enable them"
            );
            return Ok(Statistics::new_unknown(&table_schema));
        }

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
    ///
    /// Sharing one file across partitions is the separate case, and only the
    /// Rust reader does it. See [`only_the_rust_reader_shares_one_file`].
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
                .with_config(
                    SessionConfig::new()
                        .with_target_partitions(files)
                        .with_extension(
                        beacon_datafusion_ext::type_widening::ArrowTypeWidening::default_extension(
                        ),
                    ),
                )
                .with_default_features()
                .build();
            let ctx = SessionContext::new_with_state(state);
            let url = ListingTableUrl::parse(dir.path().to_string_lossy()).unwrap();
            let table =
                FastObjectTable::try_new(&ctx.state(), Arc::new(format_on(backend)), vec![url])
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

    /// One file is shared across partitions under the Rust reader, and never
    /// under netcdf-c.
    ///
    /// A file over the minimum is not divided at plan time. It goes into every
    /// partition's group, marked with [`SharedFile`], and the partitions divide
    /// it as they read it by taking subsets from one queue. So the assertion is
    /// that the file appears once per partition, carries the mark, and carries
    /// no byte range.
    ///
    /// netcdf-c opts out through
    /// [`FileSource::supports_repartitioning`]: every call it makes queues on
    /// one process-global mutex, so partitions of a file would run one at a time
    /// and pay for an extra open each.
    #[test]
    fn only_the_rust_reader_shares_one_file() {
        use datafusion::datasource::listing::PartitionedFile;
        use datafusion::datasource::table_schema::TableSchema;
        use datafusion::execution::object_store::ObjectStoreUrl;

        use super::source::SharedFile;

        // Comfortably over the minimum a file has to clear to be shared.
        const FILE_SIZE: u64 = 64 * 1024 * 1024;
        const PARTITIONS: usize = 4;

        for (backend, shares) in [(ReaderBackend::NetcdfC, false), (ReaderBackend::Oxcdf, true)] {
            let table_schema =
                TableSchema::from_file_schema(Arc::new(arrow::datatypes::Schema::empty()));
            let source = NetCDFSource::new(access_on(backend), None, table_schema);
            let config = FileScanConfigBuilder::new(
                ObjectStoreUrl::local_filesystem(),
                Arc::new(source.clone()) as Arc<dyn FileSource>,
            )
            .with_file(PartitionedFile::new("one.nc", FILE_SIZE))
            .build();

            let repartitioned = source.repartitioned(PARTITIONS, 1, None, &config).unwrap();

            match (shares, repartitioned) {
                (false, result) => assert!(
                    result.is_none(),
                    "{backend:?} must not share a file across partitions"
                ),
                (true, None) => panic!("{backend:?} must share a file across partitions"),
                (true, Some(config)) => {
                    assert_eq!(
                        config.file_groups.len(),
                        PARTITIONS,
                        "{backend:?} should give the file to every partition"
                    );

                    for group in &config.file_groups {
                        assert_eq!(group.len(), 1, "{backend:?}: one file per partition");
                        let file = group.iter().next().unwrap();

                        assert!(
                            file.range.is_none(),
                            "{backend:?}: a shared file is not divided at plan time"
                        );

                        let consumers = file
                            .extensions
                            .as_ref()
                            .and_then(|ext| {
                                (ext.as_ref() as &dyn std::any::Any).downcast_ref::<SharedFile>()
                            })
                            .map(|marked| marked.consumers);
                        assert_eq!(
                            consumers,
                            Some(PARTITIONS),
                            "{backend:?}: the mark says how many partitions hold it"
                        );
                    }
                }
            }
        }
    }

    /// A file too small to share keeps its files whole and unmarked.
    ///
    /// Every partition opening a small file to take a subset or two would cost
    /// more than it returns, and the listing has already spread these across the
    /// scan.
    #[test]
    fn a_small_file_is_not_shared() {
        use datafusion::datasource::listing::PartitionedFile;
        use datafusion::datasource::table_schema::TableSchema;
        use datafusion::execution::object_store::ObjectStoreUrl;

        let table_schema =
            TableSchema::from_file_schema(Arc::new(arrow::datatypes::Schema::empty()));
        let source = NetCDFSource::new(access_on(ReaderBackend::Oxcdf), None, table_schema);
        let config = FileScanConfigBuilder::new(
            ObjectStoreUrl::local_filesystem(),
            Arc::new(source.clone()) as Arc<dyn FileSource>,
        )
        .with_file(PartitionedFile::new("small.nc", 1024 * 1024))
        .build();

        assert!(
            source.repartitioned(4, 1, None, &config).unwrap().is_none(),
            "a file under the minimum must not be shared"
        );
    }


    /// A session with `target_partitions` partitions.
    ///
    /// Nothing is done to the split minimum here. `NetCDFSource` sets its own
    /// ([`MIN_SPLIT_SIZE`]) and ignores the session's, so a test that wants a
    /// split has to bring a scan large enough to earn one.
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

    /// Write a netCDF-4 file larger than [`MIN_SPLIT_SIZE`], and return its path.
    ///
    /// Every bundled fixture is hundreds of KB, well under the minimum, so a
    /// test that needs a real split has to make its own file.
    ///
    /// It is written **wide** rather than long, and that is not a free choice.
    /// `oxcdf` cannot read this writer's output once a single variable grows
    /// past roughly 200k values: it fails with "chunk at […] was neither cached
    /// nor fetched" while netcdf-c reads the same bytes. The limit follows one
    /// variable's chunk count, not the file's size, so 20 columns of 100k values
    /// clears 8 MB three times over and still reads back. Measured: 20x100k is
    /// 16.3 MB and reads; 12x200k is 19.4 MB and does not.
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

    /// A file over the split minimum scans in several partitions, and returns
    /// the same rows it returns in one.
    ///
    /// The partition count is the point of the feature. The row check is the
    /// guard on it: `count(*)` catches a share that overlapped another or a gap
    /// between two, and `min`/`max` catch a share that read the wrong region.
    /// None of those raise an error on their own.
    ///
    /// This is the only test that reaches the split the way a query does: a real
    /// file over the real minimum, planned and executed through SQL. The others
    /// hand the opener its ranges directly, because no bundled fixture is large
    /// enough for a scan to split one.
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
            "a file over the minimum should scan in 4 partitions:\n{}",
            datafusion::physical_plan::displayable(plan.as_ref()).indent(false)
        );

        let summary = async |ctx: &SessionContext| {
            let batches = ctx.sql(QUERY).await.unwrap().collect().await.unwrap();
            format!("{:?}", batches[0].columns())
        };

        let whole_summary = summary(&whole).await;
        assert_eq!(summary(&split).await, whole_summary);
    }

    /// A scan under the split minimum stays on one partition.
    ///
    /// Every share of a file opens that file and builds its chunk list before it
    /// reads a byte. On a small file that setup costs more than the parallelism
    /// returns, so the source declines however many partitions the session asks
    /// for. The bundled gridded fixture is a few hundred KB, well under the line.
    ///
    /// The rows are checked too: a declined split must return the same answer,
    /// not merely the same partition count.
    #[tokio::test]
    async fn a_small_scan_is_not_split() {
        const QUERY: &str = "SELECT count(*), min(analysed_sst), max(analysed_sst) FROM one";

        let whole = session();
        register(&whole, "one", ReaderBackend::Oxcdf, GRIDDED_FILE).await;

        let asked = splitting_session(4, 8192);
        register(&asked, "one", ReaderBackend::Oxcdf, GRIDDED_FILE).await;

        let plan = asked
            .sql("SELECT analysed_sst FROM one")
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        assert_eq!(
            scan_partitions(&plan),
            1,
            "a scan under {} bytes must not split:\n{}",
            super::source::MIN_SPLIT_SIZE,
            datafusion::physical_plan::displayable(plan.as_ref()).indent(false)
        );

        let summary = async |ctx: &SessionContext| {
            let batches = ctx.sql(QUERY).await.unwrap().collect().await.unwrap();
            format!("{:?}", batches[0].columns())
        };

        let asked_summary = summary(&asked).await;
        assert_eq!(asked_summary, summary(&whole).await);
    }

    /// Every column, including every attribute, comes back identical.
    ///
    /// The other comparisons check data variables, or check that attributes are
    /// *present* with the right type and shape. Neither would catch an
    /// attribute whose **value** differs between the readers. This one reads
    /// the lot: data variables, variable attributes (`analysed_sst.units`) and
    /// global attributes (`.Conventions`), the last two broadcast onto every
    /// row by the nd pipeline.
    #[tokio::test]
    async fn both_readers_return_identical_values_for_every_column() {
        use arrow::compute::concat_batches;

        // The ragged file in full: 418 rows over 147 columns, ~1 MiB.
        //
        // The gridded file gets an explicit column list, not `SELECT *`. Its
        // grid is 2.3M rows, and `LIMIT` does not bound what the scan
        // materialises: the nd broadcast emits the whole grid as one batch and
        // the limit only slices it, so `SELECT *` here costs ~10 GiB for each
        // reader. The columns below are the ones this test is actually for — a
        // global attribute, a variable attribute, and the two CF decodes.
        for (file, query) in [
            (WOD_FILE, "SELECT * FROM {table}"),
            (
                GRIDDED_FILE,
                r#"SELECT ".Conventions", "analysed_sst.units", analysed_sst, time
                   FROM {table}"#,
            ),
        ] {
            let ctx = session();
            register(&ctx, "netcdf_c", ReaderBackend::NetcdfC, file).await;
            register(&ctx, "rust", ReaderBackend::Oxcdf, file).await;

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
            let c = collect("netcdf_c").await;

            assert!(rust.num_rows() > 0, "{file}: the scan must return rows");
            // Guard the guard: a column set that lost the attributes would make
            // the comparison below pass without checking any of them.
            let schema = rust.schema();
            let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
            assert!(
                names.iter().any(|n| n.starts_with('.')),
                "{file}: no global attribute in {names:?}"
            );
            assert!(
                names.iter().any(|n| n.contains('.') && !n.starts_with('.')),
                "{file}: no variable attribute in {names:?}"
            );

            // Compare column by column, so a failure names the column.
            for (i, field) in rust.schema().fields().iter().enumerate() {
                assert_eq!(
                    rust.column(i),
                    c.column(i),
                    "{file}: column '{}' differs between the readers",
                    field.name()
                );
            }
            assert_eq!(rust, c, "{file}: batches differ");
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

    /// A gridded scan must respect the batch size. `gridded-example.nc` has the
    /// shape `[time=1, lat=1208, lon=1920]`. The engine cut only the first axis,
    /// so the short `time` axis gave one chunk that held the whole array. See
    /// issue #338.
    ///
    /// `sea_ice_fraction` is contiguous in the file, so it reports its full
    /// shape as its chunk shape and the engine computes the chunk itself.
    #[tokio::test]
    async fn a_short_first_axis_still_splits_into_batches() {
        for backend in [ReaderBackend::NetcdfC, ReaderBackend::Oxcdf] {
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
                register(&ctx, "gridded", backend, GRIDDED_FILE).await;

                let batches = ctx
                    .sql("SELECT sea_ice_fraction FROM gridded")
                    .await
                    .unwrap()
                    .collect()
                    .await
                    .unwrap();

                let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                let largest = batches.iter().map(|b| b.num_rows()).max().unwrap_or(0);
                assert_eq!(rows, 1208 * 1920, "{backend:?} must read every row");
                assert!(
                    batches.len() > 1,
                    "{backend:?} at batch size {batch_size} gave {} batch(es)",
                    batches.len()
                );
                // The chunk fills from the last axis, so it holds at most
                // `batch_size` elements. Here one row of `lon` is 1920 ≤ 4096.
                assert!(
                    largest <= batch_size,
                    "{backend:?} at batch size {batch_size} gave a batch of {largest} rows"
                );
            }
        }
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
//         use beacon_datafusion_ext::fast_object::FastObjectTable;
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
//         let table = FastObjectTable::try_new(&ctx.state(), format, vec![url])
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
