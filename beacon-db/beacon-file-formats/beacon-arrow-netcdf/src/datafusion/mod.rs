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

    /// The split minimum a scan takes from its session
    /// (`repartition_file_min_size`), and what these tests pass by hand.
    const MIN_SHARE_SIZE: usize = 10 * 1024 * 1024;

    /// A file too small to share is left whole.
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
            source
                .repartitioned(4, MIN_SHARE_SIZE, None, &config)
                .unwrap()
                .is_none(),
            "a file under the minimum must not be shared"
        );
    }

    /// A file over the minimum lands in every partition's group, and the source
    /// that comes back knows it has to be read through a share.
    ///
    /// The group count is what the scan runs on; the share is what keeps that
    /// from returning every row once per partition.
    #[test]
    fn a_large_file_is_shared_by_every_partition() {
        use datafusion::datasource::listing::PartitionedFile;
        use datafusion::datasource::table_schema::TableSchema;
        use datafusion::execution::object_store::ObjectStoreUrl;

        const PARTITIONS: usize = 4;

        let table_schema =
            TableSchema::from_file_schema(Arc::new(arrow::datatypes::Schema::empty()));
        let source = NetCDFSource::new(access_on(ReaderBackend::Oxcdf), None, table_schema);
        let config = FileScanConfigBuilder::new(
            ObjectStoreUrl::local_filesystem(),
            Arc::new(source.clone()) as Arc<dyn FileSource>,
        )
        .with_file(PartitionedFile::new("large.nc", 64 * 1024 * 1024))
        .build();

        let shared = source
            .repartitioned(PARTITIONS, MIN_SHARE_SIZE, None, &config)
            .unwrap()
            .expect("a file over the minimum is shared");

        assert_eq!(shared.file_groups.len(), PARTITIONS);
        for group in &shared.file_groups {
            assert_eq!(group.len(), 1, "every partition holds the file");
            assert!(
                group.iter().next().unwrap().range.is_none(),
                "a shared file is not divided into byte ranges"
            );
        }

        let source = shared
            .file_source()
            .as_any()
            .downcast_ref::<NetCDFSource>()
            .expect("the config carries a NetCDFSource");
        assert!(
            source.shares_file(&object_store::path::Path::from("large.nc")),
            "the source the openers come from must know the file is shared"
        );
    }

    /// An ordered scan is left alone: a partition holding an arbitrary subset of
    /// a file cannot emit its rows in file order.
    #[test]
    fn an_ordered_scan_is_not_shared() {
        use datafusion::datasource::listing::PartitionedFile;
        use datafusion::datasource::table_schema::TableSchema;
        use datafusion::execution::object_store::ObjectStoreUrl;
        use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};

        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("time", arrow::datatypes::DataType::Int64, true),
        ]));
        let table_schema = TableSchema::from_file_schema(schema.clone());
        let source = NetCDFSource::new(access_on(ReaderBackend::Oxcdf), None, table_schema);
        let config = FileScanConfigBuilder::new(
            ObjectStoreUrl::local_filesystem(),
            Arc::new(source.clone()) as Arc<dyn FileSource>,
        )
        .with_file(PartitionedFile::new("large.nc", 64 * 1024 * 1024))
        .build();

        let ordering = LexOrdering::new(vec![PhysicalSortExpr::new_default(
            datafusion::physical_expr::expressions::col("time", &schema).unwrap(),
        )])
        .unwrap();

        assert!(
            source
                .repartitioned(4, MIN_SHARE_SIZE, Some(ordering), &config)
                .unwrap()
                .is_none(),
            "an ordered scan must keep its single group"
        );
    }

    /// netcdf-c never shares a file, whatever its size: every call it makes
    /// queues on one process-global mutex, so the partitions would serialise.
    #[test]
    fn only_the_rust_reader_shares_one_file() {
        use datafusion::datasource::listing::PartitionedFile;
        use datafusion::datasource::table_schema::TableSchema;
        use datafusion::execution::object_store::ObjectStoreUrl;

        let table_schema =
            TableSchema::from_file_schema(Arc::new(arrow::datatypes::Schema::empty()));
        let source = NetCDFSource::new(access_on(ReaderBackend::NetcdfC), None, table_schema);
        let config = FileScanConfigBuilder::new(
            ObjectStoreUrl::local_filesystem(),
            Arc::new(source.clone()) as Arc<dyn FileSource>,
        )
        .with_file(PartitionedFile::new("large.nc", 64 * 1024 * 1024))
        .build();

        assert!(
            source
                .repartitioned(4, MIN_SHARE_SIZE, None, &config)
                .unwrap()
                .is_none(),
            "netcdf-c must not share a file"
        );
    }

    /// A session with `target_partitions` partitions.
    ///
    /// Nothing is done to the split minimum here, so a test that wants a share
    /// has to bring a file large enough to earn one under the session default
    /// ([`MIN_SHARE_SIZE`]).
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

    /// Write a netCDF-4 file larger than [`MIN_SHARE_SIZE`], and return its path.
    ///
    /// Every bundled fixture is hundreds of KB, well under the minimum, so a
    /// test that needs a real split has to make its own file.
    ///
    /// It is written **wide** rather than long, and that is not a free choice.
    /// `oxcdf` cannot read this writer's output once a single variable grows
    /// past roughly 200k values: it fails with "chunk at […] was neither cached
    /// nor fetched" while netcdf-c reads the same bytes. The limit follows one
    /// variable's chunk count, not the file's size, so 20 columns of 100k values
    /// clears the minimum with room to spare and still reads back. Measured:
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

    /// A bare `count(*)` over a shared file counts every row once.
    ///
    /// This is the one read that cannot inherit its division from the decode: it
    /// projects no column, so it drives the read with a column of its own and
    /// never builds an nd array. It has to take that work from the same queue as
    /// everything else. A share that read the file whole would count it once per
    /// partition, and the answer would grow with `target_partitions` — silently,
    /// because nothing about it is an error.
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

    /// A pruned scan counts what the coordinate itself says it should.
    ///
    /// The scan skips chunks whose coordinates cannot meet the predicate, so a
    /// bound that is too tight loses rows and reports a smaller count. Nothing
    /// about that raises an error, and comparing the two readers would not catch
    /// it — they share the pruning. So the expected answer comes from the
    /// coordinate column itself, read with no predicate in the plan and
    /// therefore with no pruning.
    ///
    /// The fixture's `lat` runs from about 38.8 to 48.8, so the thresholds below
    /// prune nothing, some, and everything in turn. The middle one is the one
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

    /// The predicate reaches the file source through the nd pipeline.
    ///
    /// The scan sits under an `NdSourceExec` and an `NdBroadcastExec`, and a
    /// node that does not forward filters leaves the source with nothing to
    /// prune on. That failure is invisible in a result — every row still comes
    /// back, the scan just reads the whole file — so it is asserted on the
    /// scan's own output instead.
    ///
    /// One encoded batch carries one chunk, so the scan's output rows *are* its
    /// chunk count.
    #[tokio::test]
    async fn a_predicate_reaches_the_scan_and_skips_its_chunks() {
        use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanVisitor};

        let ctx = session();
        register(&ctx, "gridded", ReaderBackend::Oxcdf, GRIDDED_FILE).await;

        /// The chunk count of the scan at the bottom of an executed plan.
        struct ScanRows(Option<usize>);
        impl ExecutionPlanVisitor for ScanRows {
            type Error = std::convert::Infallible;
            fn pre_visit(&mut self, plan: &dyn ExecutionPlan) -> Result<bool, Self::Error> {
                if plan.name().contains("DataSourceExec") {
                    self.0 = plan.metrics().and_then(|metrics| metrics.output_rows());
                    return Ok(false);
                }
                Ok(true)
            }
        }

        let chunks_read = async |sql: &str| {
            let plan = ctx
                .sql(sql)
                .await
                .unwrap()
                .create_physical_plan()
                .await
                .unwrap();
            datafusion::physical_plan::collect(plan.clone(), ctx.task_ctx())
                .await
                .unwrap();
            let mut visitor = ScanRows(None);
            datafusion::physical_plan::accept(plan.as_ref(), &mut visitor).unwrap();
            visitor.0.expect("the scan reports its output rows")
        };

        let whole = chunks_read("SELECT lat FROM gridded").await;
        assert!(whole > 1, "the fixture must hold several chunks");

        // `lat` runs from about 38.8 to 48.8, so nothing can satisfy this and
        // every chunk is skipped before it is fetched.
        assert_eq!(
            chunks_read("SELECT lat FROM gridded WHERE lat > 1000").await,
            0,
            "a predicate no row can meet must leave the scan nothing to read"
        );

        // A bound the whole file satisfies prunes nothing.
        assert_eq!(
            chunks_read("SELECT lat FROM gridded WHERE lat > 0").await,
            whole,
            "a predicate every row meets must not skip a chunk"
        );

        // And one that cuts the file reads some of it.
        let partial = chunks_read("SELECT lat FROM gridded WHERE lat > 44").await;
        assert!(
            partial > 0 && partial < whole,
            "lat > 44 should read some of the {whole} chunks, it read {partial}"
        );
    }

    /// The scan reports what each partition read, and what the predicate saved.
    ///
    /// These four counters are the only view of the sharing there is: the scan's
    /// own `output_rows` counts encoded batches, which says nothing about how a
    /// file divided between the partitions or how much of it was skipped. They
    /// went unrecorded once already, so this holds them in place.
    #[tokio::test]
    async fn the_scan_reports_what_it_read_and_what_it_skipped() {
        use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanVisitor};

        let ctx = session();
        register(&ctx, "gridded", ReaderBackend::Oxcdf, GRIDDED_FILE).await;

        /// The named counter, summed over the scan's partitions.
        struct Counter(&'static str, usize);
        impl ExecutionPlanVisitor for Counter {
            type Error = std::convert::Infallible;
            fn pre_visit(&mut self, plan: &dyn ExecutionPlan) -> Result<bool, Self::Error> {
                if plan.name().contains("DataSourceExec") {
                    self.1 = plan
                        .metrics()
                        .map(|metrics| {
                            metrics
                                .iter()
                                .filter(|metric| metric.value().name() == self.0)
                                .map(|metric| metric.value().as_usize())
                                .sum()
                        })
                        .unwrap_or(0);
                    return Ok(false);
                }
                Ok(true)
            }
        }

        let counters = async |sql: &str| {
            let plan = ctx
                .sql(sql)
                .await
                .unwrap()
                .create_physical_plan()
                .await
                .unwrap();
            datafusion::physical_plan::collect(plan.clone(), ctx.task_ctx())
                .await
                .unwrap();
            let read = |name: &'static str| {
                let mut counter = Counter(name, 0);
                datafusion::physical_plan::accept(plan.as_ref(), &mut counter).unwrap();
                counter.1
            };
            (
                read("chunks_read"),
                read("rows_read"),
                read("chunks_pruned"),
                read("rows_pruned"),
            )
        };

        // No predicate: everything is read, nothing is skipped, and the rows are
        // the grid's own — not the one-row-per-chunk the scan emits.
        let (chunks, rows, pruned, pruned_rows) = counters("SELECT lat FROM gridded").await;
        assert!(chunks > 1, "the fixture must hold several chunks");
        assert!(
            rows > chunks,
            "rows_read must count broadcast rows ({rows}), not batches ({chunks})"
        );
        assert_eq!((pruned, pruned_rows), (0, 0), "nothing to skip");

        // A predicate nothing can meet: every chunk is skipped before it is
        // read, and the rows it would have produced are accounted for.
        let (chunks, rows, pruned, pruned_rows) =
            counters("SELECT lat FROM gridded WHERE lat > 1000").await;
        assert_eq!((chunks, rows), (0, 0), "nothing is read");
        assert!(pruned > 0 && pruned_rows > 0, "and the skip is reported");
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
