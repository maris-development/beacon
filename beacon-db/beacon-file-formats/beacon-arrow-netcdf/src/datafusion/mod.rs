use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use beacon_datafusion_ext::format_ext::{DatasetMetadata, FileFormatFactoryExt, SchemaOptions};
use beacon_datafusion_ext::format_options::format_option;
use beacon_datafusion_ext::listing_factory::ListingFactory;
use beacon_datafusion_ext::type_widening::{label_by_object, session_widening};
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

/// Every filename extension that a netCDF file uses, canonical first.
///
/// `cdf` is not here: it is also the NASA Common Data Format, which this reader cannot open.
pub const NETCDF_EXTENSIONS: [&str; 4] = [NETCDF_EXTENSION, "nc3", "nc4", "netcdf"];

pub mod options;
#[cfg(test)]
mod partition_coverage_tests;
pub mod reader;
pub mod sink;
pub mod source;
pub mod statistics;
pub mod table_function;

pub use reader::NetcdfInput;
pub use table_function::ReadNetCDFFunc;

/// Runtime configuration for the NetCDF format.
///
/// Plain data with sensible defaults; the caller populates it (there is no
/// environment parsing here, so the crate stays reusable and the host decides
/// where the values come from). These are the *defaults* for a runtime — some
/// can be overridden per table via `CREATE EXTERNAL TABLE ... OPTIONS (...)`.
#[derive(Debug, Clone)]
pub struct NetcdfConfig {
    /// Whether to generate per-file statistics during planning.
    pub enable_statistics: bool,
}

impl Default for NetcdfConfig {
    fn default() -> Self {
        Self {
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
    pub listing_factory: Arc<ListingFactory>,
    pub options: NetcdfOptions,
    pub config: NetcdfConfig,
}

impl NetCDFFormatFactory {
    pub fn new(
        listing_factory: Arc<ListingFactory>,
        options: NetcdfOptions,
        config: NetcdfConfig,
    ) -> Self {
        Self {
            listing_factory,
            options,
            config,
        }
    }

    /// Whether this table wants file statistics computed at all.
    ///
    /// Only the file analyzer computes them, through
    /// [`FileFormatFactoryExt::create_for_analysis`]. This is the switch that
    /// turns even that off, per table or per runtime.
    fn statistics_wanted(
        &self,
        format_options: &std::collections::HashMap<String, String>,
    ) -> datafusion::error::Result<bool> {
        match format_option(format_options, "enable_statistics") {
            Some(value) => parse_bool_option("enable_statistics", value),
            None => Ok(self.config.enable_statistics),
        }
    }

    /// Build a [`NetcdfFormat`] with the given per-table effective settings.
    fn build_format(&self, options: NetcdfOptions, enable_statistics: bool) -> NetcdfFormat {
        NetcdfFormat::new(self.listing_factory.clone(), options)
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

        if let Some(value) = format_option(format_options, "read_dimensions") {
            options.read_dimensions = Some(
                value
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect(),
            );
        }
        if let Some(value) = format_option(format_options, "skip_unbroadcastable") {
            options.skip_unbroadcastable = parse_bool_option("skip_unbroadcastable", value)?;
        }
        if let Some(value) = format_option(format_options, "skip_invalid_files") {
            options.skip_invalid_files = parse_bool_option("skip_invalid_files", value)?;
        }
        options.writer.apply_format_options(format_options)?;
        // Parsed here only so a bad value is an error at `CREATE EXTERNAL
        // TABLE` rather than at the first analysis pass. The value itself is
        // read by `create_for_analysis`; a format built for a query computes no
        // statistics whatever it says.
        self.statistics_wanted(format_options)?;
        // A stored table can hold this option. It selects nothing.
        if let Some(value) = format_option(format_options, "use_rust_reader") {
            if !parse_bool_option("use_rust_reader", value)? {
                tracing::warn!(
                    "the NetCDF option 'use_rust_reader' has no effect; netcdf-c reads are removed"
                );
            }
        }

        Ok(Arc::new(self.build_format(
            options,
            // A query never computes statistics. See `create_for_analysis`.
            false,
        )))
    }

    fn default(&self) -> Arc<dyn FileFormat> {
        Arc::new(self.build_format(
            self.options.clone(),
            // A query never computes statistics. See `create_for_analysis`.
            false,
        ))
    }

}

impl GetExt for NetCDFFormatFactory {
    fn get_ext(&self) -> String {
        NETCDF_EXTENSION.to_string()
    }
}

impl FileFormatFactoryExt for NetCDFFormatFactory {
    /// The same format, with statistics switched on.
    ///
    /// `infer_stats` opens the file and reads every coordinate array, so only
    /// the file analyzer asks for this. See
    /// [`FileFormatFactoryExt::create_for_analysis`].
    fn create_for_analysis(
        &self,
        state: &dyn Session,
        format_options: &std::collections::HashMap<String, String>,
        _url: &datafusion::datasource::listing::ListingTableUrl,
        _listing: &ListingFactory,
    ) -> datafusion::error::Result<Arc<dyn FileFormat>> {
        let wanted = self.statistics_wanted(format_options)?;
        let format = self.create(state, format_options)?;
        let netcdf = format
            .downcast_ref::<NetcdfFormat>()
            .ok_or_else(|| {
                exec_datafusion_err!("the NetCDF factory did not produce a NetcdfFormat")
            })?
            .clone();
        Ok(Arc::new(netcdf.with_enable_statistics(wanted)))
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

    /// netCDF opts into the schema cache.
    ///
    /// The key holds the name of the reader, so the entries of a different
    /// reader are never an answer for this one.
    ///
    /// `skip_invalid_files` is in the key when it is set, because it turns an
    /// unreadable file into an empty schema. A table without it must still get
    /// the error. Without the option the key is the same as for older entries.
    ///
    /// Nothing else is. The statistics switch only decides whether ranges are
    /// computed. It changes no field and no type.
    ///
    /// TODO(#367): cache a dimension-projected read as well. `read_dimensions`
    /// decides which variables appear, so the same file has one schema per
    /// dimension set, and the key would have to carry the set in order. It is
    /// left out of this first pass to keep the key simple: a `read_netcdf` that
    /// names dimensions derives its schema, exactly as it did before the cache
    /// existed. The default read — every table, and every collector pass — is
    /// what the 9.2s was measured on, and that one is cached.
    fn schema_options_fingerprint(&self, format: &dyn FileFormat) -> Option<u64> {
        let format = format.downcast_ref::<NetcdfFormat>()?;
        if format.options.read_dimensions.is_some() {
            return None;
        }
        let options = SchemaOptions::new(NETCDF_EXTENSION).str("oxcdf");
        let options = if format.options.skip_invalid_files {
            options.str("skip_invalid_files")
        } else {
            options
        };
        Some(options.finish())
    }
}

#[derive(Debug, Clone)]
pub struct NetcdfFormat {
    pub listing_factory: Arc<ListingFactory>,
    pub options: NetcdfOptions,
    /// Whether to generate per-file statistics during planning.
    enable_statistics: bool,
}

impl NetcdfFormat {
    pub fn new(listing_factory: Arc<ListingFactory>, options: NetcdfOptions) -> Self {
        Self {
            listing_factory,
            options,
            enable_statistics: false,
        }
    }

    /// Set whether per-file statistics are generated during planning.
    pub fn with_enable_statistics(mut self, enable_statistics: bool) -> Self {
        self.enable_statistics = enable_statistics;
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

        let inputs: Vec<NetcdfInput> = objects
            .iter()
            .map(|object| NetcdfInput::new(store, object))
            .collect();

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
        // An index, not a borrowed `ObjectMeta`: `async_trait` rejects a closure
        // argument that holds a reference.
        let schemas: Vec<SchemaRef> = futures::stream::iter(inputs.into_iter().enumerate())
            .map(|(index, input)| async move {
                // A glob can match many files, so the error names the bad one.
                reader::fetch_schema(
                    input,
                    self.options.read_dimensions.clone(),
                    self.options.skip_unbroadcastable,
                    self.options.skip_invalid_files,
                )
                .await
                .map_err(|e| {
                    exec_datafusion_err!(
                        "Failed to infer the schema of NetCDF file '{}': {}",
                        objects[index].location,
                        e.message()
                    )
                })
            })
            .buffered(width)
            .try_collect()
            .await?;
        if schemas.is_empty() {
            // Return a default empty schema
            return Ok(Arc::new(arrow::datatypes::Schema::empty()));
        }
        // The rule of the session decides the result for a column that two
        // files describe differently. Each schema names its file, so a refused
        // column names both files.
        session_widening(state)
            .merge_schemas(&label_by_object(objects, &schemas))
            .map_err(|e| {
                exec_datafusion_err!(
                    "Failed to merge the schemas of the NetCDF datasets: {}",
                    e
                )
            })
    }

    async fn infer_stats(
        &self,
        _state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        object: &ObjectMeta,
    ) -> datafusion::error::Result<Statistics> {
        if !self.enable_statistics {
            return Ok(Statistics::new_unknown(&table_schema));
        }
        tracing::trace!(
            object = %object.location,
            "generating statistics for NetCDF file"
        );

        let input = NetcdfInput::new(store, object);
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
        //
        // The `PARTITIONED BY` columns are encoded with it. Their values come
        // from a file's path rather than its contents, and the reader appends
        // them per file, but they reach the plan the same way every other column
        // does — so that one decoder reads the whole batch.
        let encoded_file_schema = Arc::new(beacon_datafusion_ext::nd::encoded_schema(
            conf.file_schema(),
        ));
        let table_schema = datafusion::datasource::table_schema::TableSchema::new(
            encoded_file_schema,
            beacon_nd_array::arrow::partition::encoded_partition_cols(
                conf.table_partition_cols(),
            ),
        );
        // Preserve a projection that the scan pushed down into the incoming
        // source — rebuilding the source below would otherwise drop it.
        let projection = conf.file_source().projection().cloned();
        let source = NetCDFSource::new(self.options.read_dimensions.clone(), table_schema)
            .with_skip_unbroadcastable(self.options.skip_unbroadcastable)
            .with_skip_invalid_files(self.options.skip_invalid_files)
            .with_projection(projection);
        let conf = FileScanConfigBuilder::from(conf)
            .with_source(Arc::new(source))
            .build();

        let data_source: Arc<dyn ExecutionPlan> = DataSourceExec::from_data_source(conf);
        let nd_source = Arc::new(beacon_nd_execution::exec::NdSourceExec::try_new(
            data_source,
        )?);
        let broadcast = beacon_nd_execution::exec::NdBroadcastExec::try_new(nd_source)?;
        Ok(Arc::new(broadcast))
    }

    async fn create_writer_physical_plan(
        &self,
        input: Arc<dyn ExecutionPlan>,
        _state: &dyn Session,
        conf: FileSinkConfig,
        order_requirements: Option<LexRequirement>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        // The sinks write a local scratch file and upload it to the store of `conf`.
        match &self.options.write_dimensions {
            Some(dim_columns) if !dim_columns.is_empty() => {
                let flat_keys = self.options.writer.set_keys();
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
                )?);

                Ok(Arc::new(DataSinkExec::new(
                    Arc::new(sort_preserving_merge_exec),
                    netcdf_sink,
                    order_requirements,
                )))
            }
            _ => {
                let netcdf_sink = Arc::new(NetCDFSink::new(conf, self.options.writer.clone()));
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
            NetCDFSource::new(self.options.read_dimensions.clone(), table_schema)
                .with_skip_unbroadcastable(self.options.skip_unbroadcastable)
                .with_skip_invalid_files(self.options.skip_invalid_files),
        )
    }
}

/// The format, its options and its scans.
#[cfg(test)]
mod format_tests {
    use std::collections::HashMap;
    use std::path::PathBuf;

    use beacon_datafusion_ext::fast_object::FastObjectTable;
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
            NetcdfOptions::default(),
            NetcdfConfig::default(),
        )
    }

    /// A format ready to read the bundled test files.
    fn format_on() -> NetcdfFormat {
        NetcdfFormat::new(
            Arc::new(ListingFactory::dynamic()),
            NetcdfOptions::default(),
        )
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

    /// Register one bundled test file as a table.
    async fn register(ctx: &SessionContext, table: &str, file: &str) {
        register_path(ctx, table, &test_file(file)).await
    }

    /// The same, for a file that is not bundled — one a test wrote itself.
    async fn register_path(ctx: &SessionContext, table: &str, path: &std::path::Path) {
        let url = ListingTableUrl::parse(path.to_string_lossy()).unwrap();
        let listing = FastObjectTable::try_new(&ctx.state(), Arc::new(format_on()), vec![url])
            .await
            .unwrap_or_else(|e| panic!("register {}: {e}", path.display()));
        ctx.register_table(table, Arc::new(listing)).unwrap();
    }

    /// The reader opens a file by its content, so an alias extension reads the same rows.
    #[tokio::test]
    async fn a_file_with_an_alias_extension_reads_the_same_rows() {
        let dir = tempfile::tempdir().unwrap();
        let alias = dir.path().join("wod.nc4");
        std::fs::copy(test_file(WOD_FILE), &alias).unwrap();

        let ctx = session();
        register(&ctx, "canonical", WOD_FILE).await;
        register_path(&ctx, "alias", &alias).await;

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

    // ── statistics ─────────────────────────────────────────────────────

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

    /// A format with statistics on computes real ranges.
    #[tokio::test]
    async fn a_format_with_statistics_on_computes_ranges() {
        let ctx = session();
        let state = ctx.state();
        let (store, object) = local_object(WOD_FILE);

        let oxcdf = format_on().with_enable_statistics(true);
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
    }

    /// A format with statistics off computes nothing.
    #[tokio::test]
    async fn a_format_with_statistics_off_computes_nothing() {
        let ctx = session();
        let state = ctx.state();
        let (store, object) = local_object(WOD_FILE);

        let oxcdf = format_on().with_enable_statistics(true);
        let schema = oxcdf
            .infer_schema(&state, &store, std::slice::from_ref(&object))
            .await
            .unwrap();

        let off = format_on().with_enable_statistics(false);
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

    /// One bad file in a glob must be named, so the user can find it.
    #[tokio::test]
    async fn infer_schema_names_the_invalid_file() {
        // `put` and `head` live on this extension trait.
        use object_store::ObjectStoreExt;

        let ctx = session();
        let store = Arc::new(object_store::memory::InMemory::new());
        let location = object_store::path::Path::from("argo/broken.nc");
        store
            .put(&location, b"not a netcdf file".to_vec().into())
            .await
            .expect("the in-memory store accepts the object");
        let object = store
            .head(&location)
            .await
            .expect("the object was just written");
        let object_store: Arc<dyn ObjectStore> = store;

        let error = format_on()
            .infer_schema(&ctx.state(), &object_store, &[object])
            .await
            .expect_err("random bytes are not a NetCDF file");

        let message = error.to_string();
        assert!(
            message.contains("'argo/broken.nc'"),
            "the error must name the file: {message}"
        );
        assert_eq!(
            message.matches("Execution error").count(),
            1,
            "the error must not repeat its prefix: {message}"
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
            let format = Arc::new(format_on().with_enable_statistics(true));

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

    // ── The removed reader option ──────────────────────────────────────

    /// A stored table can still hold the option of the removed reader.
    #[tokio::test]
    async fn the_removed_reader_option_does_not_change_the_format() {
        let ctx = session();
        let state = ctx.state();
        let factory = factory();

        for value in ["false", "on"] {
            let options = HashMap::from([("use_rust_reader".to_string(), value.to_string())]);
            let format = factory.create(&state, &options).unwrap();
            assert!(format.downcast_ref::<NetcdfFormat>().is_some());
        }

        let options = HashMap::from([("use_rust_reader".to_string(), "maybe".to_string())]);
        let error = factory.create(&state, &options).unwrap_err().to_string();
        assert!(error.contains("use_rust_reader"), "{error}");
    }

    /// The key is the one that earlier releases wrote for this reader.
    #[test]
    fn the_schema_cache_key_does_not_change() {
        let factory = factory();
        let format = FileFormatFactory::default(&factory);
        let expected = SchemaOptions::new(NETCDF_EXTENSION).str("oxcdf").finish();
        assert_eq!(
            factory.schema_options_fingerprint(format.as_ref()),
            Some(expected)
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
            NetcdfOptions {
                write_dimensions: Some(vec!["lat".to_string()]),
                writer: crate::datafusion::options::NetcdfWriterOptions {
                    combine_attrs: Some(crate::attributes::CombineAttrs::Identical),
                    ..Default::default()
                },
                ..NetcdfOptions::default()
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

    // ── Partitioning ───────────────────────────────────────────────────

    /// A scan of many files runs one file for each partition: `ListingTable`
    /// splits the file groups by `target_partitions` before
    /// `FileSource::repartitioned` is ever consulted.
    #[tokio::test]
    async fn a_multi_file_scan_gets_one_partition_for_each_file() {
        use datafusion::physical_plan::ExecutionPlanProperties;

        let files = 4;
        let dir = tempfile::tempdir().unwrap();
        for i in 0..files {
            std::fs::copy(test_file(GRIDDED_FILE), dir.path().join(format!("f{i}.nc"))).unwrap();
        }

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
        let table = FastObjectTable::try_new(&ctx.state(), Arc::new(format_on()), vec![url])
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
            "the scan should read {files} files in {files} partitions"
        );
    }

    /// The split minimum a scan takes from its session
    /// (`repartition_file_min_size`), and what these tests pass by hand.
    const MIN_SHARE_SIZE: usize = 10 * 1024 * 1024;

    /// A scan of any size plans one entry per partition, and one queue.
    ///
    /// Size decides nothing here. It used to: a file under the minimum was left
    /// whole, and one over it went into every partition's group with a share.
    /// Both were guesses made at plan time, and both were measured losing to no
    /// guess at all. A queue does not guess — whoever is free takes the next
    /// file, and a partition with nothing left helps divide an open one.
    #[test]
    fn a_scan_plans_one_entry_per_partition_whatever_its_files() {
        use datafusion::datasource::listing::PartitionedFile;
        use datafusion::datasource::table_schema::TableSchema;
        use datafusion::execution::object_store::ObjectStoreUrl;

        const PARTITIONS: usize = 4;

        // One file well under the old minimum, one well over, and a collection.
        let scans: Vec<(&str, Vec<PartitionedFile>)> = vec![
            (
                "one small file",
                vec![PartitionedFile::new("small.nc", 1024 * 1024)],
            ),
            (
                "one large file",
                vec![PartitionedFile::new("large.nc", 64 * 1024 * 1024)],
            ),
            (
                "many files",
                (0..500)
                    .map(|i| PartitionedFile::new(format!("f-{i}.nc"), 1024 * 1024))
                    .collect(),
            ),
        ];

        for (what, files) in scans {
            let table_schema =
                TableSchema::from_file_schema(Arc::new(arrow::datatypes::Schema::empty()));
            let source = NetCDFSource::new(None, table_schema);
            let count = files.len();
            let mut builder = FileScanConfigBuilder::new(
                ObjectStoreUrl::local_filesystem(),
                Arc::new(source.clone()) as Arc<dyn FileSource>,
            );
            for file in files {
                builder = builder.with_file(file);
            }
            let config = builder.build();

            let planned = source
                .repartitioned(PARTITIONS, MIN_SHARE_SIZE, None, &config)
                .unwrap()
                .unwrap_or_else(|| panic!("{what}: a scan is planned morsel-driven"));

            assert_eq!(planned.file_groups.len(), PARTITIONS, "{what}");
            for group in &planned.file_groups {
                assert_eq!(
                    group.len(),
                    1,
                    "{what}: one standing entry, not a file list"
                );
            }

            let planned_source = planned
                .file_source()
                .downcast_ref::<NetCDFSource>()
                .expect("the config carries a NetCDFSource");
            assert_eq!(
                planned_source.morsel_files(),
                Some(count),
                "{what}: every file is in the queue the openers draw from"
            );
        }
    }

    /// A hive-partitioned table over copies of `GRIDDED_FILE`: one file per
    /// `year=` directory, and `year` as a `PARTITIONED BY` column.
    async fn register_partitioned(
        ctx: &SessionContext,
        table: &str,
        dir: &std::path::Path,
        years: &[&str],
    ) {
        use datafusion::datasource::listing::{ListingOptions, ListingTable, ListingTableConfig};

        for year in years {
            let part = dir.join(format!("year={year}"));
            std::fs::create_dir_all(&part).unwrap();
            std::fs::copy(test_file(GRIDDED_FILE), part.join("obs.nc")).unwrap();
        }

        let options = ListingOptions::new(Arc::new(format_on()))
            // The format finds its own files, so a suffix here would have to
            // match it. This is what `CREATE EXTERNAL TABLE` passes too.
            .with_file_extension("")
            .with_collect_stat(false)
            .with_table_partition_cols(vec![(
                "year".to_string(),
                arrow::datatypes::DataType::Utf8,
            )]);

        let url = ListingTableUrl::parse(dir.to_string_lossy()).unwrap();
        let config = ListingTableConfig::new(url)
            .with_listing_options(options)
            .infer_schema(&ctx.state())
            .await
            .unwrap_or_else(|e| panic!("the partitioned collection infers a schema: {e}"));

        ctx.register_table(table, Arc::new(ListingTable::try_new(config).unwrap()))
            .unwrap();
    }

    /// A table on the Rust reader over `path`, built from `options`.
    async fn table_with_options(
        ctx: &SessionContext,
        options: NetcdfOptions,
        path: &std::path::Path,
    ) -> datafusion::error::Result<Arc<FastObjectTable>> {
        let url = ListingTableUrl::parse(path.to_string_lossy()).unwrap();
        let format = NetcdfFormat::new(Arc::new(ListingFactory::dynamic()), options);
        FastObjectTable::try_new(&ctx.state(), Arc::new(format), vec![url])
            .await
            .map(Arc::new)
    }

    /// `WOD_FILE` is ragged, so no dimension list fits it. Without
    /// `skip_unbroadcastable` it fails the table. With it, the file is skipped
    /// and the gridded file next to it is read whole.
    #[tokio::test]
    async fn the_skip_option_reads_past_a_file_the_dimension_list_does_not_fit() {
        let dir = tempfile::tempdir().unwrap();
        for file in [GRIDDED_FILE, WOD_FILE] {
            std::fs::copy(test_file(file), dir.path().join(file)).unwrap();
        }
        let dims = Some(vec!["time".to_string(), "lat".to_string(), "lon".to_string()]);
        let ctx = session();

        let strict = NetcdfOptions {
            read_dimensions: dims.clone(),
            ..NetcdfOptions::default()
        };
        let error = table_with_options(&ctx, strict, dir.path())
            .await
            .expect_err("the ragged file fits no list")
            .to_string();
        assert!(error.contains("ragged"), "{error}");

        let lenient = NetcdfOptions {
            read_dimensions: dims,
            skip_unbroadcastable: true,
            ..NetcdfOptions::default()
        };
        let table = table_with_options(&ctx, lenient, dir.path()).await.unwrap();
        ctx.register_table("mixed", table).unwrap();
        let batches = ctx
            .sql("SELECT lat FROM mixed")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let read: usize = batches.iter().map(|batch| batch.num_rows()).sum();

        assert_eq!(read, rows_in_one_file(&ctx, "lat").await, "the gridded file alone");
    }

    /// A file the reader cannot open fails the table and names the file.
    /// With `skip_invalid_files`, the scan reads past it.
    #[tokio::test]
    async fn the_skip_option_reads_past_a_file_the_reader_cannot_open() {
        // Arrange
        let dir = tempfile::tempdir().unwrap();
        std::fs::copy(test_file(GRIDDED_FILE), dir.path().join(GRIDDED_FILE)).unwrap();
        std::fs::write(dir.path().join("broken.nc"), b"not a netcdf file").unwrap();
        let ctx = session();

        // Act
        let error = table_with_options(&ctx, NetcdfOptions::default(), dir.path())
            .await
            .expect_err("random bytes are not a NetCDF file")
            .to_string();
        let lenient = NetcdfOptions {
            skip_invalid_files: true,
            ..NetcdfOptions::default()
        };
        let table = table_with_options(&ctx, lenient, dir.path()).await.unwrap();
        ctx.register_table("mixed", table).unwrap();
        let batches = ctx
            .sql("SELECT lat FROM mixed")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let read: usize = batches.iter().map(|batch| batch.num_rows()).sum();

        // Assert
        assert!(error.contains("broken.nc"), "the error must name the file: {error}");
        assert_eq!(read, rows_in_one_file(&ctx, "lat").await, "the gridded file alone");
    }

    /// The `OPTIONS` of `CREATE EXTERNAL TABLE` reach the format.
    #[tokio::test]
    async fn the_factory_reads_the_skip_options() {
        let factory = factory();
        let ctx = SessionContext::new();
        let options = HashMap::from([
            ("skip_unbroadcastable".to_string(), "true".to_string()),
            ("skip_invalid_files".to_string(), "true".to_string()),
        ]);

        let format = factory.create(&ctx.state(), &options).unwrap();
        let format = format.downcast_ref::<NetcdfFormat>().unwrap();
        assert!(format.options.skip_unbroadcastable);
        assert!(format.options.skip_invalid_files);

        let default = factory.create(&ctx.state(), &HashMap::new()).unwrap();
        let default = default.downcast_ref::<NetcdfFormat>().unwrap();
        assert!(!default.options.skip_unbroadcastable);
        assert!(!default.options.skip_invalid_files);
    }

    /// A skipped file caches an empty schema, so the option must change the key.
    /// Without the option the key stays the same as for older cache entries.
    #[test]
    fn skip_invalid_files_changes_the_schema_cache_key() {
        let factory = factory();
        let format = |skip_invalid_files| {
            NetcdfFormat::new(
                Arc::new(ListingFactory::dynamic()),
                NetcdfOptions {
                    skip_invalid_files,
                    ..NetcdfOptions::default()
                },
            )
        };
        let expected_default = SchemaOptions::new(NETCDF_EXTENSION).str("oxcdf").finish();

        let strict = factory.schema_options_fingerprint(&format(false));
        let lenient = factory.schema_options_fingerprint(&format(true));

        assert_eq!(strict, Some(expected_default));
        assert!(lenient.is_some());
        assert_ne!(strict, lenient);
    }

    /// The rows one copy of `GRIDDED_FILE` contributes to `column`.
    async fn rows_in_one_file(ctx: &SessionContext, column: &str) -> usize {
        register(ctx, "one", GRIDDED_FILE).await;
        let batches = ctx
            .sql(&format!("SELECT {column} FROM one"))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        batches.iter().map(|batch| batch.num_rows()).sum()
    }

    /// A partitioned table reads, and every row holds the value of its own
    /// file's path.
    ///
    /// The value is in the *path*, and an nd scan reads a whole collection
    /// behind one plan entry, so `FileStream` cannot append it: it does not know
    /// which file a batch came from. The reader appends it itself instead —
    /// per file, which is per morsel.
    #[tokio::test]
    async fn a_partitioned_table_gives_every_row_the_value_of_its_path() {
        let dir = tempfile::tempdir().unwrap();
        let ctx = session();
        register_partitioned(&ctx, "obs", dir.path(), &["2023", "2024"]).await;
        let per_file = rows_in_one_file(&ctx, "analysed_sst").await;

        let rows = ctx
            .sql("SELECT year FROM obs WHERE analysed_sst IS NOT NULL OR analysed_sst IS NULL")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let mut years: Vec<String> = Vec::new();
        for batch in &rows {
            let column = batch
                .column_by_name("year")
                .expect("the partition column is in the scan");
            let values = arrow::array::AsArray::as_string::<i32>(column);
            years.extend((0..batch.num_rows()).map(|row| values.value(row).to_string()));
        }

        assert_eq!(
            years.len(),
            2 * per_file,
            "both files are read, whole: {} rows each",
            per_file
        );
        assert_eq!(
            years.iter().filter(|year| *year == "2023").count(),
            per_file,
            "and one file's worth of rows carries 2023"
        );
        assert_eq!(
            years.iter().filter(|year| *year == "2024").count(),
            per_file,
            "and the other's carries 2024"
        );
    }

    /// A scan of nothing but the partition column still counts the file's rows.
    ///
    /// It reads no column of the file, so there is no grid for the value to
    /// spread over. The read is driven by a column of its own and the value is
    /// stated over the rows it produces — see `FilePartitions::row_columns`.
    #[tokio::test]
    async fn a_group_by_on_the_partition_column_counts_every_row() {
        let dir = tempfile::tempdir().unwrap();
        let ctx = session();
        register_partitioned(&ctx, "obs", dir.path(), &["2023", "2024"]).await;
        let per_file = rows_in_one_file(&ctx, "analysed_sst").await;

        let counted = ctx
            .sql("SELECT year, count(*) AS n FROM obs GROUP BY year ORDER BY year")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let batch = &counted[0];
        assert_eq!(batch.num_rows(), 2, "one row per partition directory");
        let years = arrow::array::AsArray::as_string::<i32>(batch.column(0));
        let counts = arrow::array::AsArray::as_primitive::<arrow::datatypes::Int64Type>(
            batch.column(1),
        );
        assert_eq!((years.value(0), years.value(1)), ("2023", "2024"));
        assert_eq!(
            (counts.value(0), counts.value(1)),
            (per_file as i64, per_file as i64),
            "each directory counts one file's rows"
        );
    }

    /// A filter on the partition column leaves the other directory unlisted.
    ///
    /// Pruning happens where it does for every other format — in the listing,
    /// before a scan exists — so the plan holds only the files that can match.
    #[tokio::test]
    async fn a_filter_on_the_partition_column_prunes_files() {
        use datafusion::physical_plan::displayable;

        let dir = tempfile::tempdir().unwrap();
        let ctx = session();
        register_partitioned(&ctx, "obs", dir.path(), &["2023", "2024"]).await;

        let plan = ctx
            .sql("SELECT analysed_sst FROM obs WHERE year = '2024'")
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();

        let shown = displayable(plan.as_ref()).indent(false).to_string();
        assert!(
            shown.contains("year=2024"),
            "the matching directory is scanned: {shown}"
        );
        assert!(
            !shown.contains("year=2023"),
            "and the other one is not listed at all: {shown}"
        );
    }

    /// The same values come back when the scan divides over many partitions.
    ///
    /// That is the morsel path: every partition draws from one queue, and the
    /// entry it is handed stands for the scan rather than for a file. The values
    /// travel on the queued files instead.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_parallel_partitioned_scan_keeps_the_values_apart() {
        let dir = tempfile::tempdir().unwrap();
        let state = SessionStateBuilder::new()
            .with_config(
                SessionConfig::new()
                    .with_target_partitions(4)
                    .with_extension(
                        beacon_datafusion_ext::type_widening::ArrowTypeWidening::default_extension(
                        ),
                    ),
            )
            .with_default_features()
            .build();
        let ctx = SessionContext::new_with_state(state);
        register_partitioned(&ctx, "obs", dir.path(), &["2023", "2024"]).await;

        let counted = ctx
            .sql("SELECT year, count(*) AS n FROM obs GROUP BY year ORDER BY year")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let batch = &counted[0];
        let years = arrow::array::AsArray::as_string::<i32>(batch.column(0));
        let counts = arrow::array::AsArray::as_primitive::<arrow::datatypes::Int64Type>(
            batch.column(1),
        );
        assert_eq!(batch.num_rows(), 2);
        assert_eq!((years.value(0), years.value(1)), ("2023", "2024"));
        assert_eq!(
            counts.value(0),
            counts.value(1),
            "the two copies of one file hold the same number of rows"
        );
        assert!(counts.value(0) > 0, "and the scan read them");
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
        let source = NetCDFSource::new(None, table_schema);
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
        register_path(&whole, "many", &dir).await;

        let split = splitting_session(8, 1024);
        register_path(&split, "many", &dir).await;

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
        register_path(&ctx, "mixed", dir.path()).await;

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

    /// The writer gives a string variable no `_FillValue`, so a null string reads back as "".
    #[tokio::test]
    async fn a_null_string_reads_back_as_an_empty_string() {
        use arrow::array::{AsArray, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;

        use crate::encoders::default::DefaultEncoder;
        use crate::writer::ArrowRecordBatchWriter;

        let dir = tempfile::tempdir().expect("a temp directory");
        let path = dir.path().join("names.nc");
        let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, true)]));
        let names = StringArray::from(vec![Some("a"), None]);
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(names)]).unwrap();
        let mut writer =
            ArrowRecordBatchWriter::<DefaultEncoder>::new(&path, schema).expect("a netCDF writer");
        writer.write_record_batch(batch).expect("write the batch");
        writer.finish().expect("finish the file");

        let ctx = session();
        register_path(&ctx, "names", &path).await;
        let batches = ctx
            .sql(r#"SELECT "name" FROM names"#)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let mut read = Vec::new();
        for batch in &batches {
            let column = arrow::compute::cast(batch.column(0), &DataType::Utf8).unwrap();
            read.extend(column.as_string::<i32>().iter().map(|v| v.map(str::to_string)));
        }
        assert_eq!(read, [Some("a".to_string()), Some(String::new())]);
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
        register_path(&whole, "one", &file).await;

        let split = splitting_session(4, 8192);
        register_path(&split, "one", &file).await;

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
        register_path(&whole, "one", &file).await;

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
            register_path(&split, "one", &file).await;

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
        register(&ctx, "gridded", GRIDDED_FILE).await;

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
        register(&ctx, "gridded", GRIDDED_FILE).await;

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
        register(&ctx, "gridded", GRIDDED_FILE).await;

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

    /// A pushed-down predicate keeps the rows that match. The query reads only
    /// `lat`, so the grid is the `lat` axis: 458 of its 1208 values are above 45.
    #[tokio::test]
    async fn a_pushed_down_predicate_keeps_the_matching_rows() {
        let ctx = session();
        register(&ctx, "gridded", GRIDDED_FILE).await;

        let batches = ctx
            .sql("SELECT count(*) FROM gridded WHERE lat > 45")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let count = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(count, 458);
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
            register(&ctx, "gridded", GRIDDED_FILE).await;

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
                "batch size {batch_size} gave {} batch(es)",
                batches.len()
            );
            // The chunk fills from the last axis, so it holds at most
            // `batch_size` elements. Here one row of `lon` is 1920 ≤ 4096.
            assert!(
                largest <= batch_size,
                "batch size {batch_size} gave a batch of {largest} rows"
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
            NetcdfOptions::default(),
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
            "a.nc", "b.nc3", "c.nc4", "d.netcdf", "e.cdf", "f.NC4", "g.h5", "h.ncml", "i.parquet",
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
