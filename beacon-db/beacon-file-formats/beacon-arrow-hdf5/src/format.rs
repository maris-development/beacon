//! The HDF5 `FileFormat` and its factory.
//!
//! [`Hdf5FormatFactory`] owns the HDF5 identity. It builds an [`Hdf5Format`],
//! and keeps the netCDF format for writes.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use beacon_arrow_netcdf::datafusion::{statistics, NetCDFFormatFactory, NetcdfFormat};
use beacon_datafusion_ext::format_ext::{DatasetMetadata, FileFormatFactoryExt, SchemaOptions};
use beacon_datafusion_ext::format_options::format_option;
use beacon_datafusion_ext::listing_factory::ListingFactory;
use beacon_datafusion_ext::type_widening::{label_by_object, session_widening};
use datafusion::{
    catalog::{memory::DataSourceExec, Session},
    common::{exec_datafusion_err, GetExt, Statistics},
    datasource::{
        file_format::{file_compression_type::FileCompressionType, FileFormat, FileFormatFactory},
        listing::ListingTableUrl,
        physical_plan::{FileScanConfig, FileScanConfigBuilder, FileSinkConfig, FileSource},
        table_schema::TableSchema,
    },
    physical_expr::LexRequirement,
    physical_plan::ExecutionPlan,
};
use object_store::{ObjectMeta, ObjectStore};

use crate::{
    source::Hdf5Source, Hdf5Config, Hdf5Convention, ReadOptions, HDF5_EXTENSIONS, HDF5_FORMAT_NAME,
};

/// Parse a boolean value supplied through a `CREATE EXTERNAL TABLE` option.
fn parse_bool_option(key: &str, value: &str) -> datafusion::error::Result<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" | "on" => Ok(true),
        "false" | "0" | "no" | "off" => Ok(false),
        other => Err(exec_datafusion_err!(
            "invalid boolean for HDF5 option '{key}': '{other}'"
        )),
    }
}

/// The per-table settings a `CREATE EXTERNAL TABLE ... OPTIONS (...)` clause
/// can override, resolved against the runtime config.
struct EffectiveOptions {
    enable_statistics: bool,
    read_dimensions: Option<Vec<String>>,
    /// Skip a file that does not fit `read_dimensions`, instead of failing.
    skip_unbroadcastable: bool,
    read: ReadOptions,
}

/// A `FileFormat` factory for HDF5 files.
///
/// This factory supplies the HDF5 identity: the `STORED AS` name / `get_ext` it
/// registers under and the `.h5`/`.hdf5` files it recognizes during discovery.
#[derive(Debug, Clone)]
pub struct Hdf5FormatFactory {
    /// The netCDF factory: every write path.
    inner: NetCDFFormatFactory,
    /// The runtime settings.
    config: Hdf5Config,
    /// The `get_ext` this instance registers under. DataFusion's native format
    /// registry keys a factory only by its single `get_ext`, so `h5` and `hdf5`
    /// need one instance each (built with [`Self::with_ext`]); beacon's own
    /// registry keys by [`Self::file_extensions`] and needs only one.
    ext: String,
}

impl Hdf5FormatFactory {
    /// Wrap a netCDF factory, registering under the canonical `hdf5` name.
    pub fn wrapping(inner: NetCDFFormatFactory) -> Self {
        Self::new(inner, Hdf5Config::default())
    }

    /// Wrap a netCDF factory with explicit HDF5 settings.
    pub fn new(inner: NetCDFFormatFactory, config: Hdf5Config) -> Self {
        Self {
            inner,
            config,
            ext: HDF5_FORMAT_NAME.to_string(),
        }
    }

    /// The same factory registered under a different `get_ext` (e.g. `h5`).
    pub fn with_ext(mut self, ext: impl Into<String>) -> Self {
        self.ext = ext.into();
        self
    }

    /// The runtime settings this factory builds formats from.
    pub fn config(&self) -> &Hdf5Config {
        &self.config
    }

    /// Resolve the per-table options against the runtime config.
    fn effective_options(
        &self,
        format_options: &HashMap<String, String>,
    ) -> datafusion::error::Result<EffectiveOptions> {
        // A stored table can hold this option. It selects nothing.
        if let Some(value) = format_option(format_options, "use_rust_reader") {
            if !parse_bool_option("use_rust_reader", value)? {
                tracing::warn!(
                    "the HDF5 option 'use_rust_reader' has no effect; netcdf-c reads are removed"
                );
            }
        }
        let mut options = EffectiveOptions {
            enable_statistics: self.config.enable_statistics,
            read_dimensions: None,
            skip_unbroadcastable: false,
            read: ReadOptions {
                unify_phony_dimensions: self.config.unify_phony_dimensions,
                convention: self.config.convention,
            },
        };

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
        if let Some(value) = format_option(format_options, "enable_statistics") {
            options.enable_statistics = parse_bool_option("enable_statistics", value)?;
        }
        if let Some(value) = format_option(format_options, "unify_phony_dimensions") {
            options.read.unify_phony_dimensions =
                parse_bool_option("unify_phony_dimensions", value)?;
        }
        if let Some(value) = format_option(format_options, "convention") {
            options.read.convention = Hdf5Convention::parse(value).map_err(|value| {
                exec_datafusion_err!(
                    "unknown HDF5 convention '{value}'; the conventions are 'none' and 'optodas'"
                )
            })?;
        }

        Ok(options)
    }

    /// Build the Rust-reader format, with the netCDF format kept for writes.
    fn build_format(&self, options: EffectiveOptions, writer: Arc<dyn FileFormat>) -> Hdf5Format {
        Hdf5Format {
            ext: self.ext.clone(),
            read_dimensions: options.read_dimensions,
            skip_unbroadcastable: options.skip_unbroadcastable,
            read: options.read,
            // Carried from the effective options. Every caller but
            // `create_for_analysis` clears it first, so a query computes
            // nothing.
            enable_statistics: options.enable_statistics,
            writer,
        }
    }
}

impl GetExt for Hdf5FormatFactory {
    fn get_ext(&self) -> String {
        self.ext.clone()
    }
}

impl FileFormatFactory for Hdf5FormatFactory {
    fn create(
        &self,
        state: &dyn Session,
        format_options: &HashMap<String, String>,
    ) -> datafusion::error::Result<Arc<dyn FileFormat>> {
        let mut options = self.effective_options(format_options)?;
        // A query never computes statistics. See `create_for_analysis`.
        options.enable_statistics = false;
        let writer = self.inner.create(state, format_options)?;
        Ok(Arc::new(self.build_format(options, writer)))
    }

    fn default(&self) -> Arc<dyn FileFormat> {
        let options = EffectiveOptions {
            // A query never computes statistics. See `create_for_analysis`.
            enable_statistics: false,
            read_dimensions: None,
            skip_unbroadcastable: false,
            read: ReadOptions {
                unify_phony_dimensions: self.config.unify_phony_dimensions,
                convention: self.config.convention,
            },
        };
        Arc::new(self.build_format(options, self.inner.default()))
    }

}

impl FileFormatFactoryExt for Hdf5FormatFactory {
    /// The same format, with statistics switched on.
    ///
    /// `infer_stats` opens the file and reads every coordinate array, so only
    /// the file analyzer asks for this. See
    /// [`FileFormatFactoryExt::create_for_analysis`].
    fn create_for_analysis(
        &self,
        state: &dyn Session,
        format_options: &HashMap<String, String>,
        _url: &ListingTableUrl,
        _listing: &ListingFactory,
    ) -> datafusion::error::Result<Arc<dyn FileFormat>> {
        let options = self.effective_options(format_options)?;
        let writer = self.inner.create(state, format_options)?;
        Ok(Arc::new(self.build_format(options, writer)))
    }

    fn file_extensions(&self) -> Vec<String> {
        HDF5_EXTENSIONS.iter().map(|ext| ext.to_string()).collect()
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
                    .map(|ext| HDF5_EXTENSIONS.contains(&ext))
                    .unwrap_or(false)
            })
            .map(|obj| DatasetMetadata::new(obj.location.to_string(), self.get_ext()))
            .collect();
        Ok(datasets)
    }

    fn file_format_name(&self) -> String {
        self.get_ext()
    }

    /// HDF5 opts into the schema cache.
    ///
    /// TODO(#367): cache a dimension-projected read as well. `read_dimensions`
    /// decides which arrays appear, so the same file has one schema per
    /// dimension set, and the key would have to carry the set in order. It is
    /// left out of this first pass to keep the key simple: a `read_hdf5` that
    /// names dimensions derives its schema, exactly as it did before the cache
    /// existed. The default read is cached.
    fn schema_options_fingerprint(&self, format: &dyn FileFormat) -> Option<u64> {
        let hdf5 = format.downcast_ref::<Hdf5Format>()?;
        if hdf5.read_dimensions().is_some() {
            return None;
        }
        Some(SchemaOptions::new("hdf5").str("rust").finish())
    }
}

/// The HDF5 [`FileFormat`] on the pure-Rust reader.
///
/// Reads go through [`crate::reader`]: no process-global lock, and byte ranges
/// straight out of the scan's object store. Writes go to `writer`, which is a
/// [`NetcdfFormat`]. The HDF5 reader does not write, so this crate writes
/// with the NetCDF writer.
#[derive(Debug, Clone)]
pub struct Hdf5Format {
    /// The extension this format answers to (`hdf5` or `h5`).
    ext: String,
    /// Columns to treat as dimensions when reading, or `None` to auto-select.
    read_dimensions: Option<Vec<String>>,
    /// Skip a file that does not fit `read_dimensions`, with a warning,
    /// instead of failing the query.
    skip_unbroadcastable: bool,
    /// How this format reads one file: the naming of the invented dimensions,
    /// and the layout convention. See [`crate::ReadOptions`].
    read: ReadOptions,
    /// Whether to generate per-file statistics during planning.
    enable_statistics: bool,
    /// The format every write goes to. Always the NetCDF format.
    writer: Arc<dyn FileFormat>,
}

impl Hdf5Format {
    /// Whether this format generates per-file statistics during planning.
    pub fn statistics_enabled(&self) -> bool {
        self.enable_statistics
    }

    /// The dimensions this format reads, or `None` to auto-select a default.
    pub fn read_dimensions(&self) -> Option<&Vec<String>> {
        self.read_dimensions.as_ref()
    }

    /// Whether this format unifies the dimensions netCDF invents, by length.
    pub fn unifies_phony_dimensions(&self) -> bool {
        self.read.unify_phony_dimensions
    }

    /// The layout convention this format reads on top of the container.
    pub fn convention(&self) -> Hdf5Convention {
        self.read.convention
    }

    /// Whether this format writes through the NetCDF writer.
    ///
    /// Always true. The HDF5 reader has no writer, so a write goes to the
    /// NetCDF format. A test can check this invariant.
    pub fn writes_with_netcdf_writer(&self) -> bool {
        self.writer
            .downcast_ref::<NetcdfFormat>()
            .is_some()
    }
}

#[async_trait::async_trait]
impl FileFormat for Hdf5Format {
    fn compression_type(&self) -> Option<FileCompressionType> {
        None
    }

    fn get_ext(&self) -> String {
        self.ext.clone()
    }

    fn get_ext_with_compression(
        &self,
        _file_compression_type: &FileCompressionType,
    ) -> datafusion::error::Result<String> {
        Ok(self.ext.clone())
    }

    async fn infer_schema(
        &self,
        state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> datafusion::error::Result<SchemaRef> {
        use futures::{StreamExt, TryStreamExt};

        // Bounded: each open holds a descriptor until its schema is read, and
        // `try_join_all` would open every file in the listing at once. See the
        // same fix in `beacon_arrow_netcdf`, and issue #361.
        let width = state
            .config_options()
            .execution
            .meta_fetch_concurrency
            .max(1);
        let tasks: Vec<_> = objects
            .iter()
            .map(|object| {
                crate::open::fetch_schema(
                    store,
                    object,
                    self.read_dimensions.clone(),
                    self.skip_unbroadcastable,
                    self.read,
                )
            })
            .collect();
        let schemas: Vec<SchemaRef> = futures::stream::iter(tasks)
            .buffered(width)
            .try_collect()
            .await?;
        if schemas.is_empty() {
            return Ok(Arc::new(arrow::datatypes::Schema::empty()));
        }
        // The rule of the session decides the result for a column that two
        // files describe differently. Each schema names its file, so a refused
        // column names both files.
        session_widening(state)
            .merge_schemas(&label_by_object(objects, &schemas))
            .map_err(|e| {
                exec_datafusion_err!(
                    "Failed to merge the schemas of the HDF5 datasets: {}",
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
        // Unlike the netcdf-c path, this one can afford statistics: the reader
        // is async, holds no global lock and reads byte ranges, so a cold query
        // over a large collection stays parallel and does not block query
        // serving. That is why the capability follows the reader.
        if !self.enable_statistics {
            return Ok(Statistics::new_unknown(&table_schema));
        }

        // Reporting unknown rather than erroring is deliberate. Absent
        // statistics are always a legal answer -- DataFusion prunes nothing and
        // scans everything, which is correct, just slower.
        let dataset = match crate::open::open_dataset(store, object, self.read).await {
            Ok(dataset) => dataset,
            Err(e) => {
                tracing::warn!(
                    "Failed to open HDF5 object {} for statistics: {e}",
                    object.location
                );
                return Ok(Statistics::new_unknown(&table_schema));
            }
        };

        Ok(statistics::statistics_for_dataset(&dataset, &table_schema)
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
        let table_schema = TableSchema::new(
            encoded_file_schema,
            beacon_nd_array::arrow::partition::encoded_partition_cols(
                conf.table_partition_cols(),
            ),
        );
        // Preserve a projection that the scan pushed down into the incoming
        // source — rebuilding the source below would otherwise drop it.
        let projection = conf.file_source().projection().cloned();
        let source = Hdf5Source::new(self.read_dimensions.clone(), self.read, table_schema)
            .with_skip_unbroadcastable(self.skip_unbroadcastable)
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

    /// Writes always use the NetCDF writer.
    async fn create_writer_physical_plan(
        &self,
        input: Arc<dyn ExecutionPlan>,
        state: &dyn Session,
        conf: FileSinkConfig,
        order_requirements: Option<LexRequirement>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        self.writer
            .create_writer_physical_plan(input, state, conf, order_requirements)
            .await
    }

    fn file_source(&self, table_schema: TableSchema) -> Arc<dyn FileSource> {
        Arc::new(
            Hdf5Source::new(self.read_dimensions.clone(), self.read, table_schema)
                .with_skip_unbroadcastable(self.skip_unbroadcastable),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use beacon_arrow_netcdf::datafusion::{options::NetcdfOptions, NetcdfConfig};
    use datafusion::execution::session_state::SessionStateBuilder;
    use datafusion::prelude::SessionContext;
    use object_store::{path::Path, ObjectMeta};

    /// A factory over a minimal netCDF factory. It never opens a file in these
    /// tests — they exercise the HDF5 identity and the reader choice.
    fn factory(ext: &str, config: Hdf5Config) -> Hdf5FormatFactory {
        let listing = Arc::new(ListingFactory::new(None));
        let inner =
            NetCDFFormatFactory::new(listing, NetcdfOptions::default(), NetcdfConfig::default());
        Hdf5FormatFactory::new(inner, config).with_ext(ext)
    }

    fn session() -> SessionContext {
        SessionContext::new_with_state(SessionStateBuilder::new().with_default_features().build())
    }

    fn object(name: &str) -> ObjectMeta {
        ObjectMeta {
            location: Path::from(name),
            // A fixed epoch avoids a clock call; the value is irrelevant to discovery.
            last_modified: chrono::DateTime::from_timestamp(0, 0).unwrap(),
            size: 0,
            e_tag: None,
            version: None,
        }
    }

    // ── Identity ───────────────────────────────────────────────────────

    #[test]
    fn advertises_hdf5_extensions_and_name() {
        let f = factory("hdf5", Hdf5Config::default());
        assert_eq!(
            f.file_extensions(),
            vec!["h5".to_string(), "hdf5".to_string()]
        );
        assert_eq!(f.get_ext(), "hdf5");
        assert_eq!(f.file_format_name(), "hdf5");
    }

    #[test]
    fn with_ext_changes_only_the_registration_key() {
        let f = factory("h5", Hdf5Config::default());
        assert_eq!(f.get_ext(), "h5");
        // The recognized extensions are the same regardless of which key this instance answers to.
        assert_eq!(
            f.file_extensions(),
            vec!["h5".to_string(), "hdf5".to_string()]
        );
    }

    #[test]
    fn discovery_picks_up_h5_and_hdf5_only() {
        let f = factory("hdf5", Hdf5Config::default());
        let objects = [
            object("a.h5"),
            object("b.hdf5"),
            object("c.nc"),
            object("d.parquet"),
            object("no_extension"),
        ];
        let discovered = f.discover_datasets(&objects).unwrap();
        let paths: Vec<String> = discovered.iter().map(|d| d.file_path.clone()).collect();
        assert_eq!(paths, vec!["a.h5".to_string(), "b.hdf5".to_string()]);
        // Each discovered dataset is tagged with this factory's format name.
        assert!(discovered.iter().all(|d| d.format == "hdf5"));
    }

    // ── The backend ────────────────────────────────────────────────────

    /// The factory builds its own format, and keeps the netCDF one for writes.
    #[test]
    fn the_factory_builds_an_hdf5_format() {
        let ctx = session();
        let f = factory("hdf5", Hdf5Config::default());

        let format = f.create(&ctx.state(), &HashMap::new()).unwrap();
        assert!(format.downcast_ref::<Hdf5Format>().is_some());
        assert!(f.default().downcast_ref::<Hdf5Format>().is_some());
    }

    /// A stored table can still hold the option of the removed reader.
    #[test]
    fn the_removed_reader_option_does_not_change_the_format() {
        let ctx = session();
        let f = factory("hdf5", Hdf5Config::default());
        let options = HashMap::from([("use_rust_reader".to_string(), "false".to_string())]);
        let format = f.create(&ctx.state(), &options).unwrap();
        assert!(format.downcast_ref::<Hdf5Format>().is_some());
    }

    #[test]
    fn an_invalid_boolean_option_is_rejected_by_name() {
        let ctx = session();
        let f = factory("hdf5", Hdf5Config::default());
        let error = f
            .create(
                &ctx.state(),
                &HashMap::from([("use_rust_reader".to_string(), "maybe".to_string())]),
            )
            .unwrap_err()
            .to_string();
        assert!(error.contains("use_rust_reader"), "{error}");
        assert!(error.contains("maybe"), "{error}");
    }

    // ── Writes ─────────────────────────────────────────────────────────

    /// The HDF5 reader does not write. The format still writes, through the
    /// NetCDF writer.
    #[test]
    fn a_write_goes_to_the_netcdf_writer() {
        let ctx = session();
        let f = factory("hdf5", Hdf5Config::default());
        let format = f.create(&ctx.state(), &HashMap::new()).unwrap();
        let hdf5 = format.downcast_ref::<Hdf5Format>().unwrap();
        assert!(hdf5.writes_with_netcdf_writer());
    }

    // ── Per-table options ──────────────────────────────────────────────

    #[test]
    fn statistics_and_dimensions_come_from_the_table_options() {
        let ctx = session();
        let f = factory("hdf5", Hdf5Config::default());
        let format = f
            .create(
                &ctx.state(),
                &HashMap::from([
                    ("enable_statistics".to_string(), "false".to_string()),
                    ("read_dimensions".to_string(), "time, lat".to_string()),
                ]),
            )
            .unwrap();
        let hdf5 = format.downcast_ref::<Hdf5Format>().unwrap();
        assert!(!hdf5.statistics_enabled());
        assert_eq!(
            hdf5.read_dimensions(),
            Some(&vec!["time".to_string(), "lat".to_string()])
        );
        // The default: a plain HDF5 file reads as one table.
        assert!(hdf5.unifies_phony_dimensions());
    }

    /// One table can keep the names the reader gave, for a file whose groups
    /// hold unrelated axes of one length.
    #[test]
    fn a_table_option_turns_off_the_dimension_unification() {
        let ctx = session();
        let f = factory("hdf5", Hdf5Config::default());
        let format = f
            .create(
                &ctx.state(),
                &HashMap::from([("unify_phony_dimensions".to_string(), "false".to_string())]),
            )
            .unwrap();
        let hdf5 = format.downcast_ref::<Hdf5Format>().unwrap();
        assert!(!hdf5.unifies_phony_dimensions());
    }

    /// The runtime setting decides when the table says nothing.
    #[test]
    fn the_runtime_flag_turns_off_the_dimension_unification() {
        let ctx = session();
        let f = factory(
            "hdf5",
            Hdf5Config {
                unify_phony_dimensions: false,
                ..Hdf5Config::default()
            },
        );
        let format = f.create(&ctx.state(), &HashMap::new()).unwrap();
        let hdf5 = format.downcast_ref::<Hdf5Format>().unwrap();
        assert!(!hdf5.unifies_phony_dimensions());
    }
}
