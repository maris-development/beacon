//! The DataFusion integration: discovering Atlas collections, typing them, and
//! planning a scan over their datasets.
//!
//! [`AtlasFormatFactory`] recognizes a collection in a listing and builds an
//! [`AtlasFormat`] per table. The format infers the collection's schema, then
//! plans a scan whose entries are *datasets* rather than files — see
//! [`source`] for what the openers then do with them.

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::{Schema, SchemaRef};
use beacon_datafusion_ext::format_ext::{
    DatasetMetadata, FileFormatFactoryExt, SchemaOptions, SchemaUnit, units_over_stores,
};
use beacon_datafusion_ext::format_options::format_option;
use beacon_datafusion_ext::listing_factory::ListingFactory;
use beacon_datafusion_ext::type_widening::{LabeledSchema, session_widening};
use datafusion::{
    catalog::{Session, memory::DataSourceExec},
    common::{GetExt, Statistics, exec_datafusion_err},
    datasource::{
        file_format::{FileFormat, FileFormatFactory, file_compression_type::FileCompressionType},
        listing::{ListingTableUrl, PartitionedFile},
        physical_plan::{
            FileGroup, FileScanConfig, FileScanConfigBuilder, FileSinkConfig, FileSource,
        },
        table_schema::TableSchema,
    },
    error::{DataFusionError, Result},
    physical_expr::LexRequirement,
    physical_plan::ExecutionPlan,
};
use object_store::{ObjectMeta, ObjectStore};

use crate::compat;
use crate::store::{ATLAS_MARKER, AtlasReaderCache, get_or_open_atlas, top_level_atlas_markers};

pub mod metrics;
pub mod opener;
pub mod options;
pub mod pool;
pub mod pruning;
pub mod source;
pub mod table_function;
pub mod view;

pub use options::AtlasOptions;
pub use source::AtlasSource;
pub use table_function::ReadAtlasFunc;

/// The name this format answers to: `STORED AS ATLAS`, `read_atlas`.
pub const ATLAS_FORMAT: &str = "atlas";

/// Builds an [`AtlasFormat`] per table, over one runtime's settings and one
/// shared reader cache.
#[derive(Debug, Clone)]
pub struct AtlasFormatFactory {
    pub options: AtlasOptions,
}

impl AtlasFormatFactory {
    pub fn new(options: AtlasOptions) -> Self {
        Self { options }
    }

    /// A format with this table's effective settings, wired to the shared cache
    /// when caching is on.
    pub(crate) fn build(&self, options: AtlasOptions) -> AtlasFormat {
        AtlasFormat::new(options)
    }
}

impl FileFormatFactory for AtlasFormatFactory {
    fn create(
        &self,
        _state: &dyn Session,
        format_options: &HashMap<String, String>,
    ) -> Result<Arc<dyn FileFormat>> {
        let mut options = self.options.clone();

        if let Some(value) = format_option(format_options, "read_dimensions") {
            options.read_dimensions = Some(
                value
                    .split(',')
                    .map(|dimension| dimension.trim().to_string())
                    .filter(|dimension| !dimension.is_empty())
                    .collect(),
            );
        }
        Ok(Arc::new(self.build(options)))
    }

    fn default(&self) -> Arc<dyn FileFormat> {
        Arc::new(self.build(self.options.clone()))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl GetExt for AtlasFormatFactory {
    fn get_ext(&self) -> String {
        ATLAS_FORMAT.to_string()
    }
}

impl FileFormatFactoryExt for AtlasFormatFactory {
    /// One dataset entry per collection, named by its container object.
    ///
    /// A collection's datasets are enumerated at plan time, not here: a listing
    /// of a data lake would otherwise open every collection it found.
    fn discover_datasets(&self, objects: &[ObjectMeta]) -> Result<Vec<DatasetMetadata>> {
        let format = self.get_ext();
        Ok(top_level_atlas_markers(objects)
            .into_iter()
            .map(|marker| DatasetMetadata::new(marker.location.to_string(), format.clone()))
            .collect())
    }

    fn file_format_name(&self) -> String {
        self.get_ext()
    }

    /// One schema per collection, not per object.
    fn schema_units(&self, objects: &[ObjectMeta]) -> Vec<SchemaUnit> {
        units_over_stores(objects, &top_level_atlas_markers(objects))
    }

    /// Atlas opts into the schema cache for a collection read whole.
    ///
    /// TODO(#367): cache a dimension-projected read too. `read_dimensions`
    /// decides which arrays survive, so one collection has one schema per
    /// dimension set and the key would have to carry the set in order. Left out
    /// of this pass to keep the four nd formats saying the same thing.
    fn schema_options_fingerprint(&self, format: &dyn FileFormat) -> Option<u64> {
        let format = format.as_any().downcast_ref::<AtlasFormat>()?;
        if format.options.read_dimensions.is_some() {
            return None;
        }
        Some(SchemaOptions::new(ATLAS_FORMAT).finish())
    }

    /// The plain format. Atlas measures no column.
    ///
    /// The analyzer asks a format to measure a collection, and this one reports
    /// unknown for every column. See [`AtlasFormat::infer_stats`].
    fn create_for_analysis(
        &self,
        _state: &dyn Session,
        _format_options: &HashMap<String, String>,
        _url: &ListingTableUrl,
        _listing: &ListingFactory,
    ) -> Result<Arc<dyn FileFormat>> {
        Ok(Arc::new(AtlasFormat::default()))
    }
}

/// Reads one table's worth of Atlas collections.
#[derive(Debug, Clone)]
pub struct AtlasFormat {
    pub options: AtlasOptions,
    cache: AtlasReaderCache,
}

impl Default for AtlasFormat {
    fn default() -> Self {
        Self::new(AtlasOptions::default())
    }
}

impl AtlasFormat {
    pub fn new(options: AtlasOptions) -> Self {
        Self {
            options,
            cache: AtlasReaderCache::new(512),
        }
    }
}

/// Wrap a scan in the nd spine: `NdBroadcastExec` over `NdSourceExec` over the
/// scan.
///
/// The scan carries its columns `beacon.nd`-encoded, one chunk per row, so
/// `NdSourceExec` decodes them and `NdBroadcastExec` broadcasts them back onto
/// the logical table schema above.
pub fn nd_scan_plan(conf: FileScanConfig) -> Result<Arc<dyn ExecutionPlan>> {
    let scan: Arc<dyn ExecutionPlan> = DataSourceExec::from_data_source(conf);
    let nd_source = Arc::new(beacon_datafusion_ext::nd::exec::NdSourceExec::try_new(
        scan,
    )?);
    Ok(Arc::new(
        beacon_datafusion_ext::nd::exec::NdBroadcastExec::try_new(nd_source)?,
    ))
}

#[async_trait::async_trait]
impl FileFormat for AtlasFormat {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn compression_type(&self) -> Option<FileCompressionType> {
        None
    }

    /// The container's own name, which is what a listing matches on.
    fn get_ext(&self) -> String {
        ATLAS_MARKER.to_string()
    }

    fn get_ext_with_compression(
        &self,
        _file_compression_type: &FileCompressionType,
    ) -> Result<String> {
        Ok(ATLAS_MARKER.to_string())
    }

    /// The schema of every collection in the listing, merged.
    ///
    /// Each collection costs one open — a footer read — and the datasets behind
    /// it cost no I/O at all. Never enumerate a collection's datasets any other
    /// way here: at a million datasets that would turn planning into a scan.
    ///
    /// The footer counts every dataset, deleted ones too, so a column only a
    /// deleted dataset declares is in the schema and reads as null.
    /// `read_dimensions` does not narrow the schema: the footer holds no
    /// dimension name. The scan fills an array it drops with nulls.
    async fn infer_schema(
        &self,
        state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> Result<SchemaRef> {
        let started = std::time::Instant::now();
        let markers = top_level_atlas_markers(objects);
        if markers.is_empty() {
            return Ok(Arc::new(Schema::empty()));
        }

        // One rule for both merges: the datasets inside a collection, and the
        // collections of this table.
        let widening = session_widening(state);

        let mut schemas = Vec::with_capacity(markers.len());
        for marker in &markers {
            let atlas = get_or_open_atlas(Some(&self.cache), Arc::clone(store), marker)
                .await
                .map_err(|e| exec_datafusion_err!("{e}"))?;

            let schema =
                compat::collection_arrow_schema(&atlas.footer().collection_schema(), &widening)
                    .map_err(|e| {
                        exec_datafusion_err!(
                            "Failed to read the schema of atlas collection '{}': {e}",
                            marker.location
                        )
                    })?;
            schemas.push(LabeledSchema::new(
                Arc::new(schema),
                marker.location.as_ref(),
            ));
        }

        let schema = widening.merge_schemas(&schemas).map_err(|e| {
            exec_datafusion_err!("Failed to merge the schemas of the atlas collections: {e}")
        })?;
        tracing::debug!(
            elapsed_ms = started.elapsed().as_millis() as u64,
            collections = markers.len(),
            fields = schema.fields().len(),
            "atlas infer_schema",
        );
        Ok(schema)
    }

    /// Unknown for every column.
    ///
    /// Atlas measures nothing for the analyzer. A recorded range prunes whole
    /// collections before a scan opens them, so a range that is too narrow
    /// deletes matching rows from an answer. The scan prunes from the footer
    /// itself instead, per dataset, where the numbers are exact and cost no
    /// array read. See [`pruning`].
    async fn infer_stats(
        &self,
        _state: &dyn Session,
        _store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        _object: &ObjectMeta,
    ) -> Result<Statistics> {
        Ok(Statistics::new_unknown(&table_schema))
    }

    /// Plan one entry per collection, then wrap the scan in the nd spine.
    ///
    /// Nothing is opened here. The markers the listing found are deduped to the
    /// outermost collections and dealt round-robin over the target partitions,
    /// and each partition's opener lists, prunes and reads the collections it
    /// holds. Parallelism is therefore bounded by the collection count.
    async fn create_physical_plan(
        &self,
        state: &dyn Session,
        conf: FileScanConfig,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        beacon_nd_array::arrow::morsel::reject_partition_columns("Atlas", &conf)?;

        let listed: Vec<ObjectMeta> = conf
            .file_groups
            .iter()
            .flat_map(|group| group.files())
            .map(|file| file.object_meta.clone())
            .collect();
        let markers = top_level_atlas_markers(&listed);

        // One collection is one unit of work, and a container is never split,
        // so the deal here is the whole distribution.
        let partitions = state
            .config()
            .target_partitions()
            .clamp(1, markers.len().max(1));
        let mut dealt: Vec<Vec<PartitionedFile>> = vec![Vec::new(); partitions];
        for (index, marker) in markers.iter().enumerate() {
            dealt[index % partitions].push(PartitionedFile::from(marker.clone()));
        }
        let file_groups: Vec<FileGroup> = dealt
            .into_iter()
            .filter(|group| !group.is_empty())
            .map(FileGroup::new)
            .collect();
        tracing::debug!(
            collections = markers.len(),
            partitions = file_groups.len(),
            "atlas create_physical_plan",
        );

        // The scan carries nd columns, so the source's schema is the encoded
        // form of the logical table schema.
        let encoded = Arc::new(beacon_datafusion_ext::nd::encoded_schema(
            conf.file_schema(),
        ));
        let table_schema = TableSchema::new(encoded, conf.table_partition_cols().clone());
        // Preserve a projection already pushed into the incoming source;
        // rebuilding it below would otherwise drop it.
        let projection = conf.file_source().projection().cloned();

        let source = AtlasSource::new(
            self.options.read_dimensions.clone(),
            table_schema,
            self.cache.clone(),
        )
        .with_projection(projection);
        let conf = FileScanConfigBuilder::from(conf)
            .with_file_groups(file_groups)
            .with_source(Arc::new(source))
            .build();

        nd_scan_plan(conf)
    }

    fn file_source(&self, table_schema: TableSchema) -> Arc<dyn FileSource> {
        Arc::new(AtlasSource::new(
            self.options.read_dimensions.clone(),
            table_schema,
            self.cache.clone(),
        ))
    }

    async fn create_writer_physical_plan(
        &self,
        _input: Arc<dyn ExecutionPlan>,
        _state: &dyn Session,
        _conf: FileSinkConfig,
        _order_requirements: Option<LexRequirement>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::NotImplemented(
            "an atlas collection is written once, by `atlas create`, and Beacon does not write one"
                .to_string(),
        ))
    }
}
