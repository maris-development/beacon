use std::{any::Any, collections::HashMap, sync::Arc};

use arrow::datatypes::SchemaRef;
use beacon_binary_format::{
    object_store::ArrowBBFObjectReader, reader::async_reader::AsyncBBFReader,
};
use beacon_common::file_descriptors::file_open_parallelism;
use beacon_common::super_typing::super_type_schema;
use beacon_datafusion_ext::format_ext::{DatasetMetadata, FileFormatFactoryExt};
use datafusion::{
    catalog::{Session, memory::DataSourceExec},
    common::{GetExt, Statistics},
    datasource::{
        file_format::{FileFormat, FileFormatFactory, file_compression_type::FileCompressionType},
        physical_plan::{FileScanConfig, FileScanConfigBuilder, FileSource},
    },
    physical_plan::ExecutionPlan,
};
use futures::{StreamExt, TryStreamExt, stream};
use object_store::{ObjectMeta, ObjectStore};

use crate::datafusion::source::BBFSource;

pub mod metrics;
pub mod opener;
pub mod source;
pub mod stream_share;

/// Runtime configuration for the BBF format.
///
/// Plain data with sensible defaults; the caller populates it (no environment
/// parsing here). `split_streams_slice` is a default that a table can override
/// via `CREATE EXTERNAL TABLE ... OPTIONS (...)`.
#[derive(Clone, Debug, Default)]
pub struct BbfConfig {
    /// Whether to split each record batch into `batch_size`-row slices to bound
    /// peak memory for wide tables.
    pub split_streams_slice: bool,
}

/// Parse a boolean value supplied through a `CREATE EXTERNAL TABLE` option.
fn parse_bool_option(key: &str, value: &str) -> datafusion::error::Result<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" | "on" => Ok(true),
        "false" | "0" | "no" | "off" => Ok(false),
        other => Err(datafusion::error::DataFusionError::Execution(format!(
            "invalid boolean for BBF option '{key}': '{other}'"
        ))),
    }
}

#[derive(Clone, Debug, Default)]
pub struct BBFFormatFactory {
    pub config: BbfConfig,
}

impl BBFFormatFactory {
    pub fn new(config: BbfConfig) -> Self {
        Self { config }
    }
}

impl GetExt for BBFFormatFactory {
    fn get_ext(&self) -> String {
        "bbf".to_string()
    }
}

impl FileFormatFactory for BBFFormatFactory {
    fn create(
        &self,
        _state: &dyn Session,
        format_options: &HashMap<String, String>,
    ) -> datafusion::error::Result<Arc<dyn FileFormat>> {
        // Per-table override from `CREATE EXTERNAL TABLE ... OPTIONS (...)`,
        // defaulting to the runtime config.
        let mut split_streams_slice = self.config.split_streams_slice;
        if let Some(value) = format_options.get("split_streams_slice") {
            split_streams_slice = parse_bool_option("split_streams_slice", value)?;
        }
        Ok(Arc::new(BBFFormat { split_streams_slice }))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn default(&self) -> std::sync::Arc<dyn FileFormat> {
        std::sync::Arc::new(BBFFormat {
            split_streams_slice: self.config.split_streams_slice,
        })
    }
}

impl FileFormatFactoryExt for BBFFormatFactory {
    fn discover_datasets(
        &self,
        objects: &[ObjectMeta],
    ) -> datafusion::error::Result<Vec<DatasetMetadata>> {
        let datasets = objects
            .iter()
            .filter(|obj| {
                obj.location
                    .extension()
                    .map(|ext| ext == "bbf")
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

#[derive(Clone, Debug, Default)]
pub struct BBFFormat {
    /// Whether to split each record batch into `batch_size`-row slices.
    pub split_streams_slice: bool,
}

#[async_trait::async_trait]
impl FileFormat for BBFFormat {
    /// Returns the table provider as [`Any`](std::any::Any) so that it can be
    /// downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Returns the extension for this FileFormat, e.g. "file.csv" -> csv
    fn get_ext(&self) -> String {
        "bbf".to_string()
    }

    /// Returns the extension for this FileFormat when compressed, e.g. "file.csv.gz" -> csv
    fn get_ext_with_compression(
        &self,
        _file_compression_type: &FileCompressionType,
    ) -> datafusion::error::Result<String> {
        Ok("bbf".to_string())
    }

    /// Returns whether this instance uses compression if applicable
    fn compression_type(&self) -> Option<FileCompressionType> {
        None
    }

    async fn infer_schema(
        &self,
        _state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> datafusion::error::Result<SchemaRef> {
        let schemas = stream::iter(objects.iter().cloned())
            .map(|object| {
                let store = Arc::clone(store);
                async move {
                    let async_reader = ArrowBBFObjectReader::new(object.location, store);

                    let reader = AsyncBBFReader::new(Arc::new(async_reader), 128)
                        .await
                        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

                    Ok::<_, datafusion::error::DataFusionError>(Arc::new(reader.arrow_schema()))
                }
            })
            .buffer_unordered(file_open_parallelism())
            .try_collect::<Vec<_>>()
            .await?;

        //Supertype the schema
        let super_schema = super_type_schema(&schemas).map_err(|e| {
            datafusion::error::DataFusionError::Execution(format!("Failed to infer schema: {}", e))
        })?;

        Ok(Arc::new(super_schema))
    }

    /// Infer the statistics for the provided object. The cost and accuracy of the
    /// estimated statistics might vary greatly between file formats.
    ///
    /// `table_schema` is the (combined) schema of the overall table
    /// and may be a superset of the schema contained in this file.
    ///
    /// TODO: should the file source return statistics for only columns referred to in the table schema?
    async fn infer_stats(
        &self,
        state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        object: &ObjectMeta,
    ) -> datafusion::error::Result<Statistics> {
        return Ok(Statistics::new_unknown(&table_schema));
    }

    /// Take a list of files and convert it to the appropriate executor
    /// according to this file format.
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
        let source = BBFSource::new(table_schema)
            .with_split_streams_slice(self.split_streams_slice)
            .with_projection(projection);
        let conf = FileScanConfigBuilder::from(conf)
            .with_source(Arc::new(source))
            .build();
        Ok(DataSourceExec::from_data_source(conf))
    }

    fn file_source(
        &self,
        table_schema: datafusion::datasource::table_schema::TableSchema,
    ) -> Arc<dyn FileSource> {
        Arc::new(BBFSource::new(table_schema).with_split_streams_slice(self.split_streams_slice))
    }
}

pub mod table_function;
pub use table_function::ReadBBFFunc;
