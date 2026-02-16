use std::{any::Any, collections::HashMap, sync::Arc};

use crate::{Dataset, DatasetFormat, FileFormatFactoryExt, bbf::source::BBFSource, max_open_fd};
use arrow::datatypes::SchemaRef;
use beacon_binary_format::{
    object_store::ArrowBBFObjectReader, reader::async_reader::AsyncBBFReader,
};
use beacon_common::super_typing::super_type_schema;
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

pub mod metrics;
pub mod opener;
pub mod source;
pub mod stream_share;

#[derive(Clone, Debug)]
pub struct BBFFormatFactory;

impl GetExt for BBFFormatFactory {
    fn get_ext(&self) -> String {
        "bbf".to_string()
    }
}

impl FileFormatFactory for BBFFormatFactory {
    fn create(
        &self,
        _state: &dyn Session,
        _format_options: &HashMap<String, String>,
    ) -> datafusion::error::Result<Arc<dyn FileFormat>> {
        Ok(Arc::new(BBFFormat {}))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn default(&self) -> std::sync::Arc<dyn FileFormat> {
        std::sync::Arc::new(BBFFormat {})
    }
}

impl FileFormatFactoryExt for BBFFormatFactory {
    fn discover_datasets(
        &self,
        objects: &[ObjectMeta],
    ) -> datafusion::error::Result<Vec<crate::Dataset>> {
        let datasets = objects
            .iter()
            .filter(|obj| {
                obj.location
                    .extension()
                    .map(|ext| ext == "bbf")
                    .unwrap_or(false)
            })
            .map(|obj| Dataset {
                file_path: obj.location.to_string(),
                format: DatasetFormat::BBF,
            })
            .collect();
        Ok(datasets)
    }
}

#[derive(Clone, Debug, Default)]
pub struct BBFFormat {}

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
            .buffer_unordered(max_open_fd() as usize) // 🔑 limit open readers
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
        let source = BBFSource::default();
        let conf = FileScanConfigBuilder::from(conf)
            .with_source(Arc::new(source))
            .build();
        Ok(DataSourceExec::from_data_source(conf))
    }

    fn file_source(&self) -> Arc<dyn FileSource> {
        Arc::new(BBFSource::default())
    }
}
