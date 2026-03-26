use std::{any::Any, collections::HashMap, sync::Arc};

use arrow::datatypes::SchemaRef;
use datafusion::{
    catalog::Session,
    common::{GetExt, Statistics},
    datasource::{
        file_format::{FileFormat, FileFormatFactory, file_compression_type::FileCompressionType},
        physical_plan::{FileScanConfig, FileSource},
    },
    physical_plan::ExecutionPlan,
};
use object_store::{ObjectMeta, ObjectStore};

use crate::{Dataset, DatasetFormat, FileFormatFactoryExt, atlas::source::AtlasSource};

pub mod opener;
pub mod source;

#[derive(Debug)]
pub struct AtlasFormatFactory;

impl GetExt for AtlasFormatFactory {
    fn get_ext(&self) -> String {
        "atlas".to_string()
    }
}

impl FileFormatFactory for AtlasFormatFactory {
    fn create(
        &self,
        _state: &dyn Session,
        _format_options: &HashMap<String, String>,
    ) -> datafusion::error::Result<Arc<dyn FileFormat>> {
        Ok(self.default())
    }

    fn default(&self) -> Arc<dyn FileFormat> {
        Arc::new(AtlasFormat {})
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

impl FileFormatFactoryExt for AtlasFormatFactory {
    fn discover_datasets(&self, objects: &[ObjectMeta]) -> datafusion::error::Result<Vec<Dataset>> {
        let datasets = objects
            .iter()
            .filter(|meta| is_atlas_metadata(meta))
            .map(|meta| Dataset {
                file_path: meta.location.to_string(),
                format: DatasetFormat::Atlas,
            })
            .collect();
        Ok(datasets)
    }
}

/// Check if this ObjectMeta represents an Atlas metadata file ("atlas.json")
pub fn is_atlas_metadata(meta: &object_store::ObjectMeta) -> bool {
    // Normalize for safety, S3 paths are UTF-8 so this is fine
    let loc = meta.location.to_string().to_lowercase();
    loc.ends_with("/atlas.json")
}

#[derive(Clone, Debug)]
pub struct AtlasFormat {}

#[async_trait::async_trait]
impl FileFormat for AtlasFormat {
    /// Returns the table provider as [`Any`](std::any::Any) so that it can be
    /// downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Returns the extension for this FileFormat, e.g. "file.csv" -> csv
    fn get_ext(&self) -> String {
        "atlas.json".to_string()
    }

    /// Returns the extension for this FileFormat when compressed, e.g. "file.csv.gz" -> csv
    fn get_ext_with_compression(
        &self,
        _file_compression_type: &FileCompressionType,
    ) -> datafusion::error::Result<String> {
        Ok("atlas.json".to_string())
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
        todo!()
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
        todo!()
    }

    fn file_source(&self) -> Arc<dyn FileSource> {
        Arc::new(AtlasSource::new())
    }
}
