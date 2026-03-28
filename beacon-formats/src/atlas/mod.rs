use std::{any::Any, collections::HashMap, sync::Arc};

use arrow::datatypes::SchemaRef;
use beacon_atlas::datafusion as atlas_df;
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

use crate::{Dataset, DatasetFormat, FileFormatFactoryExt};

pub mod opener;
pub mod source;

#[derive(Debug)]
pub struct AtlasFormatFactory;

impl GetExt for AtlasFormatFactory {
    fn get_ext(&self) -> String {
        "atlas.json".to_string()
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
        Arc::new(AtlasFormat::default())
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

impl FileFormatFactoryExt for AtlasFormatFactory {
    fn discover_datasets(&self, objects: &[ObjectMeta]) -> datafusion::error::Result<Vec<Dataset>> {
        let datasets = objects
            .iter()
            .filter(|meta| atlas_df::is_atlas_metadata(meta))
            .map(|meta| Dataset {
                file_path: meta.location.to_string(),
                format: DatasetFormat::Atlas,
            })
            .collect();
        Ok(datasets)
    }
}

/// Delegates to `beacon_atlas::datafusion::AtlasFormat`.
#[derive(Clone, Debug, Default)]
pub struct AtlasFormat {
    inner: atlas_df::AtlasFormat,
}

#[async_trait::async_trait]
impl FileFormat for AtlasFormat {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_ext(&self) -> String {
        self.inner.get_ext()
    }

    fn get_ext_with_compression(
        &self,
        file_compression_type: &FileCompressionType,
    ) -> datafusion::error::Result<String> {
        self.inner.get_ext_with_compression(file_compression_type)
    }

    fn compression_type(&self) -> Option<FileCompressionType> {
        self.inner.compression_type()
    }

    async fn infer_schema(
        &self,
        state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> datafusion::error::Result<SchemaRef> {
        self.inner.infer_schema(state, store, objects).await
    }

    async fn infer_stats(
        &self,
        state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        object: &ObjectMeta,
    ) -> datafusion::error::Result<Statistics> {
        self.inner
            .infer_stats(state, store, table_schema, object)
            .await
    }

    async fn create_physical_plan(
        &self,
        state: &dyn Session,
        conf: FileScanConfig,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        self.inner.create_physical_plan(state, conf).await
    }

    fn file_source(&self) -> Arc<dyn FileSource> {
        self.inner.file_source()
    }
}
