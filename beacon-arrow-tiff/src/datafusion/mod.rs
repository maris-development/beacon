use std::any::Any;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use beacon_common::super_typing::super_type_schema;
use beacon_datafusion_ext::format_ext::{DatasetMetadata, FileFormatFactoryExt};
use datafusion::{
    catalog::{Session, memory::DataSourceExec},
    common::{GetExt, Statistics, exec_datafusion_err},
    datasource::{
        file_format::{FileFormat, FileFormatFactory, file_compression_type::FileCompressionType},
        physical_plan::{FileScanConfig, FileScanConfigBuilder, FileSource},
    },
    physical_plan::ExecutionPlan,
};
use object_store::{ObjectMeta, ObjectStore};

use crate::datafusion::{options::TiffOptions, source::TiffSource};

const TIFF_EXTENSION: &str = "tiff";
const TIF_EXTENSION: &str = "tif";

pub mod options;
pub mod reader;
pub mod source;

#[derive(Debug, Clone)]
pub struct TiffFormatFactory {
    pub options: TiffOptions,
}

impl TiffFormatFactory {
    pub fn new(options: TiffOptions) -> Self {
        Self { options }
    }
}

impl FileFormatFactory for TiffFormatFactory {
    fn create(
        &self,
        _state: &dyn Session,
        _format_options: &std::collections::HashMap<String, String>,
    ) -> datafusion::error::Result<Arc<dyn FileFormat>> {
        Ok(Arc::new(TiffFormat::new(self.options.clone())))
    }

    fn default(&self) -> Arc<dyn FileFormat> {
        Arc::new(TiffFormat::new(self.options.clone()))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl GetExt for TiffFormatFactory {
    fn get_ext(&self) -> String {
        TIFF_EXTENSION.to_string()
    }
}

impl FileFormatFactoryExt for TiffFormatFactory {
    fn discover_datasets(
        &self,
        objects: &[ObjectMeta],
    ) -> datafusion::error::Result<Vec<DatasetMetadata>> {
        let datasets = objects
            .iter()
            .filter(|obj| {
                obj.location
                    .extension()
                    .map(|ext| ext == TIFF_EXTENSION || ext == TIF_EXTENSION)
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
pub struct TiffFormat {
    pub options: TiffOptions,
}

impl TiffFormat {
    pub fn new(options: TiffOptions) -> Self {
        Self { options }
    }
}

#[async_trait::async_trait]
impl FileFormat for TiffFormat {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn compression_type(&self) -> Option<FileCompressionType> {
        None
    }

    fn get_ext(&self) -> String {
        TIFF_EXTENSION.to_string()
    }

    fn get_ext_with_compression(
        &self,
        _file_compression_type: &FileCompressionType,
    ) -> datafusion::error::Result<String> {
        Ok(TIFF_EXTENSION.to_string())
    }

    async fn infer_schema(
        &self,
        _state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> datafusion::error::Result<SchemaRef> {
        let mut tasks = vec![];
        for object in objects {
            let task = reader::fetch_schema(
                store.clone(),
                object.clone(),
                self.options.read_dimensions.clone(),
            );
            tasks.push(task);
        }

        let schemas = futures::future::try_join_all(tasks).await?;
        if schemas.is_empty() {
            return Ok(Arc::new(arrow::datatypes::Schema::empty()));
        }

        let schema = super_type_schema(&schemas).map_err(|e| {
            exec_datafusion_err!(
                "Failed to compute super type schema for TIFF datasets: {}",
                e
            )
        })?;
        Ok(schema.into())
    }

    async fn infer_stats(
        &self,
        _state: &dyn Session,
        _store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        _object: &ObjectMeta,
    ) -> datafusion::error::Result<Statistics> {
        Ok(Statistics::new_unknown(&table_schema))
    }

    async fn create_physical_plan(
        &self,
        _state: &dyn Session,
        conf: FileScanConfig,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let source = TiffSource::new(self.options.read_dimensions.clone());

        let conf = FileScanConfigBuilder::from(conf)
            .with_source(Arc::new(source))
            .build();

        Ok(DataSourceExec::from_data_source(conf))
    }

    fn file_source(&self) -> Arc<dyn FileSource> {
        Arc::new(TiffSource::new(self.options.read_dimensions.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::datasource::physical_plan::{FileScanConfigBuilder, FileSource};
    use datafusion::execution::object_store::ObjectStoreUrl;
    use futures::StreamExt;
    use object_store::memory::InMemory;
    use object_store::path::Path;

    const TEST_TIF_BYTES: &[u8] = include_bytes!("../../test-files/test.tif");

    async fn put_fixture(store: &Arc<InMemory>, path: &Path, bytes: &[u8]) -> ObjectMeta {
        store
            .put(path, bytes::Bytes::copy_from_slice(bytes).into())
            .await
            .expect("should write TIFF fixture bytes");
        store
            .head(path)
            .await
            .expect("should fetch object metadata")
    }

    #[tokio::test]
    async fn infer_schema_reads_real_stripped_geotiff_fixture() {
        let store = Arc::new(InMemory::new());
        let object_store: Arc<dyn ObjectStore> = store.clone();
        let path = Path::from("tests/datafusion/test.tif");
        let object = put_fixture(&store, &path, TEST_TIF_BYTES).await;

        let schema = reader::fetch_schema(object_store, object, None)
            .await
            .expect("real stripped GeoTIFF should produce a schema");

        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert!(
            field_names.contains(&"band.0"),
            "schema should contain band.0"
        );
        assert!(
            field_names.contains(&"geo.lat"),
            "schema should contain geo.lat"
        );
        assert!(
            field_names.contains(&"geo.lon"),
            "schema should contain geo.lon"
        );
        assert!(
            field_names.contains(&"image.width"),
            "schema should contain image.width"
        );
        println!("Schema is: {:?}", schema);
    }

    #[tokio::test]
    async fn opener_streams_record_batches_for_real_fixture() {
        let store = Arc::new(InMemory::new());
        let object_store: Arc<dyn ObjectStore> = store.clone();
        let path = Path::from("tests/datafusion/test2.tif");
        let object = put_fixture(&store, &path, TEST_TIF_BYTES).await;

        let table_schema = reader::fetch_schema(object_store.clone(), object.clone(), None)
            .await
            .expect("schema");

        let source = source::TiffSource::new(None);
        let file_opener = {
            let conf = FileScanConfigBuilder::new(
                ObjectStoreUrl::parse("memory://").expect("url"),
                table_schema,
                Arc::new(source.clone()) as Arc<dyn FileSource>,
            )
            .build();
            source.create_file_opener(object_store, &conf, 0)
        };

        let stream = file_opener
            .open(
                datafusion::datasource::physical_plan::FileMeta {
                    object_meta: object,
                    range: None,
                    extensions: None,
                    metadata_size_hint: None,
                },
                datafusion::datasource::listing::PartitionedFile::new("ignored", 0),
            )
            .expect("open")
            .await
            .expect("stream");

        let batches: Vec<_> = stream.collect().await;
        assert!(!batches.is_empty(), "should produce at least one batch");
        let first = batches[0].as_ref().expect("first batch");
        assert!(first.num_rows() > 0);
        assert!(first.num_columns() > 0);
    }
}
