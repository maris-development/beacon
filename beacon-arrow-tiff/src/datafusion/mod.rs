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
    use arrow::array::{UInt16Array, UInt32Array, UInt64Array};
    use datafusion::datasource::listing::PartitionedFile;
    use datafusion::datasource::physical_plan::{FileMeta, FileScanConfigBuilder, FileSource};
    use datafusion::execution::object_store::ObjectStoreUrl;
    use futures::StreamExt;
    use object_store::memory::InMemory;
    use object_store::path::Path;

    const REAL_TILED_TIFF_BYTES: &[u8] = include_bytes!("../../test-files/TEMP_AVG_20201201.tif");

    fn ifd_entry(tag: u16, value_type: u16, count: u32, value: u32) -> [u8; 12] {
        let mut entry = [0u8; 12];
        entry[0..2].copy_from_slice(&tag.to_le_bytes());
        entry[2..4].copy_from_slice(&value_type.to_le_bytes());
        entry[4..8].copy_from_slice(&count.to_le_bytes());
        entry[8..12].copy_from_slice(&value.to_le_bytes());
        entry
    }

    fn build_tiff(entries: &[[u8; 12]], image_data: &[u8]) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(b"II");
        bytes.extend_from_slice(&42u16.to_le_bytes());
        bytes.extend_from_slice(&8u32.to_le_bytes());

        bytes.extend_from_slice(&(entries.len() as u16).to_le_bytes());
        for entry in entries {
            bytes.extend_from_slice(entry);
        }
        bytes.extend_from_slice(&0u32.to_le_bytes());
        bytes.extend_from_slice(image_data);
        bytes
    }

    fn minimal_tiled_tiff_bytes() -> Vec<u8> {
        let data_offset = 8 + 2 + (10 * 12) + 4;
        let entries = vec![
            ifd_entry(256, 4, 1, 1),
            ifd_entry(257, 4, 1, 1),
            ifd_entry(258, 3, 1, 8),
            ifd_entry(259, 3, 1, 1),
            ifd_entry(262, 3, 1, 1),
            ifd_entry(277, 3, 1, 1),
            ifd_entry(322, 4, 1, 1),
            ifd_entry(323, 4, 1, 1),
            ifd_entry(324, 4, 1, data_offset),
            ifd_entry(325, 4, 1, 1),
        ];
        build_tiff(&entries, &[0u8])
    }

    fn minimal_stripped_tiff_bytes() -> Vec<u8> {
        let data_offset = 8 + 2 + (9 * 12) + 4;
        let entries = vec![
            ifd_entry(256, 4, 1, 1),
            ifd_entry(257, 4, 1, 1),
            ifd_entry(258, 3, 1, 8),
            ifd_entry(259, 3, 1, 1),
            ifd_entry(262, 3, 1, 1),
            ifd_entry(273, 4, 1, data_offset),
            ifd_entry(277, 3, 1, 1),
            ifd_entry(278, 4, 1, 1),
            ifd_entry(279, 4, 1, 1),
        ];
        build_tiff(&entries, &[0u8])
    }

    async fn put_fixture(store: &Arc<InMemory>, path: &Path, bytes: Vec<u8>) -> ObjectMeta {
        store
            .put(path, bytes::Bytes::from(bytes).into())
            .await
            .expect("should write TIFF fixture bytes");
        store
            .head(path)
            .await
            .expect("should fetch object metadata")
    }

    #[tokio::test]
    async fn fetch_schema_from_memory_store_contains_expected_fields() {
        let store = Arc::new(InMemory::new());
        let object_store: Arc<dyn ObjectStore> = store.clone();
        let path = Path::from("tests/datafusion/minimal-tiled.tiff");
        let object = put_fixture(&store, &path, minimal_tiled_tiff_bytes()).await;

        let schema = reader::fetch_schema(object_store, object, None)
            .await
            .expect("schema should be inferred for tiled TIFF");

        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert!(
            names.contains(&"image.width"),
            "expected image.width in {names:?}"
        );
        assert!(
            names.contains(&"image.tile_width"),
            "expected image.tile_width in {names:?}"
        );
    }

    #[tokio::test]
    async fn infer_schema_uses_supplied_object_store() {
        let store = Arc::new(InMemory::new());
        let object_store: Arc<dyn ObjectStore> = store.clone();
        let path = Path::from("tests/datafusion/minimal-tiled-2.tiff");
        let object = put_fixture(&store, &path, minimal_tiled_tiff_bytes()).await;

        let format = TiffFormat::new(Default::default());
        let ctx = datafusion::prelude::SessionContext::new();

        let schema = format
            .infer_schema(&ctx.state(), &object_store, &[object])
            .await
            .expect("infer_schema should succeed with session object store");

        assert!(!schema.fields().is_empty());
    }

    #[tokio::test]
    async fn opener_streams_record_batches_for_tiled_tiff() {
        let store = Arc::new(InMemory::new());
        let object_store: Arc<dyn ObjectStore> = store.clone();
        let path = Path::from("tests/datafusion/minimal-tiled-3.tiff");
        let object = put_fixture(&store, &path, minimal_tiled_tiff_bytes()).await;

        let table_schema = reader::fetch_schema(object_store.clone(), object.clone(), None)
            .await
            .expect("table schema");

        let source = source::TiffSource::new(None);
        let file_opener = {
            let conf = FileScanConfigBuilder::new(
                ObjectStoreUrl::parse("memory://").expect("memory object store url should parse"),
                table_schema,
                Arc::new(source.clone()) as Arc<dyn FileSource>,
            )
            .build();
            source.create_file_opener(object_store, &conf, 0)
        };

        let file_meta = FileMeta {
            object_meta: object,
            range: None,
            extensions: None,
            metadata_size_hint: None,
        };

        let stream = file_opener
            .open(file_meta, PartitionedFile::new("ignored", 0))
            .expect("open")
            .await
            .expect("open future should resolve to stream");

        let batches: Vec<_> = stream.collect().await;
        assert!(!batches.is_empty(), "should produce at least one batch");

        let first = batches[0].as_ref().expect("first batch should be ok");
        assert_eq!(first.num_rows(), 1);
        assert!(first.num_columns() > 0);
    }

    #[tokio::test]
    async fn opener_errors_for_non_tiled_tiff() {
        let store = Arc::new(InMemory::new());
        let object_store: Arc<dyn ObjectStore> = store.clone();

        let tiled_path = Path::from("tests/datafusion/minimal-tiled-4.tiff");
        let stripped_path = Path::from("tests/datafusion/minimal-stripped.tiff");

        let tiled_object = put_fixture(&store, &tiled_path, minimal_tiled_tiff_bytes()).await;
        let stripped_object =
            put_fixture(&store, &stripped_path, minimal_stripped_tiff_bytes()).await;

        let table_schema = reader::fetch_schema(object_store.clone(), tiled_object, None)
            .await
            .expect("table schema from tiled object");

        let source = source::TiffSource::new(None);
        let file_opener = {
            let conf = FileScanConfigBuilder::new(
                ObjectStoreUrl::parse("memory://").expect("memory object store url should parse"),
                table_schema,
                Arc::new(source.clone()) as Arc<dyn FileSource>,
            )
            .build();
            source.create_file_opener(object_store, &conf, 0)
        };

        let file_meta = FileMeta {
            object_meta: stripped_object,
            range: None,
            extensions: None,
            metadata_size_hint: None,
        };

        let result = file_opener
            .open(file_meta, PartitionedFile::new("ignored", 0))
            .expect("open should build future")
            .await;

        match result {
            Ok(_) => panic!("non-tiled TIFF should fail during read task"),
            Err(err) => {
                assert!(
                    err.to_string()
                        .contains("Non-tiled TIFF is not supported yet"),
                    "unexpected error: {err}"
                );
            }
        }
    }

    #[tokio::test]
    async fn opener_reads_band_count_from_tiled_fixture() {
        let store = Arc::new(InMemory::new());
        let object_store: Arc<dyn ObjectStore> = store.clone();
        let path = Path::from("tests/datafusion/minimal-tiled-band-count.tiff");
        let object = put_fixture(&store, &path, minimal_tiled_tiff_bytes()).await;

        let table_schema = reader::fetch_schema(object_store.clone(), object.clone(), None)
            .await
            .expect("table schema");

        let source = source::TiffSource::new(None);
        let file_opener = {
            let conf = FileScanConfigBuilder::new(
                ObjectStoreUrl::parse("memory://").expect("memory object store url should parse"),
                table_schema,
                Arc::new(source.clone()) as Arc<dyn FileSource>,
            )
            .build();
            source.create_file_opener(object_store, &conf, 0)
        };

        let file_meta = FileMeta {
            object_meta: object,
            range: None,
            extensions: None,
            metadata_size_hint: None,
        };

        let stream = file_opener
            .open(file_meta, PartitionedFile::new("ignored", 0))
            .expect("open")
            .await
            .expect("open future should resolve to stream");

        let batches: Vec<_> = stream.collect().await;
        assert!(!batches.is_empty(), "should produce at least one batch");

        let first = batches[0].as_ref().expect("first batch should be ok");
        let band_idx = first
            .schema()
            .fields()
            .iter()
            .position(|f| f.name() == "image.samples_per_pixel")
            .expect("batch should contain image.samples_per_pixel column");

        let band_column = first.column(band_idx);
        let band_count = match band_column.data_type() {
            arrow::datatypes::DataType::UInt16 => band_column
                .as_any()
                .downcast_ref::<UInt16Array>()
                .expect("uint16 column")
                .value(0) as u64,
            arrow::datatypes::DataType::UInt32 => band_column
                .as_any()
                .downcast_ref::<UInt32Array>()
                .expect("uint32 column")
                .value(0) as u64,
            arrow::datatypes::DataType::UInt64 => band_column
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect("uint64 column")
                .value(0),
            other => panic!("unexpected band column type: {other:?}"),
        };

        assert_eq!(band_count, 1, "samples_per_pixel should be 1");
    }

    #[tokio::test]
    async fn infer_schema_rejects_real_fixture_when_not_tiled() {
        let store = Arc::new(InMemory::new());
        let object_store: Arc<dyn ObjectStore> = store.clone();
        let path = Path::from("tests/datafusion/TEMP_AVG_20201201.tif");
        let object = put_fixture(&store, &path, REAL_TILED_TIFF_BYTES.to_vec()).await;

        let err = reader::fetch_schema(object_store, object, None)
            .await
            .expect_err("real fixture should fail until non-tiled TIFFs are supported");

        assert!(
            err.to_string()
                .contains("Non-tiled TIFF is not supported yet"),
            "unexpected error: {err}"
        );
    }
}
