use std::any::Any;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use beacon_common::super_typing::super_type_schema;
use beacon_datafusion_ext::format_ext::{DatasetMetadata, FileFormatFactoryExt};
use beacon_object_storage::DatasetsStore;
use datafusion::{
    catalog::{memory::DataSourceExec, Session},
    common::{exec_datafusion_err, GetExt, Statistics},
    datasource::{
        file_format::{file_compression_type::FileCompressionType, FileFormat, FileFormatFactory},
        physical_plan::{FileScanConfig, FileScanConfigBuilder, FileSinkConfig, FileSource},
    },
    physical_expr::LexRequirement,
    physical_plan::ExecutionPlan,
};
use object_store::{ObjectMeta, ObjectStore};

use crate::datafusion::{options::NetcdfOptions, source::NetCDFSource};

const NETCDF_EXTENSION: &str = "nc";

pub mod options;
pub mod reader;
pub mod sink;
pub mod source;

#[derive(Debug, Clone)]
pub struct NetCDFFormatFactory {
    pub datasets_object_store: Arc<DatasetsStore>,
    pub options: NetcdfOptions,
}

impl NetCDFFormatFactory {
    pub fn new(datasets_object_store: Arc<DatasetsStore>, options: NetcdfOptions) -> Self {
        Self {
            datasets_object_store,
            options,
        }
    }
}

impl FileFormatFactory for NetCDFFormatFactory {
    fn create(
        &self,
        _state: &dyn Session,
        _format_options: &std::collections::HashMap<String, String>,
    ) -> datafusion::error::Result<Arc<dyn FileFormat>> {
        Ok(Arc::new(NetcdfFormat::new(
            self.datasets_object_store.clone(),
            self.options.clone(),
        )))
    }

    fn default(&self) -> Arc<dyn FileFormat> {
        Arc::new(NetcdfFormat::new(
            self.datasets_object_store.clone(),
            self.options.clone(),
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
    pub datasets_object_store: Arc<DatasetsStore>,
    pub options: NetcdfOptions,
}

impl NetcdfFormat {
    pub fn new(datasets_object_store: Arc<DatasetsStore>, options: NetcdfOptions) -> Self {
        Self {
            datasets_object_store,
            options,
        }
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
        _state: &dyn Session,
        _store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> datafusion::error::Result<SchemaRef> {
        let mut tasks = vec![];
        for object in objects {
            let task = reader::fetch_schema(
                self.datasets_object_store.clone(),
                object.clone(),
                self.options.read_dimensions.clone(),
            );
            tasks.push(task);
        }
        let schemas = futures::future::try_join_all(tasks).await?;
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
        let source = NetCDFSource::new(
            self.datasets_object_store.clone(),
            self.options.read_dimensions.clone(),
        );
        let conf = FileScanConfigBuilder::from(conf)
            .with_source(Arc::new(source))
            .build();
        Ok(DataSourceExec::from_data_source(conf))
    }

    async fn create_writer_physical_plan(
        &self,
        _input: Arc<dyn ExecutionPlan>,
        _state: &dyn Session,
        _conf: FileSinkConfig,
        _order_requirements: Option<LexRequirement>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        todo!()
    }

    fn file_source(&self) -> Arc<dyn FileSource> {
        Arc::new(NetCDFSource::new(
            self.datasets_object_store.clone(),
            self.options.read_dimensions.clone(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use beacon_object_storage::get_datasets_object_store;
    use datafusion::datasource::listing::PartitionedFile;
    use datafusion::datasource::physical_plan::FileMeta;
    use datafusion::execution::object_store::ObjectStoreUrl;
    use futures::StreamExt;
    use object_store::path::Path;
    use std::path::PathBuf;
    use std::sync::Once;

    static TEST_FIXTURES: Once = Once::new();

    fn ensure_test_fixtures() {
        TEST_FIXTURES.call_once(|| {
            let dst_dir: PathBuf =
                beacon_config::DATASETS_DIR_PATH.join("beacon-arrow-netcdf-tests");
            std::fs::create_dir_all(&dst_dir).expect("create test dir");

            for name in ["wod_ctd_1964.nc", "gridded-example.nc"] {
                let src = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                    .join("test_files")
                    .join(name);
                let dst = dst_dir.join(name);
                if !dst.exists() {
                    std::fs::copy(&src, &dst)
                        .unwrap_or_else(|e| panic!("copy {name} into datasets dir: {e}"));
                }
            }
        });
    }

    fn wod_object_meta() -> ObjectMeta {
        ObjectMeta {
            location: Path::from("beacon-arrow-netcdf-tests/wod_ctd_1964.nc"),
            last_modified: chrono::Utc::now(),
            size: 0,
            e_tag: None,
            version: None,
        }
    }

    fn gridded_object_meta() -> ObjectMeta {
        ObjectMeta {
            location: Path::from("beacon-arrow-netcdf-tests/gridded-example.nc"),
            last_modified: chrono::Utc::now(),
            size: 0,
            e_tag: None,
            version: None,
        }
    }

    async fn test_store() -> Arc<DatasetsStore> {
        ensure_test_fixtures();
        get_datasets_object_store().await
    }

    fn test_format(store: Arc<DatasetsStore>) -> NetcdfFormat {
        NetcdfFormat::new(store, NetcdfOptions::default())
    }

    // ── fetch_schema ───────────────────────────────────────────────────

    #[tokio::test]
    async fn fetch_schema_returns_fields_for_ragged_file() {
        let store = test_store().await;
        let schema = reader::fetch_schema(store, wod_object_meta(), None)
            .await
            .expect("schema");
        assert!(!schema.fields().is_empty());
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert!(names.contains(&"lat"), "expected lat in {names:?}");
        assert!(names.contains(&"lon"), "expected lon in {names:?}");
        assert!(names.contains(&"z"), "expected z in {names:?}");
        assert!(
            names.contains(&"Temperature"),
            "expected Temperature in {names:?}"
        );
    }

    #[tokio::test]
    async fn fetch_schema_returns_fields_for_gridded_file() {
        let store = test_store().await;
        let schema = reader::fetch_schema(store, gridded_object_meta(), None)
            .await
            .expect("schema");
        assert!(!schema.fields().is_empty());
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert!(
            names.contains(&"analysed_sst"),
            "expected analysed_sst in {names:?}"
        );
    }

    #[tokio::test]
    async fn fetch_schema_with_dimensions_limits_fields() {
        let store = test_store().await;
        let full = reader::fetch_schema(store.clone(), gridded_object_meta(), None)
            .await
            .expect("full schema");
        let projected =
            reader::fetch_schema(store, gridded_object_meta(), Some(vec!["time".to_string()]))
                .await
                .expect("projected schema");
        assert!(
            projected.fields().len() < full.fields().len(),
            "dimension projection should reduce fields: projected {} vs full {}",
            projected.fields().len(),
            full.fields().len(),
        );
    }

    // ── infer_schema ───────────────────────────────────────────────────

    #[tokio::test]
    async fn infer_schema_single_file() {
        let store = test_store().await;
        let format = test_format(store.clone());
        let dummy_store: Arc<dyn ObjectStore> =
            Arc::new(object_store::local::LocalFileSystem::new());
        let ctx = datafusion::prelude::SessionContext::new();

        let schema = format
            .infer_schema(&ctx.state(), &dummy_store, &[gridded_object_meta()])
            .await
            .expect("infer schema");
        assert!(!schema.fields().is_empty());
    }

    #[tokio::test]
    async fn infer_schema_multiple_files_merges() {
        let store = test_store().await;
        let format = test_format(store.clone());
        let dummy_store: Arc<dyn ObjectStore> =
            Arc::new(object_store::local::LocalFileSystem::new());
        let ctx = datafusion::prelude::SessionContext::new();

        let schema = format
            .infer_schema(
                &ctx.state(),
                &dummy_store,
                &[gridded_object_meta(), gridded_object_meta()],
            )
            .await
            .expect("merged schema");
        assert!(!schema.fields().is_empty());
    }

    #[tokio::test]
    async fn infer_schema_empty_objects_returns_error() {
        let store = test_store().await;
        let format = test_format(store.clone());
        let dummy_store: Arc<dyn ObjectStore> =
            Arc::new(object_store::local::LocalFileSystem::new());
        let ctx = datafusion::prelude::SessionContext::new();

        let result = format.infer_schema(&ctx.state(), &dummy_store, &[]).await;
        assert!(result.is_err());
    }

    // ── file_source ────────────────────────────────────────────────────

    #[tokio::test]
    async fn file_source_returns_netcdf_type() {
        let store = test_store().await;
        let format = test_format(store);
        let source = format.file_source();
        assert_eq!(source.file_type(), "netcdf");
    }

    // ── FileOpener produces batches ────────────────────────────────────

    #[tokio::test]
    async fn opener_streams_batches_for_ragged_file() {
        let store = test_store().await;
        let table_schema = reader::fetch_schema(store.clone(), wod_object_meta(), None)
            .await
            .expect("schema");

        let opener = source::NetCDFSource::new(store, None);
        let file_opener = {
            let conf = FileScanConfigBuilder::new(
                ObjectStoreUrl::local_filesystem(),
                table_schema.clone(),
                Arc::new(opener.clone()) as Arc<dyn FileSource>,
            )
            .build();
            opener.create_file_opener(
                Arc::new(object_store::local::LocalFileSystem::new()),
                &conf,
                0,
            )
        };

        let file_meta = FileMeta {
            object_meta: wod_object_meta(),
            range: None,
            extensions: None,
            metadata_size_hint: None,
        };

        let stream = file_opener
            .open(file_meta, PartitionedFile::new("ignored", 0))
            .expect("open")
            .await
            .expect("stream future");

        let batches: Vec<_> = stream.collect().await;
        assert!(!batches.is_empty(), "should produce at least one batch");

        let first = batches[0].as_ref().expect("first batch ok");
        assert!(first.num_columns() > 0);
        assert!(first.num_rows() > 0);
    }

    #[tokio::test]
    async fn opener_streams_batches_for_gridded_file() {
        let store = test_store().await;
        let table_schema = reader::fetch_schema(store.clone(), gridded_object_meta(), None)
            .await
            .expect("schema");

        let opener = source::NetCDFSource::new(store, None);
        let file_opener = {
            let conf = FileScanConfigBuilder::new(
                ObjectStoreUrl::local_filesystem(),
                table_schema.clone(),
                Arc::new(opener.clone()) as Arc<dyn FileSource>,
            )
            .build();
            opener.create_file_opener(
                Arc::new(object_store::local::LocalFileSystem::new()),
                &conf,
                0,
            )
        };

        let file_meta = FileMeta {
            object_meta: gridded_object_meta(),
            range: None,
            extensions: None,
            metadata_size_hint: None,
        };

        let stream = file_opener
            .open(file_meta, PartitionedFile::new("ignored", 0))
            .expect("open")
            .await
            .expect("stream future");

        let batches: Vec<_> = stream.collect().await;
        assert!(!batches.is_empty(), "should produce at least one batch");

        let first = batches[0].as_ref().expect("first batch ok");
        assert!(first.num_columns() > 0);
        assert!(first.num_rows() > 0);
    }

    #[tokio::test]
    async fn opener_with_projection_selects_columns() {
        let store = test_store().await;
        let table_schema = reader::fetch_schema(store.clone(), gridded_object_meta(), None)
            .await
            .expect("schema");

        // Project to only the first column.
        let projected_schema: SchemaRef = Arc::new(table_schema.project(&[0]).expect("project"));

        let opener = source::NetCDFSource::new(store, None);
        let file_opener = {
            let conf = FileScanConfigBuilder::new(
                ObjectStoreUrl::local_filesystem(),
                table_schema.clone(),
                Arc::new(opener.clone()) as Arc<dyn FileSource>,
            )
            .with_projection(Some(vec![0]))
            .build();
            opener.create_file_opener(
                Arc::new(object_store::local::LocalFileSystem::new()),
                &conf,
                0,
            )
        };

        let file_meta = FileMeta {
            object_meta: gridded_object_meta(),
            range: None,
            extensions: None,
            metadata_size_hint: None,
        };

        let stream = file_opener
            .open(file_meta, PartitionedFile::new("ignored", 0))
            .expect("open")
            .await
            .expect("stream future");

        let batches: Vec<_> = stream.collect().await;
        assert!(!batches.is_empty());

        let first = batches[0].as_ref().expect("first batch ok");
        assert_eq!(
            first.num_columns(),
            projected_schema.fields().len(),
            "batch should have only the projected columns"
        );
    }

    #[tokio::test]
    async fn opener_with_read_dimensions_limits_columns() {
        let store = test_store().await;
        // Full schema without dimension filter.
        let full_schema = reader::fetch_schema(store.clone(), gridded_object_meta(), None)
            .await
            .expect("full schema");

        // Schema with dimension filter.
        let dim_schema = reader::fetch_schema(
            store.clone(),
            gridded_object_meta(),
            Some(vec!["time".to_string()]),
        )
        .await
        .expect("dim schema");

        let opener = source::NetCDFSource::new(store, Some(vec!["time".to_string()]));
        let file_opener = {
            let conf = FileScanConfigBuilder::new(
                ObjectStoreUrl::local_filesystem(),
                dim_schema.clone(),
                Arc::new(opener.clone()) as Arc<dyn FileSource>,
            )
            .build();
            opener.create_file_opener(
                Arc::new(object_store::local::LocalFileSystem::new()),
                &conf,
                0,
            )
        };

        let file_meta = FileMeta {
            object_meta: gridded_object_meta(),
            range: None,
            extensions: None,
            metadata_size_hint: None,
        };

        let stream = file_opener
            .open(file_meta, PartitionedFile::new("ignored", 0))
            .expect("open")
            .await
            .expect("stream future");

        let batches: Vec<_> = stream.collect().await;
        assert!(!batches.is_empty());

        let first = batches[0].as_ref().expect("first batch ok");
        assert!(
            first.num_columns() < full_schema.fields().len(),
            "dimension-filtered batch should have fewer columns ({}) than full ({})",
            first.num_columns(),
            full_schema.fields().len(),
        );
    }
}
