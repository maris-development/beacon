use std::{any::Any, sync::Arc};

use arrow::datatypes::SchemaRef;
use beacon_datafusion_ext::format_ext::DatasetMetadata;
use datafusion::{
    catalog::Session,
    common::{GetExt, Statistics},
    datasource::{
        file_format::{FileFormat, FileFormatFactory, file_compression_type::FileCompressionType},
        physical_plan::{FileScanConfig, FileSinkConfig, FileSource},
    },
    physical_expr::LexRequirement,
    physical_plan::ExecutionPlan,
};
use object_store::{ObjectMeta, ObjectStore};

use beacon_common::super_typing::super_type_schema;
use futures::{StreamExt, TryStreamExt, stream};

use beacon_common::file_descriptors::file_open_parallelism;
use beacon_datafusion_ext::format_ext::FileFormatFactoryExt;

pub const DEFAULT_ARROW_EXTENSION: &str = "arrow";

#[derive(Debug)]
pub struct ArrowFormatFactory;

impl FileFormatFactory for ArrowFormatFactory {
    fn create(
        &self,
        _state: &dyn Session,
        _format_options: &std::collections::HashMap<String, String>,
    ) -> datafusion::error::Result<Arc<dyn FileFormat>> {
        Ok(Arc::new(ArrowFormat::new()))
    }

    fn default(&self) -> Arc<dyn FileFormat> {
        Arc::new(ArrowFormat::new())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl GetExt for ArrowFormatFactory {
    fn get_ext(&self) -> String {
        DEFAULT_ARROW_EXTENSION.to_string()
    }
}

impl FileFormatFactoryExt for ArrowFormatFactory {
    fn discover_datasets(
        &self,
        objects: &[ObjectMeta],
    ) -> datafusion::error::Result<Vec<DatasetMetadata>> {
        let datasets = objects
            .iter()
            .filter(|obj| {
                obj.location
                    .extension()
                    .map(|ext| ext == DEFAULT_ARROW_EXTENSION || ext == "feather")
                    .unwrap_or(false)
            })
            .map(|obj| {
                DatasetMetadata::new(
                    obj.location.to_string(),
                    DEFAULT_ARROW_EXTENSION.to_string(),
                )
            })
            .collect();
        Ok(datasets)
    }

    fn file_format_name(&self) -> String {
        self.get_ext()
    }

    fn file_extensions(&self) -> Vec<String> {
        vec!["arrow".to_string(), "feather".to_string()]
    }
}

#[derive(Debug)]
pub struct ArrowFormat {
    inner_format: datafusion::datasource::file_format::arrow::ArrowFormat,
}

impl ArrowFormat {
    pub fn new() -> Self {
        Self {
            inner_format: datafusion::datasource::file_format::arrow::ArrowFormat,
        }
    }
}

impl Default for ArrowFormat {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl FileFormat for ArrowFormat {
    fn as_any(&self) -> &dyn Any {
        self.inner_format.as_any()
    }

    fn compression_type(&self) -> Option<FileCompressionType> {
        self.inner_format.compression_type()
    }

    fn get_ext(&self) -> String {
        self.inner_format.get_ext()
    }

    fn get_ext_with_compression(
        &self,
        _file_compression_type: &FileCompressionType,
    ) -> datafusion::error::Result<String> {
        self.inner_format
            .get_ext_with_compression(_file_compression_type)
    }

    async fn infer_schema(
        &self,
        state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> datafusion::error::Result<SchemaRef> {
        //Retrieve the schema for each object
        let schemas = stream::iter(objects.iter().cloned())
            .map(|object| {
                let store = Arc::clone(store);
                async move {
                    self.inner_format
                        .infer_schema(state, &store, &[object])
                        .await
                }
            })
            .buffer_unordered(file_open_parallelism()) // tune this
            .try_collect::<Vec<_>>()
            .await?;

        let super_schema = super_type_schema(&schemas).map_err(|e| {
            datafusion::error::DataFusionError::Execution(format!("Failed to infer schema: {}", e))
        })?;

        Ok(Arc::new(super_schema))
    }

    async fn infer_stats(
        &self,
        state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        object: &ObjectMeta,
    ) -> datafusion::error::Result<Statistics> {
        self.inner_format
            .infer_stats(state, store, table_schema, object)
            .await
    }

    async fn create_physical_plan(
        &self,
        state: &dyn Session,
        conf: FileScanConfig,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        self.inner_format.create_physical_plan(state, conf).await
    }

    async fn create_writer_physical_plan(
        &self,
        input: Arc<dyn ExecutionPlan>,
        state: &dyn Session,
        conf: FileSinkConfig,
        order_requirements: Option<LexRequirement>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        self.inner_format
            .create_writer_physical_plan(input, state, conf, order_requirements)
            .await
    }

    fn file_source(
        &self,
        table_schema: datafusion::datasource::table_schema::TableSchema,
    ) -> Arc<dyn FileSource> {
        self.inner_format.file_source(table_schema)
    }
}

pub mod table_function;
pub use table_function::ReadArrowFunc;

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int64Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::prelude::SessionContext;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::{ObjectStoreExt, PutPayload};

    /// Encodes a one-column batch and stores it, returning the object metadata the
    /// format needs. `as_file` selects the IPC *file* container (magic + footer)
    /// over the IPC *stream* container.
    async fn put_ipc(
        store: &Arc<InMemory>,
        path: &Path,
        field: Field,
        column: arrow::array::ArrayRef,
        as_file: bool,
    ) -> ObjectMeta {
        let schema = Arc::new(Schema::new(vec![field]));
        let batch = RecordBatch::try_new(schema.clone(), vec![column]).expect("valid batch");
        let mut buf: Vec<u8> = Vec::new();
        if as_file {
            let mut writer =
                arrow::ipc::writer::FileWriter::try_new(&mut buf, &schema).expect("ipc writer");
            writer.write(&batch).expect("write batch");
            writer.finish().expect("finish ipc file");
        } else {
            let mut writer =
                arrow::ipc::writer::StreamWriter::try_new(&mut buf, &schema).expect("ipc writer");
            writer.write(&batch).expect("write batch");
            writer.finish().expect("finish ipc stream");
        }
        store
            .put(path, PutPayload::from(buf))
            .await
            .expect("should write arrow fixture");
        store.head(path).await.expect("should stat arrow fixture")
    }

    /// Materializes empty objects so extension-only logic can be exercised.
    async fn metas(locations: &[&str]) -> Vec<ObjectMeta> {
        let store = Arc::new(InMemory::new());
        let mut out = Vec::new();
        for loc in locations {
            let path = Path::from(*loc);
            store
                .put(&path, PutPayload::from(Vec::new()))
                .await
                .expect("put");
            out.push(store.head(&path).await.expect("head"));
        }
        out
    }

    /// The Arrow format is also spelled `.feather`; both must resolve to the same
    /// canonical `arrow` format identity.
    #[test]
    fn factory_advertises_arrow_and_feather_extensions() {
        let factory = ArrowFormatFactory;
        assert_eq!(factory.get_ext(), DEFAULT_ARROW_EXTENSION);
        assert_eq!(factory.file_format_name(), "arrow");
        assert_eq!(factory.file_extensions(), vec!["arrow", "feather"]);
    }

    /// Discovery must keep both spellings, label them with the canonical format
    /// name, and skip unrelated files.
    #[tokio::test]
    async fn discover_datasets_accepts_arrow_and_feather_only() {
        let objects = metas(&[
            "d/a.arrow",
            "d/b.feather",
            "d/c.csv",
            "d/plain",
            "d/e.arrows",
        ])
        .await;
        let datasets = ArrowFormatFactory.discover_datasets(&objects).unwrap();
        let paths: Vec<&str> = datasets.iter().map(|d| d.file_path.as_str()).collect();
        assert_eq!(paths, vec!["d/a.arrow", "d/b.feather"]);
        assert!(datasets.iter().all(|d| d.format == DEFAULT_ARROW_EXTENSION));
    }

    /// Schema inference across several IPC files goes through Beacon's supertyping,
    /// so an Int64 and a Float64 column of the same name widen to Float64 instead
    /// of erroring or silently taking the first file's type.
    #[tokio::test]
    async fn infer_schema_super_types_across_multiple_files() {
        let store = Arc::new(InMemory::new());
        let object_store: Arc<dyn ObjectStore> = store.clone();
        let a = put_ipc(
            &store,
            &Path::from("ints.arrow"),
            Field::new("value", DataType::Int64, true),
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            false,
        )
        .await;
        let b = put_ipc(
            &store,
            &Path::from("floats.arrow"),
            Field::new("value", DataType::Float64, true),
            Arc::new(Float64Array::from(vec![1.5, 2.5])),
            false,
        )
        .await;

        let ctx = SessionContext::new();
        let schema = ArrowFormat::default()
            .infer_schema(&ctx.state(), &object_store, &[a, b])
            .await
            .expect("both IPC files should be readable");

        assert_eq!(schema.fields().len(), 1);
        assert_eq!(schema.field(0).data_type(), &DataType::Float64);
    }

    /// Files that are not valid Arrow IPC must surface as an error rather than an
    /// empty schema, otherwise a corrupt file would silently drop all its columns.
    #[tokio::test]
    async fn infer_schema_errors_on_non_arrow_bytes() {
        let store = Arc::new(InMemory::new());
        let object_store: Arc<dyn ObjectStore> = store.clone();
        let path = Path::from("garbage.arrow");
        store
            .put(&path, PutPayload::from(b"not an arrow file".to_vec()))
            .await
            .expect("put");
        let object = store.head(&path).await.expect("head");

        let ctx = SessionContext::new();
        let result = ArrowFormat::default()
            .infer_schema(&ctx.state(), &object_store, &[object])
            .await;
        assert!(result.is_err(), "malformed IPC file should not infer");
    }

    /// KNOWN LIMITATION (see report): when the object store hands back a *streamed*
    /// payload (in-memory, S3, HTTP — anything that is not a local file), DataFusion
    /// infers the schema with `infer_stream_schema`, which assumes the IPC *file*
    /// magic is padded to 8 bytes. `arrow` >= 54 pads it to 64, so an IPC file
    /// written by the current arrow version cannot be schema-inferred from a remote
    /// store. This test pins the behaviour so the day it starts working is visible.
    #[tokio::test]
    async fn infer_schema_of_ipc_file_container_fails_over_streaming_store() {
        let store = Arc::new(InMemory::new());
        let object_store: Arc<dyn ObjectStore> = store.clone();
        let obj = put_ipc(
            &store,
            &Path::from("file_container.arrow"),
            Field::new("value", DataType::Int64, true),
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            true,
        )
        .await;

        let ctx = SessionContext::new();
        let result = ArrowFormat::default()
            .infer_schema(&ctx.state(), &object_store, &[obj])
            .await;
        assert!(
            result.is_err(),
            "unexpected success: the 64-byte magic padding issue may have been fixed"
        );
    }

    /// The Arrow IPC format is uncompressed at the container level; the written
    /// extension must therefore stay `arrow`.
    #[test]
    fn extension_is_arrow_regardless_of_compression() {
        let format = ArrowFormat::default();
        assert_eq!(format.get_ext(), "arrow");
        assert_eq!(
            format
                .get_ext_with_compression(&FileCompressionType::UNCOMPRESSED)
                .unwrap(),
            "arrow"
        );
    }
}
