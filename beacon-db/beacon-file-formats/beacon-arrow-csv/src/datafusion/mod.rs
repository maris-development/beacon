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

pub const DEFAULT_CSV_EXTENSION: &str = "csv";

#[derive(Debug, Default)]
pub struct CsvFormatFactory;

impl GetExt for CsvFormatFactory {
    fn get_ext(&self) -> String {
        DEFAULT_CSV_EXTENSION.to_string()
    }
}

/// The default number of records the CSV reader samples to infer a schema.
const DEFAULT_INFER_RECORDS: usize = 1000;

impl FileFormatFactory for CsvFormatFactory {
    fn create(
        &self,
        _state: &dyn Session,
        format_options: &std::collections::HashMap<String, String>,
    ) -> datafusion::error::Result<Arc<dyn FileFormat>> {
        // `CREATE EXTERNAL TABLE … STORED AS CSV OPTIONS ('delimiter' '\t', …)`.
        // DataFusion prefixes bare OPTIONS keys with `format.`, so look up both
        // spellings, and resolve escapes (`\t`) the same way `read_csv` does.
        let option = |key: &str| {
            format_options
                .get(key)
                .or_else(|| format_options.get(&format!("format.{key}")))
        };

        let delimiter = match option("delimiter") {
            Some(value) => crate::parse_delimiter(value)
                .map_err(datafusion::error::DataFusionError::Execution)?,
            None => b',',
        };
        let infer_records = option("infer_records")
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(DEFAULT_INFER_RECORDS);

        Ok(Arc::new(CsvFormat::new(delimiter, infer_records)))
    }

    fn default(&self) -> Arc<dyn FileFormat> {
        Arc::new(CsvFormat::new(b',', DEFAULT_INFER_RECORDS))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl FileFormatFactoryExt for CsvFormatFactory {
    fn discover_datasets(
        &self,
        objects: &[ObjectMeta],
    ) -> datafusion::error::Result<Vec<DatasetMetadata>> {
        let datasets = objects
            .iter()
            .filter(|obj| {
                obj.location
                    .extension()
                    .map(|ext| ext == "csv" || ext == "tsv")
                    .unwrap_or(false)
            })
            .map(|obj| DatasetMetadata::new(obj.location.to_string(), self.get_ext()))
            .collect();
        Ok(datasets)
    }

    fn file_format_name(&self) -> String {
        self.get_ext()
    }

    fn file_extensions(&self) -> Vec<String> {
        vec!["csv".to_string(), "tsv".to_string()]
    }
}

#[derive(Debug)]
pub struct CsvFormat {
    inner_format: datafusion::datasource::file_format::csv::CsvFormat,
}

impl CsvFormat {
    pub fn new(delimiter: u8, infer_records: usize) -> Self {
        Self {
            inner_format: datafusion::datasource::file_format::csv::CsvFormat::default()
                .with_delimiter(delimiter)
                .with_schema_infer_max_rec(infer_records),
        }
    }
}

#[async_trait::async_trait]
impl FileFormat for CsvFormat {
    fn as_any(&self) -> &dyn Any {
        self
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

        //Supertype the schema
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
pub use table_function::ReadCsvFunc;

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::{ObjectStoreExt, PutPayload};

    /// Materializes empty objects at `locations` in a scratch store and returns
    /// their metadata; `discover_datasets` only inspects the location.
    async fn metas(locations: &[&str]) -> Vec<ObjectMeta> {
        let store = Arc::new(InMemory::new());
        let mut out = Vec::new();
        for loc in locations {
            let path = Path::from(*loc);
            out.push(put(&store, &path, "").await);
        }
        out
    }

    /// Writes `bytes` to an in-memory store and returns the resulting metadata.
    async fn put(store: &Arc<InMemory>, path: &Path, bytes: &str) -> ObjectMeta {
        store
            .put(path, PutPayload::from(bytes.to_string()))
            .await
            .expect("should write CSV fixture");
        store.head(path).await.expect("should stat CSV fixture")
    }

    /// The factory must claim both `csv` and its `tsv` alias, because the session
    /// registry only keys formats by `get_ext()` and alias resolution relies on
    /// `file_extensions()`.
    #[test]
    fn factory_advertises_csv_and_tsv_extensions() {
        let factory = CsvFormatFactory;
        assert_eq!(factory.get_ext(), DEFAULT_CSV_EXTENSION);
        assert_eq!(factory.file_format_name(), "csv");
        assert_eq!(factory.file_extensions(), vec!["csv", "tsv"]);
    }

    /// Dataset discovery must accept both spellings and skip anything else, so a
    /// mixed directory listing does not produce bogus CSV datasets.
    #[tokio::test]
    async fn discover_datasets_selects_only_csv_and_tsv_objects() {
        let objects = metas(&[
            "a/data.csv",
            "a/data.tsv",
            "a/data.parquet",
            "a/no_extension",
        ])
        .await;
        let datasets = CsvFormatFactory.discover_datasets(&objects).unwrap();
        let paths: Vec<&str> = datasets.iter().map(|d| d.file_path.as_str()).collect();
        assert_eq!(paths, vec!["a/data.csv", "a/data.tsv"]);
        assert!(datasets.iter().all(|d| d.format == "csv"));
    }

    /// Schemas of multiple CSV files are merged with Beacon's super-typing rather
    /// than taken from the first file: an Int64 column plus a Float64 column of the
    /// same name must widen to Float64.
    #[tokio::test]
    async fn infer_schema_super_types_columns_across_files() {
        let store = Arc::new(InMemory::new());
        let object_store: Arc<dyn ObjectStore> = store.clone();
        let a = put(&store, &Path::from("ints.csv"), "value\n1\n2\n").await;
        let b = put(&store, &Path::from("floats.csv"), "value\n1.5\n2.5\n").await;

        let ctx = SessionContext::new();
        let format = CsvFormat::new(b',', 1000);
        let schema = format
            .infer_schema(&ctx.state(), &object_store, &[a, b])
            .await
            .expect("both CSV files should be inferable");

        assert_eq!(schema.fields().len(), 1);
        assert_eq!(schema.field(0).name(), "value");
        assert_eq!(
            schema.field(0).data_type(),
            &arrow::datatypes::DataType::Float64
        );
    }

    /// A non-default delimiter must reach the wrapped DataFusion format, otherwise
    /// `read_csv(..., ';')` would infer a single blob column.
    #[tokio::test]
    async fn infer_schema_honors_custom_delimiter() {
        let store = Arc::new(InMemory::new());
        let object_store: Arc<dyn ObjectStore> = store.clone();
        let obj = put(&store, &Path::from("semi.csv"), "a;b\n1;2\n").await;

        let ctx = SessionContext::new();
        let schema = CsvFormat::new(b';', 100)
            .infer_schema(&ctx.state(), &object_store, &[obj.clone()])
            .await
            .expect("semicolon CSV should be inferable");
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, vec!["a", "b"]);

        // With the default comma the same bytes are one column named "a;b".
        let schema = CsvFormat::new(b',', 100)
            .infer_schema(&ctx.state(), &object_store, &[obj])
            .await
            .expect("comma parse should still succeed");
        assert_eq!(schema.fields().len(), 1);
    }

    /// Compression must be reflected in the written extension so that COPY TO
    /// produces `foo.csv.gz` rather than `foo.csv`.
    #[test]
    fn extension_reflects_compression_type() {
        let format = CsvFormat::new(b',', 1000);
        assert_eq!(format.get_ext(), "csv");
        assert_eq!(
            format.compression_type(),
            Some(FileCompressionType::UNCOMPRESSED)
        );
        assert_eq!(
            format
                .get_ext_with_compression(&FileCompressionType::UNCOMPRESSED)
                .unwrap(),
            "csv"
        );
        assert_eq!(
            format
                .get_ext_with_compression(&FileCompressionType::GZIP)
                .unwrap(),
            "csv.gz"
        );
    }
}
