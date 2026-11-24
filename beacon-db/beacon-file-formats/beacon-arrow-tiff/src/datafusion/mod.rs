use std::sync::Arc;

use arrow::datatypes::{Schema, SchemaRef};
use beacon_datafusion_ext::format_ext::{DatasetMetadata, FileFormatFactoryExt, SchemaOptions};
use beacon_datafusion_ext::type_widening::{label_by_object, session_widening};
use beacon_nd_execution::exec::nd_scan_plan;
use beacon_nd_scan::{NdScanOptions, NdSource, encoded_table_schema, file_schema, open_for_read};
use datafusion::{
    catalog::Session,
    common::{GetExt, Statistics, exec_datafusion_err},
    datasource::{
        file_format::{FileFormat, FileFormatFactory, file_compression_type::FileCompressionType},
        physical_plan::{FileScanConfig, FileScanConfigBuilder, FileSource},
        table_schema::TableSchema,
    },
    error::{DataFusionError, Result},
    physical_plan::ExecutionPlan,
};
use futures::{StreamExt, TryStreamExt};
use object_store::{ObjectMeta, ObjectStore};

use crate::TiffOpener;
use crate::datafusion::options::TiffOptions;

const TIFF_EXTENSION: &str = "tiff";
const TIF_EXTENSION: &str = "tif";

pub mod options;
pub mod table_function;
pub use table_function::ReadTiffFunc;

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
    ) -> Result<Arc<dyn FileFormat>> {
        Ok(Arc::new(TiffFormat::new(self.options.clone())))
    }

    fn default(&self) -> Arc<dyn FileFormat> {
        Arc::new(TiffFormat::new(self.options.clone()))
    }
}

impl GetExt for TiffFormatFactory {
    fn get_ext(&self) -> String {
        TIFF_EXTENSION.to_string()
    }
}

impl FileFormatFactoryExt for TiffFormatFactory {
    /// TIFF opts into the schema cache on its name alone. `TiffOptions` carries
    /// nothing today, so there is nothing else to tell two reads apart.
    fn schema_options_fingerprint(&self, format: &dyn FileFormat) -> Option<u64> {
        format.downcast_ref::<TiffFormat>()?;
        Some(SchemaOptions::new("tiff").finish())
    }

    fn discover_datasets(&self, objects: &[ObjectMeta]) -> Result<Vec<DatasetMetadata>> {
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

    fn file_extensions(&self) -> Vec<String> {
        vec![TIFF_EXTENSION.to_string(), TIF_EXTENSION.to_string()]
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

    fn scan_options() -> NdScanOptions {
        NdScanOptions {
            min_slice_size: Some(0),
            ..NdScanOptions::default()
        }
    }

    fn source(&self, table_schema: TableSchema) -> NdSource {
        NdSource::new(Arc::new(TiffOpener), Self::scan_options(), table_schema)
    }

    /// The schema of one file.
    async fn file_schema(
        &self,
        store: &Arc<dyn ObjectStore>,
        object: &ObjectMeta,
    ) -> Result<SchemaRef> {
        let opened = open_for_read(
            &TiffOpener,
            Arc::clone(store),
            object,
            &Self::scan_options(),
        )
        .await?;
        let Some(dataset) = opened else {
            return Ok(Arc::new(Schema::empty()));
        };
        Ok(Arc::new(file_schema(dataset.as_ref())))
    }
}

#[async_trait::async_trait]
impl FileFormat for TiffFormat {
    fn compression_type(&self) -> Option<FileCompressionType> {
        None
    }

    fn get_ext(&self) -> String {
        TIFF_EXTENSION.to_string()
    }

    fn get_ext_with_compression(
        &self,
        _file_compression_type: &FileCompressionType,
    ) -> Result<String> {
        Ok(TIFF_EXTENSION.to_string())
    }

    async fn infer_schema(
        &self,
        state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> Result<SchemaRef> {
        // Bounded: each open holds a descriptor until its schema is read (issue #361).
        let width = state
            .config_options()
            .execution
            .meta_fetch_concurrency
            .max(1);
        // An index, not a borrowed `ObjectMeta`: `async_trait` rejects a closure
        // argument that holds a reference.
        let schemas: Vec<SchemaRef> = futures::stream::iter(0..objects.len())
            .map(|index| async move {
                let object = &objects[index];
                self.file_schema(store, object).await.map_err(|e| {
                    exec_datafusion_err!(
                        "Failed to infer the schema of TIFF file '{}': {}",
                        object.location,
                        e.message()
                    )
                })
            })
            .buffered(width)
            .try_collect()
            .await?;
        if schemas.is_empty() {
            return Ok(Arc::new(Schema::empty()));
        }

        // The rule of the session decides the result for a column that two
        // files describe differently. Each schema names its file, so a refused
        // column names both files.
        session_widening(state)
            .merge_schemas(&label_by_object(objects, &schemas))
            .map_err(|e| {
                exec_datafusion_err!("Failed to merge the schemas of the TIFF datasets: {}", e)
            })
    }

    async fn infer_stats(
        &self,
        _state: &dyn Session,
        _store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        _object: &ObjectMeta,
    ) -> Result<Statistics> {
        Ok(Statistics::new_unknown(&table_schema))
    }

    async fn create_physical_plan(
        &self,
        _state: &dyn Session,
        conf: FileScanConfig,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let partition_columns: Vec<&str> = conf
            .table_partition_cols()
            .iter()
            .map(|field| field.name().as_str())
            .collect();
        if !partition_columns.is_empty() {
            return Err(DataFusionError::NotImplemented(format!(
                "TIFF does not support partitioned tables (PARTITIONED BY {})",
                partition_columns.join(", ")
            )));
        }
        // Keep a projection that the scan pushed into the incoming source.
        let projection = conf.file_source().projection().cloned();
        let source = self
            .source(encoded_table_schema(conf.file_schema()))
            .with_projection(projection);
        let conf = FileScanConfigBuilder::from(conf)
            .with_source(Arc::new(source))
            .build();
        nd_scan_plan(conf)
    }

    fn file_source(&self, table_schema: TableSchema) -> Arc<dyn FileSource> {
        Arc::new(self.source(table_schema))
    }
}
