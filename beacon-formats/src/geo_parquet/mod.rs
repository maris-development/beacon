use std::{any::Any, fmt::Debug, sync::Arc};

use arrow::datatypes::SchemaRef;
use datafusion::{
    catalog::Session,
    common::{GetExt, Statistics},
    datasource::{
        file_format::{FileFormat, FileFormatFactory, file_compression_type::FileCompressionType},
        physical_plan::{FileScanConfig, FileSinkConfig, FileSource},
        sink::{DataSink, DataSinkExec},
    },
    physical_expr::LexRequirement,
    physical_plan::ExecutionPlan,
};

use object_store::{ObjectMeta, ObjectStore};

pub mod sink;

const GEOPARQUET_EXTENSION: &str = "geoparquet";

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct GeoParquetOptions {
    pub longitude_column: Option<String>,
    pub latitude_column: Option<String>,
}

#[derive(Debug, Clone)]
pub struct GeoParquetFormatFactory {
    pub options: GeoParquetOptions,
}

impl GeoParquetFormatFactory {
    pub fn new(options: GeoParquetOptions) -> Self {
        Self { options }
    }
}

impl Default for GeoParquetFormatFactory {
    fn default() -> Self {
        Self {
            options: GeoParquetOptions {
                longitude_column: None,
                latitude_column: None,
            },
        }
    }
}

impl FileFormatFactory for GeoParquetFormatFactory {
    fn create(
        &self,
        _state: &dyn Session,
        _format_options: &std::collections::HashMap<String, String>,
    ) -> datafusion::error::Result<Arc<dyn FileFormat>> {
        Ok(Arc::new(GeoParquetFormat::new(self.options.clone())))
    }

    fn default(&self) -> Arc<dyn FileFormat> {
        Arc::new(GeoParquetFormat::new(self.options.clone()))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl GetExt for GeoParquetFormatFactory {
    fn get_ext(&self) -> String {
        GEOPARQUET_EXTENSION.to_string()
    }
}

#[derive(Debug, Clone)]
pub struct GeoParquetFormat {
    pub options: GeoParquetOptions,
}

impl GeoParquetFormat {
    pub fn new(options: GeoParquetOptions) -> Self {
        Self { options }
    }
}

#[async_trait::async_trait]
impl FileFormat for GeoParquetFormat {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn compression_type(&self) -> Option<FileCompressionType> {
        None
    }

    fn get_ext(&self) -> String {
        GEOPARQUET_EXTENSION.to_string()
    }

    fn get_ext_with_compression(
        &self,
        _file_compression_type: &FileCompressionType,
    ) -> datafusion::error::Result<String> {
        Ok(GEOPARQUET_EXTENSION.to_string())
    }

    async fn infer_schema(
        &self,
        _state: &dyn Session,
        _store: &Arc<dyn ObjectStore>,
        _objects: &[ObjectMeta],
    ) -> datafusion::error::Result<SchemaRef> {
        return Err(datafusion::error::DataFusionError::NotImplemented(
            "GeoParquet format does not support schema inference yet".to_string(),
        ));
    }

    async fn infer_stats(
        &self,
        _state: &dyn Session,
        _store: &Arc<dyn ObjectStore>,
        _table_schema: SchemaRef,
        _object: &ObjectMeta,
    ) -> datafusion::error::Result<Statistics> {
        return Err(datafusion::error::DataFusionError::NotImplemented(
            "GeoParquet format does not support statistics inference yet".to_string(),
        ));
    }

    async fn create_physical_plan(
        &self,
        _state: &dyn Session,
        _conf: FileScanConfig,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        return Err(datafusion::error::DataFusionError::NotImplemented(
            "GeoParquet format does not support physical plan creation yet".to_string(),
        ));
    }

    async fn create_writer_physical_plan(
        &self,
        input: Arc<dyn ExecutionPlan>,
        state: &dyn Session,
        conf: FileSinkConfig,
        order_requirements: Option<LexRequirement>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let object_store = state.runtime_env().object_store(&conf.object_store_url)?;

        let schema = input.schema();
        let columns = schema
            .fields()
            .iter()
            .map(|f| f.name().to_string())
            .collect::<Vec<_>>();
        // Find longitude column
        let longitude = if let Some(lon_col) = &self.options.longitude_column {
            if columns.iter().any(|c| c == lon_col) {
                lon_col.clone()
            } else {
                return Err(datafusion::error::DataFusionError::Execution(format!(
                    "Specified longitude column '{}' not found in input schema",
                    lon_col
                )));
            }
        } else {
            match columns.iter().find(|c| is_lon_column(c)) {
                Some(col) => col.clone(),
                None => {
                    return Err(datafusion::error::DataFusionError::Execution(
                        "Could not automatically find longitude column. Please specify it in the options.".to_string(),
                    ));
                }
            }
        };
        // Find latitude column
        let latitude = if let Some(lat_col) = &self.options.latitude_column {
            if columns.iter().any(|c| c == lat_col) {
                lat_col.clone()
            } else {
                return Err(datafusion::error::DataFusionError::Execution(format!(
                    "Specified latitude column '{}' not found in input schema",
                    lat_col
                )));
            }
        } else {
            match columns.iter().find(|c| is_lat_column(c)) {
                Some(col) => col.clone(),
                None => {
                    return Err(datafusion::error::DataFusionError::Execution(
                        "Could not automatically find latitude column. Please specify it in the options.".to_string(),
                    ));
                }
            }
        };

        let sink = Arc::new(sink::GeoParquetSink::new(
            input.clone(),
            conf,
            object_store,
            &longitude,
            &latitude,
        )) as Arc<dyn DataSink>;

        Ok(Arc::new(DataSinkExec::new(input, sink, order_requirements)))
    }

    fn file_source(&self) -> Arc<dyn FileSource> {
        panic!("GeoParquetFormat does not support file source");
    }
}

fn is_lat_column(name: &str) -> bool {
    const LAT_VARIANTS: [&str; 4] = ["lat", "latitude", "y", "northing"];
    let lower = name.to_lowercase();
    LAT_VARIANTS.iter().any(|&pat| lower.contains(pat))
}

fn is_lon_column(name: &str) -> bool {
    const LON_VARIANTS: [&str; 5] = ["lon", "long", "lng", "longitude", "easting"];
    let lower = name.to_lowercase();
    LON_VARIANTS
        .iter()
        .any(|&pat| lower.contains(pat) || lower == "x")
}
