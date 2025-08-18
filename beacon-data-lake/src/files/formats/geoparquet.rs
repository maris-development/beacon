use std::{
    any::Any,
    fmt::{Debug, Formatter},
    io::BufWriter,
    sync::Arc,
};

use arrow::{
    array::AsArray,
    datatypes::{Float64Type, Schema, SchemaRef},
};
use datafusion::{
    catalog::Session,
    common::{GetExt, Statistics},
    datasource::{
        file_format::{FileFormat, FileFormatFactory, file_compression_type::FileCompressionType},
        physical_plan::{FileScanConfig, FileSinkConfig, FileSource},
        sink::{DataSink, DataSinkExec},
    },
    execution::{SendableRecordBatchStream, TaskContext},
    physical_expr::LexRequirement,
    physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan},
};
use futures::StreamExt;

use geoarrow::array::AsGeoArrowArray;
use geoarrow::datatypes::{Dimension, Metadata, PointType};
use geoarrow_array::GeoArrowArray;
use geoparquet::writer::{GeoParquetRecordBatchEncoder, GeoParquetWriterOptions};
use object_store::{ObjectMeta, ObjectStore};
use parquet::arrow::{async_writer::ParquetObjectWriter, AsyncArrowWriter};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct GeoParquetOptions {
    pub longitude_column: String,
    pub latitude_column: String,
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
        "geo_parquet".to_string()
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
        "geo_parquet".to_string()
    }

    fn get_ext_with_compression(
        &self,
        _file_compression_type: &FileCompressionType,
    ) -> datafusion::error::Result<String> {
        Ok("geo_parquet".to_string())
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
        _state: &dyn Session,
        conf: FileSinkConfig,
        order_requirements: Option<LexRequirement>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let sink = Arc::new(GeoParquetSink::new(
            input.clone(),
            conf,
            self.options.clone(),
        ));

        Ok(Arc::new(DataSinkExec::new(input, sink, order_requirements)))
    }

    fn file_source(&self) -> Arc<dyn FileSource> {
        panic!("GeoParquetFormat does not support file source");
    }
}

struct GeoMapper {
    longitude_idx: usize,
    latitude_idx: usize,
    output_schema: SchemaRef,
}

impl GeoMapper {
    pub fn new(input_schema: &Schema, geo_options: &GeoParquetOptions) -> Self {
        let longitude_idx = input_schema
            .index_of(&geo_options.longitude_column)
            .expect("Longitude column not found in schema");
        let latitude_idx = input_schema
            .index_of(&geo_options.latitude_column)
            .expect("Latitude column not found in schema");

        // Create a new schema with the geometry field added
        let mut fields = input_schema.fields().clone().to_vec();

        let point_type = PointType::new(
            geoarrow::datatypes::Dimension::XY,
            Arc::new(Metadata::default()),
        );
        let point_field = point_type.to_field("geometry", true);
        fields.push(Arc::new(point_field));

        let output_schema = Arc::new(Schema::new(fields));

        Self {
            longitude_idx,
            latitude_idx,
            output_schema,
        }
    }

    pub fn map(
        &self,
        batch: &arrow::record_batch::RecordBatch,
    ) -> arrow::record_batch::RecordBatch {
        let longitude_array = batch.column(self.longitude_idx);
        let latitude_array = batch.column(self.latitude_idx);

        let mut point_builder = geoarrow::array::PointBuilder::new(PointType::new(
            geoarrow::datatypes::Dimension::XY,
            Arc::new(Metadata::default()),
        ));

        // Cast the longitude and latitude arrays to Float64Array
        let casted_longitude = arrow::compute::cast_with_options(
            longitude_array,
            &arrow::datatypes::DataType::Float64,
            &arrow::compute::CastOptions {
                safe: true,
                format_options: Default::default(),
            },
        )
        .unwrap();

        let casted_latitude = arrow::compute::cast_with_options(
            latitude_array,
            &arrow::datatypes::DataType::Float64,
            &arrow::compute::CastOptions {
                safe: true,
                format_options: Default::default(),
            },
        )
        .unwrap();

        let longitude_array = casted_longitude.as_primitive::<Float64Type>();
        let latitude_array = casted_latitude.as_primitive::<Float64Type>();

        // Zip and iterate
        latitude_array
            .iter()
            .zip(longitude_array.iter())
            .for_each(|(lat, lon)| {
                if let (Some(lat), Some(lon)) = (lat, lon) {
                    point_builder.push_coord(Some(&(lon, lat)));
                } else {
                    point_builder.push_null();
                }
            });

        let point_array = point_builder.finish();
        let arrow_point_array = point_array.to_array_ref();
        let mut current_columns = batch.columns().to_vec();
        current_columns.push(arrow_point_array);

        arrow::record_batch::RecordBatch::try_new(self.output_schema.clone(), current_columns)
            .expect("Failed to create Geo Arrow RecordBatch")
    }
}

struct GeoParquetSink {
    input: Arc<dyn ExecutionPlan>,
    file_sink_config: FileSinkConfig,
    object_store: Arc<dyn ObjectStore>,
    output_path: object_store::path::Path,
    mapper: GeoMapper,
}

impl GeoParquetSink {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        file_sink_config: FileSinkConfig,
        options: GeoParquetOptions,
    ) -> Self {
        let mapper = GeoMapper::new(input.schema().as_ref(), &options);

        Self {
            input,
            file_sink_config,
            mapper,
        }
    }

    fn create_writer(&self, output_path: &str) -> GeoParquetWriter<BufWriter<std::fs::File>> {
        let file = std::fs::File::create(output_path)
            .map_err(datafusion::error::DataFusionError::IoError)
            .unwrap();

        parquet::arrow::AsyncArrowWriter::

        let buf_writer = std::io::BufWriter::new(file);

        let options = GeoParquetWriterOptions {
            encoding: geoarrow::io::parquet::GeoParquetWriterEncoding::Native,
            ..Default::default()
        };

        geoarrow::io::parquet::GeoParquetWriter::try_new(
            buf_writer,
            &self.mapper.output_schema,
            &options,
        )
        .unwrap()
    }
}

impl Debug for GeoParquetSink {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "GeoParquetSink {{ input: {:?}}}", self.input)
    }
}

impl DisplayAs for GeoParquetSink {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "GeoParquetSink")
    }
}

#[async_trait::async_trait]
impl DataSink for GeoParquetSink {
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Returns the sink schema
    fn schema(&self) -> &SchemaRef {
        &self.mapper.output_schema
    }

    fn metrics(&self) -> Option<datafusion::physical_plan::metrics::MetricsSet> {
        None
    }

    async fn write_all(
        &self,
        data: SendableRecordBatchStream,
        _context: &Arc<TaskContext>,
    ) -> datafusion::error::Result<u64> {
        let output_path = format!(
            "{}/{}",
            beacon_config::DATA_DIR.to_string_lossy(),
            self.file_sink_config.table_paths[0].prefix()
        );
        let object_writer = ParquetObjectWriter::new(self.file_sink_config.object_store_url)
        let mut parquet_options = GeoParquetWriterOptions::default();
        let encoder = GeoParquetRecordBatchEncoder::try_new(&self.mapper.output_schema, &parquet_options).unwrap();
        let arrow_writer = AsyncArrowWriter::try_new(file, encoder.target_schema(), None).unwrap();

        let mut writer = self.create_writer(&output_path);

        let mut rows_written: u64 = 0;

        let mut pinned_steam = std::pin::pin!(data);

        while let Some(batch) = pinned_steam.next().await {
            let batch = batch?;
            let mapped_batch = self.mapper.map(&batch);
            writer.write_batch(&mapped_batch).unwrap();
            rows_written += mapped_batch.num_rows() as u64;
        }

        writer.finish().unwrap();

        Ok(rows_written)
    }
}
