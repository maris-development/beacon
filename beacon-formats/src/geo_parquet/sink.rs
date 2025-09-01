use std::{any::Any, fmt::Formatter, sync::Arc};

use arrow::{
    array::{ArrayRef, AsArray},
    compute::{CastOptions, cast_with_options},
    datatypes::{DataType, Float64Type, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use datafusion::{
    datasource::{physical_plan::FileSinkConfig, sink::DataSink},
    execution::{SendableRecordBatchStream, TaskContext},
    physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan},
};
use futures::StreamExt;
use geoarrow::{
    array::PointBuilder,
    datatypes::{Dimension, Metadata, PointType},
};
use geoarrow_array::GeoArrowArray;
use geoparquet::writer::{GeoParquetRecordBatchEncoder, GeoParquetWriterOptionsBuilder};
use object_store::ObjectStore;
use parquet::arrow::{AsyncArrowWriter, async_writer::ParquetObjectWriter};

/// Maps longitude and latitude columns in an Arrow RecordBatch to a geometry column of Point type.
///
/// # Example
/// ```ignore
/// let mapper = GeoMapper::new(&input_schema, "lon", "lat");
/// let output_batch = mapper.map(&input_batch);
/// ```
pub struct GeoMapper {
    longitude_idx: usize,
    latitude_idx: usize,
    output_schema: SchemaRef,
}

impl GeoMapper {
    /// Creates a new `GeoMapper` given an input schema and the names of longitude and latitude columns.
    ///
    /// Adds a new geometry field to the output schema.
    pub fn new(input_schema: &Schema, longitude_column: &str, latitude_column: &str) -> Self {
        let longitude_idx = input_schema
            .index_of(longitude_column)
            .expect("Longitude column not found in schema");
        let latitude_idx = input_schema
            .index_of(latitude_column)
            .expect("Latitude column not found in schema");

        // Clone fields and add geometry field
        let mut fields = input_schema.fields().clone().to_vec();
        let point_type = PointType::new(Dimension::XY, Arc::new(Metadata::default()));
        let point_field = Arc::new(point_type.to_field("geometry", true));
        fields.push(point_field);

        let output_schema = Arc::new(Schema::new(fields));

        Self {
            longitude_idx,
            latitude_idx,
            output_schema,
        }
    }

    /// Maps a RecordBatch by adding a geometry column constructed from longitude and latitude columns.
    ///
    /// Returns a new RecordBatch with the geometry column appended.
    pub fn map(&self, batch: &RecordBatch) -> RecordBatch {
        let longitude_array = batch.column(self.longitude_idx);
        let latitude_array = batch.column(self.latitude_idx);

        // Cast columns to Float64
        let cast_opts = CastOptions {
            safe: true,
            format_options: Default::default(),
        };
        let binding = cast_with_options(longitude_array, &DataType::Float64, &cast_opts)
            .expect("Failed to cast longitude column");
        let longitude_f64 = binding.as_primitive::<Float64Type>();
        let binding = cast_with_options(latitude_array, &DataType::Float64, &cast_opts)
            .expect("Failed to cast latitude column");
        let latitude_f64 = binding.as_primitive::<Float64Type>();

        // Build geometry points
        let mut point_builder =
            PointBuilder::new(PointType::new(Dimension::XY, Arc::new(Metadata::default())));
        for (lat, lon) in latitude_f64.iter().zip(longitude_f64.iter()) {
            match (lon, lat) {
                (Some(lon), Some(lat)) => point_builder.push_coord(Some(&(lon, lat))),
                _ => point_builder.push_null(),
            }
        }

        let geometry_array: ArrayRef = point_builder.finish().to_array_ref();

        // Append geometry column
        let mut columns = batch.columns().to_vec();
        columns.push(geometry_array);

        RecordBatch::try_new(self.output_schema.clone(), columns)
            .expect("Failed to create output RecordBatch with geometry")
    }
}
/// A DataFusion sink for writing RecordBatches to GeoParquet format.
///
/// This sink takes input batches, maps longitude and latitude columns to a geometry column,
/// encodes them as GeoParquet, and writes to an object store.
pub struct GeoParquetSink {
    /// The input execution plan.
    input: Arc<dyn ExecutionPlan>,
    /// Configuration for the file sink.
    file_sink_config: FileSinkConfig,
    /// The object store to write to.
    object_store: Arc<dyn ObjectStore>,
    /// Mapper for converting lon/lat columns to geometry.
    mapper: GeoMapper,
}

impl GeoParquetSink {
    /// Creates a new `GeoParquetSink`.
    ///
    /// # Arguments
    /// * `input` - The input execution plan.
    /// * `file_sink_config` - Configuration for the file sink.
    /// * `object_store` - The object store to write to.
    /// * `longitude_column` - Name of the longitude column.
    /// * `latitude_column` - Name of the latitude column.
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        file_sink_config: FileSinkConfig,
        object_store: Arc<dyn ObjectStore>,
        longitude_column: &str,
        latitude_column: &str,
    ) -> Self {
        let mapper = GeoMapper::new(input.schema().as_ref(), longitude_column, latitude_column);

        Self {
            input,
            file_sink_config,
            object_store,
            mapper,
        }
    }
}

impl std::fmt::Debug for GeoParquetSink {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GeoParquetSink")
            .field("input", &self.input)
            .finish()
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

    /// Returns the output schema with the geometry column.
    fn schema(&self) -> &SchemaRef {
        &self.mapper.output_schema
    }

    fn metrics(&self) -> Option<datafusion::physical_plan::metrics::MetricsSet> {
        None
    }

    /// Writes all batches from the stream to GeoParquet format.
    ///
    /// # Arguments
    /// * `data` - The stream of record batches.
    /// * `_context` - The task context.
    ///
    /// # Returns
    /// The number of rows written.
    async fn write_all(
        &self,
        mut data: SendableRecordBatchStream,
        _context: &Arc<TaskContext>,
    ) -> datafusion::error::Result<u64> {
        // Build GeoParquet encoder with GeoArrow encoding
        let mut encoder = {
            let options = GeoParquetWriterOptionsBuilder::default()
                .set_encoding(geoparquet::writer::GeoParquetWriterEncoding::GeoArrow)
                .build();

            GeoParquetRecordBatchEncoder::try_new(&self.mapper.output_schema, &options)
                .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?
        };

        // Prepare output writer
        let output_path = self.file_sink_config.table_paths[0].prefix();
        let object_writer =
            ParquetObjectWriter::new(self.object_store.clone(), output_path.clone());

        let mut arrow_writer =
            AsyncArrowWriter::try_new(object_writer, encoder.target_schema(), None)
                .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

        let mut rows_written: u64 = 0;

        while let Some(batch) = data.next().await {
            let batch = batch?;
            let mapped_batch = self.mapper.map(&batch);
            let encoded_batch = encoder
                .encode_record_batch(&mapped_batch)
                .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
            arrow_writer
                .write(&encoded_batch)
                .await
                .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
            rows_written += mapped_batch.num_rows() as u64;
        }

        arrow_writer
            .finish()
            .await
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

        Ok(rows_written)
    }
}
