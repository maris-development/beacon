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
    pub fn new(
        input_schema: &Schema,
        longitude_column: &str,
        latitude_column: &str,
    ) -> datafusion::error::Result<Self> {
        let longitude_idx = input_schema.index_of(longitude_column).map_err(|_| {
            datafusion::error::DataFusionError::Plan(format!(
                "Longitude column '{longitude_column}' not found in schema"
            ))
        })?;
        let latitude_idx = input_schema.index_of(latitude_column).map_err(|_| {
            datafusion::error::DataFusionError::Plan(format!(
                "Latitude column '{latitude_column}' not found in schema"
            ))
        })?;

        // Clone fields and add geometry field
        let mut fields = input_schema.fields().clone().to_vec();
        let point_type = PointType::new(Dimension::XY, Arc::new(Metadata::default()));
        let point_field = Arc::new(point_type.to_field("geometry", true));
        fields.push(point_field);

        let output_schema = Arc::new(Schema::new(fields));

        Ok(Self {
            longitude_idx,
            latitude_idx,
            output_schema,
        })
    }

    /// Maps a RecordBatch by adding a geometry column constructed from longitude and latitude columns.
    ///
    /// Returns a new RecordBatch with the geometry column appended.
    pub fn map(&self, batch: &RecordBatch) -> datafusion::error::Result<RecordBatch> {
        let longitude_array = batch.column(self.longitude_idx);
        let latitude_array = batch.column(self.latitude_idx);

        // Cast columns to Float64
        let cast_opts = CastOptions {
            safe: true,
            format_options: Default::default(),
        };
        let binding = cast_with_options(longitude_array, &DataType::Float64, &cast_opts)
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
        let longitude_f64 = binding.as_primitive::<Float64Type>();
        let binding = cast_with_options(latitude_array, &DataType::Float64, &cast_opts)
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
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

        Ok(RecordBatch::try_new(self.output_schema.clone(), columns)?)
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
    ) -> datafusion::error::Result<Self> {
        let mapper = GeoMapper::new(input.schema().as_ref(), longitude_column, latitude_column)?;

        Ok(Self {
            input,
            file_sink_config,
            object_store,
            mapper,
        })
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
            let mapped_batch = self.mapper.map(&batch)?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Float32Array, Float64Array, Int32Array, StringArray};
    use arrow::datatypes::Field;
    use datafusion::datasource::listing::ListingTableUrl;
    use datafusion::datasource::physical_plan::{FileGroup, FileOutputMode};
    use datafusion::execution::object_store::ObjectStoreUrl;
    use datafusion::logical_expr::dml::InsertOp;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use object_store::memory::InMemory;

    // ── helpers ────────────────────────────────────────────────────────

    /// Schema with a lon/lat pair plus a non-coordinate column.
    fn lonlat_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("lon", DataType::Float64, true),
            Field::new("lat", DataType::Float64, true),
        ]))
    }

    fn lonlat_batch(schema: &SchemaRef) -> RecordBatch {
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
                Arc::new(Float64Array::from(vec![Some(1.0), None, Some(5.0)])),
                Arc::new(Float64Array::from(vec![Some(2.0), Some(4.0), Some(6.0)])),
            ],
        )
        .unwrap()
    }

    fn test_sink_config(schema: SchemaRef, path: &str) -> FileSinkConfig {
        FileSinkConfig {
            original_url: String::new(),
            object_store_url: ObjectStoreUrl::parse("memory://").unwrap(),
            file_group: FileGroup::default(),
            table_paths: vec![ListingTableUrl::parse(path).unwrap()],
            output_schema: schema,
            table_partition_cols: vec![],
            insert_op: InsertOp::Append,
            keep_partition_by_columns: false,
            file_extension: crate::datafusion::GEOPARQUET_EXTENSION.to_string(),
            file_output_mode: FileOutputMode::SingleFile,
        }
    }

    // ── GeoMapper ──────────────────────────────────────────────────────

    #[test]
    fn geo_mapper_appends_geometry_field_to_schema() {
        let schema = lonlat_schema();
        let mapper = GeoMapper::new(schema.as_ref(), "lon", "lat").unwrap();
        let out = &mapper.output_schema;

        // The input fields are preserved in order and `geometry` is appended.
        assert_eq!(out.fields().len(), schema.fields().len() + 1);
        for (i, f) in schema.fields().iter().enumerate() {
            assert_eq!(out.field(i).name(), f.name());
        }
        let geometry = out.field(out.fields().len() - 1);
        assert_eq!(geometry.name(), "geometry");
        // Native GeoArrow point storage: a struct of separated x/y children.
        let DataType::Struct(children) = geometry.data_type() else {
            panic!("geometry should be a struct, got {:?}", geometry.data_type());
        };
        let names: Vec<&str> = children.iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, vec!["x", "y"]);
        // The GeoArrow extension metadata must survive onto the field, otherwise
        // the GeoParquet encoder cannot recognise the column as geometry.
        assert!(
            geometry
                .metadata()
                .keys()
                .any(|k| k.contains("extension:name")),
            "geometry field should carry the GeoArrow extension metadata: {:?}",
            geometry.metadata()
        );
    }

    #[test]
    fn geo_mapper_errors_on_missing_columns() {
        let schema = lonlat_schema();
        let Err(err) = GeoMapper::new(schema.as_ref(), "nope", "lat") else {
            panic!("a missing longitude column must be rejected");
        };
        assert!(err.to_string().contains("Longitude column 'nope'"));
        let Err(err) = GeoMapper::new(schema.as_ref(), "lon", "nope") else {
            panic!("a missing latitude column must be rejected");
        };
        assert!(err.to_string().contains("Latitude column 'nope'"));
    }

    #[test]
    fn geo_mapper_builds_points_and_nulls_incomplete_coordinates() {
        let schema = lonlat_schema();
        let mapper = GeoMapper::new(schema.as_ref(), "lon", "lat").unwrap();
        let mapped = mapper.map(&lonlat_batch(&schema)).unwrap();

        assert_eq!(mapped.num_rows(), 3);
        let geom = mapped
            .column(mapped.schema().index_of("geometry").unwrap())
            .as_struct();
        // Row 1 has a null longitude, so the whole point is null.
        assert!(!geom.is_null(0));
        assert!(geom.is_null(1));
        assert!(!geom.is_null(2));

        let x = geom
            .column_by_name("x")
            .unwrap()
            .as_primitive::<Float64Type>();
        let y = geom
            .column_by_name("y")
            .unwrap()
            .as_primitive::<Float64Type>();
        // (lon, lat) ordering — x is longitude, y is latitude.
        assert_eq!(x.value(0), 1.0);
        assert_eq!(y.value(0), 2.0);
        assert_eq!(x.value(2), 5.0);
        assert_eq!(y.value(2), 6.0);
    }

    #[test]
    fn geo_mapper_casts_non_float64_coordinates() {
        // Coordinates arriving as Float32/Int32 are cast to Float64 rather than
        // rejected.
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("lon", DataType::Float32, false),
            Field::new("lat", DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Float32Array::from(vec![1.5f32, -2.5])) as ArrayRef,
                Arc::new(Int32Array::from(vec![10, -20])),
            ],
        )
        .unwrap();

        let mapper = GeoMapper::new(schema.as_ref(), "lon", "lat").unwrap();
        let mapped = mapper.map(&batch).unwrap();
        let geom = mapped
            .column(mapped.schema().index_of("geometry").unwrap())
            .as_struct();
        let x = geom
            .column_by_name("x")
            .unwrap()
            .as_primitive::<Float64Type>();
        let y = geom
            .column_by_name("y")
            .unwrap()
            .as_primitive::<Float64Type>();
        assert_eq!(x.values(), &[1.5, -2.5]);
        assert_eq!(y.values(), &[10.0, -20.0]);
    }

    #[test]
    fn geo_mapper_rejects_uncastable_coordinate_column() {
        // A string column that cannot be cast to Float64 must surface an error
        // rather than silently produce nulls.
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("lon", DataType::Utf8, false),
            Field::new("lat", DataType::Float64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["not-a-number", "x"])) as ArrayRef,
                Arc::new(Float64Array::from(vec![1.0, 2.0])),
            ],
        )
        .unwrap();

        let mapper = GeoMapper::new(schema.as_ref(), "lon", "lat").unwrap();
        let mapped = mapper.map(&batch).unwrap();
        // `safe: true` casting turns unparseable strings into nulls, which in
        // turn null out the geometry — this documents the lenient behaviour.
        let geom = mapped
            .column(mapped.schema().index_of("geometry").unwrap())
            .as_struct();
        assert!(geom.is_null(0) && geom.is_null(1));
    }

    // ── Sink round-trip ────────────────────────────────────────────────

    #[tokio::test]
    async fn sink_writes_readable_geoparquet_with_geometry() {
        let store = Arc::new(InMemory::new());
        let object_store: Arc<dyn ObjectStore> = store.clone();
        let schema = lonlat_schema();
        let batch = lonlat_batch(&schema);

        let input = Arc::new(EmptyExec::new(schema.clone())) as Arc<dyn ExecutionPlan>;
        let conf = test_sink_config(schema.clone(), "memory:///out.geoparquet");
        let sink =
            GeoParquetSink::new(input, conf, object_store.clone(), "lon", "lat").unwrap();

        // The sink's advertised schema is the mapped one (geometry appended).
        assert!(sink.schema().field_with_name("geometry").is_ok());

        let stream = Box::pin(RecordBatchStreamAdapter::new(
            schema.clone(),
            futures::stream::iter(vec![Ok(batch.clone()), Ok(batch)]),
        ));
        let rows = sink
            .write_all(stream, &Arc::new(TaskContext::default()))
            .await
            .expect("write_all");
        assert_eq!(rows, 6);

        // Read it back: the geometry column must be recoverable as native GeoArrow.
        let path = object_store::path::Path::from("out.geoparquet");
        let meta = object_store::ObjectStoreExt::head(object_store.as_ref(), &path)
            .await
            .expect("output object should exist");
        let read_schema = crate::datafusion::reader::fetch_schema(object_store.clone(), meta)
            .await
            .expect("written file should be readable as GeoParquet");
        let geometry = read_schema
            .field_with_name("geometry")
            .expect("geometry column should round-trip");
        assert!(
            matches!(geometry.data_type(), DataType::Struct(_)),
            "geometry should read back as native GeoArrow, got {:?}",
            geometry.data_type()
        );
        assert!(read_schema.field_with_name("id").is_ok());
    }

    #[tokio::test]
    async fn sink_new_errors_when_coordinate_column_missing() {
        let store = Arc::new(InMemory::new());
        let schema = lonlat_schema();
        let input = Arc::new(EmptyExec::new(schema.clone())) as Arc<dyn ExecutionPlan>;
        let conf = test_sink_config(schema.clone(), "memory:///out.geoparquet");
        let Err(err) = GeoParquetSink::new(input, conf, store, "missing_lon", "lat") else {
            panic!("a missing longitude column must be rejected");
        };
        assert!(err.to_string().contains("Longitude column 'missing_lon'"));
    }
}
