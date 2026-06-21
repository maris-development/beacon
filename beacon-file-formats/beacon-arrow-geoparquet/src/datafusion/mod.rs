use std::{any::Any, fmt::Debug, sync::Arc};

use arrow::datatypes::SchemaRef;
use beacon_common::{file_descriptors::file_open_parallelism, super_typing::super_type_schema};
use beacon_datafusion_ext::format_ext::{DatasetMetadata, FileFormatFactoryExt};
use datafusion::{
    catalog::{Session, memory::DataSourceExec},
    common::{GetExt, Statistics, exec_datafusion_err},
    datasource::{
        file_format::{FileFormat, FileFormatFactory, file_compression_type::FileCompressionType},
        physical_plan::{FileScanConfig, FileScanConfigBuilder, FileSinkConfig, FileSource},
        sink::{DataSink, DataSinkExec},
        table_schema::TableSchema,
    },
    physical_expr::LexRequirement,
    physical_plan::ExecutionPlan,
};
use futures::{StreamExt, TryStreamExt, stream};

use object_store::{ObjectMeta, ObjectStore};

use crate::datafusion::source::GeoParquetSource;

pub mod opener;
mod reader;
pub mod sink;
pub mod source;

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

impl FileFormatFactoryExt for GeoParquetFormatFactory {
    fn discover_datasets(
        &self,
        objects: &[ObjectMeta],
    ) -> datafusion::error::Result<Vec<DatasetMetadata>> {
        let datasets = objects
            .iter()
            .filter(|obj| {
                obj.location
                    .extension()
                    .map(|ext| ext == GEOPARQUET_EXTENSION)
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
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> datafusion::error::Result<SchemaRef> {
        // Read each file's GeoArrow schema concurrently, then merge into a
        // single super-typed schema (mirrors the plain Parquet format).
        let schemas = stream::iter(objects.iter().cloned())
            .map(|object| {
                let store = Arc::clone(store);
                async move { reader::fetch_schema(store, object).await }
            })
            .buffer_unordered(file_open_parallelism())
            .try_collect::<Vec<_>>()
            .await?;

        if schemas.is_empty() {
            return Ok(Arc::new(arrow::datatypes::Schema::empty()));
        }

        let super_schema = super_type_schema(&schemas).map_err(|e| {
            exec_datafusion_err!("Failed to compute super type schema for GeoParquet: {}", e)
        })?;

        Ok(Arc::new(super_schema))
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
        let table_schema = TableSchema::new(
            conf.file_schema().clone(),
            conf.table_partition_cols().clone(),
        );
        // Preserve a projection that the scan pushed down into the incoming
        // source — rebuilding the source below would otherwise drop it.
        let projection = conf.file_source().projection().cloned();
        let source = GeoParquetSource::new(table_schema).with_projection(projection);

        let conf = FileScanConfigBuilder::from(conf)
            .with_source(Arc::new(source))
            .build();

        Ok(DataSourceExec::from_data_source(conf))
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
        )?) as Arc<dyn DataSink>;

        Ok(Arc::new(DataSinkExec::new(input, sink, order_requirements)))
    }

    fn file_source(
        &self,
        table_schema: datafusion::datasource::table_schema::TableSchema,
    ) -> Arc<dyn FileSource> {
        Arc::new(GeoParquetSource::new(table_schema))
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, AsArray, Int32Array};
    use arrow::datatypes::{DataType, Field, Float64Type, Int32Type, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::datasource::listing::PartitionedFile;
    use datafusion::datasource::physical_plan::{FileOpener, FileScanConfigBuilder};
    use datafusion::execution::object_store::ObjectStoreUrl;
    use futures::StreamExt;
    use geoarrow::array::PointBuilder;
    use geoarrow::datatypes::{Dimension, Metadata, PointType};
    use geoarrow_array::GeoArrowArray;
    use geoparquet::writer::{
        GeoParquetRecordBatchEncoder, GeoParquetWriterEncoding, GeoParquetWriterOptionsBuilder,
    };
    use object_store::ObjectStore;
    use object_store::ObjectStoreExt;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use parquet::arrow::ArrowWriter;

    /// Build an in-memory GeoParquet file with a native GeoArrow point column
    /// (`geometry`) and an `id` column, returning the encoded bytes.
    fn write_geoparquet_fixture() -> Vec<u8> {
        let point_type = PointType::new(Dimension::XY, Arc::new(Metadata::default()));
        let geometry_field = Arc::new(point_type.to_field("geometry", true));
        let id_field = Arc::new(Field::new("id", DataType::Int32, false));
        let schema = Arc::new(Schema::new(vec![id_field, geometry_field]));

        let mut point_builder =
            PointBuilder::new(PointType::new(Dimension::XY, Arc::new(Metadata::default())));
        for coord in [(1.0_f64, 2.0_f64), (3.0, 4.0), (5.0, 6.0)] {
            point_builder.push_coord(Some(&coord));
        }
        let geometry: ArrayRef = point_builder.finish().to_array_ref();
        let ids: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));

        let batch = RecordBatch::try_new(schema.clone(), vec![ids, geometry]).unwrap();

        let options = GeoParquetWriterOptionsBuilder::default()
            .set_encoding(GeoParquetWriterEncoding::GeoArrow)
            .build();
        let mut encoder = GeoParquetRecordBatchEncoder::try_new(&schema, &options).unwrap();

        let mut buf: Vec<u8> = Vec::new();
        {
            let mut writer = ArrowWriter::try_new(&mut buf, encoder.target_schema(), None).unwrap();
            let encoded = encoder.encode_record_batch(&batch).unwrap();
            writer.write(&encoded).unwrap();
            let kv = encoder.into_keyvalue().unwrap();
            writer.append_key_value_metadata(kv);
            writer.finish().unwrap();
        }
        buf
    }

    async fn put_fixture(store: &Arc<InMemory>, path: &Path) -> ObjectMeta {
        let bytes = write_geoparquet_fixture();
        store
            .put(path, bytes::Bytes::from(bytes).into())
            .await
            .expect("write fixture");
        store.head(path).await.expect("head fixture")
    }

    #[tokio::test]
    async fn infer_schema_decodes_geometry_to_native_geoarrow() {
        let store = Arc::new(InMemory::new());
        let object_store: Arc<dyn ObjectStore> = store.clone();
        let path = Path::from("test.geoparquet");
        let object = put_fixture(&store, &path).await;

        let format = GeoParquetFormat::new(GeoParquetOptions {
            longitude_column: None,
            latitude_column: None,
        });
        let ctx = datafusion::prelude::SessionContext::new();

        let schema = format
            .infer_schema(&ctx.state(), &object_store, &[object])
            .await
            .expect("infer schema");

        let geometry = schema.field_with_name("geometry").expect("geometry field");
        // Geometry is decoded to its native GeoArrow storage (a struct of x/y
        // child arrays for the Separated coord layout), not left as opaque WKB
        // binary. Note the GeoArrow extension *metadata* is dropped here because
        // Beacon's `super_type_schema` rebuilds fields without metadata — the
        // native struct layout is what survives at the table-schema level.
        let DataType::Struct(children) = geometry.data_type() else {
            panic!(
                "geometry should decode to a native GeoArrow struct, got {:?}",
                geometry.data_type()
            );
        };
        let child_names: Vec<&str> = children.iter().map(|f| f.name().as_str()).collect();
        assert!(
            child_names.contains(&"x") && child_names.contains(&"y"),
            "geometry struct should have x/y children, got {child_names:?}"
        );
        assert!(schema.field_with_name("id").is_ok());
    }

    #[tokio::test]
    async fn opener_reads_back_rows_and_geometry() {
        let store = Arc::new(InMemory::new());
        let object_store: Arc<dyn ObjectStore> = store.clone();
        let path = Path::from("test.geoparquet");
        let object = put_fixture(&store, &path).await;

        let table_schema = reader::fetch_schema(object_store.clone(), object.clone())
            .await
            .expect("schema");
        let ts = TableSchema::from_file_schema(table_schema);
        let source = GeoParquetSource::new(ts);

        let conf = FileScanConfigBuilder::new(
            ObjectStoreUrl::parse("memory://").unwrap(),
            Arc::new(source.clone()) as Arc<dyn FileSource>,
        )
        .build();
        let opener = source
            .create_file_opener(object_store, &conf, 0)
            .expect("opener");

        let stream = opener
            .open(PartitionedFile::from(object))
            .expect("open")
            .await
            .expect("stream");
        let batches: Vec<_> = stream
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .expect("batches ok");

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);

        let full = arrow::compute::concat_batches(&batches[0].schema(), &batches).expect("concat");
        let id_idx = full.schema().index_of("id").expect("id column");
        let ids = full.column(id_idx).as_primitive::<Int32Type>();
        assert_eq!(ids.values(), &[1, 2, 3]);

        // Geometry column round-trips as a native GeoArrow struct (x, y children).
        let geom_idx = full.schema().index_of("geometry").expect("geometry column");
        let geom = full.column(geom_idx).as_struct();
        let x = geom.column_by_name("x").expect("x child").as_primitive::<Float64Type>();
        let y = geom.column_by_name("y").expect("y child").as_primitive::<Float64Type>();
        assert_eq!(x.values(), &[1.0, 3.0, 5.0]);
        assert_eq!(y.values(), &[2.0, 4.0, 6.0]);
    }

    #[tokio::test]
    async fn opener_applies_column_projection() {
        let store = Arc::new(InMemory::new());
        let object_store: Arc<dyn ObjectStore> = store.clone();
        let path = Path::from("test.geoparquet");
        let object = put_fixture(&store, &path).await;

        let file_schema = reader::fetch_schema(object_store.clone(), object.clone())
            .await
            .expect("schema");
        // Project to the `id` column only — geometry should be dropped.
        let id_idx = file_schema.index_of("id").expect("id column");
        let projected: SchemaRef = Arc::new(file_schema.project(&[id_idx]).expect("project"));

        let opener = opener::GeoParquetOpener::new(object_store, projected, 128 * 1024);
        let stream = opener
            .open(PartitionedFile::from(object))
            .expect("open")
            .await
            .expect("stream");
        let batches: Vec<_> = stream
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .expect("batches ok");

        let full = arrow::compute::concat_batches(&batches[0].schema(), &batches).expect("concat");
        let out_schema = full.schema();
        let names: Vec<&str> = out_schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, vec!["id"], "only the projected column should remain");
        assert_eq!(full.num_rows(), 3);
        assert_eq!(
            full.column(0).as_primitive::<Int32Type>().values(),
            &[1, 2, 3]
        );
    }

    #[tokio::test]
    async fn discover_datasets_filters_by_extension() {
        let factory = <GeoParquetFormatFactory as Default>::default();
        let objects = vec![
            object_meta("a.geoparquet"),
            object_meta("nested/b.geoparquet"),
            object_meta("c.parquet"),
            object_meta("d.csv"),
            object_meta("no_extension"),
        ];

        let discovered = factory.discover_datasets(&objects).expect("discover");
        let paths: Vec<&str> = discovered.iter().map(|d| d.file_path.as_str()).collect();

        assert_eq!(paths, vec!["a.geoparquet", "nested/b.geoparquet"]);
        assert!(discovered.iter().all(|d| d.format == "geoparquet"));
    }

    #[tokio::test]
    async fn reads_plain_parquet_without_geo_metadata() {
        // A regular Parquet file (no `geo` key) should still be readable: the
        // schema falls back to plain Arrow and no geometry decoding occurs.
        let store = Arc::new(InMemory::new());
        let object_store: Arc<dyn ObjectStore> = store.clone();
        let path = Path::from("plain.parquet");

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![7, 8]))])
                .unwrap();
        let mut buf: Vec<u8> = Vec::new();
        {
            let mut writer = ArrowWriter::try_new(&mut buf, schema.clone(), None).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }
        store
            .put(&path, bytes::Bytes::from(buf).into())
            .await
            .unwrap();
        let object = store.head(&path).await.unwrap();

        let inferred = reader::fetch_schema(object_store.clone(), object.clone())
            .await
            .expect("plain parquet should infer a schema");
        assert!(inferred.field_with_name("id").is_ok());
        assert!(inferred.field_with_name("geometry").is_err());

        let opener =
            opener::GeoParquetOpener::new(object_store, inferred.clone(), 128 * 1024);
        let stream = opener
            .open(PartitionedFile::from(object))
            .expect("open")
            .await
            .expect("stream");
        let batches: Vec<_> = stream
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .expect("batches ok");
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 2);
    }

    fn object_meta(path: &str) -> ObjectMeta {
        ObjectMeta {
            location: Path::from(path),
            last_modified: Default::default(),
            size: 0,
            e_tag: None,
            version: None,
        }
    }
}

pub mod table_function;
pub use table_function::ReadGeoParquetFunc;
