use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion::{
    catalog::{Session, memory::DataSourceExec},
    common::{GetExt, Statistics},
    datasource::{
        file_format::{FileFormat, FileFormatFactory, file_compression_type::FileCompressionType},
        physical_plan::{FileScanConfig, FileScanConfigBuilder, FileSource},
    },
    physical_plan::ExecutionPlan,
};
use object_store::{ObjectMeta, ObjectStore, path::Path};

use super::schema::global_schema_to_arrow;
use super::source::AtlasSource;
use crate::reader::AtlasReader;

const ATLAS_EXTENSION: &str = "atlas";

/// DataFusion [`FileFormat`] for atlas collections.
///
/// Reads an atlas collection by opening the `collection.atlas` footer file.
/// The table schema is derived from the collection's global schema.
#[derive(Debug, Clone)]
pub struct AtlasFormat {
    pub object_store: Arc<dyn ObjectStore>,
    pub cache_capacity_bytes: u64,
}

impl AtlasFormat {
    pub fn new(object_store: Arc<dyn ObjectStore>, cache_capacity_bytes: u64) -> Self {
        Self {
            object_store,
            cache_capacity_bytes,
        }
    }

    /// Derive the base path of the collection from the footer file path.
    ///
    /// E.g. `my-collection/collection.atlas` → `my-collection`
    fn base_path_from_footer(location: &Path) -> Path {
        let s = location.to_string();
        let base = s
            .strip_suffix("/collection.atlas")
            .or_else(|| s.strip_suffix("collection.atlas"))
            .unwrap_or(&s);
        Path::from(base)
    }
}

impl GetExt for AtlasFormat {
    fn get_ext(&self) -> String {
        ATLAS_EXTENSION.to_string()
    }
}

#[async_trait::async_trait]
impl FileFormat for AtlasFormat {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn compression_type(&self) -> Option<FileCompressionType> {
        None
    }

    fn get_ext(&self) -> String {
        ATLAS_EXTENSION.to_string()
    }

    fn get_ext_with_compression(
        &self,
        _file_compression_type: &FileCompressionType,
    ) -> datafusion::error::Result<String> {
        Ok(ATLAS_EXTENSION.to_string())
    }

    async fn infer_schema(
        &self,
        _state: &dyn Session,
        _store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> datafusion::error::Result<SchemaRef> {
        let Some(object) = objects.first() else {
            return Ok(Arc::new(arrow::datatypes::Schema::empty()));
        };

        let base_path = Self::base_path_from_footer(&object.location);

        let reader = AtlasReader::open(
            self.object_store.clone(),
            base_path,
            self.cache_capacity_bytes,
        )
        .await
        .map_err(|e| {
            datafusion::error::DataFusionError::Execution(format!(
                "Failed to open atlas collection: {e}"
            ))
        })?;

        Ok(global_schema_to_arrow(reader.global_schema()))
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
        let source = AtlasSource::new(self.object_store.clone(), self.cache_capacity_bytes);
        let conf = FileScanConfigBuilder::from(conf)
            .with_source(Arc::new(source))
            .build();
        Ok(DataSourceExec::from_data_source(conf))
    }

    fn file_source(&self) -> Arc<dyn FileSource> {
        Arc::new(AtlasSource::new(
            self.object_store.clone(),
            self.cache_capacity_bytes,
        ))
    }
}

/// Factory for creating [`AtlasFormat`] instances.
///
/// Register this with a DataFusion [`SessionContext`] to enable automatic
/// recognition of `.atlas` files:
///
/// ```ignore
/// ctx.register_file_format(Arc::new(AtlasFormatFactory::new(store, 64 * 1024 * 1024)));
/// ```
#[derive(Debug, Clone)]
pub struct AtlasFormatFactory {
    object_store: Arc<dyn ObjectStore>,
    cache_capacity_bytes: u64,
}

impl AtlasFormatFactory {
    pub fn new(object_store: Arc<dyn ObjectStore>, cache_capacity_bytes: u64) -> Self {
        Self {
            object_store,
            cache_capacity_bytes,
        }
    }
}

impl GetExt for AtlasFormatFactory {
    fn get_ext(&self) -> String {
        ATLAS_EXTENSION.to_string()
    }
}

impl FileFormatFactory for AtlasFormatFactory {
    fn create(
        &self,
        _state: &dyn Session,
        _format_options: &HashMap<String, String>,
    ) -> datafusion::error::Result<Arc<dyn FileFormat>> {
        Ok(Arc::new(AtlasFormat::new(
            self.object_store.clone(),
            self.cache_capacity_bytes,
        )))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn default(&self) -> Arc<dyn FileFormat> {
        Arc::new(AtlasFormat::new(
            self.object_store.clone(),
            self.cache_capacity_bytes,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow::datatypes::DataType as ArrowDataType;
    use arrow::record_batch::RecordBatch;
    use beacon_nd_array::NdArray;
    use beacon_nd_array::NdArrayD;
    use beacon_nd_array::dataset::Dataset;
    use datafusion::datasource::listing::PartitionedFile;
    use datafusion::datasource::physical_plan::{FileMeta, FileScanConfigBuilder};
    use datafusion::execution::object_store::ObjectStoreUrl;
    use datafusion::prelude::SessionContext;
    use futures::TryStreamExt;
    use indexmap::IndexMap;
    use object_store::memory::InMemory;

    use crate::writer::AtlasWriter;
    use array_format::NoCompression;

    async fn make_dataset(name: &str, arrays: Vec<(&str, Arc<dyn NdArrayD>)>) -> Dataset {
        let map: IndexMap<String, Arc<dyn NdArrayD>> = arrays
            .into_iter()
            .map(|(n, a)| (n.to_string(), a))
            .collect();
        Dataset::new(name.to_string(), map).await
    }

    async fn write_test_collection(store: Arc<InMemory>) {
        let mut writer = AtlasWriter::new(
            store.clone(),
            Path::from("test-collection"),
            NoCompression,
            64 * 1024,
        );

        let temp1 = NdArray::<f32>::try_new_from_vec_in_mem(
            vec![15.0, 16.0, 17.0],
            vec![3],
            vec!["obs".into()],
            None,
        )
        .unwrap();
        let depth1 = NdArray::<f64>::try_new_from_vec_in_mem(
            vec![10.0, 20.0, 30.0],
            vec![3],
            vec!["obs".into()],
            None,
        )
        .unwrap();
        let ds1 = make_dataset(
            "dataset-1",
            vec![
                ("temperature", Arc::new(temp1)),
                ("depth", Arc::new(depth1)),
            ],
        )
        .await;

        let temp2 = NdArray::<f32>::try_new_from_vec_in_mem(
            vec![20.0, 21.0],
            vec![2],
            vec!["obs".into()],
            None,
        )
        .unwrap();
        let salinity = NdArray::<f64>::try_new_from_vec_in_mem(
            vec![35.0, 36.0],
            vec![2],
            vec!["obs".into()],
            None,
        )
        .unwrap();
        let ds2 = make_dataset(
            "dataset-2",
            vec![
                ("temperature", Arc::new(temp2)),
                ("salinity", Arc::new(salinity)),
            ],
        )
        .await;

        writer.write_dataset(&ds1).await.unwrap();
        writer.write_dataset(&ds2).await.unwrap();
        writer.flush().await.unwrap();
    }

    fn footer_object_meta() -> ObjectMeta {
        ObjectMeta {
            location: Path::from("test-collection/collection.atlas"),
            last_modified: chrono::Utc::now(),
            size: 0,
            e_tag: None,
            version: None,
        }
    }

    #[tokio::test]
    async fn test_infer_schema_returns_global_schema() {
        let store = Arc::new(InMemory::new());
        write_test_collection(store.clone()).await;

        let format = AtlasFormat::new(store.clone(), 64 * 1024 * 1024);
        let ctx = SessionContext::new();
        let state = ctx.state();

        let schema = format
            .infer_schema(
                &state,
                &(store.clone() as Arc<dyn ObjectStore>),
                &[footer_object_meta()],
            )
            .await
            .unwrap();

        // Global schema should have all 3 columns: temperature, depth, salinity
        assert_eq!(schema.fields().len(), 3);

        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert!(field_names.contains(&"temperature"));
        assert!(field_names.contains(&"depth"));
        assert!(field_names.contains(&"salinity"));

        // temperature should be F32 (both datasets have F32)
        let temp_field = schema.field_with_name("temperature").unwrap();
        assert_eq!(*temp_field.data_type(), ArrowDataType::Float32);

        // depth should be F64
        let depth_field = schema.field_with_name("depth").unwrap();
        assert_eq!(*depth_field.data_type(), ArrowDataType::Float64);

        // salinity should be F64
        let sal_field = schema.field_with_name("salinity").unwrap();
        assert_eq!(*sal_field.data_type(), ArrowDataType::Float64);

        // All fields nullable
        assert!(temp_field.is_nullable());
        assert!(depth_field.is_nullable());
        assert!(sal_field.is_nullable());
    }

    #[tokio::test]
    async fn test_file_opener_reads_all_datasets() {
        let store = Arc::new(InMemory::new());
        write_test_collection(store.clone()).await;

        let format = AtlasFormat::new(store.clone(), 64 * 1024 * 1024);
        let ctx = SessionContext::new();
        let state = ctx.state();

        let schema = format
            .infer_schema(
                &state,
                &(store.clone() as Arc<dyn ObjectStore>),
                &[footer_object_meta()],
            )
            .await
            .unwrap();

        // Create a file opener directly to test the streaming
        let source = AtlasSource::new(store.clone(), 64 * 1024 * 1024);
        let config = FileScanConfigBuilder::new(
            ObjectStoreUrl::parse("memory://").unwrap(),
            schema.clone(),
            Arc::new(source.clone()) as Arc<dyn FileSource>,
        )
        .with_file(PartitionedFile::new("test-collection/collection.atlas", 0))
        .build();

        let opener = source.create_file_opener(store.clone(), &config, 0);

        let file_meta = FileMeta {
            object_meta: footer_object_meta(),
            range: None,
            extensions: None,
            metadata_size_hint: None,
        };

        let partitioned_file = PartitionedFile::new("test-collection/collection.atlas", 0);
        let batch_stream_future = opener.open(file_meta, partitioned_file).unwrap();
        let batch_stream = batch_stream_future.await.unwrap();

        let batches: Vec<RecordBatch> = batch_stream.try_collect().await.unwrap();

        // We should have at least one batch from each dataset
        assert!(!batches.is_empty());

        // Total rows: dataset-1 has 3 rows, dataset-2 has 2 rows = 5 total
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 5);

        // All batches should have the global schema's columns
        for batch in &batches {
            assert_eq!(batch.num_columns(), 3);
        }
    }

    #[tokio::test]
    async fn test_file_opener_projected_read() {
        let store = Arc::new(InMemory::new());
        write_test_collection(store.clone()).await;

        let format = AtlasFormat::new(store.clone(), 64 * 1024 * 1024);
        let ctx = SessionContext::new();
        let state = ctx.state();

        let full_schema = format
            .infer_schema(
                &state,
                &(store.clone() as Arc<dyn ObjectStore>),
                &[footer_object_meta()],
            )
            .await
            .unwrap();

        // Project to only "temperature" column
        let projected_schema: SchemaRef = Arc::new(arrow::datatypes::Schema::new(vec![
            full_schema.field_with_name("temperature").unwrap().clone(),
        ]));

        let source = AtlasSource::new(store.clone(), 64 * 1024 * 1024);
        let config = FileScanConfigBuilder::new(
            ObjectStoreUrl::parse("memory://").unwrap(),
            full_schema.clone(),
            Arc::new(source.clone()) as Arc<dyn FileSource>,
        )
        .with_file(PartitionedFile::new("test-collection/collection.atlas", 0))
        .with_projection(Some(vec![
            full_schema
                .fields()
                .iter()
                .position(|f| f.name() == "temperature")
                .unwrap(),
        ]))
        .build();

        let opener = source.create_file_opener(store.clone(), &config, 0);

        let file_meta = FileMeta {
            object_meta: footer_object_meta(),
            range: None,
            extensions: None,
            metadata_size_hint: None,
        };

        let partitioned_file = PartitionedFile::new("test-collection/collection.atlas", 0);
        let batch_stream_future = opener.open(file_meta, partitioned_file).unwrap();
        let batch_stream = batch_stream_future.await.unwrap();

        let batches: Vec<RecordBatch> = batch_stream.try_collect().await.unwrap();
        assert!(!batches.is_empty());

        // Each batch should have only 1 column (temperature)
        for batch in &batches {
            assert_eq!(batch.num_columns(), 1);
            assert_eq!(batch.schema().field(0).name(), "temperature");
        }

        // Total rows should still be 5 (3 + 2)
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 5);
    }

    #[tokio::test]
    async fn test_empty_collection_infers_empty_schema() {
        let store = Arc::new(InMemory::new());

        // Write a collection with no datasets
        let mut writer = AtlasWriter::new(
            store.clone(),
            Path::from("test-collection"),
            NoCompression,
            64 * 1024,
        );
        writer.flush().await.unwrap();

        let format = AtlasFormat::new(store.clone(), 64 * 1024 * 1024);
        let ctx = SessionContext::new();
        let state = ctx.state();

        let schema = format
            .infer_schema(
                &state,
                &(store.clone() as Arc<dyn ObjectStore>),
                &[footer_object_meta()],
            )
            .await
            .unwrap();

        assert_eq!(schema.fields().len(), 0);
    }

    #[tokio::test]
    async fn test_datasets_with_different_schemas_null_filled() {
        // Dataset-1: temperature(F32), depth(F64)
        // Dataset-2: temperature(F32), salinity(F64)
        // Global schema: temperature(F32), depth(F64), salinity(F64)
        //
        // When reading dataset-1, salinity should be null-filled.
        // When reading dataset-2, depth should be null-filled.
        let store = Arc::new(InMemory::new());
        write_test_collection(store.clone()).await;

        let format = AtlasFormat::new(store.clone(), 64 * 1024 * 1024);
        let ctx = SessionContext::new();
        let state = ctx.state();

        let schema = format
            .infer_schema(
                &state,
                &(store.clone() as Arc<dyn ObjectStore>),
                &[footer_object_meta()],
            )
            .await
            .unwrap();

        let source = AtlasSource::new(store.clone(), 64 * 1024 * 1024);
        let config = FileScanConfigBuilder::new(
            ObjectStoreUrl::parse("memory://").unwrap(),
            schema.clone(),
            Arc::new(source.clone()) as Arc<dyn FileSource>,
        )
        .with_file(PartitionedFile::new("test-collection/collection.atlas", 0))
        .build();

        let opener = source.create_file_opener(store.clone(), &config, 0);

        let file_meta = FileMeta {
            object_meta: footer_object_meta(),
            range: None,
            extensions: None,
            metadata_size_hint: None,
        };

        let partitioned_file = PartitionedFile::new("test-collection/collection.atlas", 0);
        let batch_stream = opener
            .open(file_meta, partitioned_file)
            .unwrap()
            .await
            .unwrap();

        let batches: Vec<RecordBatch> = batch_stream.try_collect().await.unwrap();

        // All batches must conform to the same output schema (3 columns)
        for batch in &batches {
            assert_eq!(batch.num_columns(), 3);
            let batch_schema = batch.schema();
            assert!(batch_schema.field_with_name("temperature").is_ok());
            assert!(batch_schema.field_with_name("depth").is_ok());
            assert!(batch_schema.field_with_name("salinity").is_ok());
        }

        // Collect by dataset: dataset-1 has 3 rows, dataset-2 has 2 rows
        // We can verify null counts on the missing columns
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 5);

        // Count nulls in "depth" and "salinity" across all batches
        let mut depth_null_count = 0usize;
        let mut salinity_null_count = 0usize;
        for batch in &batches {
            let depth_idx = batch.schema().index_of("depth").unwrap();
            let sal_idx = batch.schema().index_of("salinity").unwrap();
            depth_null_count += batch.column(depth_idx).null_count();
            salinity_null_count += batch.column(sal_idx).null_count();
        }
        // dataset-2 (2 rows) doesn't have depth → 2 nulls
        assert_eq!(depth_null_count, 2);
        // dataset-1 (3 rows) doesn't have salinity → 3 nulls
        assert_eq!(salinity_null_count, 3);
    }

    #[tokio::test]
    async fn test_datasets_with_type_widening() {
        // Dataset with i32 column and another with f64 column of same name.
        // Global schema super-types to f64. Schema adapter should cast i32 → f64.
        let store = Arc::new(InMemory::new());

        let mut writer = AtlasWriter::new(
            store.clone(),
            Path::from("test-collection"),
            NoCompression,
            64 * 1024,
        );

        let values_i32 = NdArray::<i32>::try_new_from_vec_in_mem(
            vec![1, 2, 3],
            vec![3],
            vec!["obs".into()],
            None,
        )
        .unwrap();
        let ds1 = make_dataset("ds-int", vec![("value", Arc::new(values_i32))]).await;

        let values_f64 = NdArray::<f64>::try_new_from_vec_in_mem(
            vec![4.5, 5.5],
            vec![2],
            vec!["obs".into()],
            None,
        )
        .unwrap();
        let ds2 = make_dataset("ds-float", vec![("value", Arc::new(values_f64))]).await;

        writer.write_dataset(&ds1).await.unwrap();
        writer.write_dataset(&ds2).await.unwrap();
        writer.flush().await.unwrap();

        let format = AtlasFormat::new(store.clone(), 64 * 1024 * 1024);
        let ctx = SessionContext::new();
        let state = ctx.state();

        let schema = format
            .infer_schema(
                &state,
                &(store.clone() as Arc<dyn ObjectStore>),
                &[footer_object_meta()],
            )
            .await
            .unwrap();

        // Global schema should super-type to F64
        assert_eq!(schema.fields().len(), 1);
        assert_eq!(
            *schema.field_with_name("value").unwrap().data_type(),
            ArrowDataType::Float64
        );

        // Read through the opener
        let source = AtlasSource::new(store.clone(), 64 * 1024 * 1024);
        let config = FileScanConfigBuilder::new(
            ObjectStoreUrl::parse("memory://").unwrap(),
            schema.clone(),
            Arc::new(source.clone()) as Arc<dyn FileSource>,
        )
        .with_file(PartitionedFile::new("test-collection/collection.atlas", 0))
        .build();

        let opener = source.create_file_opener(store.clone(), &config, 0);
        let file_meta = FileMeta {
            object_meta: footer_object_meta(),
            range: None,
            extensions: None,
            metadata_size_hint: None,
        };

        let batch_stream = opener
            .open(
                file_meta,
                PartitionedFile::new("test-collection/collection.atlas", 0),
            )
            .unwrap()
            .await
            .unwrap();

        let batches: Vec<RecordBatch> = batch_stream.try_collect().await.unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 5);

        // All batches should have F64 "value" column (i32 was cast to f64)
        for batch in &batches {
            assert_eq!(batch.num_columns(), 1);
            assert_eq!(*batch.schema().field(0).data_type(), ArrowDataType::Float64);
        }
    }

    #[tokio::test]
    async fn test_datasets_disjoint_schemas() {
        // Completely disjoint schemas: ds-a has "x", ds-b has "y"
        // Global schema: x(F32), y(F64)
        // Each dataset should have nulls for the column it's missing.
        let store = Arc::new(InMemory::new());

        let mut writer = AtlasWriter::new(
            store.clone(),
            Path::from("test-collection"),
            NoCompression,
            64 * 1024,
        );

        let x_arr = NdArray::<f32>::try_new_from_vec_in_mem(
            vec![1.0, 2.0],
            vec![2],
            vec!["obs".into()],
            None,
        )
        .unwrap();
        let ds_a = make_dataset("ds-a", vec![("x", Arc::new(x_arr))]).await;

        let y_arr = NdArray::<f64>::try_new_from_vec_in_mem(
            vec![10.0, 20.0, 30.0],
            vec![3],
            vec!["obs".into()],
            None,
        )
        .unwrap();
        let ds_b = make_dataset("ds-b", vec![("y", Arc::new(y_arr))]).await;

        writer.write_dataset(&ds_a).await.unwrap();
        writer.write_dataset(&ds_b).await.unwrap();
        writer.flush().await.unwrap();

        let format = AtlasFormat::new(store.clone(), 64 * 1024 * 1024);
        let ctx = SessionContext::new();
        let state = ctx.state();

        let schema = format
            .infer_schema(
                &state,
                &(store.clone() as Arc<dyn ObjectStore>),
                &[footer_object_meta()],
            )
            .await
            .unwrap();

        assert_eq!(schema.fields().len(), 2);

        let source = AtlasSource::new(store.clone(), 64 * 1024 * 1024);
        let config = FileScanConfigBuilder::new(
            ObjectStoreUrl::parse("memory://").unwrap(),
            schema.clone(),
            Arc::new(source.clone()) as Arc<dyn FileSource>,
        )
        .with_file(PartitionedFile::new("test-collection/collection.atlas", 0))
        .build();

        let opener = source.create_file_opener(store.clone(), &config, 0);
        let file_meta = FileMeta {
            object_meta: footer_object_meta(),
            range: None,
            extensions: None,
            metadata_size_hint: None,
        };

        let batch_stream = opener
            .open(
                file_meta,
                PartitionedFile::new("test-collection/collection.atlas", 0),
            )
            .unwrap()
            .await
            .unwrap();

        let batches: Vec<RecordBatch> = batch_stream.try_collect().await.unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 5); // 2 + 3

        // All batches have both columns
        for batch in &batches {
            assert_eq!(batch.num_columns(), 2);
        }

        // x is null for ds-b's 3 rows, y is null for ds-a's 2 rows
        let mut x_null_count = 0usize;
        let mut y_null_count = 0usize;
        for batch in &batches {
            let x_idx = batch.schema().index_of("x").unwrap();
            let y_idx = batch.schema().index_of("y").unwrap();
            x_null_count += batch.column(x_idx).null_count();
            y_null_count += batch.column(y_idx).null_count();
        }
        assert_eq!(x_null_count, 3); // ds-b has 3 rows, no "x"
        assert_eq!(y_null_count, 2); // ds-a has 2 rows, no "y"
    }

    #[tokio::test]
    async fn test_pruning_filters_datasets() {
        use datafusion::datasource::physical_plan::FileOpener;
        use datafusion::physical_plan::PhysicalExpr;
        use datafusion::{logical_expr::Operator, physical_expr::expressions};

        let store = Arc::new(InMemory::new());

        // Write 3 datasets with distinct temperature ranges so pruning can exclude some.
        // ds-cold: temperature [1.0, 2.0, 3.0]   → min=1, max=3
        // ds-warm: temperature [10.0, 11.0, 12.0] → min=10, max=12
        // ds-hot:  temperature [30.0, 31.0, 32.0] → min=30, max=32
        let mut writer = AtlasWriter::new(
            store.clone(),
            Path::from("prune-test"),
            NoCompression,
            64 * 1024,
        );

        let cold = NdArray::<f32>::try_new_from_vec_in_mem(
            vec![1.0, 2.0, 3.0],
            vec![3],
            vec!["obs".into()],
            None,
        )
        .unwrap();
        let ds_cold = make_dataset("ds-cold", vec![("temperature", Arc::new(cold))]).await;

        let warm = NdArray::<f32>::try_new_from_vec_in_mem(
            vec![10.0, 11.0, 12.0],
            vec![3],
            vec!["obs".into()],
            None,
        )
        .unwrap();
        let ds_warm = make_dataset("ds-warm", vec![("temperature", Arc::new(warm))]).await;

        let hot = NdArray::<f32>::try_new_from_vec_in_mem(
            vec![30.0, 31.0, 32.0],
            vec![3],
            vec!["obs".into()],
            None,
        )
        .unwrap();
        let ds_hot = make_dataset("ds-hot", vec![("temperature", Arc::new(hot))]).await;

        writer.write_dataset(&ds_cold).await.unwrap();
        writer.write_dataset(&ds_warm).await.unwrap();
        writer.write_dataset(&ds_hot).await.unwrap();
        writer.flush().await.unwrap();

        // Build schema
        let format = AtlasFormat::new(store.clone(), 64 * 1024 * 1024);
        let ctx = SessionContext::new();
        let state = ctx.state();
        let schema = format
            .infer_schema(
                &state,
                &(store.clone() as Arc<dyn ObjectStore>),
                &[ObjectMeta {
                    location: Path::from("prune-test/collection.atlas"),
                    last_modified: chrono::Utc::now(),
                    size: 0,
                    e_tag: None,
                    version: None,
                }],
            )
            .await
            .unwrap();

        // Predicate: temperature > 20.0 (should prune ds-cold and ds-warm)
        let col_expr = expressions::col("temperature", &schema).unwrap();
        let lit_expr = expressions::lit(datafusion::scalar::ScalarValue::Float32(Some(20.0)));
        let predicate: Arc<dyn PhysicalExpr> =
            Arc::new(datafusion::physical_expr::expressions::BinaryExpr::new(
                col_expr,
                Operator::Gt,
                lit_expr,
            ));

        // Create source with predicate set
        let mut source = AtlasSource::new(store.clone(), 64 * 1024 * 1024);
        source.predicate = Some(predicate);

        let config = FileScanConfigBuilder::new(
            ObjectStoreUrl::parse("memory://").unwrap(),
            schema.clone(),
            Arc::new(source.clone()) as Arc<dyn FileSource>,
        )
        .with_file(PartitionedFile::new("prune-test/collection.atlas", 0))
        .build();

        let opener = source.create_file_opener(store.clone(), &config, 0);
        let file_meta = FileMeta {
            object_meta: ObjectMeta {
                location: Path::from("prune-test/collection.atlas"),
                last_modified: chrono::Utc::now(),
                size: 0,
                e_tag: None,
                version: None,
            },
            range: None,
            extensions: None,
            metadata_size_hint: None,
        };
        let partitioned_file = PartitionedFile::new("prune-test/collection.atlas", 0);
        let batch_stream = opener
            .open(file_meta, partitioned_file)
            .unwrap()
            .await
            .unwrap();
        let batches: Vec<RecordBatch> = batch_stream.try_collect().await.unwrap();

        // Only ds-hot should survive pruning (3 rows)
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);
    }

    #[tokio::test]
    async fn test_pruning_without_statistics_reads_all() {
        use datafusion::datasource::physical_plan::FileOpener;
        use datafusion::physical_plan::PhysicalExpr;
        use datafusion::{logical_expr::Operator, physical_expr::expressions};

        let store = Arc::new(InMemory::new());

        // Write a single dataset
        let mut writer = AtlasWriter::new(
            store.clone(),
            Path::from("no-stats-test"),
            NoCompression,
            64 * 1024,
        );

        let arr = NdArray::<f32>::try_new_from_vec_in_mem(
            vec![5.0, 6.0],
            vec![2],
            vec!["obs".into()],
            None,
        )
        .unwrap();
        let ds = make_dataset("ds-only", vec![("temperature", Arc::new(arr))]).await;
        writer.write_dataset(&ds).await.unwrap();
        writer.flush().await.unwrap();

        // Build schema
        let format = AtlasFormat::new(store.clone(), 64 * 1024 * 1024);
        let ctx = SessionContext::new();
        let state = ctx.state();
        let schema = format
            .infer_schema(
                &state,
                &(store.clone() as Arc<dyn ObjectStore>),
                &[ObjectMeta {
                    location: Path::from("no-stats-test/collection.atlas"),
                    last_modified: chrono::Utc::now(),
                    size: 0,
                    e_tag: None,
                    version: None,
                }],
            )
            .await
            .unwrap();

        // Predicate on a column that doesn't have statistics files (nonexistent col)
        let col_expr = expressions::col("temperature", &schema).unwrap();
        let lit_expr = expressions::lit(datafusion::scalar::ScalarValue::Float32(Some(100.0)));
        let predicate: Arc<dyn PhysicalExpr> =
            Arc::new(datafusion::physical_expr::expressions::BinaryExpr::new(
                col_expr,
                Operator::Lt,
                lit_expr,
            ));

        let mut source = AtlasSource::new(store.clone(), 64 * 1024 * 1024);
        source.predicate = Some(predicate);

        let config = FileScanConfigBuilder::new(
            ObjectStoreUrl::parse("memory://").unwrap(),
            schema.clone(),
            Arc::new(source.clone()) as Arc<dyn FileSource>,
        )
        .with_file(PartitionedFile::new("no-stats-test/collection.atlas", 0))
        .build();

        let opener = source.create_file_opener(store.clone(), &config, 0);
        let file_meta = FileMeta {
            object_meta: ObjectMeta {
                location: Path::from("no-stats-test/collection.atlas"),
                last_modified: chrono::Utc::now(),
                size: 0,
                e_tag: None,
                version: None,
            },
            range: None,
            extensions: None,
            metadata_size_hint: None,
        };
        let partitioned_file = PartitionedFile::new("no-stats-test/collection.atlas", 0);
        let batch_stream = opener
            .open(file_meta, partitioned_file)
            .unwrap()
            .await
            .unwrap();
        let batches: Vec<RecordBatch> = batch_stream.try_collect().await.unwrap();

        // Without statistics, pruning should be unable to rule out anything → all 2 rows returned
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);
    }
}
