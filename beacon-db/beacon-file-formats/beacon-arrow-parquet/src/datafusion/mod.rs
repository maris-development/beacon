use std::{any::Any, sync::Arc};

use arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use beacon_datafusion_ext::format_ext::DatasetMetadata;
use datafusion::{
    catalog::Session,
    common::{Column, DFSchema, GetExt, Statistics},
    datasource::{
        file_format::{FileFormat, FileFormatFactory, file_compression_type::FileCompressionType},
        physical_plan::{FileScanConfig, FileSinkConfig, FileSource},
    },
    physical_expr::LexRequirement,
    physical_plan::{ExecutionPlan, PhysicalExpr, projection::ProjectionExec},
    prelude::{Expr, cast},
};
use object_store::{ObjectMeta, ObjectStore};

use beacon_common::super_typing::super_type_schema;
use futures::{StreamExt, TryStreamExt, stream};

use beacon_common::file_descriptors::file_open_parallelism;
use beacon_datafusion_ext::format_ext::FileFormatFactoryExt;

#[derive(Debug)]
pub struct ParquetFormatFactory;

impl GetExt for ParquetFormatFactory {
    fn get_ext(&self) -> String {
        "parquet".to_string()
    }
}

impl FileFormatFactory for ParquetFormatFactory {
    fn create(
        &self,
        _state: &dyn Session,
        _format_options: &std::collections::HashMap<String, String>,
    ) -> datafusion::error::Result<Arc<dyn FileFormat>> {
        Ok(Arc::new(ParquetFormat::new()))
    }

    fn default(&self) -> Arc<dyn FileFormat> {
        Arc::new(ParquetFormat::new())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl FileFormatFactoryExt for ParquetFormatFactory {
    fn discover_datasets(
        &self,
        objects: &[ObjectMeta],
    ) -> datafusion::error::Result<Vec<DatasetMetadata>> {
        let datasets = objects
            .iter()
            .filter(|obj| {
                obj.location
                    .extension()
                    .map(|ext| ext == "parquet")
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

#[derive(Debug)]
pub struct ParquetFormat {
    inner: datafusion::datasource::file_format::parquet::ParquetFormat,
}

impl ParquetFormat {
    pub fn new() -> Self {
        Self {
            inner: datafusion::datasource::file_format::parquet::ParquetFormat::default()
                .with_enable_pruning(true)
                .with_skip_metadata(true)
                .with_force_view_types(false),
        }
    }
}

impl Default for ParquetFormat {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl FileFormat for ParquetFormat {
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Returns whether this instance uses compression if applicable
    fn compression_type(&self) -> Option<FileCompressionType> {
        None
    }

    fn get_ext(&self) -> String {
        self.inner.get_ext()
    }

    fn get_ext_with_compression(
        &self,
        file_compression_type: &FileCompressionType,
    ) -> datafusion::error::Result<String> {
        self.inner.get_ext_with_compression(file_compression_type)
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
                async move { self.inner.infer_schema(state, &store, &[object]).await }
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
        self.inner
            .infer_stats(state, store, table_schema, object)
            .await
    }

    /// Take a list of files and convert it to the appropriate executor
    /// according to this file format.
    async fn create_physical_plan(
        &self,
        state: &dyn Session,
        conf: FileScanConfig,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        self.inner.create_physical_plan(state, conf).await
    }

    async fn create_writer_physical_plan(
        &self,
        input: Arc<dyn ExecutionPlan>,
        state: &dyn Session,
        mut conf: FileSinkConfig,
        order_requirements: Option<LexRequirement>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        // ensure all Timestamp(Second) columns are cast to Timestamp(Millisecond)
        let adjusted_input = cast_ts_seconds_to_ms(input, state)?;
        let output_schema = adjusted_input.schema();
        conf.output_schema = output_schema;

        self.inner
            .create_writer_physical_plan(adjusted_input, state, conf, order_requirements)
            .await
    }

    fn file_source(&self, table_schema: datafusion::datasource::table_schema::TableSchema) -> Arc<dyn FileSource> {
        self.inner.file_source(table_schema)
    }
}

fn cast_ts_seconds_to_ms(
    input: Arc<dyn ExecutionPlan>,
    session: &dyn Session,
) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
    let schema = input.schema();
    let df_schema: DFSchema = DFSchema::try_from(schema.clone())?;

    // Build a projection: cast SECOND -> MILLISECOND; keep everything else as-is
    let exprs: Vec<(Arc<dyn PhysicalExpr>, String)> = schema
        .fields()
        .iter()
        .map(|f| {
            let name = f.name().to_string();
            let expr: Arc<dyn PhysicalExpr> = match f.data_type() {
                DataType::Timestamp(TimeUnit::Second, tz) => {
                    // keep timezone if present
                    let target = DataType::Timestamp(TimeUnit::Millisecond, tz.clone());
                    let expr = cast(Expr::Column(Column::new_unqualified(&name)), target);
                    session.create_physical_expr(expr, &df_schema)?
                }
                _ => session
                    .create_physical_expr(Expr::Column(Column::new_unqualified(&name)), &df_schema)?,
            };
            Ok((expr, name))
        })
        .collect::<datafusion::error::Result<Vec<_>>>()?;

    // Wrap the input with the projection so downstream sees the casted schema
    let projected = Arc::new(ProjectionExec::try_new(exprs, input)?);
    Ok(projected)
}

pub mod table_function;
pub use table_function::ReadParquetFunc;

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Float64Array, Int64Array, RecordBatch};
    use arrow::datatypes::{Field, Schema};
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::prelude::SessionContext;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::{ObjectStoreExt, PutPayload};

    /// Writes a single-column parquet file into an in-memory store.
    async fn put_parquet(
        store: &Arc<InMemory>,
        path: &Path,
        field: Field,
        column: ArrayRef,
    ) -> ObjectMeta {
        let schema = Arc::new(Schema::new(vec![field]));
        let batch = RecordBatch::try_new(schema.clone(), vec![column]).expect("valid batch");
        let mut buf: Vec<u8> = Vec::new();
        {
            let mut writer =
                datafusion::parquet::arrow::ArrowWriter::try_new(&mut buf, schema, None)
                    .expect("parquet writer");
            writer.write(&batch).expect("write batch");
            writer.close().expect("close parquet file");
        }
        store
            .put(path, PutPayload::from(buf))
            .await
            .expect("should write parquet fixture");
        store.head(path).await.expect("should stat parquet fixture")
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

    /// Parquet has a single spelling, so the alias list must collapse to the
    /// canonical extension used by the session registry.
    #[test]
    fn factory_extension_identity_is_parquet() {
        let factory = ParquetFormatFactory;
        assert_eq!(factory.get_ext(), "parquet");
        assert_eq!(factory.file_format_name(), "parquet");
        assert_eq!(factory.file_extensions(), vec!["parquet"]);
    }

    /// Only `.parquet` objects may be surfaced as parquet datasets; look-alikes
    /// such as `.parq` or extension-less files must be skipped.
    #[tokio::test]
    async fn discover_datasets_selects_only_parquet_objects() {
        let objects = metas(&["p/a.parquet", "p/b.parq", "p/c.csv", "p/plain"]).await;
        let datasets = ParquetFormatFactory.discover_datasets(&objects).unwrap();
        let paths: Vec<&str> = datasets.iter().map(|d| d.file_path.as_str()).collect();
        assert_eq!(paths, vec!["p/a.parquet"]);
        assert_eq!(datasets[0].format, "parquet");
    }

    /// Multi-file inference must super-type rather than fail on a type mismatch:
    /// Int64 + Float64 for the same column widens to Float64.
    #[tokio::test]
    async fn infer_schema_super_types_across_multiple_files() {
        let store = Arc::new(InMemory::new());
        let object_store: Arc<dyn ObjectStore> = store.clone();
        let a = put_parquet(
            &store,
            &Path::from("ints.parquet"),
            Field::new("value", DataType::Int64, true),
            Arc::new(Int64Array::from(vec![1, 2, 3])),
        )
        .await;
        let b = put_parquet(
            &store,
            &Path::from("floats.parquet"),
            Field::new("value", DataType::Float64, true),
            Arc::new(Float64Array::from(vec![1.5])),
        )
        .await;

        let ctx = SessionContext::new();
        let schema = ParquetFormat::default()
            .infer_schema(&ctx.state(), &object_store, &[a, b])
            .await
            .expect("both parquet files should be inferable");
        assert_eq!(schema.fields().len(), 1);
        assert_eq!(schema.field(0).data_type(), &DataType::Float64);
    }

    /// `force_view_types(false)` is deliberate: string columns must come back as
    /// `Utf8`, not `Utf8View`, because the rest of Beacon assumes the former.
    #[tokio::test]
    async fn infer_schema_keeps_utf8_instead_of_utf8_view() {
        let store = Arc::new(InMemory::new());
        let object_store: Arc<dyn ObjectStore> = store.clone();
        let obj = put_parquet(
            &store,
            &Path::from("strings.parquet"),
            Field::new("name", DataType::Utf8, true),
            Arc::new(arrow::array::StringArray::from(vec!["a", "b"])),
        )
        .await;

        let ctx = SessionContext::new();
        let schema = ParquetFormat::default()
            .infer_schema(&ctx.state(), &object_store, &[obj])
            .await
            .expect("parquet file should be inferable");
        assert_eq!(schema.field(0).data_type(), &DataType::Utf8);
    }

    /// Parquet is internally compressed, so the format reports no container-level
    /// compression and the written extension never gains a suffix.
    #[test]
    fn extension_ignores_container_compression() {
        let format = ParquetFormat::default();
        assert_eq!(format.get_ext(), "parquet");
        assert_eq!(format.compression_type(), None);
        assert_eq!(
            format
                .get_ext_with_compression(&FileCompressionType::UNCOMPRESSED)
                .unwrap(),
            "parquet"
        );
    }

    /// Parquet cannot represent second-granularity timestamps, so the writer path
    /// rewrites them to milliseconds. Everything else, including column order,
    /// names and the timezone of the rewritten column, must be preserved.
    #[test]
    fn cast_ts_seconds_to_ms_rewrites_only_second_timestamps() {
        let ctx = SessionContext::new();
        let state = ctx.state();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "naive_ts",
                DataType::Timestamp(TimeUnit::Second, None),
                true,
            ),
            Field::new(
                "utc_ts",
                DataType::Timestamp(TimeUnit::Second, Some("UTC".into())),
                true,
            ),
            Field::new(
                "micro_ts",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
        ]));

        let input = Arc::new(EmptyExec::new(schema.clone()));
        let projected = cast_ts_seconds_to_ms(input, &state).expect("projection should build");
        let out = projected.schema();

        let names: Vec<&str> = out.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, vec!["id", "naive_ts", "utc_ts", "micro_ts"]);
        assert_eq!(out.field(0).data_type(), &DataType::Int64);
        assert_eq!(
            out.field(1).data_type(),
            &DataType::Timestamp(TimeUnit::Millisecond, None)
        );
        assert_eq!(
            out.field(2).data_type(),
            &DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            "timezone must survive the second -> millisecond rewrite"
        );
        assert_eq!(
            out.field(3).data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, None),
            "non-second timestamps must be left alone"
        );
    }

    /// A schema with no second-granularity timestamps must survive the rewrite
    /// unchanged, so the extra projection is a no-op for the common case.
    #[test]
    fn cast_ts_seconds_to_ms_is_a_noop_without_second_timestamps() {
        let ctx = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, true),
        ]));
        let projected = cast_ts_seconds_to_ms(Arc::new(EmptyExec::new(schema.clone())), &ctx.state())
            .expect("projection should build");
        assert_eq!(projected.schema().fields(), schema.fields());
    }
}
