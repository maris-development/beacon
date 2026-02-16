use std::{any::Any, sync::Arc};

use arrow::datatypes::{DataType, SchemaRef, TimeUnit};
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

use crate::{Dataset, DatasetFormat, FileFormatFactoryExt, max_open_fd};

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
    ) -> datafusion::error::Result<Vec<crate::Dataset>> {
        let datasets = objects
            .iter()
            .filter(|obj| {
                obj.location
                    .extension()
                    .map(|ext| ext == "parquet")
                    .unwrap_or(false)
            })
            .map(|obj| Dataset {
                file_path: obj.location.to_string(),
                format: DatasetFormat::Parquet,
            })
            .collect();
        Ok(datasets)
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
            .buffer_unordered(max_open_fd() as usize) // tune this
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

    fn file_source(&self) -> Arc<dyn FileSource> {
        self.inner.file_source()
    }
}

fn cast_ts_seconds_to_ms(
    input: Arc<dyn ExecutionPlan>,
    session: &dyn Session,
) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
    let schema = input.schema();
    let df_schema: DFSchema = DFSchema::try_from(schema.clone()).unwrap();

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
                    session.create_physical_expr(expr, &df_schema).unwrap()
                }
                _ => session
                    .create_physical_expr(Expr::Column(Column::new_unqualified(&name)), &df_schema)
                    .unwrap(),
            };
            (expr, name)
        })
        .collect();

    // Wrap the input with the projection so downstream sees the casted schema
    let projected = Arc::new(ProjectionExec::try_new(exprs, input)?);
    Ok(projected)
}
