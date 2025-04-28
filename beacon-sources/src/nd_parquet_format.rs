use std::{any::Any, fmt::Formatter, sync::Arc};

use arrow::{compute::kernels::partition, datatypes::SchemaRef};
use async_stream::try_stream;
use beacon_common::super_typing;
use datafusion::{
    common::{GetExt, Statistics},
    datasource::{
        file_format::{
            file_compression_type::FileCompressionType, parquet::ParquetFormat, FileFormat,
            FileFormatFactory,
        },
        physical_plan::{FileScanConfig, ParquetExec},
        schema_adapter::{DefaultSchemaAdapterFactory, SchemaAdapterFactory},
    },
    execution::{SendableRecordBatchStream, SessionState, TaskContext},
    physical_expr::EquivalenceProperties,
    physical_plan::{
        stream::RecordBatchStreamAdapter, DisplayAs, DisplayFormatType, ExecutionPlan,
        PhysicalExpr, PlanProperties,
    },
};
use futures::StreamExt;
use nd_arrow_array::nd_record_batch::NdRecordBatch;
use object_store::{ObjectMeta, ObjectStore};

use crate::util;

#[derive(Debug)]
pub struct NdParquetFormatFactory;

impl FileFormatFactory for NdParquetFormatFactory {
    fn create(
        &self,
        state: &SessionState,
        format_options: &std::collections::HashMap<String, String>,
    ) -> datafusion::error::Result<Arc<dyn FileFormat>> {
        Ok(Arc::new(NdParquetFormat::new()))
    }

    fn default(&self) -> Arc<dyn FileFormat> {
        Arc::new(NdParquetFormat::new())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl GetExt for NdParquetFormatFactory {
    fn get_ext(&self) -> String {
        nd_arrow_array::consts::PARQUET_EXTENSION.to_string()
    }
}

#[derive(Debug)]
pub struct NdParquetFormat {
    inner_format: ParquetFormat,
}

impl NdParquetFormat {
    pub fn new() -> Self {
        Self {
            inner_format: ParquetFormat::default()
                .with_enable_pruning(false)
                .with_skip_metadata(false)
                .with_force_view_types(false),
        }
    }
}

#[async_trait::async_trait]
impl FileFormat for NdParquetFormat {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_ext(&self) -> String {
        nd_arrow_array::consts::PARQUET_EXTENSION.to_string()
    }

    fn get_ext_with_compression(
        &self,
        _file_compression_type: &FileCompressionType,
    ) -> datafusion::error::Result<String> {
        Ok(nd_arrow_array::consts::PARQUET_EXTENSION.to_string())
    }

    async fn infer_schema(
        &self,
        state: &SessionState,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> datafusion::error::Result<SchemaRef> {
        //Retrieve the schema for each object
        let mut schemas = Vec::new();
        for object in objects {
            let schema = self
                .inner_format
                .infer_schema(state, store, &[object.clone()])
                .await?;

            schemas.push(Arc::new(
                nd_arrow_array::nd_schema::translate_from_nd_schema_to_simplified(&schema),
            ));
        }

        //Supertype the schema
        let super_schema = super_typing::super_type_schema(&schemas).map_err(|e| {
            datafusion::error::DataFusionError::Execution(format!("Failed to infer schema: {}", e))
        })?;

        Ok(Arc::new(super_schema))
    }

    async fn infer_stats(
        &self,
        state: &SessionState,
        store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        object: &ObjectMeta,
    ) -> datafusion::error::Result<Statistics> {
        Ok(Statistics::new_unknown(&table_schema))
    }

    async fn create_physical_plan(
        &self,
        state: &SessionState,
        mut conf: FileScanConfig,
        filters: Option<&Arc<dyn PhysicalExpr>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        //Translate simplified schema to nd schema
        let schema = conf.file_schema.clone();
        let nd_schema = nd_arrow_array::nd_schema::translate_simplified_to_nd_schema(&schema);

        conf.file_schema = nd_schema;

        //Create the physical plan
        let plan = self
            .inner_format
            .create_physical_plan(state, conf, filters)
            .await?;

        todo!()
    }
}

#[derive(Debug)]
pub struct NdParquetExec {
    inner_exec: Arc<dyn ExecutionPlan>,
    plan_properties: PlanProperties,
}

impl NdParquetExec {
    pub fn new(inner_exec: Arc<dyn ExecutionPlan>) -> Self {
        let schema = inner_exec.schema();
        let simplified_schema =
            nd_arrow_array::nd_schema::translate_from_nd_schema_to_simplified(&schema);

        let input_props = inner_exec.properties();
        let eq_props = input_props
            .eq_properties
            .clone()
            .with_new_schema(simplified_schema.into())
            .unwrap();
        let plan_properties = input_props.clone().with_eq_properties(eq_props);
        Self {
            inner_exec,
            plan_properties,
        }
    }
}

impl DisplayAs for NdParquetExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => write!(f, "NdParquetExec"),
            DisplayFormatType::Verbose => write!(f, "NdParquetExec"),
        }
    }
}
#[async_trait::async_trait]
impl ExecutionPlan for NdParquetExec {
    fn name(&self) -> &str {
        "NdParquetExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.inner_exec.schema()
    }

    fn properties(&self) -> &PlanProperties {
        self.inner_exec.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.inner_exec.children()
    }

    fn with_new_children(
        self: Arc<Self>,
        _new_children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Err(datafusion::error::DataFusionError::NotImplemented(
            "NdParquetExec does not support with_new_children".to_string(),
        ))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::error::Result<SendableRecordBatchStream> {
        let mut stream = self.inner_exec.execute(partition, context.clone())?;

        let stream = try_stream! {
            while let Some(batch) = stream.next().await {
                let batch = batch?;
                let nd_batch = nd_arrow_array::nd_record_batch::NdRecordBatch::from_arrow_encoded_record_batch_impl(batch).map_err(|e| {
                    datafusion::error::DataFusionError::Execution(format!(
                        "Failed to convert batch to NdRecordBatch: {}",
                        e
                    ))
                })?;
                let broadcasted = nd_batch.broadcast_to_record_batch().map_err(|e| {
                    datafusion::error::DataFusionError::Execution(format!(
                        "Failed to broadcast NdRecordBatch: {}",
                        e
                    ))
                })?;

                drop(nd_batch);

                yield broadcasted;
            }
        };
        let adapter = RecordBatchStreamAdapter::new(
            self.plan_properties.eq_properties.schema().clone(),
            stream,
        );

        Ok(Box::pin(adapter))
    }
}
