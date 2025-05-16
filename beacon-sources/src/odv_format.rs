use std::{any::Any, fmt::Formatter, sync::Arc};

use arrow::datatypes::{Schema, SchemaRef};
use async_stream::try_stream;
use beacon_arrow_odv::writer::OdvOptions;
use beacon_common::super_typing;
use datafusion::{
    common::{GetExt, Statistics},
    datasource::{
        file_format::{
            file_compression_type::FileCompressionType, FileFormat, FileFormatFactory,
            FilePushdownSupport,
        },
        physical_plan::{FileScanConfig, FileSinkConfig},
        schema_adapter::{DefaultSchemaAdapterFactory, SchemaAdapterFactory},
    },
    execution::{SendableRecordBatchStream, SessionState, TaskContext},
    physical_expr::EquivalenceProperties,
    physical_plan::{
        stream::RecordBatchStreamAdapter, DisplayAs, DisplayFormatType, ExecutionPlan,
        PhysicalExpr, PlanProperties,
    },
};
use object_store::{ObjectMeta, ObjectStore};

#[derive(Debug)]
pub struct OdvFileFormatFactory {
    options: Option<OdvOptions>,
}

impl OdvFileFormatFactory {
    pub fn new(options: Option<OdvOptions>) -> Self {
        OdvFileFormatFactory { options }
    }

    pub fn options(&self) -> &Option<OdvOptions> {
        &self.options
    }

    pub fn set_options(&mut self, options: OdvOptions) {
        self.options = Some(options);
    }

    pub fn clear_options(&mut self) {
        self.options = None;
    }
}

impl GetExt for OdvFileFormatFactory {
    fn get_ext(&self) -> String {
        "txt".to_string()
    }
}

impl FileFormatFactory for OdvFileFormatFactory {
    fn create(
        &self,
        state: &SessionState,
        format_options: &std::collections::HashMap<String, String>,
    ) -> datafusion::error::Result<Arc<dyn FileFormat>> {
        Ok(Arc::new(OdvFormat))
    }

    fn default(&self) -> Arc<dyn FileFormat> {
        Arc::new(OdvFormat)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Debug)]
pub struct OdvFormat;

#[async_trait::async_trait]
impl FileFormat for OdvFormat {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_ext(&self) -> String {
        "txt".to_string()
    }

    fn get_ext_with_compression(
        &self,
        _file_compression_type: &FileCompressionType,
    ) -> datafusion::error::Result<String> {
        Ok("txt".to_string())
    }

    async fn infer_schema(
        &self,
        state: &SessionState,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> datafusion::error::Result<SchemaRef> {
        let schemas = objects
            .iter()
            .map(|object| object.location.clone())
            .map(|p| {
                let reader = beacon_arrow_odv::reader::OdvReader::new(
                    format!(
                        "{}/{}",
                        beacon_config::DATA_DIR.to_string_lossy(),
                        p.to_string()
                    ),
                    4096,
                )
                .map_err(|e| {
                    datafusion::error::DataFusionError::Execution(format!(
                        "Failed to create ODV reader: {}",
                        e
                    ))
                })?;
                Ok(reader.schema())
            })
            .collect::<anyhow::Result<Vec<_>>>()
            .map_err(|e| datafusion::error::DataFusionError::Internal(e.to_string()))?;
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
        conf: FileScanConfig,
        filters: Option<&Arc<dyn PhysicalExpr>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(OdvExec::new(conf)))
    }
}

#[derive(Debug)]
pub struct OdvExec {
    plan_properties: PlanProperties,
    file_scan_config: FileScanConfig,
    projection: Option<Arc<[usize]>>,
    table_schema: SchemaRef,
    schema_adapter_factory: Arc<dyn SchemaAdapterFactory>,
}

impl OdvExec {
    pub fn new(file_scan_conf: FileScanConfig) -> Self {
        let projected_schema = file_scan_conf
            .projection
            .as_ref()
            .map(|p| Arc::new(file_scan_conf.file_schema.project(p).unwrap()))
            .unwrap_or(file_scan_conf.file_schema.clone());

        Self {
            plan_properties: Self::plan_properties(
                file_scan_conf.file_groups.len(),
                projected_schema,
            ),
            projection: file_scan_conf.projection.clone().map(Arc::from),
            schema_adapter_factory: Arc::new(DefaultSchemaAdapterFactory),
            table_schema: file_scan_conf.file_schema.clone(),
            file_scan_config: file_scan_conf,
        }
    }

    pub fn file_scan_config(&self) -> &FileScanConfig {
        &self.file_scan_config
    }

    fn plan_properties(num_partitions: usize, schema: SchemaRef) -> PlanProperties {
        let schema = schema.clone();

        PlanProperties::new(
            EquivalenceProperties::new(schema),
            datafusion::physical_plan::Partitioning::UnknownPartitioning(num_partitions),
            datafusion::physical_plan::execution_plan::EmissionType::Incremental,
            datafusion::physical_plan::execution_plan::Boundedness::Bounded,
        )
    }

    fn read_partition(&self, partition: usize) -> SendableRecordBatchStream {
        let partition = self.file_scan_config.file_groups[partition].clone();

        let table_schema = self.table_schema.clone();
        let projected_table_schema = if let Some(projection) = &self.projection {
            Arc::new(table_schema.project(projection).unwrap())
        } else {
            table_schema.clone()
        };

        let schema_adapter = self
            .schema_adapter_factory
            .create(projected_table_schema.clone(), self.table_schema.clone());

        let stream = try_stream! {
            for sub_partition in partition {
                let file_path = sub_partition.path().to_string();
                let mut reader = beacon_arrow_odv::reader::OdvReader::new(format!(
                    "{}/{}",
                    beacon_config::DATA_DIR.to_string_lossy(),
                    file_path
                ), 4096).map_err(|e| {
                    datafusion::error::DataFusionError::Execution(format!("Failed to create ODV reader: {}", e))
                })?;

                let file_schema = reader.schema().clone();

                let (schema_mapper, adapted_projection) = schema_adapter
                    .map_schema(&file_schema)
                    .expect("map_schema failed");

                while let Some(batch)= reader.read(Some(&adapted_projection)) {
                    let batch = batch.map_err(|e| {
                        datafusion::error::DataFusionError::Execution(format!("Failed to read ODV batch: {}", e))
                    })?;

                    let mapped_batch = schema_mapper.map_batch(batch).unwrap();

                    yield mapped_batch;
                }
            }
        };

        let adapter = RecordBatchStreamAdapter::new(self.schema(), stream);

        Box::pin(adapter)
    }
}

impl DisplayAs for OdvExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "OdvExec")
    }
}

impl ExecutionPlan for OdvExec {
    fn name(&self) -> &'static str {
        "OdvExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.plan_properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> datafusion::error::Result<SendableRecordBatchStream> {
        Ok(self.read_partition(partition))
    }
}
