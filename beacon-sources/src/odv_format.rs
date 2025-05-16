use std::{
    any::Any,
    fmt::{Debug, Formatter},
    fs::File,
    io::BufWriter,
    sync::Arc,
};

use arrow::datatypes::SchemaRef;
use async_stream::try_stream;
use beacon_arrow_odv::writer::{AsyncOdvWriter, OdvOptions};
use beacon_common::super_typing;
use datafusion::{
    common::{GetExt, Statistics},
    datasource::{
        file_format::{file_compression_type::FileCompressionType, FileFormat, FileFormatFactory},
        physical_plan::{FileScanConfig, FileSinkConfig},
        schema_adapter::{DefaultSchemaAdapterFactory, SchemaAdapterFactory},
    },
    execution::{SendableRecordBatchStream, SessionState, TaskContext},
    physical_expr::{EquivalenceProperties, LexRequirement},
    physical_plan::{
        insert::{DataSink, DataSinkExec},
        stream::RecordBatchStreamAdapter,
        DisplayAs, DisplayFormatType, ExecutionPlan, PhysicalExpr, PlanProperties,
    },
};
use futures::StreamExt;
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
        match self.options {
            Some(ref options) => {
                let format = OdvFormat::new_with_options(options.clone());
                return Ok(Arc::new(format) as Arc<dyn FileFormat>);
            }
            None => {
                return Ok(Arc::new(OdvFormat::new()) as Arc<dyn FileFormat>);
            }
        }
    }

    fn default(&self) -> Arc<dyn FileFormat> {
        Arc::new(OdvFormat::new())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Debug)]
pub struct OdvFormat {
    options: Option<OdvOptions>,
}

impl OdvFormat {
    pub fn new() -> Self {
        OdvFormat { options: None }
    }
    pub fn new_with_options(options: OdvOptions) -> Self {
        OdvFormat {
            options: Some(options),
        }
    }
}

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

    async fn create_writer_physical_plan(
        &self,
        input: Arc<dyn ExecutionPlan>,
        state: &SessionState,
        conf: FileSinkConfig,
        order_requirements: Option<LexRequirement>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let sink_schema = Arc::clone(conf.output_schema());
        let sink = Arc::new(OdvSink {
            input: Arc::clone(&input),
            config: conf,
            odv_options: self.options.clone(),
        });

        Ok(Arc::new(DataSinkExec::new(
            input,
            sink,
            sink_schema,
            order_requirements,
        )))
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

pub struct OdvSink {
    input: Arc<dyn ExecutionPlan>,
    config: FileSinkConfig,
    odv_options: Option<OdvOptions>,
}

impl Debug for OdvSink {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "OdvSink")
    }
}

impl DisplayAs for OdvSink {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "OdvSink")
    }
}

#[async_trait::async_trait]
impl DataSink for OdvSink {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn metrics(&self) -> Option<datafusion::physical_plan::metrics::MetricsSet> {
        None
    }

    async fn write_all(
        &self,
        data: SendableRecordBatchStream,
        context: &Arc<TaskContext>,
    ) -> datafusion::error::Result<u64> {
        let arrow_schema = self.config.output_schema().clone();

        let mut rows_written: u64 = 0;

        let odv_options = self.odv_options.clone().unwrap_or(
            OdvOptions::try_from_arrow_schema(arrow_schema.clone()).map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!(
                    "Failed to implicitly define ODV settings: {}",
                    e
                ))
            })?,
        );
        let temp_dir = tempfile::tempdir()?;
        let mut odv_writer =
            AsyncOdvWriter::new(odv_options, arrow_schema.clone(), temp_dir.path())
                .await
                .map_err(|e| {
                    datafusion::error::DataFusionError::Execution(format!(
                        "Failed to create ODV writer: {}",
                        e
                    ))
                })?;

        let mut stream = std::pin::pin!(data);

        while let Some(batch) = stream.next().await {
            let batch = batch?;
            rows_written += batch.num_rows() as u64;
            odv_writer.write(batch).await.map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!(
                    "Failed to write ODV batch: {}",
                    e
                ))
            })?;
        }

        let output_path = format!(
            "{}/{}",
            beacon_config::DATA_DIR.to_string_lossy(),
            self.config.table_paths[0].prefix().to_string()
        );

        let mut zip_file_writer = BufWriter::new(File::create(output_path).map_err(|e| {
            datafusion::error::DataFusionError::Execution(format!(
                "Failed to create ODV zip file writer: {}",
                e
            ))
        })?);

        odv_writer
            .finish_to_archive(&mut zip_file_writer)
            .map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!(
                    "Failed to create ODV zip archive: {}",
                    e
                ))
            })?;

        Ok(rows_written)
    }
}
