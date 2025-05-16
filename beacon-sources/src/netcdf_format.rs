use std::{
    any::Any,
    fmt::{Debug, Formatter},
    sync::{atomic::AtomicU64, Arc},
};

use arrow::datatypes::SchemaRef;
use async_stream::try_stream;
use beacon_arrow_netcdf::{encoders::default::DefaultEncoder, writer::ArrowRecordBatchWriter};
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

use beacon_common::super_typing;

#[derive(Debug)]
pub struct NetCDFFileFormatFactory;

impl FileFormatFactory for NetCDFFileFormatFactory {
    fn create(
        &self,
        state: &SessionState,
        format_options: &std::collections::HashMap<String, String>,
    ) -> datafusion::error::Result<Arc<dyn FileFormat>> {
        Ok(Arc::new(NetCDFFormat::new()))
    }

    fn default(&self) -> Arc<dyn FileFormat> {
        Arc::new(NetCDFFormat::new())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl GetExt for NetCDFFileFormatFactory {
    fn get_ext(&self) -> String {
        "nc".to_string()
    }
}

#[derive(Debug)]
pub struct NetCDFFormat;

impl NetCDFFormat {
    pub fn new() -> Self {
        Self
    }

    pub fn read_arrow_schema(&self, file_path: &str) -> anyhow::Result<SchemaRef> {
        let reader = beacon_arrow_netcdf::reader::NetCDFArrowReader::new(file_path)?;
        Ok(reader.schema())
    }
}

#[async_trait::async_trait]
impl FileFormat for NetCDFFormat {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_ext(&self) -> String {
        "nc".to_string()
    }

    fn get_ext_with_compression(
        &self,
        _file_compression_type: &FileCompressionType,
    ) -> datafusion::error::Result<String> {
        Ok("nc".to_string())
    }

    async fn infer_schema(
        &self,
        state: &SessionState,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> datafusion::error::Result<SchemaRef> {
        let schemas = objects
            .iter()
            .map(|p| {
                self.read_arrow_schema(&format!(
                    "{}/{}",
                    beacon_config::DATA_DIR.to_string_lossy(),
                    p.location.to_string()
                ))
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
        _filters: Option<&Arc<dyn PhysicalExpr>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(NetCDFExec::new(conf)))
    }

    async fn create_writer_physical_plan(
        &self,
        input: Arc<dyn ExecutionPlan>,
        state: &SessionState,
        conf: FileSinkConfig,
        order_requirements: Option<LexRequirement>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let sink_schema = Arc::clone(conf.output_schema());
        let sink = Arc::new(NetCDFSink {
            input: Arc::clone(&input),
            conf: conf,
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
pub struct NetCDFExec {
    plan_properties: PlanProperties,
    file_scan_config: FileScanConfig,
    projection: Option<Arc<[usize]>>,
    table_schema: SchemaRef,
    schema_adapter_factory: Arc<dyn SchemaAdapterFactory>,
}

impl NetCDFExec {
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
                let reader = beacon_arrow_netcdf::reader::NetCDFArrowReader::new(format!(
                    "{}/{}",
                    beacon_config::DATA_DIR.to_string_lossy(),
                    file_path
                ))
                    .expect("NetCDFArrowReader::new failed");

                let file_schema = reader.schema().clone();

                let (schema_mapper, adapted_projection) = schema_adapter
                    .map_schema(&file_schema)
                    .expect("map_schema failed");

                let batch = reader
                    .read_as_batch(Some(adapted_projection))
                    .expect("read_as_batch failed");

                let mapped_batch = schema_mapper.map_batch(batch).unwrap();

                yield mapped_batch;
            }
        };

        let adapter = RecordBatchStreamAdapter::new(self.schema(), stream);

        Box::pin(adapter)
    }
}

impl DisplayAs for NetCDFExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "NetCDFExec")
    }
}

impl ExecutionPlan for NetCDFExec {
    fn name(&self) -> &'static str {
        "NetCDFExec"
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

pub struct NetCDFSink {
    input: Arc<dyn ExecutionPlan>,
    conf: FileSinkConfig,
}

impl Debug for NetCDFSink {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "NetCDFSink")
    }
}

impl DisplayAs for NetCDFSink {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "NetCDFSink")
    }
}

#[async_trait::async_trait]
impl DataSink for NetCDFSink {
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
        let arrow_schema = self.conf.output_schema().clone();
        let location = self.conf.object_store_url.as_str();
        println!("Writing to NetCDF: {}", location);
        let mut rows_written: u64 = 0;

        let mut nc_writer = ArrowRecordBatchWriter::<DefaultEncoder>::new(location, arrow_schema)
            .map_err(|e| {
            datafusion::error::DataFusionError::Execution(format!(
                "Failed to create NetCDF ArrowRecordBatchWriter: {}",
                e
            ))
        })?;

        let mut pinned_steam = std::pin::pin!(data);

        while let Some(batch) = pinned_steam.next().await {
            let batch = batch?;
            rows_written += batch.num_rows() as u64;
            nc_writer.write_record_batch(batch).map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!(
                    "Failed to write record batch to NetCDF: {}",
                    e
                ))
            })?;
        }

        nc_writer.finish().map_err(|e| {
            datafusion::error::DataFusionError::Execution(format!(
                "Failed to finish writing NetCDF: {}",
                e
            ))
        })?;

        Ok(rows_written)
    }
}
