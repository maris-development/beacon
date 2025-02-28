use std::{
    any::Any,
    fmt::Formatter,
    sync::{Arc, Mutex},
};

use arrow::{
    datatypes::{Schema, SchemaRef},
    error::ArrowError,
};
use async_stream::{stream, try_stream};
use beacon_arrow_netcdf::encoders::default::DefaultEncoder;
use datafusion::{
    common::{not_impl_err, Statistics},
    datasource::{
        file_format::{
            file_compression_type::FileCompressionType, FileFormat, FilePushdownSupport,
        },
        physical_plan::{FileMeta, FileOpenFuture, FileOpener, FileScanConfig, FileSinkConfig},
        schema_adapter::{DefaultSchemaAdapterFactory, SchemaAdapterFactory},
    },
    execution::{SendableRecordBatchStream, SessionState, TaskContext},
    logical_expr::dml::InsertOp,
    physical_expr::{EquivalenceProperties, LexRequirement},
    physical_plan::{
        insert::{DataSink, DataSinkExec},
        memory::MemoryStream,
        metrics::MetricsSet,
        stream::RecordBatchStreamAdapter,
        DisplayAs, DisplayFormatType, ExecutionPlan, PhysicalExpr, PlanProperties,
    },
    prelude::Expr,
};
use futures::{pin_mut, StreamExt};
use object_store::{ObjectMeta, ObjectStore};

use crate::super_typing;

#[derive(Debug)]
pub struct NetCDFFormat {
    schema_adapter: Arc<dyn SchemaAdapterFactory>,
}

impl NetCDFFormat {
    pub fn new() -> Self {
        Self {
            schema_adapter: Arc::new(DefaultSchemaAdapterFactory),
        }
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
            .map(|object| object.location.filename())
            .flat_map(|p| p.map(|p| self.read_arrow_schema(&p)))
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
        if conf.insert_op != InsertOp::Append {
            return not_impl_err!("Only Append is supported");
        }
        let file_path = conf.table_paths.get(0).unwrap().to_string();

        let sink_schema = Arc::clone(conf.output_schema());
        let sink = Arc::new(NetCDFSink {
            sink_conf: conf,
            nc_writer: Arc::new(Mutex::new(
                beacon_arrow_netcdf::writer::ArrowRecordBatchWriter::new(
                    file_path,
                    sink_schema.clone(),
                )
                .expect("ArrowRecordBatchWriter::new failed"),
            )),
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
        Self {
            plan_properties: Self::plan_properties(
                file_scan_conf.file_groups.len(),
                file_scan_conf.file_schema.clone(),
            ),
            projection: file_scan_conf.projection.clone().map(Arc::from),
            schema_adapter_factory: Arc::new(DefaultSchemaAdapterFactory),
            table_schema: file_scan_conf.file_schema.clone(),
            file_scan_config: file_scan_conf,
        }
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
                let reader = beacon_arrow_netcdf::reader::NetCDFArrowReader::new(file_path)
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
    sink_conf: FileSinkConfig,
    nc_writer: Arc<Mutex<beacon_arrow_netcdf::writer::ArrowRecordBatchWriter<DefaultEncoder>>>,
}

impl std::fmt::Debug for NetCDFSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CsvSink").finish()
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

    fn metrics(&self) -> Option<MetricsSet> {
        None
    }

    async fn write_all(
        &self,
        data: SendableRecordBatchStream,
        _context: &Arc<TaskContext>,
    ) -> datafusion::error::Result<u64> {
        pin_mut!(data);
        while let Some(batch) = data.next().await {
            let batch = batch?;
            self.nc_writer
                .lock()
                .unwrap()
                .write_record_batch(batch)
                .expect("write_record_batch failed");
        }

        self.nc_writer
            .lock()
            .unwrap()
            .finish()
            .expect("finish failed");

        Ok(0)
    }
}
