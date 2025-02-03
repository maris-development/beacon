use std::{any::Any, fmt::Formatter, path::PathBuf, sync::Arc};

use arrow::{
    array::ArrayRef,
    datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit},
};
use datafusion::{
    catalog::{Session, TableFunctionImpl, TableProvider},
    datasource::TableType,
    execution::{SendableRecordBatchStream, TaskContext},
    physical_expr::EquivalenceProperties,
    physical_plan::{
        memory::MemoryStream, DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
    },
    prelude::Expr,
};
use glob::glob;

#[derive(Debug)]
pub struct DatasetsProviderFunction;

impl TableFunctionImpl for DatasetsProviderFunction {
    fn call(&self, args: &[Expr]) -> datafusion::error::Result<Arc<dyn TableProvider>> {
        return Ok(Arc::new(DatasetsProvider::new()));
    }
}

#[derive(Debug)]
pub struct DatasetsProvider {
    directory: PathBuf,
    schema: SchemaRef,
}

impl DatasetsProvider {
    pub fn new() -> Self {
        let dir = PathBuf::from(beacon_config::CONFIG.datasets_dir.clone());

        let schema = Arc::new(Schema::new(vec![
            Field::new("filename", DataType::Utf8, false),
            Field::new("path", DataType::Utf8, false),
            Field::new("file_extension", DataType::Utf8, false),
            Field::new("size", DataType::UInt64, false),
            Field::new(
                "created_at",
                DataType::Timestamp(TimeUnit::Second, None),
                false,
            ),
            Field::new(
                "modified_at",
                DataType::Timestamp(TimeUnit::Second, None),
                false,
            ),
        ]));

        Self {
            directory: dir,
            schema,
        }
    }
}

#[async_trait::async_trait]
impl TableProvider for DatasetsProvider {
    /// Returns the table provider as [`Any`](std::any::Any) so that it can be
    /// downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Get a reference to the schema for this table
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    /// Get the type of this table for metadata/catalog purposes.
    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        //Project the schema
        let projected_schema = match projection {
            Some(projections) => Arc::new(self.schema.project(&projections)?),
            None => self.schema.clone(),
        };

        println!("Schema: {:?}", projected_schema);
        Ok(Arc::new(DatasetsExec::new(
            self.directory.clone(),
            projected_schema,
            projection.cloned(),
        )) as Arc<dyn ExecutionPlan>)
    }
}

#[derive(Debug)]
pub struct DatasetsExec {
    plan_properties: PlanProperties,
    projection: Option<Vec<usize>>,
    directory: PathBuf,
}

impl DatasetsExec {
    pub fn new(directory: PathBuf, schema: SchemaRef, projection: Option<Vec<usize>>) -> Self {
        let plan_properties = Self::plan_properties(1, schema);

        Self {
            plan_properties,
            directory,
            projection,
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

    fn read_partition_to_batch(
        &self,
        _partition: usize,
    ) -> datafusion::error::Result<SendableRecordBatchStream> {
        let mut names = vec![];
        let mut paths = vec![];
        let mut file_extensions = vec![];
        let mut sizes = vec![];
        let mut created_ats = vec![];
        let mut modified_ats = vec![];

        glob(format!("{}/**/*", self.directory.to_string_lossy()).as_str())
            .unwrap()
            .filter_map(Result::ok)
            .for_each(|entry| {
                let metadata = entry.metadata().unwrap();
                let created_at = metadata
                    .created()
                    .unwrap()
                    .duration_since(std::time::UNIX_EPOCH);
                let modified_at = metadata
                    .modified()
                    .unwrap()
                    .duration_since(std::time::UNIX_EPOCH);

                names.push(entry.file_name().unwrap().to_string_lossy().to_string());
                paths.push(entry.to_string_lossy().to_string());
                file_extensions.push(entry.extension().unwrap().to_string_lossy().to_string());
                sizes.push(metadata.len());
                created_ats.push(created_at.unwrap().as_secs() as i64);
                modified_ats.push(modified_at.unwrap().as_secs() as i64);
            });

        let mut named_arrays = vec![
            Arc::new(arrow::array::StringArray::from(names)) as ArrayRef,
            Arc::new(arrow::array::StringArray::from(paths)) as ArrayRef,
            Arc::new(arrow::array::StringArray::from(file_extensions)) as ArrayRef,
            Arc::new(arrow::array::UInt64Array::from(sizes)) as ArrayRef,
            Arc::new(arrow::array::TimestampSecondArray::from(created_ats)) as ArrayRef,
            Arc::new(arrow::array::TimestampSecondArray::from(modified_ats)) as ArrayRef,
        ];

        //Remove arrays that are not in the projection
        if let Some(projection) = &self.projection {
            let mut new_named_arrays = vec![];
            for i in projection {
                new_named_arrays.push(named_arrays[*i].clone());
            }
            named_arrays = new_named_arrays;
        }

        let batch = arrow::record_batch::RecordBatch::try_new(
            self.plan_properties.eq_properties.schema().clone(),
            named_arrays,
        )?;

        Ok(Box::pin(
            MemoryStream::try_new(vec![batch], self.schema(), None).unwrap(),
        ))
    }
}

impl DisplayAs for DatasetsExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "DatasetsExec")
    }
}

#[async_trait::async_trait]
impl ExecutionPlan for DatasetsExec {
    fn name(&self) -> &str {
        "Datasets"
    }

    fn as_any(&self) -> &dyn std::any::Any {
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
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::error::Result<SendableRecordBatchStream> {
        Ok(self
            .read_partition_to_batch(partition)
            .map_err(|e| datafusion::error::DataFusionError::External(e.into()))?)
    }
}
