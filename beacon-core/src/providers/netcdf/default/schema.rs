use std::{any::Any, fmt::Formatter, path::PathBuf, sync::Arc};

use arrow::{
    array::{Array, ArrayRef, ListBuilder, RecordBatch, StringArray},
    datatypes::{Field, Fields, SchemaRef},
};
use datafusion::{
    catalog::{Session, TableFunctionImpl, TableProvider},
    datasource::TableType,
    execution::{SendableRecordBatchStream, TaskContext},
    physical_expr::EquivalenceProperties,
    physical_plan::{
        memory::MemoryStream, projection, DisplayAs, DisplayFormatType, ExecutionPlan,
        PlanProperties,
    },
    prelude::Expr,
    scalar::ScalarValue,
};
use netcdf::{Attribute, AttributeValue};

use crate::providers::{netcdf::util, util::find_in};

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct NetCDFSchemaFunction;

impl TableFunctionImpl for NetCDFSchemaFunction {
    fn call(
        &self,
        args: &[datafusion::prelude::Expr],
    ) -> datafusion::error::Result<std::sync::Arc<dyn datafusion::catalog::TableProvider>> {
        //Expect 1 argument, which is a string glob path
        if args.len() != 1 {
            return Err(datafusion::error::DataFusionError::Execution(
                "NetCDF provider expects 1 argument".to_string(),
            ));
        }

        //Get the string literal from the argument
        match &args[0] {
            datafusion::prelude::Expr::Literal(ScalarValue::Utf8(Some(path))) => {
                let paths = find_in(&path, "./")
                    .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;

                let provider = NetCDFSchemaProvider::new(paths)
                    .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;

                Ok(std::sync::Arc::new(provider))
            }
            value => {
                // println!("{:?}", value);
                return Err(datafusion::error::DataFusionError::Execution(
                    "NetCDF provider expects a string literal".to_string(),
                ));
            }
        }
    }
}

#[derive(Debug)]
pub struct NetCDFSchemaProvider {
    files: Vec<PathBuf>,
    provider_schema: SchemaRef,
}

impl NetCDFSchemaProvider {
    pub fn new(paths: Vec<PathBuf>) -> anyhow::Result<Self> {
        let fields = vec![
            arrow::datatypes::Field::new("filename", arrow::datatypes::DataType::Utf8, true),
            arrow::datatypes::Field::new("path", arrow::datatypes::DataType::Utf8, true),
            arrow::datatypes::Field::new("column", arrow::datatypes::DataType::Utf8, true),
            arrow::datatypes::Field::new("dtype", arrow::datatypes::DataType::Utf8, true),
        ];

        let schema = arrow::datatypes::Schema::new(fields);

        Ok(Self {
            files: paths,
            provider_schema: Arc::new(schema),
        })
    }
}

#[async_trait::async_trait]
impl TableProvider for NetCDFSchemaProvider {
    /// Returns the table provider as [`Any`](std::any::Any) so that it can be
    /// downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Get a reference to the schema for this table
    fn schema(&self) -> SchemaRef {
        self.provider_schema.clone()
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
            Some(projections) => Arc::new(self.provider_schema.project(&projections)?),
            None => self.provider_schema.clone(),
        };
        Ok(Arc::new(NetCDFSchemaExec::new(
            self.files.clone(),
            projected_schema,
            projection.cloned(),
        )) as Arc<dyn ExecutionPlan>)
    }
}

#[derive(Debug)]
pub struct NetCDFSchemaExec {
    plan_properties: PlanProperties,
    projection: Option<Vec<usize>>,
    files: Vec<PathBuf>,
}

impl NetCDFSchemaExec {
    pub fn new(files: Vec<PathBuf>, schema: SchemaRef, projection: Option<Vec<usize>>) -> Self {
        Self {
            plan_properties: Self::plan_properties(files.len(), schema),
            files,
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

    fn global_attribute_to_row(
        path: &PathBuf,
        attribute: &Attribute,
    ) -> anyhow::Result<Vec<ArrayRef>> {
        let mut arrays = vec![];
        arrays.push(Arc::new(arrow::array::StringArray::from(vec![path
            .file_name()
            .map(|s| s.to_string_lossy().to_string())]))
            as ArrayRef);
        arrays.push(Arc::new(arrow::array::StringArray::from(vec![path
            .to_string_lossy()
            .to_string()])) as ArrayRef);
        arrays.push(Arc::new(arrow::array::StringArray::from(vec![format!(
            ".{}",
            attribute.name()
        )])) as ArrayRef);
        arrays.push(Arc::new(arrow::array::StringArray::from(vec![format!(
            "{:?}",
            util::nc_attribute_to_arrow_type(attribute).unwrap_or(arrow::datatypes::DataType::Null)
        )])) as ArrayRef);

        Ok(arrays)
    }

    fn variable_attribute_to_row(
        path: &PathBuf,
        var: &netcdf::Variable,
        attribute: &Attribute,
    ) -> anyhow::Result<Vec<ArrayRef>> {
        let mut arrays = vec![];
        arrays.push(Arc::new(arrow::array::StringArray::from(vec![path
            .file_name()
            .map(|s| s.to_string_lossy().to_string())]))
            as ArrayRef);
        arrays.push(Arc::new(arrow::array::StringArray::from(vec![path
            .to_string_lossy()
            .to_string()])) as ArrayRef);
        arrays.push(Arc::new(arrow::array::StringArray::from(vec![format!(
            "{}.{}",
            var.name().to_string(),
            attribute.name()
        )])) as ArrayRef);
        arrays.push(Arc::new(arrow::array::StringArray::from(vec![format!(
            "{:?}",
            util::nc_attribute_to_arrow_type(attribute).unwrap_or(arrow::datatypes::DataType::Null)
        )])) as ArrayRef);

        Ok(arrays)
    }

    fn variable_to_row(path: &PathBuf, var: &netcdf::Variable) -> anyhow::Result<Vec<ArrayRef>> {
        let mut arrays = vec![];
        arrays.push(Arc::new(arrow::array::StringArray::from(vec![path
            .file_name()
            .map(|s| s.to_string_lossy().to_string())]))
            as ArrayRef);
        arrays.push(Arc::new(arrow::array::StringArray::from(vec![path
            .to_string_lossy()
            .to_string()])) as ArrayRef);
        arrays.push(Arc::new(arrow::array::StringArray::from(vec![var
            .name()
            .to_string()])) as ArrayRef);
        arrays.push(Arc::new(arrow::array::StringArray::from(vec![format!(
            "{:?}",
            util::nc_type_to_arrow_type(&var.vartype()).unwrap_or(arrow::datatypes::DataType::Null)
        )])) as ArrayRef);

        Ok(arrays)
    }

    fn transpose(matrix: Vec<Vec<ArrayRef>>) -> anyhow::Result<Vec<ArrayRef>> {
        let mut transposed_arrays: Vec<Vec<Arc<dyn Array>>> = vec![];

        for arrays in matrix {
            for (i, a) in arrays.iter().enumerate() {
                match transposed_arrays.get_mut(i) {
                    Some(arr) => arr.push(a.clone()),
                    None => {
                        transposed_arrays.push(vec![a.clone()]);
                    }
                }
            }
        }

        //Concat tall the arrays together
        let mut concatted_arrays = vec![];
        for arrays in transposed_arrays {
            concatted_arrays.push(arrow::compute::concat(
                &arrays.iter().map(|a| a.as_ref()).collect::<Vec<_>>(),
            )?);
        }

        Ok(concatted_arrays)
    }

    fn read_partition_to_batch(
        &self,
        partition: usize,
    ) -> anyhow::Result<SendableRecordBatchStream> {
        let path = self
            .files
            .get(partition)
            .ok_or_else(|| anyhow::anyhow!("Partition {} does not exist", partition))?;

        let file = netcdf::open(path)?;

        let mut rows = vec![];

        for var in file.variables() {
            rows.push(Self::variable_to_row(path, &var)?);
            for attr in var.attributes() {
                rows.push(Self::variable_attribute_to_row(path, &var, &attr)?);
            }
        }

        for attr in file.attributes() {
            rows.push(Self::global_attribute_to_row(path, &attr)?);
        }

        //Merge arrays
        let mut concatted = Self::transpose(rows)?;

        //Apply projection
        if let Some(projection) = &self.projection {
            concatted = concatted
                .into_iter()
                .enumerate()
                .filter_map(|(i, a)| {
                    if projection.contains(&i) {
                        Some(a)
                    } else {
                        None
                    }
                })
                .collect();
        }

        let record_batch = RecordBatch::try_new(self.schema(), concatted)?;

        Ok(Box::pin(
            MemoryStream::try_new(vec![record_batch], self.schema(), None).unwrap(),
        ))
    }
}

impl DisplayAs for NetCDFSchemaExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "NetCDFSchemaExec")
    }
}

#[async_trait::async_trait]
impl ExecutionPlan for NetCDFSchemaExec {
    fn name(&self) -> &str {
        "NetCDFSchema"
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
