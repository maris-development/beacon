use std::{any::Any, fmt::Formatter, path::PathBuf, sync::Arc};

use arrow::array::*;
use arrow::buffer::*;
use arrow::datatypes::SchemaRef;
use datafusion::catalog::TableFunctionImpl;
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::scalar::ScalarValue;
use datafusion::{
    catalog::{Session, TableProvider},
    datasource::TableType,
    execution::{SendableRecordBatchStream, TaskContext},
    physical_expr::EquivalenceProperties,
    physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties},
    prelude::Expr,
};
use nd_arrow_array::NdArrowArray;
use netcdf::Extents;

use crate::providers::netcdf::util;
use crate::providers::util::find_in;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct NetCDFDataFunction;

impl TableFunctionImpl for NetCDFDataFunction {
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
                let paths = find_in(&path, "./data/datasets")
                    .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;

                let provider = NetCDFDataProvider::new(paths)
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
pub struct NetCDFDataProvider {
    files: Vec<Arc<netcdf::File>>,
    provider_schema: SchemaRef,
}

impl NetCDFDataProvider {
    pub fn new(paths: Vec<PathBuf>) -> anyhow::Result<Self> {
        let files = paths
            .iter()
            .map(|path| netcdf::open(path).map(Arc::new))
            .collect::<Result<Vec<_>, _>>()?;
        // Determine the schema
        // Read all the global attributes, variables and variable attributes as fields
        let mut schemas = vec![];

        for file in &files {
            let mut fields = util::global_attributes_as_fields(&file);
            fields.extend(util::variables_as_fields(&file));

            let schema = datafusion::arrow::datatypes::Schema::new(fields);
            schemas.push(schema);
        }

        // Merge all the schemas
        let schema = datafusion::arrow::datatypes::Schema::try_merge(schemas)?;
        // println!("Schema: {:?}", schema);
        Ok(Self {
            files,
            provider_schema: Arc::new(schema),
        })
    }
}

#[async_trait::async_trait]
impl TableProvider for NetCDFDataProvider {
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

        Ok(
            Arc::new(NetCDFExec::new(self.files.clone(), projected_schema))
                as Arc<dyn ExecutionPlan>,
        )
    }
}

#[derive(Debug)]
pub struct NetCDFExec {
    plan_properties: PlanProperties,
    netcdf_files: Vec<Arc<netcdf::File>>,
}

impl NetCDFExec {
    pub fn new(files: Vec<Arc<netcdf::File>>, schema: SchemaRef) -> Self {
        Self {
            plan_properties: Self::plan_properties(files.len(), schema),
            netcdf_files: files,
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
        partition: usize,
    ) -> anyhow::Result<SendableRecordBatchStream> {
        let mut arrays = vec![];
        let nc_file = self.netcdf_files[partition].as_ref();
        let reader = NcReader::new(&nc_file);

        for column in self.schema().fields().iter().map(|v| v.name()) {
            //Global attribute
            if column.starts_with('.') {
                arrays.push(reader.read_global_attribute(&column[1..]).unwrap().unwrap());
            } else if column.contains('.') {
                let variable = column.split('.').next().unwrap();
                let attribute = column.split('.').last().unwrap();
                //Variable attribute
                arrays.push(
                    reader
                        .read_variable_attribute(variable, attribute)
                        .unwrap()
                        .unwrap(),
                );
            } else {
                //Variable
                arrays.push(reader.read_variable(&column).unwrap().unwrap());
            }
        }

        //Find the largest shape
        let largest_shape = NdArrowArray::find_broadcast_shape(&arrays).unwrap();

        //Broadcast all the arrays to the largest shape
        let mut broadcasted_arrays = vec![];
        for array in arrays {
            broadcasted_arrays.push(array.broadcast(&largest_shape).into_arrow_array());
        }

        let record_batch = RecordBatch::try_new(self.schema().clone(), broadcasted_arrays).unwrap();

        Ok(Box::pin(
            MemoryStream::try_new(vec![record_batch], self.schema(), None).unwrap(),
        ))
    }
}

impl DisplayAs for NetCDFExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "NetCDFExec")
    }
}

#[async_trait::async_trait]
impl ExecutionPlan for NetCDFExec {
    fn name(&self) -> &str {
        "NetCDF"
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

struct NcReader<'a> {
    file: &'a netcdf::File,
}

impl<'a> NcReader<'a> {
    pub fn new(file: &'a netcdf::File) -> Self {
        Self { file }
    }

    fn generic_binary_to_string_array(
        binary_array: GenericBinaryArray<i32>,
        fill_value: Option<String>,
    ) -> StringArray {
        StringArray::from_iter(binary_array.iter().map(|buf| match buf {
            Some(buf) => {
                let string = String::from_utf8_lossy(buf).to_string();
                let trimmed_string = string.trim_end_matches('\0').to_string();

                if trimmed_string.is_empty() {
                    return None;
                }

                match fill_value.as_ref() {
                    Some(fill_val) => {
                        if fill_val == &trimmed_string {
                            None
                        } else {
                            Some(trimmed_string)
                        }
                    }
                    None => Some(trimmed_string),
                }
            }
            None => None,
        }))
    }

    pub fn read_variable(&self, name: &str) -> anyhow::Result<Option<NdArrowArray>> {
        let var = self.file.variable(name);
        match var {
            Some(var) => {
                let dimensions: Vec<_> = var
                    .dimensions()
                    .iter()
                    .map(|d| (d.name().to_string(), d.len()))
                    .collect();

                let shape = nd_arrow_array::shape::Shape::new_inferred(dimensions);

                match var.vartype() {
                    netcdf::types::NcVariableType::Int(int_type) => match int_type {
                        netcdf::types::IntType::U8 => {
                            let values = var.get_values::<u8, _>(Extents::All)?;
                            let array = Arc::new(UInt8Array::from(values));
                            Ok(Some(NdArrowArray::new(array, shape)))
                        }
                        netcdf::types::IntType::U16 => {
                            let values = var.get_values::<u16, _>(Extents::All)?;
                            let array = Arc::new(UInt16Array::from(values));
                            Ok(Some(NdArrowArray::new(array, shape)))
                        }
                        netcdf::types::IntType::U32 => {
                            let values = var.get_values::<u32, _>(Extents::All)?;
                            let array = Arc::new(UInt32Array::from(values));
                            Ok(Some(NdArrowArray::new(array, shape)))
                        }
                        netcdf::types::IntType::U64 => {
                            let values = var.get_values::<u64, _>(Extents::All)?;
                            let array = Arc::new(UInt64Array::from(values));
                            Ok(Some(NdArrowArray::new(array, shape)))
                        }
                        netcdf::types::IntType::I8 => {
                            let values = var.get_values::<i8, _>(Extents::All)?;
                            let array = Arc::new(Int8Array::from(values));
                            Ok(Some(NdArrowArray::new(array, shape)))
                        }
                        netcdf::types::IntType::I16 => {
                            let values = var.get_values::<i16, _>(Extents::All)?;
                            let array = Arc::new(Int16Array::from(values));
                            Ok(Some(NdArrowArray::new(array, shape)))
                        }
                        netcdf::types::IntType::I32 => {
                            let values = var.get_values::<i32, _>(Extents::All)?;
                            let array = Arc::new(Int32Array::from(values));
                            Ok(Some(NdArrowArray::new(array, shape)))
                        }
                        netcdf::types::IntType::I64 => {
                            let values = var.get_values::<i64, _>(Extents::All)?;
                            let array = Arc::new(Int64Array::from(values));
                            Ok(Some(NdArrowArray::new(array, shape)))
                        }
                    },
                    netcdf::types::NcVariableType::Float(float_type) => match float_type {
                        netcdf::types::FloatType::F32 => {
                            let values = var.get_values::<f32, _>(Extents::All)?;

                            let array = if let Some(fill_value) = var.fill_value::<f32>().unwrap() {
                                let mut builder = Float32Array::builder(values.len());
                                for v in values.iter() {
                                    if v == &fill_value {
                                        builder.append_null();
                                    } else {
                                        builder.append_value(*v);
                                    }
                                }

                                builder.finish()
                            } else {
                                Float32Array::from(values)
                            };

                            Ok(Some(NdArrowArray::new(Arc::new(array), shape)))
                        }
                        netcdf::types::FloatType::F64 => {
                            let values = var.get_values::<f64, _>(Extents::All)?;

                            let array = if let Some(fill_value) = var.fill_value::<f64>().unwrap() {
                                Float64Array::from_iter(values.iter().map(|v| {
                                    if v == &fill_value {
                                        None
                                    } else {
                                        Some(*v)
                                    }
                                }))
                            } else {
                                Float64Array::from(values)
                            };

                            Ok(Some(NdArrowArray::new(Arc::new(array), shape)))
                        }
                    },
                    netcdf::types::NcVariableType::Char => {
                        let raw_buffer = var.get_raw_values(Extents::All)?;

                        //Check if it is a string array
                        let (buffer, offset_buffer, shape) = if true {
                            //Length of the string array is the length of the last dimension
                            let len = var.dimensions().last().unwrap().len();
                            let num_elements = var
                                .dimensions()
                                .iter()
                                .rev()
                                .skip(1)
                                .fold(1, |acc, d| acc * d.len());
                            let mut offsets = vec![0i32; num_elements + 1];
                            for i in 0..num_elements {
                                offsets[i + 1] = offsets[i] + len as i32;
                            }

                            let mut dimensions = vec![];
                            for dims in var.dimensions()[..var.dimensions().len() - 1].iter() {
                                dimensions.push((dims.name().to_string(), dims.len()));
                            }
                            //Create a new shape with the last dimension removed
                            let shape = nd_arrow_array::shape::Shape::new_inferred(dimensions);

                            (
                                Buffer::from_vec(raw_buffer),
                                OffsetBuffer::new(ScalarBuffer::from(offsets)),
                                shape,
                            )
                        } else {
                            let mut offsets = vec![0i32; raw_buffer.len() + 1];
                            for i in 0..raw_buffer.len() {
                                offsets[i + 1] = offsets[i] + 1;
                            }
                            (
                                Buffer::from_vec(raw_buffer),
                                OffsetBuffer::new(ScalarBuffer::from(offsets)),
                                shape,
                            )
                        };
                        let byte_array = GenericBinaryArray::new(offset_buffer, buffer, None);
                        let string_array = Self::generic_binary_to_string_array(byte_array, None);

                        Ok(Some(NdArrowArray::new(Arc::new(string_array), shape)))
                    }
                    _ => {
                        return Err(anyhow::anyhow!(
                            "Unsupported NetCDF variable type: {:?}",
                            var.vartype()
                        ))
                    }
                }
            }
            None => Ok(None),
        }
    }

    pub fn read_global_attribute(&self, name: &str) -> anyhow::Result<Option<NdArrowArray>> {
        let attr = self.file.attribute(name);
        match attr {
            Some(attr) => {
                let value = attr.value()?;
                let scalar = util::attribute_to_scalar(&value)?;
                let shape = nd_arrow_array::shape::Shape::scalar_shape();

                Ok(Some(NdArrowArray::new(scalar.into_inner(), shape)))
            }
            None => Ok(None),
        }
    }

    pub fn read_variable_attribute(
        &self,
        variable_name: &str,
        attribute_name: &str,
    ) -> anyhow::Result<Option<NdArrowArray>> {
        let var = self.file.variable(variable_name);
        match var {
            Some(var) => {
                let attr = var.attribute(attribute_name);
                match attr {
                    Some(attr) => {
                        let value = attr.value()?;
                        let scalar = util::attribute_to_scalar(&value)?;
                        let shape = nd_arrow_array::shape::Shape::scalar_shape();

                        Ok(Some(NdArrowArray::new(scalar.into_inner(), shape)))
                    }
                    None => Ok(None),
                }
            }
            None => Ok(None),
        }
    }
}
