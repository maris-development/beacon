use std::{collections::HashMap, path::Path, sync::Arc};

use arrow::{
    array::{Array, ArrayRef, AsArray, RecordBatch, Scalar},
    datatypes::Schema,
};
use arrow_schema::FieldRef;
use beacon_nd_arrow::{array::backend::ArrayBackend, NdArrowArray};
use ndarray::{ArrayBase, ArrayD};
use netcdf::{
    types::{FloatType, IntType, NcVariableType},
    Attribute, AttributeValue, Extents, Variable,
};

use crate::{
    backend::{AttributeBackend, VariableBackend},
    decoders::{
        self,
        cf_time::{extract_epoch, extract_units},
        DefaultVariableDecoder, VariableDecoder,
    },
};

pub struct NetCDFArrowReader {
    file_schema: arrow::datatypes::SchemaRef,
    file_arrays: Vec<NdArrowArray<Arc<dyn ArrayBackend>>>,
    file: Arc<netcdf::File>,
}

impl NetCDFArrowReader {
    pub fn new<P: AsRef<Path>>(path: P) -> anyhow::Result<Self> {
        let file = netcdf::open(path)?;
        let file_ref = Arc::new(file);

        let mut file_schema_fields = Vec::new();
        let mut file_arrays = Vec::new();

        // Process variables and their attributes
        for variable in file_ref.variables() {
            let variable_field = Self::variable_to_field(&variable)?;

            // Get attributes for the variable
            let variable_attributes = Self::variable_attribute_values(&variable)?;

            // Fill Value from attributes
            let fill_value = variable_attributes
                .get("_FillValue")
                .cloned()
                .map(|a| a.into_inner());

            let mut variable_decoder: Arc<dyn VariableDecoder> = Arc::new(
                DefaultVariableDecoder::new(variable_field.clone(), variable.vartype(), fill_value),
            );

            // Find CF time variables and decode them
            if let Some(units_val) = variable_attributes.get("units") {
                if let Some(units_str) = units_val.clone().into_inner().as_string_opt::<i32>() {
                    let units_str_l = units_str.value(0).to_lowercase();
                    let units = extract_units(&units_str_l);
                    let epoch = extract_epoch(&units_str_l);
                    if let (Some(units), Some(epoch)) = (units, epoch) {
                        variable_decoder = Arc::new(decoders::cf_time::CFTimeVariableDecoder::new(
                            variable_field.clone(),
                            variable_decoder.clone(),
                            epoch,
                            units,
                        ));
                    }
                }
            }
            let variable_backend = Arc::new(VariableBackend::new(
                variable_decoder,
                Arc::clone(&file_ref),
                variable.dimensions().iter().map(|d| d.len()).collect(),
                variable
                    .dimensions()
                    .iter()
                    .map(|d| d.name().to_string())
                    .collect(),
            )) as Arc<dyn ArrayBackend>;
            let variable_array =
                NdArrowArray::new(variable_backend, variable_field.data_type().clone())?;

            file_schema_fields.push(variable_field);
            file_arrays.push(variable_array);

            // Process variable attributes as separate fields
            for (attr_name, attr_value) in variable_attributes {
                let attr_type = attr_value.clone().into_inner().data_type().clone();
                let attr_field = Arc::new(arrow_schema::Field::new(
                    format!("{}.{}", variable.name(), attr_name),
                    attr_type,
                    true,
                ));

                let attribute_backend =
                    Arc::new(AttributeBackend::new(attr_field.clone(), attr_value))
                        as Arc<dyn ArrayBackend>;

                let attribute_array =
                    NdArrowArray::new(attribute_backend, attr_field.data_type().clone())?;

                file_schema_fields.push(attr_field);
                file_arrays.push(attribute_array);
            }
        }

        let file_schema = Arc::new(Schema::new(file_schema_fields));

        Ok(Self {
            file_schema,
            file_arrays,
            file: file_ref,
        })
    }

    pub fn dimensions(&self) -> Vec<(String, usize)> {
        self.file
            .dimensions()
            .map(|d| (d.name().to_string(), d.len()))
            .collect()
    }

    pub fn schema(&self) -> arrow::datatypes::SchemaRef {
        self.file_schema.clone()
    }

    pub fn read_as_batch<P: AsRef<[usize]>>(
        &self,
        projection: Option<P>,
    ) -> anyhow::Result<RecordBatch> {
        todo!()
    }

    pub fn read_column(
        &self,
        column_name: &str,
    ) -> anyhow::Result<NdArrowArray<Arc<dyn ArrayBackend>>> {
        todo!()
    }

    fn variable_to_field(variable: &netcdf::Variable) -> anyhow::Result<FieldRef> {
        let dtype = match variable.vartype() {
            NcVariableType::Int(IntType::I8) => Ok(arrow_schema::DataType::Int8),
            NcVariableType::Int(IntType::I16) => Ok(arrow_schema::DataType::Int16),
            NcVariableType::Int(IntType::I32) => Ok(arrow_schema::DataType::Int32),
            NcVariableType::Int(IntType::I64) => Ok(arrow_schema::DataType::Int64),
            NcVariableType::Int(IntType::U8) => Ok(arrow_schema::DataType::UInt8),
            NcVariableType::Int(IntType::U16) => Ok(arrow_schema::DataType::UInt16),
            NcVariableType::Int(IntType::U32) => Ok(arrow_schema::DataType::UInt32),
            NcVariableType::Int(IntType::U64) => Ok(arrow_schema::DataType::UInt64),
            NcVariableType::Float(FloatType::F32) => Ok(arrow_schema::DataType::Float32),
            NcVariableType::Float(FloatType::F64) => Ok(arrow_schema::DataType::Float64),
            NcVariableType::Char => Ok(arrow_schema::DataType::Utf8),
            NcVariableType::String => Ok(arrow_schema::DataType::Utf8),
            _ => Err(anyhow::anyhow!(
                "Unsupported NetCDF variable type: {:?}",
                variable.vartype()
            )),
        };

        Ok(Arc::new(arrow_schema::Field::new(
            variable.name(),
            dtype?,
            true,
        )))
    }

    fn global_attribute_values(
        file: &netcdf::File,
    ) -> anyhow::Result<HashMap<String, Scalar<ArrayRef>>> {
        let mut attribute_values = HashMap::new();
        for attribute in file.attributes() {
            let value = attribute.value()?;
            let scalar = Self::attribute_value_to_scalar(&value)?;
            attribute_values.insert(attribute.name().to_string(), scalar);
        }
        Ok(attribute_values)
    }

    fn variable_attribute_values(
        variable: &netcdf::Variable,
    ) -> anyhow::Result<HashMap<String, Scalar<ArrayRef>>> {
        let mut attribute_values = HashMap::new();
        for attribute in variable.attributes() {
            let value = attribute.value()?;
            let scalar = Self::attribute_value_to_scalar(&value)?;
            attribute_values.insert(attribute.name().to_string(), scalar);
        }
        Ok(attribute_values)
    }

    fn attribute_value_to_scalar(value: &AttributeValue) -> anyhow::Result<Scalar<ArrayRef>> {
        match value {
            AttributeValue::Uchar(value) => {
                Ok(Scalar::new(Arc::new(arrow::array::UInt8Array::from(vec![
                    *value,
                ]))))
            }
            AttributeValue::Schar(value) => {
                Ok(Scalar::new(Arc::new(arrow::array::Int8Array::from(vec![
                    *value,
                ]))))
            }
            AttributeValue::Ushort(value) => Ok(Scalar::new(Arc::new(
                arrow::array::UInt16Array::from(vec![*value]),
            ))),
            AttributeValue::Short(value) => {
                Ok(Scalar::new(Arc::new(arrow::array::Int16Array::from(vec![
                    *value,
                ]))))
            }
            AttributeValue::Uint(value) => Ok(Scalar::new(Arc::new(
                arrow::array::UInt32Array::from(vec![*value]),
            ))),
            AttributeValue::Int(value) => {
                Ok(Scalar::new(Arc::new(arrow::array::Int32Array::from(vec![
                    *value,
                ]))))
            }
            AttributeValue::Ulonglong(value) => Ok(Scalar::new(Arc::new(
                arrow::array::UInt64Array::from(vec![*value]),
            ))),
            AttributeValue::Longlong(value) => {
                Ok(Scalar::new(Arc::new(arrow::array::Int64Array::from(vec![
                    *value,
                ]))))
            }
            AttributeValue::Float(value) => Ok(Scalar::new(Arc::new(
                arrow::array::Float32Array::from(vec![*value]),
            ))),
            AttributeValue::Double(value) => Ok(Scalar::new(Arc::new(
                arrow::array::Float64Array::from(vec![*value]),
            ))),
            AttributeValue::Str(value) => Ok(Scalar::new(Arc::new(
                arrow::array::StringArray::from(vec![value.clone()]),
            ))),
            _ => Err(anyhow::anyhow!(
                "Unsupported NetCDF attribute value type: {:?}",
                value
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use netcdf::Options;

    use super::*;

    #[test]
    fn test_name() {
        let path = Path::new("https://s3.eu-west-3.amazonaws.com/argo-gdac-sandbox/pub/dac/aoml/13857/13857_prof.nc#mode=bytes");
        println!("{:?}", path);
        let ds = netcdf::open(path).unwrap();

        println!("{:?}", ds.dimensions().collect::<Vec<_>>());
    }
}
