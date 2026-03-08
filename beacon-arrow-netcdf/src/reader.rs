use std::{collections::HashMap, path::Path, sync::Arc};

use arrow::{
    array::{Array, ArrayRef, AsArray, Scalar},
    datatypes::{DataType, Schema},
};
use arrow_schema::{FieldRef, TimeUnit};
use beacon_nd_arrow::{
    array::backend::ArrayBackend, stream::NdBatchStreamAdapter, NdArrowArrayDispatch, NdRecordBatch,
};
use futures::Stream;
use netcdf::{
    types::{FloatType, IntType, NcVariableType},
    AttributeValue,
};

use crate::{
    backend::{AttributeBackend, VariableBackend},
    decoders::{
        self,
        cf_time::{extract_epoch, extract_units},
        DefaultVariableDecoder, VariableDecoder,
    },
};

/// Reads a NetCDF file and exposes its variables as Arrow arrays.
///
/// Each NetCDF variable becomes a column in the Arrow schema.  Variable
/// attributes are surfaced as additional columns using the dotted name
/// convention `"variable_name.attribute_name"`.
///
/// CF conventions are applied automatically:
/// - Variables whose last dimension name starts with `string`, `strlen`, or
///   `strnlen` are decoded as UTF-8 strings instead of raw characters.
/// - Variables with a `units` attribute that follows the CF time pattern
///   (e.g. `"days since 1950-01-01"`) are decoded as
///   `Timestamp(Nanosecond, None)`.
///
/// # Example
/// ```no_run
/// use beacon_arrow_netcdf::reader::NetCDFArrowReader;
///
/// let reader = NetCDFArrowReader::new("data.nc").unwrap();
/// println!("{:?}", reader.schema());
/// let columns = reader.read_columns::<Vec<_>>(None).unwrap();
/// ```
pub struct NetCDFArrowReader {
    file_schema: arrow::datatypes::SchemaRef,
    file_arrays: Vec<NdArrowArrayDispatch<Arc<dyn ArrayBackend>>>,
    file: Arc<netcdf::File>,
}

impl NetCDFArrowReader {
    /// Open a NetCDF file and build the Arrow schema and lazy column wrappers.
    ///
    /// This call does **not** read any variable data; data is fetched lazily
    /// when [`read_columns`](Self::read_columns) or
    /// [`read_as_arrow_stream`](Self::read_as_arrow_stream) is called.
    ///
    /// # Errors
    /// Returns an error if the file cannot be opened or if the schema cannot
    /// be built (e.g. an unsupported NetCDF variable type is encountered).
    pub fn new<P: AsRef<Path>>(path: P) -> anyhow::Result<Self> {
        let file = netcdf::open(path)?;
        let file_ref = Arc::new(file);

        let mut file_schema_fields = Vec::new();
        let mut file_arrays = Vec::new();

        // Process variables and their attributes
        for variable in file_ref.variables() {
            let mut variable_field = if let Ok(field) = Self::variable_to_field(&variable) {
                field
            } else {
                // Skip variables with unsupported types instead of failing the entire reader.
                continue;
            };

            // Get attributes for the variable
            let variable_attributes = Self::variable_attribute_values(&variable, false)?;

            // Fill Value from attributes
            let fill_value = variable_attributes
                .get("_FillValue")
                .cloned()
                .map(|a| a.into_inner());

            let mut variable_decoder: Arc<dyn VariableDecoder> =
                Arc::new(DefaultVariableDecoder::new(
                    variable_field.clone(),
                    variable.vartype(),
                    fill_value.clone(),
                ));

            let mut variable_dimensions = variable.dimensions().to_vec();
            let last_dimension = variable.dimensions().last();

            if let Some(last_dim) = last_dimension {
                // Check for CF string convention and wrap decoder if needed
                if variable.vartype() == NcVariableType::Char {
                    let dim_name = last_dim.name().to_lowercase();
                    if dim_name.starts_with("string")
                        || dim_name.starts_with("strlen")
                        || dim_name.starts_with("strnlen")
                    {
                        variable_decoder = Arc::new(decoders::strings::StringVariableDecoder::new(
                            variable_field.clone(),
                            variable.vartype(),
                            fill_value,
                            Some(last_dim.len()),
                        ));
                        variable_dimensions.pop(); // Remove the string length dimension
                    }
                } else if variable.vartype() == NcVariableType::String {
                    variable_decoder = Arc::new(decoders::strings::StringVariableDecoder::new(
                        variable_field.clone(),
                        variable.vartype(),
                        fill_value,
                        None,
                    ));
                }
            }

            // Find CF time variables and decode them
            if let Some(units_val) = variable_attributes.get("units") {
                if let Some(units_str) = units_val.clone().into_inner().as_string_opt::<i32>() {
                    let units_str_l = units_str.value(0).to_lowercase();
                    let units = extract_units(&units_str_l);
                    let epoch = extract_epoch(&units_str_l);
                    if let (Some(units), Some(epoch)) = (units, epoch) {
                        let ts_field = Arc::new(arrow_schema::Field::new(
                            variable_field.name(),
                            DataType::Timestamp(TimeUnit::Nanosecond, None),
                            variable_field.is_nullable(),
                        ));
                        variable_decoder = Arc::new(decoders::cf_time::CFTimeVariableDecoder::new(
                            ts_field.clone(),
                            variable_decoder.clone(),
                            epoch,
                            units,
                        ));
                        variable_field = ts_field;
                    }
                }
            }
            let variable_backend = Arc::new(VariableBackend::new(
                variable_decoder,
                Arc::clone(&file_ref),
                variable_dimensions.iter().map(|d| d.len()).collect(),
                variable_dimensions
                    .iter()
                    .map(|d| d.name().to_string())
                    .collect(),
            )) as Arc<dyn ArrayBackend>;
            let variable_array =
                NdArrowArrayDispatch::new(variable_backend, variable_field.data_type().clone())?;

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
                    NdArrowArrayDispatch::new(attribute_backend, attr_field.data_type().clone())?;

                file_schema_fields.push(attr_field);
                file_arrays.push(attribute_array);
            }
        }

        // Process global attributes as separate fields
        for (attr_name, attr_value) in Self::global_attribute_values(&file_ref, false)? {
            let attr_type = attr_value.clone().into_inner().data_type().clone();
            let attr_field = Arc::new(arrow_schema::Field::new(attr_name.clone(), attr_type, true));

            let attribute_backend = Arc::new(AttributeBackend::new(attr_field.clone(), attr_value))
                as Arc<dyn ArrayBackend>;

            let attribute_array =
                NdArrowArrayDispatch::new(attribute_backend, attr_field.data_type().clone())?;

            file_schema_fields.push(attr_field);
            file_arrays.push(attribute_array);
        }

        let file_schema = Arc::new(Schema::new(file_schema_fields));

        Ok(Self {
            file_schema,
            file_arrays,
            file: file_ref,
        })
    }

    /// Return the named dimensions (and their length) declared in the file.
    pub fn dimensions(&self) -> Vec<(String, usize)> {
        self.file
            .dimensions()
            .map(|d| (d.name().to_string(), d.len()))
            .collect()
    }

    /// Return the Arrow schema derived from the file's variables and their
    /// attributes.
    pub fn schema(&self) -> arrow::datatypes::SchemaRef {
        self.file_schema.clone()
    }

    /// Return a new reader restricted to variables that use at least one of
    /// the specified dimension names.
    ///
    /// Variable attribute fields (e.g. `"temperature.units"`) are kept
    /// alongside their parent variable. Variables that share none of the
    /// requested dimensions — as well as their attributes — are dropped.
    ///
    /// # Errors
    /// Never returns an error in practice; the `Result` wrapper is present for
    /// API consistency with the other methods.
    ///
    /// # Example
    /// ```no_run
    /// use beacon_arrow_netcdf::reader::NetCDFArrowReader;
    ///
    /// let reader = NetCDFArrowReader::new("data.nc").unwrap();
    /// let projected = reader
    ///     .project_with_dimensions(&["N_PROF".to_string(), "N_LEVELS".to_string()])
    ///     .unwrap();
    /// println!("{:?}", projected.schema());
    /// ```
    pub fn project_with_dimensions(&self, dimensions: &[String]) -> anyhow::Result<Self> {
        let dimension_set: std::collections::HashSet<&str> =
            dimensions.iter().map(String::as_str).collect();

        // Collect the names of variables that have at least one requested dimension.
        let kept_variables: std::collections::HashSet<String> = self
            .file
            .variables()
            .filter(|var| {
                var.dimensions()
                    .iter()
                    .any(|d| dimension_set.contains(d.name().as_str()))
            })
            .map(|var| var.name().to_string())
            .collect();

        // Keep each field (and its parallel array) that either IS a kept variable
        // or is an attribute of one (named "variable_name.attribute_name").
        let mut new_fields = Vec::new();
        let mut new_arrays = Vec::new();

        for (field, array) in self
            .file_schema
            .fields()
            .iter()
            .zip(self.file_arrays.iter())
        {
            let field_name = field.name().as_str();
            let belongs = kept_variables.iter().any(|var_name| {
                field_name == var_name || field_name.starts_with(&format!("{var_name}."))
            });
            if belongs {
                new_fields.push(field.clone());
                new_arrays.push(array.clone());
            }
        }

        Ok(Self {
            file_schema: Arc::new(Schema::new(new_fields)),
            file_arrays: new_arrays,
            file: Arc::clone(&self.file),
        })
    }

    /// Return a new reader containing only the columns at the given zero-based
    /// indices.
    ///
    /// The resulting schema and column list follow the **same order as the
    /// provided indices**, matching the behaviour of Arrow's
    /// [`Schema::project`](arrow_schema::Schema::project).  Duplicate indices
    /// are allowed and produce duplicate columns.
    ///
    /// # Errors
    /// Returns an error if any index is out of bounds.
    ///
    /// # Example
    /// ```no_run
    /// use beacon_arrow_netcdf::reader::NetCDFArrowReader;
    ///
    /// let reader = NetCDFArrowReader::new("data.nc").unwrap();
    /// // Keep only the second and first columns (in that order).
    /// let projected = reader.project(&[1, 0]).unwrap();
    /// assert_eq!(projected.schema().fields().len(), 2);
    /// ```
    pub fn project<P: AsRef<[usize]>>(&self, indices: P) -> anyhow::Result<Self> {
        // Arrow's Schema::project validates bounds and reorders fields to match
        // the provided index order.
        let new_schema = self.file_schema.project(indices.as_ref())?;
        // Mirror the same ordering for the column wrappers.
        let new_arrays = indices
            .as_ref()
            .iter()
            .map(|&i| {
                self.file_arrays
                    .get(i)
                    .cloned()
                    .ok_or_else(|| anyhow::anyhow!("Projection index {} out of bounds", i))
            })
            .collect::<anyhow::Result<Vec<_>>>()?;

        Ok(Self {
            file_schema: Arc::new(new_schema),
            file_arrays: new_arrays,
            file: Arc::clone(&self.file),
        })
    }

    /// Stream the file as a sequence of [`RecordBatch`]es.
    ///
    /// Each batch contains at most `chunk_size` rows.  An optional
    /// `projection` selects a subset of columns by zero-based index.
    ///
    /// # Errors
    /// Propagates errors from column reading or batch construction.
    pub fn read_as_arrow_stream<P: AsRef<[usize]>>(
        &self,
        projection: Option<P>,
        chunk_size: usize,
    ) -> anyhow::Result<NdBatchStreamAdapter> {
        let full_schema = self.schema();
        // Build the schema that matches the (possibly projected) column list.
        let schema = match &projection {
            Some(p) => {
                let fields: Vec<_> = p
                    .as_ref()
                    .iter()
                    .map(|&i| full_schema.field(i).clone())
                    .collect();
                Arc::new(Schema::new(fields))
            }
            None => full_schema,
        };
        let arrays = self.read_columns(projection)?;

        let nd_batch = NdRecordBatch::new(schema, arrays)?;

        let stream = NdBatchStreamAdapter::new(nd_batch, chunk_size)?;

        Ok(stream)
    }

    /// Return all (or a projected subset of) columns as lazy
    /// [`NdArrowArray`] wrappers.
    ///
    /// Pass `None` to retrieve every column.  Pass a slice of column indices
    /// to retrieve only those columns.
    ///
    /// # Errors
    /// Returns an error if any projection index is out of bounds.
    pub fn read_columns<P: AsRef<[usize]>>(
        &self,
        projection: Option<P>,
    ) -> anyhow::Result<Vec<NdArrowArrayDispatch<Arc<dyn ArrayBackend>>>> {
        let columns = if let Some(projection) = projection {
            projection
                .as_ref()
                .iter()
                .map(|&i| {
                    self.file_arrays
                        .get(i)
                        .cloned()
                        .ok_or_else(|| anyhow::anyhow!("Projection index {} out of bounds", i))
                })
                .collect::<Result<Vec<_>, _>>()?
        } else {
            self.file_arrays.clone()
        };
        Ok(columns)
    }

    /// Return the column with the given name as a lazy [`NdArrowArray`].
    ///
    /// # Errors
    /// Returns an error if no column with `column_name` exists in the schema.
    pub fn read_column(
        &self,
        column_name: &str,
    ) -> anyhow::Result<NdArrowArrayDispatch<Arc<dyn ArrayBackend>>> {
        let index = self
            .file_schema
            .fields()
            .iter()
            .position(|f| f.name() == column_name)
            .ok_or_else(|| anyhow::anyhow!("Column {} not found", column_name))?;
        self.read_columns(Some(&[index]))
            .map(|mut cols| cols.remove(0))
    }

    /// Map a NetCDF variable type to an Arrow [`DataType`](arrow_schema::DataType).
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

    /// Collect every attribute of a NetCDF *file* into a map of Arrow scalars.
    #[allow(dead_code)]
    fn global_attribute_values(
        file: &netcdf::File,
        fail_unsupported: bool,
    ) -> anyhow::Result<HashMap<String, Scalar<ArrayRef>>> {
        let mut attribute_values = HashMap::new();
        for attribute in file.attributes() {
            let value = attribute.value()?;
            let scalar = match Self::attribute_value_to_scalar(&value) {
                Ok(scalar) => scalar,
                Err(err) => {
                    if fail_unsupported {
                        return Err(err);
                    } else {
                        // Skip unsupported attribute types.
                        continue;
                    }
                }
            };
            attribute_values.insert(attribute.name().to_string(), scalar);
        }
        Ok(attribute_values)
    }

    /// Collect every attribute of a NetCDF *variable* into a map of Arrow scalars.
    fn variable_attribute_values(
        variable: &netcdf::Variable,
        fail_unsupported: bool,
    ) -> anyhow::Result<HashMap<String, Scalar<ArrayRef>>> {
        let mut attribute_values = HashMap::new();
        for attribute in variable.attributes() {
            let value = attribute.value()?;
            let scalar = match Self::attribute_value_to_scalar(&value) {
                Ok(scalar) => scalar,
                Err(err) => {
                    if fail_unsupported {
                        return Err(err);
                    } else {
                        // Skip unsupported attribute types.
                        continue;
                    }
                }
            };
            attribute_values.insert(attribute.name().to_string(), scalar);
        }
        Ok(attribute_values)
    }

    /// Convert a single [`AttributeValue`] to a one-element Arrow [`Scalar`].
    ///
    /// # Errors
    /// Returns an error for attribute types that have no Arrow equivalent
    /// (e.g. opaque / user-defined types).
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
    use std::path::Path;

    use arrow::array::AsArray;
    use arrow::datatypes::{DataType, Float64Type, Int32Type, TimestampNanosecondType};
    use arrow_schema::TimeUnit;
    use futures::StreamExt;
    use tempfile::Builder;

    use super::*;
    use crate::NcChar;

    // ── NetCDF file factory helpers ────────────────────────────────────────

    /// Create a temp NetCDF file with a single f64 variable and an optional
    /// `units` attribute (used to trigger CF-time decoding).
    fn make_f64_nc(var_name: &str, values: &[f64], units: Option<&str>) -> tempfile::NamedTempFile {
        let tmp = Builder::new().suffix(".nc").tempfile().unwrap();
        let mut nc = netcdf::create(tmp.path()).unwrap();
        nc.add_dimension("obs", values.len()).unwrap();
        let mut var = nc.add_variable::<f64>(var_name, &["obs"]).unwrap();
        var.put_values(values, netcdf::Extents::All).unwrap();
        if let Some(u) = units {
            var.put_attribute("units", u).unwrap();
        }
        drop(nc);
        tmp
    }

    /// Create a temp NetCDF file with a single i32 variable.
    fn make_i32_nc(var_name: &str, values: &[i32]) -> tempfile::NamedTempFile {
        let tmp = Builder::new().suffix(".nc").tempfile().unwrap();
        let mut nc = netcdf::create(tmp.path()).unwrap();
        nc.add_dimension("obs", values.len()).unwrap();
        let mut var = nc.add_variable::<i32>(var_name, &["obs"]).unwrap();
        var.put_values(values, netcdf::Extents::All).unwrap();
        drop(nc);
        tmp
    }

    /// Create a temp NetCDF file with a 2-D char variable (obs x strlen)
    /// following the CF string convention.
    fn make_char_nc(var_name: &str, strings: &[&str], str_len: usize) -> tempfile::NamedTempFile {
        let tmp = Builder::new().suffix(".nc").tempfile().unwrap();
        let mut nc = netcdf::create(tmp.path()).unwrap();
        nc.add_dimension("obs", strings.len()).unwrap();
        let strlen_dim = format!("strlen{}", str_len);
        nc.add_dimension(&strlen_dim, str_len).unwrap();
        let mut var = nc
            .add_variable::<NcChar>(var_name, &["obs", &strlen_dim])
            .unwrap();
        let mut buf: Vec<NcChar> = Vec::with_capacity(strings.len() * str_len);
        for s in strings {
            let bytes = s.as_bytes();
            for i in 0..str_len {
                buf.push(NcChar(*bytes.get(i).unwrap_or(&b'\0')));
            }
        }
        var.put_values::<NcChar, _>(&buf, netcdf::Extents::All)
            .unwrap();
        drop(nc);
        tmp
    }

    /// Create a temp NetCDF file with two numeric variables.
    fn make_multi_var_nc(
        f64_name: &str,
        f64_values: &[f64],
        i32_name: &str,
        i32_values: &[i32],
    ) -> tempfile::NamedTempFile {
        let tmp = Builder::new().suffix(".nc").tempfile().unwrap();
        let mut nc = netcdf::create(tmp.path()).unwrap();
        nc.add_dimension("obs", f64_values.len()).unwrap();
        {
            let mut var = nc.add_variable::<f64>(f64_name, &["obs"]).unwrap();
            var.put_values(f64_values, netcdf::Extents::All).unwrap();
        }
        {
            let mut var = nc.add_variable::<i32>(i32_name, &["obs"]).unwrap();
            var.put_values(i32_values, netcdf::Extents::All).unwrap();
        }
        drop(nc);
        tmp
    }

    // ── new() ──────────────────────────────────────────────────────────────

    #[test]
    fn test_new_opens_valid_file() {
        let tmp = make_f64_nc("lat", &[1.0, 2.0, 3.0], None);
        NetCDFArrowReader::new(tmp.path()).expect("should open a valid NetCDF file");
    }

    #[test]
    fn test_new_returns_error_for_nonexistent_file() {
        let result = NetCDFArrowReader::new("/nonexistent/path/file.nc");
        assert!(result.is_err(), "should fail for a missing file");
    }

    // ── schema() ──────────────────────────────────────────────────────────

    #[test]
    fn test_schema_contains_variable_field() {
        let tmp = make_f64_nc("temperature", &[20.0, 21.0], None);
        let reader = NetCDFArrowReader::new(tmp.path()).unwrap();
        let schema = reader.schema();
        let field = schema.field_with_name("temperature").unwrap();
        assert_eq!(field.data_type(), &DataType::Float64);
        assert!(field.is_nullable());
    }

    #[test]
    fn test_schema_maps_i32_to_int32() {
        let tmp = make_i32_nc("cycle", &[1, 2, 3]);
        let reader = NetCDFArrowReader::new(tmp.path()).unwrap();
        let field = reader.schema().field_with_name("cycle").unwrap().clone();
        assert_eq!(field.data_type(), &DataType::Int32);
    }

    #[test]
    fn test_schema_char_variable_maps_to_utf8() {
        let tmp = make_char_nc("station", &["ABC", "DEF"], 8);
        let reader = NetCDFArrowReader::new(tmp.path()).unwrap();
        let field = reader.schema().field_with_name("station").unwrap().clone();
        assert_eq!(field.data_type(), &DataType::Utf8);
    }

    #[test]
    fn test_schema_cf_time_variable_maps_to_timestamp() {
        let tmp = make_f64_nc("time", &[0.0, 1.0], Some("days since 1950-01-01"));
        let reader = NetCDFArrowReader::new(tmp.path()).unwrap();
        let field = reader.schema().field_with_name("time").unwrap().clone();
        assert_eq!(
            field.data_type(),
            &DataType::Timestamp(TimeUnit::Nanosecond, None),
            "CF time variable should be decoded as Timestamp(Nanosecond)"
        );
    }

    #[test]
    fn test_schema_attribute_surfaced_as_dotted_field() {
        let tmp = Builder::new().suffix(".nc").tempfile().unwrap();
        {
            let mut nc = netcdf::create(tmp.path()).unwrap();
            nc.add_dimension("obs", 2).unwrap();
            let mut var = nc.add_variable::<f64>("pres", &["obs"]).unwrap();
            var.put_values(&[1.0f64, 2.0], netcdf::Extents::All)
                .unwrap();
            var.put_attribute("units", "dbar").unwrap();
        }
        let reader = NetCDFArrowReader::new(tmp.path()).unwrap();
        let schema = reader.schema();
        let units_field = schema.field_with_name("pres.units").unwrap();
        assert_eq!(units_field.data_type(), &DataType::Utf8);
    }

    // ── dimensions() ──────────────────────────────────────────────────────

    #[test]
    fn test_dimensions_returns_correct_name_and_size() {
        let tmp = make_f64_nc("lat", &[1.0, 2.0, 3.0], None);
        let reader = NetCDFArrowReader::new(tmp.path()).unwrap();
        let dims = reader.dimensions();
        assert_eq!(dims.len(), 1);
        assert_eq!(dims[0].0, "obs");
        assert_eq!(dims[0].1, 3);
    }

    #[test]
    fn test_dimensions_empty_for_dimless_file() {
        let tmp = Builder::new().suffix(".nc").tempfile().unwrap();
        {
            netcdf::create(tmp.path()).unwrap();
        }
        let reader = NetCDFArrowReader::new(tmp.path()).unwrap();
        assert!(reader.dimensions().is_empty());
    }

    // ── read_columns() ────────────────────────────────────────────────────

    #[test]
    fn test_read_columns_no_projection_returns_all() {
        let tmp = make_multi_var_nc("a", &[1.0, 2.0], "b", &[3, 4]);
        let reader = NetCDFArrowReader::new(tmp.path()).unwrap();
        let cols = reader.read_columns::<Vec<_>>(None).unwrap();
        assert_eq!(cols.len(), reader.schema().fields().len());
    }

    #[test]
    fn test_read_columns_with_projection() {
        let tmp = make_multi_var_nc("x", &[1.0, 2.0], "y", &[10, 20]);
        let reader = NetCDFArrowReader::new(tmp.path()).unwrap();
        let cols = reader.read_columns(Some(&[0usize])).unwrap();
        assert_eq!(cols.len(), 1);
    }

    #[test]
    fn test_read_columns_projection_out_of_bounds_errors() {
        let tmp = make_f64_nc("v", &[1.0], None);
        let reader = NetCDFArrowReader::new(tmp.path()).unwrap();
        assert!(reader.read_columns(Some(&[999usize])).is_err());
    }

    // ── read_column() ─────────────────────────────────────────────────────

    #[test]
    fn test_read_column_by_name_succeeds() {
        let tmp = make_f64_nc("salinity", &[35.0, 36.0], None);
        let reader = NetCDFArrowReader::new(tmp.path()).unwrap();
        reader
            .read_column("salinity")
            .expect("should find 'salinity'");
    }

    #[test]
    fn test_read_column_missing_name_errors() {
        let tmp = make_f64_nc("temp", &[20.0], None);
        let reader = NetCDFArrowReader::new(tmp.path()).unwrap();
        assert!(reader.read_column("nonexistent").is_err());
    }

    #[test]
    fn test_read_column_shape_matches_variable_length() {
        let values = vec![1.0f64, 2.0, 3.0, 4.0, 5.0];
        let tmp = make_f64_nc("lon", &values, None);
        let reader = NetCDFArrowReader::new(tmp.path()).unwrap();
        let col = reader.read_column("lon").unwrap();
        assert_eq!(col.shape(), &[5]);
    }

    // ── data correctness ──────────────────────────────────────────────────

    #[test]
    fn test_f64_values_read_correctly() {
        let values = vec![1.5f64, 2.5, 3.5];
        let tmp = make_f64_nc("depth", &values, None);
        let reader = NetCDFArrowReader::new(tmp.path()).unwrap();
        let col = reader.read_column("depth").unwrap();

        let array = futures::executor::block_on(col.as_arrow_array_ref())
            .expect("as_arrow_array_ref should succeed");

        let typed = array.as_primitive::<Float64Type>();
        for (i, &expected) in values.iter().enumerate() {
            assert!(
                (typed.value(i) - expected).abs() < 1e-10,
                "mismatch at index {i}"
            );
        }
    }

    #[test]
    fn test_i32_values_read_correctly() {
        let values = vec![10i32, 20, 30];
        let tmp = make_i32_nc("cycle", &values);
        let reader = NetCDFArrowReader::new(tmp.path()).unwrap();
        let col = reader.read_column("cycle").unwrap();

        let array = futures::executor::block_on(col.as_arrow_array_ref())
            .expect("as_arrow_array_ref should succeed");

        let typed = array.as_primitive::<Int32Type>();
        for (i, &expected) in values.iter().enumerate() {
            assert_eq!(typed.value(i), expected, "mismatch at index {i}");
        }
    }

    #[test]
    fn test_cf_time_values_decoded_to_nanoseconds() {
        // 1 day offset from 1970-01-01 = 86400 seconds = 86400e9 ns
        let values = vec![0.0f64, 1.0];
        let tmp = make_f64_nc("time", &values, Some("days since 1970-01-01T00:00:00Z"));
        let reader = NetCDFArrowReader::new(tmp.path()).unwrap();
        let col = reader.read_column("time").unwrap();

        let array = futures::executor::block_on(col.as_arrow_array_ref())
            .expect("as_arrow_array_ref should succeed");

        let ts = array.as_primitive::<TimestampNanosecondType>();
        assert_eq!(ts.value(0), 0, "epoch offset 0 should map to 0 ns");
        let one_day_ns: i64 = 86_400 * 1_000_000_000;
        assert!(
            (ts.value(1) - one_day_ns).abs() < 1_000,
            "1 day should be ~86400e9 ns, got {}",
            ts.value(1)
        );
    }

    #[test]
    fn test_char_variable_decoded_as_string() {
        use arrow::array::StringArray;

        let tmp = make_char_nc("station_id", &["ALPHA", "BETA"], 8);
        let reader = NetCDFArrowReader::new(tmp.path()).unwrap();
        let col = reader.read_column("station_id").unwrap();

        let array = futures::executor::block_on(col.as_arrow_array_ref())
            .expect("as_arrow_array_ref should succeed");

        let sa = array
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("expected StringArray");
        assert_eq!(sa.value(0), "ALPHA");
        assert_eq!(sa.value(1), "BETA");
    }

    // ── read_as_arrow_stream() ─────────────────────────────────────────────

    #[test]
    fn test_stream_yields_correct_row_count() {
        let values: Vec<f64> = (0..10).map(|i| i as f64).collect();
        let tmp = make_f64_nc("v", &values, None);
        let reader = NetCDFArrowReader::new(tmp.path()).unwrap();

        let stream = reader
            .read_as_arrow_stream::<Vec<_>>(None, 4)
            .expect("stream creation should succeed");

        let total_rows: usize = futures::executor::block_on(async {
            futures::pin_mut!(stream);
            let mut count = 0usize;
            while let Some(batch) = stream.next().await {
                count += batch.expect("batch should not error").num_rows();
            }
            count
        });

        assert_eq!(total_rows, values.len());
    }

    #[test]
    fn test_stream_projection_limits_columns() {
        let tmp = make_multi_var_nc("a", &[1.0, 2.0], "b", &[10, 20]);
        let reader = NetCDFArrowReader::new(tmp.path()).unwrap();

        let stream = reader
            .read_as_arrow_stream(Some(&[0usize]), 10)
            .expect("stream creation should succeed");

        let first_batch = futures::executor::block_on(async {
            futures::pin_mut!(stream);
            stream.next().await.unwrap().unwrap()
        });

        assert_eq!(first_batch.num_columns(), 1);
    }

    // ── project() ─────────────────────────────────────────────────────────

    #[test]
    fn test_project_reduces_column_count() {
        let tmp = make_multi_var_nc("temp", &[1.0, 2.0, 3.0], "salt", &[10, 20, 30]);
        let reader = NetCDFArrowReader::new(tmp.path()).unwrap();
        assert!(reader.schema().fields().len() >= 2);

        let projected = reader.project(&[0]).unwrap();
        assert_eq!(projected.schema().fields().len(), 1);
    }

    #[test]
    fn test_project_preserves_index_order() {
        // Columns at indices 1 then 0 should appear in that order in the result.
        let tmp = make_multi_var_nc("alpha", &[1.0, 2.0], "beta", &[10, 20]);
        let reader = NetCDFArrowReader::new(tmp.path()).unwrap();
        // schema: [alpha, beta, ...]  — pick first two fields in reversed order.
        let projected = reader.project(&[1, 0]).unwrap();
        let orig_field_1 = reader.schema().field(1).name().clone();
        assert_eq!(projected.schema().field(0).name(), &orig_field_1);
    }

    #[test]
    fn test_project_out_of_bounds_errors() {
        let tmp = make_f64_nc("x", &[1.0, 2.0], None);
        let reader = NetCDFArrowReader::new(tmp.path()).unwrap();
        let col_count = reader.schema().fields().len();
        assert!(reader.project(&[col_count]).is_err());
    }

    #[test]
    fn test_project_empty_returns_empty_schema() {
        let tmp = make_f64_nc("x", &[1.0, 2.0], None);
        let reader = NetCDFArrowReader::new(tmp.path()).unwrap();
        let projected = reader.project(Vec::<usize>::new().as_slice()).unwrap();
        assert_eq!(projected.schema().fields().len(), 0);
        assert_eq!(projected.read_columns::<Vec<_>>(None).unwrap().len(), 0);
    }

    #[test]
    fn test_project_schema_and_array_count_match() {
        let tmp = make_multi_var_nc("a", &[1.0], "b", &[2]);
        let reader = NetCDFArrowReader::new(tmp.path()).unwrap();
        let projected = reader.project(&[0, 1]).unwrap();
        assert_eq!(
            projected.schema().fields().len(),
            projected.read_columns::<Vec<_>>(None).unwrap().len()
        );
    }

    #[test]
    fn test_project_result_is_readable() {
        let tmp = make_f64_nc("temperature", &[1.0, 2.0, 3.0], None);
        let reader = NetCDFArrowReader::new(tmp.path()).unwrap();
        // index 0 is the "temperature" variable column
        let projected = reader.project(&[0]).unwrap();
        let col = projected.read_column("temperature").unwrap();
        let arr = futures::executor::block_on(col.as_arrow_array_ref()).unwrap();
        assert_eq!(arr.len(), 3);
    }

    #[test]
    fn test_project_duplicate_indices_allowed() {
        // Duplicating an index should produce two columns from the same field.
        let tmp = make_f64_nc("v", &[1.0, 2.0], None);
        let reader = NetCDFArrowReader::new(tmp.path()).unwrap();
        let projected = reader.project(&[0, 0]).unwrap();
        assert_eq!(projected.schema().fields().len(), 2);
        assert_eq!(
            projected.schema().field(0).name(),
            projected.schema().field(1).name()
        );
    }

    // ── project_with_dimensions() ─────────────────────────────────────────

    #[test]
    fn test_project_with_dimensions_keeps_matching_variable() {
        // File has "obs1d" [obs_a] and "obs2" [obs_b]; projecting obs_a should keep only obs1d
        let tmp = Builder::new().suffix(".nc").tempfile().unwrap();
        {
            let mut nc = netcdf::create(tmp.path()).unwrap();
            nc.add_dimension("obs_a", 3).unwrap();
            nc.add_dimension("obs_b", 5).unwrap();
            let mut v1 = nc.add_variable::<f64>("obs1d", &["obs_a"]).unwrap();
            v1.put_values(&[1.0f64, 2.0, 3.0], netcdf::Extents::All)
                .unwrap();
            let mut v2 = nc.add_variable::<f64>("obs2", &["obs_b"]).unwrap();
            v2.put_values(&[10.0f64, 20.0, 30.0, 40.0, 50.0], netcdf::Extents::All)
                .unwrap();
        }
        let reader = NetCDFArrowReader::new(tmp.path()).unwrap();
        let projected = reader
            .project_with_dimensions(&["obs_a".to_string()])
            .unwrap();

        let schema = projected.schema();
        assert!(
            schema.field_with_name("obs1d").is_ok(),
            "obs1d should be kept"
        );
        assert!(
            schema.field_with_name("obs2").is_err(),
            "obs2 should be dropped"
        );
    }

    #[test]
    fn test_project_with_dimensions_keeps_variable_attributes() {
        // Attributes of kept variables must be kept too
        let tmp = Builder::new().suffix(".nc").tempfile().unwrap();
        {
            let mut nc = netcdf::create(tmp.path()).unwrap();
            nc.add_dimension("obs", 2).unwrap();
            nc.add_dimension("other", 4).unwrap();
            let mut v1 = nc.add_variable::<f64>("temp", &["obs"]).unwrap();
            v1.put_values(&[20.0f64, 21.0], netcdf::Extents::All)
                .unwrap();
            v1.put_attribute("units", "celsius").unwrap();
            let mut v2 = nc.add_variable::<f64>("pres", &["other"]).unwrap();
            v2.put_values(&[1000.0f64, 900.0, 800.0, 700.0], netcdf::Extents::All)
                .unwrap();
            v2.put_attribute("units", "hPa").unwrap();
        }
        let reader = NetCDFArrowReader::new(tmp.path()).unwrap();
        let projected = reader
            .project_with_dimensions(&["obs".to_string()])
            .unwrap();

        let schema = projected.schema();
        assert!(schema.field_with_name("temp").is_ok());
        assert!(
            schema.field_with_name("temp.units").is_ok(),
            "temp attribute should be kept"
        );
        assert!(
            schema.field_with_name("pres").is_err(),
            "pres should be dropped"
        );
        assert!(
            schema.field_with_name("pres.units").is_err(),
            "pres attribute should also be dropped"
        );
    }

    #[test]
    fn test_project_with_dimensions_empty_result_when_no_match() {
        let tmp = make_f64_nc("lat", &[1.0, 2.0], None);
        let reader = NetCDFArrowReader::new(tmp.path()).unwrap();
        let projected = reader
            .project_with_dimensions(&["nonexistent_dim".to_string()])
            .unwrap();
        assert_eq!(projected.schema().fields().len(), 0);
    }

    #[test]
    fn test_project_with_dimensions_multiple_dims_union() {
        // Variables matching ANY of the requested dimensions are kept
        let tmp = make_mixed_dim_nc(4, 3, 2);
        let reader = NetCDFArrowReader::new(tmp.path()).unwrap();

        let projected = reader
            .project_with_dimensions(&["n_obs".to_string()])
            .unwrap();

        let schema = projected.schema();
        // All three variables share n_obs as their first dimension
        assert!(schema.field_with_name("obs1d").is_ok());
        assert!(schema.field_with_name("obs2d").is_ok());
        assert!(schema.field_with_name("obs3d").is_ok());
    }

    #[test]
    fn test_project_with_dimensions_result_is_usable_for_reading() {
        let tmp = make_mixed_dim_nc(5, 3, 2);
        let reader = NetCDFArrowReader::new(tmp.path()).unwrap();
        let projected = reader
            .project_with_dimensions(&["n_obs".to_string()])
            .unwrap();

        // Should be able to read a column from the projected reader
        let col = projected.read_column("obs1d").unwrap();
        let array = futures::executor::block_on(col.as_arrow_array_ref()).unwrap();
        assert_eq!(array.len(), 5);
    }

    #[test]
    fn test_project_with_dimensions_result_schema_matches_read_columns() {
        let tmp = make_mixed_dim_nc(4, 2, 2);
        let reader = NetCDFArrowReader::new(tmp.path()).unwrap();
        let projected = reader
            .project_with_dimensions(&["n_obs".to_string()])
            .unwrap();

        let cols = projected.read_columns::<Vec<_>>(None).unwrap();
        assert_eq!(cols.len(), projected.schema().fields().len());
    }

    // ── mixed-dimension streaming tests ───────────────────────────────────

    /// A file with three variables of different ranks sharing the same leading
    /// dimension `n_obs`:
    /// - `obs1d` : f64  shape [n_obs]
    /// - `obs2d` : f32  shape [n_obs, n_param]
    /// - `obs3d` : i32  shape [n_obs, n_calib, n_param]
    ///
    /// Because `NdRecordBatch` only accepts arrays that are broadcastable to
    /// the same shape, these variables must be projected individually (or in
    /// groups that share an identical shape) when streaming.
    fn make_mixed_dim_nc(n_obs: usize, n_param: usize, n_calib: usize) -> tempfile::NamedTempFile {
        let tmp = Builder::new().suffix(".nc").tempfile().unwrap();
        let mut nc = netcdf::create(tmp.path()).unwrap();
        nc.add_dimension("n_obs", n_obs).unwrap();
        nc.add_dimension("n_param", n_param).unwrap();
        nc.add_dimension("n_calib", n_calib).unwrap();
        {
            let mut v = nc.add_variable::<f64>("obs1d", &["n_obs"]).unwrap();
            let data: Vec<f64> = (0..n_obs).map(|i| i as f64).collect();
            v.put_values(&data, netcdf::Extents::All).unwrap();
        }
        {
            let mut v = nc
                .add_variable::<f32>("obs2d", &["n_obs", "n_param"])
                .unwrap();
            let data: Vec<f32> = (0..n_obs * n_param).map(|i| i as f32).collect();
            v.put_values(&data, netcdf::Extents::All).unwrap();
        }
        {
            let mut v = nc
                .add_variable::<i32>("obs3d", &["n_obs", "n_calib", "n_param"])
                .unwrap();
            let data: Vec<i32> = (0..(n_obs * n_calib * n_param) as i32).collect();
            v.put_values(&data, netcdf::Extents::All).unwrap();
        }
        drop(nc);
        tmp
    }

    /// A file with two 2-D variables of the **same** shape [n_obs, n_param].
    fn make_two_2d_nc(n_obs: usize, n_param: usize) -> tempfile::NamedTempFile {
        let tmp = Builder::new().suffix(".nc").tempfile().unwrap();
        let mut nc = netcdf::create(tmp.path()).unwrap();
        nc.add_dimension("n_obs", n_obs).unwrap();
        nc.add_dimension("n_param", n_param).unwrap();
        {
            let mut v = nc.add_variable::<f32>("a", &["n_obs", "n_param"]).unwrap();
            let data: Vec<f32> = (0..n_obs * n_param).map(|i| i as f32).collect();
            v.put_values(&data, netcdf::Extents::All).unwrap();
        }
        {
            let mut v = nc.add_variable::<f32>("b", &["n_obs", "n_param"]).unwrap();
            let data: Vec<f32> = (0..n_obs * n_param).map(|i| (i * 10) as f32).collect();
            v.put_values(&data, netcdf::Extents::All).unwrap();
        }
        drop(nc);
        tmp
    }

    /// A file with two 3-D variables of the **same** shape [n_obs, n_calib, n_param].
    fn make_two_3d_nc(n_obs: usize, n_calib: usize, n_param: usize) -> tempfile::NamedTempFile {
        let tmp = Builder::new().suffix(".nc").tempfile().unwrap();
        let mut nc = netcdf::create(tmp.path()).unwrap();
        nc.add_dimension("n_obs", n_obs).unwrap();
        nc.add_dimension("n_calib", n_calib).unwrap();
        nc.add_dimension("n_param", n_param).unwrap();
        {
            let mut v = nc
                .add_variable::<i32>("c", &["n_obs", "n_calib", "n_param"])
                .unwrap();
            let data: Vec<i32> = (0..(n_obs * n_calib * n_param) as i32).collect();
            v.put_values(&data, netcdf::Extents::All).unwrap();
        }
        {
            let mut v = nc
                .add_variable::<i32>("d", &["n_obs", "n_calib", "n_param"])
                .unwrap();
            let data: Vec<i32> = (0..(n_obs * n_calib * n_param) as i32)
                .map(|x| x * -1)
                .collect();
            v.put_values(&data, netcdf::Extents::All).unwrap();
        }
        drop(nc);
        tmp
    }

    /// Collect a stream into batches and return them all.
    fn collect_batches(
        stream: impl futures::Stream<Item = anyhow::Result<arrow::array::RecordBatch>>,
    ) -> Vec<arrow::array::RecordBatch> {
        futures::executor::block_on(async {
            futures::pin_mut!(stream);
            let mut batches = Vec::new();
            while let Some(batch) = stream.next().await {
                batches.push(batch.expect("batch should not error"));
            }
            batches
        })
    }

    // Schema reflects all three ranks
    #[test]
    fn test_mixed_dim_schema_has_all_variables() {
        let tmp = make_mixed_dim_nc(4, 3, 2);
        let reader = NetCDFArrowReader::new(tmp.path()).unwrap();
        let schema = reader.schema();
        assert!(schema.field_with_name("obs1d").is_ok());
        assert!(schema.field_with_name("obs2d").is_ok());
        assert!(schema.field_with_name("obs3d").is_ok());
        assert_eq!(schema.fields().len(), 3);
    }

    // Shapes reported by NdArrowArray reflect the full ND shape
    #[test]
    fn test_mixed_dim_column_shapes() {
        let (n_obs, n_param, n_calib) = (5, 3, 2);
        let tmp = make_mixed_dim_nc(n_obs, n_param, n_calib);
        let reader = NetCDFArrowReader::new(tmp.path()).unwrap();

        let col1d = reader.read_column("obs1d").unwrap();
        let col2d = reader.read_column("obs2d").unwrap();
        let col3d = reader.read_column("obs3d").unwrap();

        assert_eq!(col1d.shape(), &[n_obs]);
        assert_eq!(col2d.shape(), &[n_obs, n_param]);
        assert_eq!(col3d.shape(), &[n_obs, n_calib, n_param]);
    }

    // Stream the 1-D column alone — standard chunking behaviour
    #[test]
    fn test_stream_1d_column_chunked_exact_division() {
        let n_obs = 12usize;
        let chunk = 4usize;
        let tmp = make_mixed_dim_nc(n_obs, 3, 2);
        let reader = NetCDFArrowReader::new(tmp.path()).unwrap();

        // obs1d is column 0
        let stream = reader
            .read_as_arrow_stream(Some(&[0usize]), chunk)
            .expect("1-D projection stream should succeed");
        let batches = collect_batches(stream);

        assert_eq!(batches.len(), n_obs / chunk, "expect {}", n_obs / chunk);
        for batch in &batches {
            assert_eq!(batch.num_rows(), chunk);
            assert_eq!(batch.num_columns(), 1);
        }
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, n_obs);
    }

    #[test]
    fn test_stream_1d_column_chunked_with_remainder() {
        let n_obs = 10usize;
        let chunk = 3usize;
        let tmp = make_mixed_dim_nc(n_obs, 2, 1);
        let reader = NetCDFArrowReader::new(tmp.path()).unwrap();

        let stream = reader
            .read_as_arrow_stream(Some(&[0usize]), chunk)
            .expect("1-D projection stream should succeed");
        let batches = collect_batches(stream);

        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, n_obs);
        assert_eq!(
            batches.last().unwrap().num_rows(),
            n_obs % chunk,
            "last batch holds the remainder rows"
        );
    }

    #[test]
    fn test_stream_1d_chunk_size_one() {
        let n_obs = 7usize;
        let tmp = make_mixed_dim_nc(n_obs, 2, 1);
        let reader = NetCDFArrowReader::new(tmp.path()).unwrap();

        let stream = reader
            .read_as_arrow_stream(Some(&[0usize]), 1)
            .expect("1-D stream should succeed");
        let batches = collect_batches(stream);

        assert_eq!(batches.len(), n_obs, "one batch per observation");
        for (i, b) in batches.iter().enumerate() {
            assert_eq!(b.num_rows(), 1, "batch {i} must be 1 row");
        }
    }

    // Stream a 2-D column alone — the stream flattens all dimensions so the
    // total row count equals n_obs * n_param.
    #[test]
    fn test_stream_2d_column_chunked() {
        let (n_obs, n_param) = (9usize, 4usize);
        let total_elements = n_obs * n_param;
        let tmp = make_mixed_dim_nc(n_obs, n_param, 2);
        let reader = NetCDFArrowReader::new(tmp.path()).unwrap();

        // obs2d is column 1
        let stream = reader
            .read_as_arrow_stream(Some(&[1usize]), 4)
            .expect("2-D projection stream should succeed");
        let batches = collect_batches(stream);

        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            total, total_elements,
            "total rows == n_obs * n_param (flattened)"
        );
        for batch in &batches {
            assert_eq!(batch.num_columns(), 1);
        }
    }

    // Stream a 3-D column alone — flattened row count is n_obs * n_calib * n_param.
    #[test]
    fn test_stream_3d_column_chunked() {
        let (n_obs, n_param, n_calib) = (8usize, 3usize, 2usize);
        let total_elements = n_obs * n_calib * n_param;
        let tmp = make_mixed_dim_nc(n_obs, n_param, n_calib);
        let reader = NetCDFArrowReader::new(tmp.path()).unwrap();

        // obs3d is column 2
        let stream = reader
            .read_as_arrow_stream(Some(&[2usize]), 6)
            .expect("3-D projection stream should succeed");
        let batches = collect_batches(stream);

        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            total, total_elements,
            "total rows == n_obs * n_calib * n_param (flattened)"
        );
        for batch in &batches {
            assert_eq!(batch.num_columns(), 1);
        }
    }

    // Two 2-D columns with identical shape can be co-streamed.
    // Total rows = n_obs * n_param (all elements, flattened).
    #[test]
    fn test_stream_two_2d_same_shape_chunked() {
        let (n_obs, n_param) = (6usize, 4usize);
        let total_elements = n_obs * n_param;
        let chunk = 4usize;
        let tmp = make_two_2d_nc(n_obs, n_param);
        let reader = NetCDFArrowReader::new(tmp.path()).unwrap();

        let stream = reader
            .read_as_arrow_stream::<Vec<_>>(None, chunk)
            .expect("2-D multi-column stream should succeed");
        let batches = collect_batches(stream);

        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            total, total_elements,
            "total rows == n_obs * n_param (flattened)"
        );
        assert_eq!(batches.len(), total_elements / chunk);
        for batch in &batches {
            assert_eq!(batch.num_columns(), 2);
            assert_eq!(batch.num_rows(), chunk);
        }
    }

    // Two 3-D columns with identical shape can be co-streamed.
    // Total rows = n_obs * n_calib * n_param (all elements, flattened).
    #[test]
    fn test_stream_two_3d_same_shape_chunked() {
        let (n_obs, n_calib, n_param) = (6usize, 2usize, 3usize);
        let total_elements = n_obs * n_calib * n_param;
        let chunk = 6usize;
        let tmp = make_two_3d_nc(n_obs, n_calib, n_param);
        let reader = NetCDFArrowReader::new(tmp.path()).unwrap();

        let stream = reader
            .read_as_arrow_stream::<Vec<_>>(None, chunk)
            .expect("3-D multi-column stream should succeed");
        let batches = collect_batches(stream);

        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            total, total_elements,
            "total rows == n_obs * n_calib * n_param (flattened)"
        );
        assert_eq!(batches.len(), total_elements / chunk);
        for batch in &batches {
            assert_eq!(batch.num_columns(), 2);
            assert_eq!(batch.num_rows(), chunk);
        }
    }

    // 1-D values are preserved correctly across chunk boundaries
    #[test]
    fn test_stream_1d_values_correct_across_chunks() {
        let n_obs = 9usize;
        let tmp = make_mixed_dim_nc(n_obs, 2, 1);
        let reader = NetCDFArrowReader::new(tmp.path()).unwrap();

        let stream = reader
            .read_as_arrow_stream(Some(&[0usize]), 4)
            .expect("stream creation should succeed");

        let mut all_values: Vec<f64> = Vec::new();
        futures::executor::block_on(async {
            futures::pin_mut!(stream);
            while let Some(batch) = stream.next().await {
                let batch = batch.expect("batch should not error");
                let col = batch.column(0);
                let typed = col
                    .as_any()
                    .downcast_ref::<arrow::array::Float64Array>()
                    .expect("obs1d should be Float64Array");
                for i in 0..typed.len() {
                    all_values.push(typed.value(i));
                }
            }
        });

        assert_eq!(all_values.len(), n_obs);
        for (i, &v) in all_values.iter().enumerate() {
            assert!(
                (v - i as f64).abs() < 1e-10,
                "expected {i} at index {i}, got {v}"
            );
        }
    }

    // ── remote integration tests (require network, skipped by default) ─────

    #[test]
    #[ignore = "requires network access"]
    fn test_remote_argo_schema() {
        let path = "https://s3.eu-west-3.amazonaws.com/argo-gdac-sandbox/pub/dac/aoml/13857/13857_prof.nc#mode=bytes";
        let reader = NetCDFArrowReader::new(Path::new(path)).unwrap();
        assert!(!reader.schema().fields().is_empty());
        assert!(!reader.dimensions().is_empty());
    }

    #[test]
    #[ignore = "requires network access"]
    fn test_remote_argo_read_all_columns() {
        let path = "https://s3.eu-west-3.amazonaws.com/argo-gdac-sandbox/pub/dac/aoml/13857/13857_prof.nc#mode=bytes";
        let reader = NetCDFArrowReader::new(Path::new(path)).unwrap();
        let columns = reader.read_columns::<Vec<_>>(None).unwrap();
        assert!(!columns.is_empty());
    }
}
