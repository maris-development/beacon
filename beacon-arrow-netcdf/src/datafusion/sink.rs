//! DataFusion sinks that materialize Arrow [`RecordBatch`]es into NetCDF files.
//!
//! The module provides two [`DataSink`] implementations:
//!
//! * [`NetCDFSink`] — writes each incoming batch directly via the high-level
//!   [`ArrowRecordBatchWriter`], producing a *flat* NetCDF file with a single
//!   unlimited `obs` dimension. Suitable for classic table-shaped / ragged data.
//!
//! * [`NetCDFNdSink`] — reshapes tabular rows into N-dimensional slabs using
//!   previously collected unique dimension values (via
//!   [`UniqueValuesHandleCollection`]), producing a *gridded* NetCDF file whose
//!   variables are indexed by named dimensions (e.g. `lat × lon × time`).
//!
//! # Write mode selection
//!
//! The write mode is controlled by [`NetcdfOptions::write_dimensions`]:
//!
//! | `write_dimensions`           | Sink used       | NetCDF layout           |
//! |------------------------------|-----------------|-------------------------|
//! | `None` / empty               | [`NetCDFSink`]  | flat, unlimited `obs`   |
//! | `Some(["lat", "lon", …])`  | [`NetCDFNdSink`]| gridded, named dims     |

use std::{any::Any, env::temp_dir, fmt::Formatter, sync::Arc};

use crate::{encoders::default::DefaultEncoder, writer::ArrowRecordBatchWriter};
use arrow::{
    array::{Array, ArrowPrimitiveType, PrimitiveArray, RecordBatch},
    datatypes::{DataType, FieldRef, SchemaRef},
};
use beacon_datafusion_ext::unique_values::{
    ColumnValueMap, ErasedColumnValues, UniqueColumnValues, UniqueValuesHandleCollection,
};
use datafusion::{
    datasource::{physical_plan::FileSinkConfig, sink::DataSink},
    error::DataFusionError,
    execution::{SendableRecordBatchStream, TaskContext},
    physical_plan::{DisplayAs, DisplayFormatType},
};
use futures::StreamExt;
use ndarray::ArrayBase;
use netcdf::{Extents, FileMut, NcTypeDescriptor, VariableMut};
use ordered_float::OrderedFloat;

// ─── Flat sink ─────────────────────────────────────────────────────────────

/// DataFusion [`DataSink`] that writes batches to a flat NetCDF file.
///
/// Every incoming [`RecordBatch`] is forwarded to an
/// [`ArrowRecordBatchWriter<DefaultEncoder>`] which serializes columns into
/// NetCDF variables along a single unlimited `obs` dimension.
///
/// The output file is written to `std::env::temp_dir()` joined with the path
/// from [`FileSinkConfig::table_paths`].
#[derive(Debug, Clone)]
pub struct NetCDFSink {
    sink_config: FileSinkConfig,
}

impl NetCDFSink {
    /// Create a new flat sink bound to the given [`FileSinkConfig`].
    pub fn new(sink_config: FileSinkConfig) -> Self {
        Self { sink_config }
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

    fn schema(&self) -> &SchemaRef {
        self.sink_config.output_schema()
    }

    fn metrics(&self) -> Option<datafusion::physical_plan::metrics::MetricsSet> {
        None
    }

    async fn write_all(
        &self,
        data: SendableRecordBatchStream,
        _context: &Arc<TaskContext>,
    ) -> datafusion::error::Result<u64> {
        let arrow_schema = self.sink_config.output_schema().clone();
        let file_path = self.sink_config.table_paths[0].prefix();
        let full_path = temp_dir().join(file_path.as_ref());
        tracing::info!("Writing NetCDF to path: {:?}", full_path);

        let mut rows_written: u64 = 0;
        let mut nc_writer = ArrowRecordBatchWriter::<DefaultEncoder>::new(full_path, arrow_schema)
            .map_err(|e| {
                DataFusionError::Execution(format!(
                    "Failed to create NetCDF ArrowRecordBatchWriter: {}",
                    e
                ))
            })?;

        let mut pinned_stream = std::pin::pin!(data);

        while let Some(batch) = pinned_stream.next().await {
            let batch = batch?;
            rows_written += batch.num_rows() as u64;
            nc_writer.write_record_batch(batch).map_err(|e| {
                DataFusionError::Execution(format!("Failed to write record batch to NetCDF: {}", e))
            })?;
        }

        nc_writer.finish().map_err(|e| {
            DataFusionError::Execution(format!("Failed to finish writing NetCDF: {}", e))
        })?;

        Ok(rows_written)
    }
}

// ─── N-dimensional sink ────────────────────────────────────────────────────

/// DataFusion [`DataSink`] that reshapes tabular rows into an N-dimensional
/// NetCDF file.
///
/// The first `ndims` columns listed in the [`UniqueValuesHandleCollection`]
/// become named dimension axes; the remaining columns become data variables
/// indexed by those dimensions.
///
/// # Constraints
///
/// * All output columns must have **primitive** Arrow data types — strings
///   and nested types are not supported.
/// * Dimension values must have been collected by an upstream
///   [`UniqueValuesExec`](beacon_datafusion_ext::unique_values::UniqueValuesExec)
///   node in the physical plan.
#[derive(Debug, Clone)]
pub struct NetCDFNdSink {
    sink_config: FileSinkConfig,
    ndims: usize,
    unique_values: UniqueValuesHandleCollection,
}

impl NetCDFNdSink {
    /// Create a new N-dimensional sink.
    ///
    /// The sink validates that every output column is primitive because complex
    /// Arrow types cannot be represented in NetCDF variables.
    pub fn new(
        sink_config: FileSinkConfig,
        ndims: usize,
        unique_values: UniqueValuesHandleCollection,
    ) -> Result<Self, DataFusionError> {
        tracing::info!("Creating NetCDFNdSink with {} dimensions", ndims);
        for field in sink_config.output_schema().fields() {
            if !field.data_type().is_primitive() {
                return Err(DataFusionError::Execution(format!(
                    "NetCDFNdSink only supports primitive data types, found {:?} in column '{}'",
                    field.data_type(),
                    field.name()
                )));
            }
        }

        Ok(Self {
            sink_config,
            ndims,
            unique_values,
        })
    }
}

impl DisplayAs for NetCDFNdSink {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "NetCDFNdSink")
    }
}

#[async_trait::async_trait]
impl DataSink for NetCDFNdSink {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> &SchemaRef {
        self.sink_config.output_schema()
    }

    fn metrics(&self) -> Option<datafusion::physical_plan::metrics::MetricsSet> {
        None
    }

    async fn write_all(
        &self,
        data: SendableRecordBatchStream,
        _context: &Arc<TaskContext>,
    ) -> datafusion::error::Result<u64> {
        let output_path = temp_dir().join(self.sink_config.table_paths[0].prefix().as_ref());
        tracing::info!("Writing ND NetCDF to path: {:?}", output_path);

        let mut rows_written: u64 = 0;
        let mut nc_file = netcdf::create(output_path).map_err(|e| {
            DataFusionError::Execution(format!("Failed to create NetCDF file: {}", e))
        })?;

        let mut pinned_stream = std::pin::pin!(data);
        let mut batches = vec![];
        while let Some(batch) = pinned_stream.next().await {
            let batch = batch?;
            rows_written += batch.num_rows() as u64;
            batches.push(batch);
        }

        let concatted_batch =
            arrow::compute::concat_batches(&pinned_stream.schema(), batches.iter()).map_err(
                |e| {
                    DataFusionError::Execution(format!(
                        "Failed to concatenate batches in NetCDFNdSink: {}",
                        e
                    ))
                },
            )?;

        let unique_values_collection: Arc<ColumnValueMap> = self
            .unique_values
            .unique_values()
            .ok_or_else(|| {
                DataFusionError::Execution(
                    "No dimensions values collected for NetCDFNdSink".to_string(),
                )
            })
            .map(Arc::new)?;

        let dimension_fields: Vec<FieldRef> = unique_values_collection
            .keys()
            .take(self.ndims)
            .cloned()
            .collect();

        if dimension_fields.len() != self.ndims {
            return Err(DataFusionError::Execution(format!(
                "Expected {} dimension columns but only collected {}",
                self.ndims,
                dimension_fields.len()
            )));
        }

        let value_fields: Vec<FieldRef> = self
            .sink_config
            .output_schema()
            .fields()
            .iter()
            .filter(|field| !unique_values_collection.contains_key(*field))
            .cloned()
            .collect();

        let dim_names: Vec<&str> = dimension_fields.iter().map(|f| f.name().as_str()).collect();

        let dimension_schema = Arc::new(arrow::datatypes::Schema::new(
            dimension_fields
                .iter()
                .map(|f| f.as_ref().clone())
                .collect::<Vec<_>>(),
        ));

        create_dimension_variables(&mut nc_file, &unique_values_collection)?;

        for field in &value_fields {
            create_variable(&mut nc_file, field, &dim_names)?;
        }

        let dimension_columns = RecordBatch::try_new(
            dimension_schema.clone(),
            dimension_fields
                .iter()
                .map(|field| {
                    concatted_batch
                        .column(concatted_batch.schema().index_of(field.name()).unwrap())
                        .clone()
                })
                .collect(),
        )
        .map_err(|e| {
            DataFusionError::Execution(format!(
                "Failed to create dimension columns record batch: {}",
                e
            ))
        })?;

        for field in &value_fields {
            let value_array =
                concatted_batch.column(concatted_batch.schema().index_of(field.name()).unwrap());

            let mut variable = nc_file.variable_mut(field.name()).ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "Variable '{}' not found in NetCDF file",
                    field.name()
                ))
            })?;

            write_ndarray_slab(
                &mut variable,
                unique_values_collection.clone(),
                dimension_columns.clone(),
                value_array.clone(),
            )?;
        }

        nc_file.close().map_err(|e| {
            DataFusionError::Execution(format!("Failed to finish writing NetCDF: {}", e))
        })?;

        Ok(rows_written)
    }
}

// ─── NetCDF variable / dimension helpers ───────────────────────────────────

fn create_variable<'a>(
    file: &'a mut FileMut,
    variable: &FieldRef,
    dims: &[&str],
) -> datafusion::error::Result<VariableMut<'a>> {
    match variable.data_type() {
        DataType::Boolean => file
            .add_variable::<u8>(variable.name(), dims)
            .map_err(|e| df_err(variable.name(), "create boolean variable", e)),
        DataType::Int8 => file
            .add_variable::<i8>(variable.name(), dims)
            .map_err(|e| df_err(variable.name(), "create Int8 variable", e)),
        DataType::Int16 => file
            .add_variable::<i16>(variable.name(), dims)
            .map_err(|e| df_err(variable.name(), "create Int16 variable", e)),
        DataType::Int32 | DataType::Date32 | DataType::Time32(_) => file
            .add_variable::<i32>(variable.name(), dims)
            .map_err(|e| df_err(variable.name(), "create Int32 variable", e)),
        DataType::Int64
        | DataType::Date64
        | DataType::Time64(_)
        | DataType::Duration(_)
        | DataType::Interval(_) => file
            .add_variable::<i64>(variable.name(), dims)
            .map_err(|e| df_err(variable.name(), "create Int64 variable", e)),
        DataType::Timestamp(time_unit, _) => {
            let mut nc_variable = file
                .add_variable::<i64>(variable.name(), dims)
                .map_err(|e| df_err(variable.name(), "create Timestamp variable", e))?;
            let unit = match time_unit {
                arrow::datatypes::TimeUnit::Second => "seconds",
                arrow::datatypes::TimeUnit::Millisecond => "milliseconds",
                arrow::datatypes::TimeUnit::Microsecond => "microseconds",
                arrow::datatypes::TimeUnit::Nanosecond => "nanoseconds",
            };
            nc_variable
                .put_attribute("units", format!("{} since 1970-01-01T00:00:00Z", unit))
                .map_err(|e| df_err(variable.name(), "write timestamp units attribute", e))?;
            nc_variable
                .put_attribute("calendar", "gregorian")
                .map_err(|e| df_err(variable.name(), "write timestamp calendar attribute", e))?;
            nc_variable
                .put_attribute("standard_name", "time")
                .map_err(|e| {
                    df_err(
                        variable.name(),
                        "write timestamp standard_name attribute",
                        e,
                    )
                })?;
            Ok(nc_variable)
        }
        DataType::UInt8 => file
            .add_variable::<u8>(variable.name(), dims)
            .map_err(|e| df_err(variable.name(), "create UInt8 variable", e)),
        DataType::UInt16 => file
            .add_variable::<u16>(variable.name(), dims)
            .map_err(|e| df_err(variable.name(), "create UInt16 variable", e)),
        DataType::UInt32 => file
            .add_variable::<u32>(variable.name(), dims)
            .map_err(|e| df_err(variable.name(), "create UInt32 variable", e)),
        DataType::UInt64 => file
            .add_variable::<u64>(variable.name(), dims)
            .map_err(|e| df_err(variable.name(), "create UInt64 variable", e)),
        DataType::Float32 => file
            .add_variable::<f32>(variable.name(), dims)
            .map_err(|e| df_err(variable.name(), "create Float32 variable", e)),
        DataType::Float64 => file
            .add_variable::<f64>(variable.name(), dims)
            .map_err(|e| df_err(variable.name(), "create Float64 variable", e)),
        _ => Err(DataFusionError::Execution(format!(
            "Cannot create variable for unsupported data type {:?} (column '{}')",
            variable.data_type(),
            variable.name()
        ))),
    }
}

/// Create one NetCDF dimension + coordinate variable per entry in `unique_values`.
fn create_dimension_variables(
    file: &mut FileMut,
    unique_values: &ColumnValueMap,
) -> datafusion::error::Result<()> {
    for (field_ref, erased_values) in unique_values {
        create_dimension_for_field(file, field_ref, erased_values)?;
    }
    Ok(())
}

/// Define a NetCDF dimension with the length of `erased_values` and write the
/// coordinate values into a same-named variable.
fn create_dimension_for_field(
    file: &mut FileMut,
    field: &FieldRef,
    erased_values: &ErasedColumnValues,
) -> datafusion::error::Result<()> {
    let dim_name = field.name().to_string();
    ensure_dimension(file, &dim_name, erased_values.len())?;

    let dims = vec![dim_name.as_str()];

    match field.data_type() {
        DataType::Boolean => {
            let values = typed_values::<bool>(&dim_name, erased_values)?;
            let encoded: Vec<u8> = values.into_iter().map(|v| if v { 1 } else { 0 }).collect();
            let mut variable = create_variable(file, field, &dims)?;
            write_numeric_dimension_values::<u8>(&mut variable, &encoded)?;
        }
        DataType::Int8 => {
            let values = typed_values::<i8>(&dim_name, erased_values)?;
            let mut variable = create_variable(file, field, &dims)?;
            write_numeric_dimension_values::<i8>(&mut variable, &values)?;
        }
        DataType::Int16 => {
            let values = typed_values::<i16>(&dim_name, erased_values)?;
            let mut variable = create_variable(file, field, &dims)?;
            write_numeric_dimension_values::<i16>(&mut variable, &values)?;
        }
        DataType::Int32 | DataType::Date32 | DataType::Time32(_) => {
            let values = typed_values::<i32>(&dim_name, erased_values)?;
            let mut variable = create_variable(file, field, &dims)?;
            write_numeric_dimension_values::<i32>(&mut variable, &values)?;
        }
        DataType::Int64
        | DataType::Timestamp(_, _)
        | DataType::Date64
        | DataType::Time64(_)
        | DataType::Duration(_)
        | DataType::Interval(_) => {
            let values = typed_values::<i64>(&dim_name, erased_values)?;
            let mut variable = create_variable(file, field, &dims)?;
            write_numeric_dimension_values::<i64>(&mut variable, &values)?;
        }
        DataType::UInt8 => {
            let values = typed_values::<u8>(&dim_name, erased_values)?;
            let mut variable = create_variable(file, field, &dims)?;
            write_numeric_dimension_values::<u8>(&mut variable, &values)?;
        }
        DataType::UInt16 => {
            let values = typed_values::<u16>(&dim_name, erased_values)?;
            let mut variable = create_variable(file, field, &dims)?;
            write_numeric_dimension_values::<u16>(&mut variable, &values)?;
        }
        DataType::UInt32 => {
            let values = typed_values::<u32>(&dim_name, erased_values)?;
            let mut variable = create_variable(file, field, &dims)?;
            write_numeric_dimension_values::<u32>(&mut variable, &values)?;
        }
        DataType::UInt64 => {
            let values = typed_values::<u64>(&dim_name, erased_values)?;
            let mut variable = create_variable(file, field, &dims)?;
            write_numeric_dimension_values::<u64>(&mut variable, &values)?;
        }
        DataType::Float32 => {
            let ordered = typed_values::<OrderedFloat<f32>>(&dim_name, erased_values)?;
            let floats: Vec<f32> = ordered.into_iter().map(|v| v.into_inner()).collect();
            let mut variable = create_variable(file, field, &dims)?;
            write_numeric_dimension_values::<f32>(&mut variable, &floats)?;
        }
        DataType::Float64 => {
            let ordered = typed_values::<OrderedFloat<f64>>(&dim_name, erased_values)?;
            let floats: Vec<f64> = ordered.into_iter().map(|v| v.into_inner()).collect();
            let mut variable = create_variable(file, field, &dims)?;
            write_numeric_dimension_values::<f64>(&mut variable, &floats)?;
        }
        other => {
            return Err(DataFusionError::Execution(format!(
                "Cannot create dimension for unsupported data type {:?} (column '{}')",
                other,
                field.name()
            )));
        }
    }

    Ok(())
}

/// Write a 1-D slice of coordinate values into `variable`.
fn write_numeric_dimension_values<T>(
    variable: &mut VariableMut,
    values: &[T],
) -> datafusion::error::Result<()>
where
    T: NcTypeDescriptor + Copy,
{
    if values.is_empty() {
        return Ok(());
    }

    let extents = vec![0..values.len()];
    variable
        .put_values::<T, _>(values, extents)
        .map_err(|e| df_err(&variable.name(), "write numeric coordinate", e))?;

    Ok(())
}

/// Ensure a NetCDF dimension named `name` exists with the given `len`.
///
/// If the dimension already exists with the same length, this is a no-op.
/// A length mismatch returns an error.
fn ensure_dimension(file: &mut FileMut, name: &str, len: usize) -> datafusion::error::Result<()> {
    if let Some(dimension) = file.dimension(name) {
        if dimension.len() != len {
            return Err(DataFusionError::Execution(format!(
                "Dimension '{}' already exists with length {}, cannot redefine with length {}",
                name,
                dimension.len(),
                len
            )));
        }
    } else {
        file.add_dimension(name, len)
            .map_err(|e| df_err(name, "create dimension", e))?;
    }

    Ok(())
}

/// Downcast type-erased unique values to a concrete `Vec<T>`.
///
/// Returns an error when the erased container does not hold `UniqueColumnValues<T>`.
fn typed_values<T: Clone + Ord + Send + Sync + 'static>(
    field_name: &str,
    erased_values: &ErasedColumnValues,
) -> datafusion::error::Result<Vec<T>> {
    erased_values
        .as_any()
        .downcast_ref::<UniqueColumnValues<T>>()
        .map(|column| column.values.clone())
        .ok_or_else(|| {
            DataFusionError::Execution(format!(
                "Unique value collector for column '{}' had unexpected type",
                field_name
            ))
        })
}

/// Build a [`DataFusionError::Execution`] with a structured message.
fn df_err(field: &str, action: &str, err: impl std::fmt::Display) -> DataFusionError {
    DataFusionError::Execution(format!(
        "Failed to {} for column '{}': {}",
        action, field, err
    ))
}

// ─── N-dimensional slab helpers ────────────────────────────────────────────

/// Create an N-dimensional array filled with `fill_value` whose shape is `dims`.
fn typed_ndarray_slab<T: NcTypeDescriptor + Clone>(
    dims: &[usize],
    fill_value: T,
) -> ArrayBase<ndarray::OwnedRepr<T>, ndarray::Dim<ndarray::IxDynImpl>> {
    ArrayBase::from_elem(ndarray::IxDyn(dims), fill_value)
}

/// Translate absolute slab positions to zero-based offsets relative to `extents`.
fn relative_slab_positions(
    extents: &[std::ops::Range<usize>],
    slab_positions: &[Option<Vec<usize>>],
) -> Vec<Option<Vec<usize>>> {
    slab_positions
        .iter()
        .map(|pos_opt| {
            pos_opt.as_ref().map(|positions| {
                positions
                    .iter()
                    .enumerate()
                    .map(|(dim_idx, &pos)| pos - extents[dim_idx].start)
                    .collect()
            })
        })
        .collect()
}

/// Write an N-dimensional slab into `variable` by mapping each row of
/// `value_column` to its position in the dimension grid described by
/// `unique_values` and `dimension_columns`.
fn write_ndarray_slab(
    variable: &mut VariableMut,
    unique_values: Arc<ColumnValueMap>,
    dimension_columns: RecordBatch,
    value_column: arrow::array::ArrayRef,
) -> datafusion::error::Result<()> {
    if dimension_columns.num_rows() != value_column.len() {
        return Err(DataFusionError::Execution(format!(
            "Row count mismatch: dimension columns have {} rows but value column has {} elements",
            dimension_columns.num_rows(),
            value_column.len()
        )));
    }

    let positions = dimension_slab_positions(dimension_columns, unique_values)?;
    let extents = infer_extents(&positions)?;
    let slab_positions = relative_slab_positions(&extents, &positions);
    let dims = infer_slab_dims(&extents);
    let nc_extents: Extents = extents.into();

    if dims.len() != variable.dimensions().len() {
        return Err(DataFusionError::Execution(format!(
            "Inferred slab dimensions length {} does not match variable dimensions length {}",
            dims.len(),
            variable.dimensions().len()
        )));
    }

    match value_column.data_type().clone() {
        DataType::Int8 => {
            let slab = fill_primitive_slab::<arrow::datatypes::Int8Type>(
                &dims,
                &slab_positions,
                &value_column,
            )?;
            variable.put(slab.view(), nc_extents).map_err(|e| {
                DataFusionError::Execution(format!(
                    "Failed to write slab Int8 data to variable '{}': {}",
                    variable.name(),
                    e
                ))
            })?;
        }
        DataType::Int16 => {
            let slab = fill_primitive_slab::<arrow::datatypes::Int16Type>(
                &dims,
                &slab_positions,
                &value_column,
            )?;
            variable.put(slab.view(), nc_extents).map_err(|e| {
                DataFusionError::Execution(format!(
                    "Failed to write slab Int16 data to variable '{}': {}",
                    variable.name(),
                    e
                ))
            })?;
        }
        DataType::Int32 | DataType::Date32 | DataType::Time32(_) => {
            let slab = fill_primitive_slab::<arrow::datatypes::Int32Type>(
                &dims,
                &slab_positions,
                &value_column,
            )?;
            variable.put(slab.view(), nc_extents).map_err(|e| {
                DataFusionError::Execution(format!(
                    "Failed to write slab Int32 data to variable '{}': {}",
                    variable.name(),
                    e
                ))
            })?;
        }
        DataType::Int64 | DataType::Date64 | DataType::Time64(_) => {
            let slab = fill_primitive_slab::<arrow::datatypes::Int64Type>(
                &dims,
                &slab_positions,
                &value_column,
            )?;
            variable.put(slab.view(), nc_extents).map_err(|e| {
                DataFusionError::Execution(format!(
                    "Failed to write slab Int64 data to variable '{}': {}",
                    variable.name(),
                    e
                ))
            })?;
        }
        DataType::Timestamp(unit, _) => {
            let slab = match unit {
                arrow::datatypes::TimeUnit::Second => fill_primitive_slab::<
                    arrow::datatypes::TimestampSecondType,
                >(
                    &dims, &slab_positions, &value_column
                )?,
                arrow::datatypes::TimeUnit::Millisecond => fill_primitive_slab::<
                    arrow::datatypes::TimestampMillisecondType,
                >(
                    &dims, &slab_positions, &value_column
                )?,
                arrow::datatypes::TimeUnit::Microsecond => fill_primitive_slab::<
                    arrow::datatypes::TimestampMicrosecondType,
                >(
                    &dims, &slab_positions, &value_column
                )?,
                arrow::datatypes::TimeUnit::Nanosecond => fill_primitive_slab::<
                    arrow::datatypes::TimestampNanosecondType,
                >(
                    &dims, &slab_positions, &value_column
                )?,
            };
            variable.put(slab.view(), nc_extents).map_err(|e| {
                DataFusionError::Execution(format!(
                    "Failed to write slab Timestamp data to variable '{}': {}",
                    variable.name(),
                    e
                ))
            })?;
        }
        DataType::UInt8 => {
            let slab = fill_primitive_slab::<arrow::datatypes::UInt8Type>(
                &dims,
                &slab_positions,
                &value_column,
            )?;
            variable.put(slab.view(), nc_extents).map_err(|e| {
                DataFusionError::Execution(format!(
                    "Failed to write slab UInt8 data to variable '{}': {}",
                    variable.name(),
                    e
                ))
            })?;
        }
        DataType::UInt16 => {
            let slab = fill_primitive_slab::<arrow::datatypes::UInt16Type>(
                &dims,
                &slab_positions,
                &value_column,
            )?;
            variable.put(slab.view(), nc_extents).map_err(|e| {
                DataFusionError::Execution(format!(
                    "Failed to write slab UInt16 data to variable '{}': {}",
                    variable.name(),
                    e
                ))
            })?;
        }
        DataType::UInt32 => {
            let slab = fill_primitive_slab::<arrow::datatypes::UInt32Type>(
                &dims,
                &slab_positions,
                &value_column,
            )?;
            variable.put(slab.view(), nc_extents).map_err(|e| {
                DataFusionError::Execution(format!(
                    "Failed to write slab UInt32 data to variable '{}': {}",
                    variable.name(),
                    e
                ))
            })?;
        }
        DataType::UInt64 => {
            let slab = fill_primitive_slab::<arrow::datatypes::UInt64Type>(
                &dims,
                &slab_positions,
                &value_column,
            )?;
            variable.put(slab.view(), nc_extents).map_err(|e| {
                DataFusionError::Execution(format!(
                    "Failed to write slab UInt64 data to variable '{}': {}",
                    variable.name(),
                    e
                ))
            })?;
        }
        DataType::Float32 => {
            let slab = fill_primitive_slab::<arrow::datatypes::Float32Type>(
                &dims,
                &slab_positions,
                &value_column,
            )?;
            variable.put(slab.view(), nc_extents).map_err(|e| {
                DataFusionError::Execution(format!(
                    "Failed to write slab Float32 data to variable '{}': {}",
                    variable.name(),
                    e
                ))
            })?;
        }
        DataType::Float64 => {
            let slab = fill_primitive_slab::<arrow::datatypes::Float64Type>(
                &dims,
                &slab_positions,
                &value_column,
            )?;
            variable.put(slab.view(), nc_extents).map_err(|e| {
                DataFusionError::Execution(format!(
                    "Failed to write slab Float64 data to variable '{}': {}",
                    variable.name(),
                    e
                ))
            })?;
        }
        other => {
            return Err(DataFusionError::Execution(format!(
                "slab writing not implemented for data type {:?}",
                other
            )));
        }
    }

    Ok(())
}

/// Compute the bounding-box extents for each axis from the given slab positions.
///
/// Returns one `Range<usize>` per axis whose `start..end` covers every
/// coordinate that appears in `positions`.
fn infer_extents(
    positions: &[Option<Vec<usize>>],
) -> Result<Vec<std::ops::Range<usize>>, DataFusionError> {
    let mut extents = Vec::new();

    for pos in positions.iter().flatten() {
        for (axis, coord) in pos.iter().enumerate() {
            if extents.len() <= axis {
                extents.push(std::ops::Range {
                    start: usize::MAX,
                    end: 0,
                });
            }

            let extent = &mut extents[axis];
            extent.start = extent.start.min(*coord);
            extent.end = extent.end.max(coord + 1);
        }
    }

    if extents.is_empty() {
        return Err(DataFusionError::Execution(
            "Cannot infer extents from empty slab positions".to_string(),
        ));
    }

    Ok(extents)
}

/// Convert axis extents to dimension sizes (`end - start` per axis).
fn infer_slab_dims(extents: &[std::ops::Range<usize>]) -> Vec<usize> {
    extents
        .iter()
        .map(|extent| extent.end - extent.start)
        .collect()
}

/// Populate an N-dimensional slab from a flat Arrow primitive array using the
/// supplied `slab_positions`.
///
/// Cells that are not covered by any row are filled with `T::Native::min_value()`
/// (the NetCDF "missing" sentinel).
fn fill_primitive_slab<T>(
    dims: &[usize],
    slab_positions: &[Option<Vec<usize>>],
    value_column: &arrow::array::ArrayRef,
) -> datafusion::error::Result<
    ArrayBase<
        ndarray::OwnedRepr<<T as ArrowPrimitiveType>::Native>,
        ndarray::Dim<ndarray::IxDynImpl>,
    >,
>
where
    T: ArrowPrimitiveType,
    T::Native: NcTypeDescriptor + Clone + Default,
    T::Native: num_traits::Bounded,
{
    use num_traits::Bounded;
    let array = value_column
        .as_ref()
        .as_any()
        .downcast_ref::<PrimitiveArray<T>>()
        .ok_or_else(|| {
            DataFusionError::Execution(format!(
                "Value column could not be downcast to PrimitiveArray<{}>",
                std::any::type_name::<T>()
            ))
        })?;

    let mut base = typed_ndarray_slab(dims, T::Native::min_value());

    for (maybe_position, maybe_value) in slab_positions.iter().zip(array.iter()) {
        let Some(position) = maybe_position else {
            continue;
        };
        let Some(value) = maybe_value else {
            continue;
        };

        let ix = ndarray::IxDyn(position.as_slice());
        base.get_mut(&ix)
            .map(|cell| *cell = value.clone())
            .ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "inferred slab position: {:?} was out of bounds",
                    &ix
                ))
            })?;
    }

    Ok(base)
}

/// For every row in `dimension_columns`, look up the position of each
/// dimension value inside the sorted `unique_values` vectors and return
/// one `Option<Vec<usize>>` per row.
///
/// A row receives `None` when any of its dimension columns is null or
/// could not be found in the unique-value set.
fn dimension_slab_positions(
    dimension_columns: RecordBatch,
    unique_values: Arc<ColumnValueMap>,
) -> datafusion::error::Result<Vec<Option<Vec<usize>>>> {
    let mut slab_positions = vec![
        Vec::<usize>::with_capacity(dimension_columns.num_columns());
        dimension_columns.num_rows()
    ];

    let schema = dimension_columns.schema();
    let num_rows = dimension_columns.num_rows();

    for column_index in 0..dimension_columns.num_columns() {
        let field = schema.field(column_index);

        let array = dimension_columns.column(column_index);
        let erased_values = unique_values.get(field).ok_or_else(|| {
            DataFusionError::Execution(format!(
                "Missing unique values for dimension column '{}'",
                field.name()
            ))
        })?;

        macro_rules! push_primitive_positions {
            ($arrow_ty:ty, $native_ty:ty) => {{
                let typed_vals = typed_values::<$native_ty>(field.name(), erased_values)?;
                let primitive_array = array
                    .as_any()
                    .downcast_ref::<PrimitiveArray<$arrow_ty>>()
                    .ok_or_else(|| {
                        DataFusionError::Execution(format!(
                            "Dimension column '{}' could not be downcast to expected primitive type",
                            field.name()
                        ))
                    })?;
                for row_index in 0..num_rows {
                    if primitive_array.is_null(row_index) {
                        continue;
                    }
                    let value = primitive_array.value(row_index);
                    if let Ok(pos) = typed_vals.binary_search(&value) {
                        slab_positions[row_index].push(pos);
                    }
                }
            }};
        }

        macro_rules! push_float_positions {
            ($arrow_ty:ty, $native_ty:ty) => {{
                let typed_vals =
                    typed_values::<OrderedFloat<$native_ty>>(field.name(), erased_values)?;
                let primitive_array = array
                    .as_any()
                    .downcast_ref::<PrimitiveArray<$arrow_ty>>()
                    .ok_or_else(|| {
                        DataFusionError::Execution(format!(
                            "Dimension column '{}' could not be downcast to expected float type",
                            field.name()
                        ))
                    })?;
                for row_index in 0..num_rows {
                    if primitive_array.is_null(row_index) {
                        continue;
                    }
                    let ordered_value =
                        OrderedFloat::<$native_ty>(primitive_array.value(row_index));
                    if let Ok(pos) = typed_vals.binary_search(&ordered_value) {
                        slab_positions[row_index].push(pos);
                    }
                }
            }};
        }

        match field.data_type() {
            DataType::Int8 => {
                push_primitive_positions!(arrow::datatypes::Int8Type, i8);
            }
            DataType::Int16 => {
                push_primitive_positions!(arrow::datatypes::Int16Type, i16);
            }
            DataType::Int32 | DataType::Date32 | DataType::Time32(_) => {
                push_primitive_positions!(arrow::datatypes::Int32Type, i32);
            }
            DataType::Int64
            | DataType::Date64
            | DataType::Time64(_)
            | DataType::Duration(_)
            | DataType::Interval(_) => {
                push_primitive_positions!(arrow::datatypes::Int64Type, i64);
            }
            DataType::Timestamp(unit, _) => match unit {
                arrow::datatypes::TimeUnit::Second => {
                    push_primitive_positions!(arrow::datatypes::TimestampSecondType, i64)
                }
                arrow::datatypes::TimeUnit::Millisecond => {
                    push_primitive_positions!(arrow::datatypes::TimestampMillisecondType, i64);
                }
                arrow::datatypes::TimeUnit::Microsecond => {
                    push_primitive_positions!(arrow::datatypes::TimestampMicrosecondType, i64)
                }
                arrow::datatypes::TimeUnit::Nanosecond => {
                    push_primitive_positions!(arrow::datatypes::TimestampNanosecondType, i64)
                }
            },
            DataType::UInt8 => {
                push_primitive_positions!(arrow::datatypes::UInt8Type, u8);
            }
            DataType::UInt16 => {
                push_primitive_positions!(arrow::datatypes::UInt16Type, u16);
            }
            DataType::UInt32 => {
                push_primitive_positions!(arrow::datatypes::UInt32Type, u32);
            }
            DataType::UInt64 => {
                push_primitive_positions!(arrow::datatypes::UInt64Type, u64);
            }
            DataType::Float32 => {
                push_float_positions!(arrow::datatypes::Float32Type, f32);
            }
            DataType::Float64 => {
                push_float_positions!(arrow::datatypes::Float64Type, f64);
            }
            unsupported => {
                return Err(DataFusionError::Execution(format!(
                    "Unsupported data type {:?} for dimension column '{}'",
                    unsupported,
                    field.name()
                )));
            }
        }
    }

    Ok(slab_positions
        .iter()
        .map(|slab_pos| {
            if slab_pos.len() == dimension_columns.num_columns() {
                Some(slab_pos.clone())
            } else {
                None
            }
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int32Array, Int64Array};
    use arrow::datatypes::{Field, Schema};
    use beacon_datafusion_ext::unique_values::{UniqueColumnValues, UniqueValuesHandleCollection};
    use datafusion::datasource::physical_plan::FileGroup;
    use datafusion::execution::object_store::ObjectStoreUrl;
    use datafusion::logical_expr::dml::InsertOp;
    use ordered_float::OrderedFloat;
    use std::sync::Arc;

    // ── helpers ────────────────────────────────────────────────────────

    /// Build a minimal [`FileSinkConfig`] for testing.
    fn test_sink_config(schema: SchemaRef) -> FileSinkConfig {
        FileSinkConfig {
            original_url: String::new(),
            object_store_url: ObjectStoreUrl::local_filesystem(),
            file_group: FileGroup::default(),
            table_paths: vec![],
            output_schema: schema,
            table_partition_cols: vec![],
            insert_op: InsertOp::Append,
            keep_partition_by_columns: false,
            file_extension: "nc".to_string(),
        }
    }

    // ── helpers ────────────────────────────────────────────────────────

    /// Build a [`ColumnValueMap`] from sorted vecs for two i32 dimension columns.
    fn two_i32_dims(
        lat_vals: Vec<i32>,
        lon_vals: Vec<i32>,
    ) -> (Arc<ColumnValueMap>, Vec<FieldRef>) {
        let lat_field: FieldRef = Arc::new(Field::new("lat", DataType::Int32, false));
        let lon_field: FieldRef = Arc::new(Field::new("lon", DataType::Int32, false));

        let mut map = ColumnValueMap::new();
        map.insert(
            lat_field.clone(),
            Box::new(UniqueColumnValues::<i32> { values: lat_vals }),
        );
        map.insert(
            lon_field.clone(),
            Box::new(UniqueColumnValues::<i32> { values: lon_vals }),
        );
        (Arc::new(map), vec![lat_field, lon_field])
    }

    // ── infer_extents ──────────────────────────────────────────────────

    #[test]
    fn test_infer_extents_basic() {
        let positions = vec![Some(vec![0, 0]), Some(vec![1, 2]), Some(vec![2, 1])];
        let extents = infer_extents(&positions).unwrap();
        assert_eq!(extents.len(), 2);
        assert_eq!(extents[0], 0..3);
        assert_eq!(extents[1], 0..3);
    }

    #[test]
    fn test_infer_extents_with_offset() {
        let positions = vec![Some(vec![3, 5]), Some(vec![4, 7])];
        let extents = infer_extents(&positions).unwrap();
        assert_eq!(extents[0], 3..5);
        assert_eq!(extents[1], 5..8);
    }

    #[test]
    fn test_infer_extents_skips_none() {
        let positions = vec![None, Some(vec![1, 2]), None, Some(vec![3, 4])];
        let extents = infer_extents(&positions).unwrap();
        assert_eq!(extents[0], 1..4);
        assert_eq!(extents[1], 2..5);
    }

    #[test]
    fn test_infer_extents_all_none() {
        let positions: Vec<Option<Vec<usize>>> = vec![None, None];
        let err = infer_extents(&positions).unwrap_err();
        assert!(err.to_string().contains("empty slab positions"));
    }

    #[test]
    fn test_infer_extents_empty_input() {
        let positions: Vec<Option<Vec<usize>>> = vec![];
        let err = infer_extents(&positions).unwrap_err();
        assert!(err.to_string().contains("empty slab positions"));
    }

    // ── infer_slab_dims ────────────────────────────────────────────────

    #[test]
    fn test_infer_slab_dims() {
        let extents = vec![0..3, 2..5, 10..13];
        assert_eq!(infer_slab_dims(&extents), vec![3, 3, 3]);
    }

    #[test]
    fn test_infer_slab_dims_single() {
        let extents = vec![0..1];
        assert_eq!(infer_slab_dims(&extents), vec![1]);
    }

    // ── relative_slab_positions ────────────────────────────────────────

    #[test]
    fn test_relative_slab_positions_with_offset() {
        let extents = vec![2..5, 10..13];
        let positions = vec![Some(vec![2, 10]), None, Some(vec![4, 12])];
        let result = relative_slab_positions(&extents, &positions);
        assert_eq!(result[0], Some(vec![0, 0]));
        assert_eq!(result[1], None);
        assert_eq!(result[2], Some(vec![2, 2]));
    }

    #[test]
    fn test_relative_slab_positions_zero_based() {
        let extents = vec![0..2, 0..3];
        let positions = vec![Some(vec![1, 2])];
        let result = relative_slab_positions(&extents, &positions);
        assert_eq!(result[0], Some(vec![1, 2]));
    }

    // ── fill_primitive_slab ────────────────────────────────────────────

    #[test]
    fn test_fill_primitive_slab_2d() {
        // 2×2 grid, place values at (0,0) and (1,1)
        let dims = vec![2, 2];
        let positions: Vec<Option<Vec<usize>>> = vec![Some(vec![0, 0]), Some(vec![1, 1])];
        let values: arrow::array::ArrayRef = Arc::new(Int32Array::from(vec![10, 20]));

        let slab =
            fill_primitive_slab::<arrow::datatypes::Int32Type>(&dims, &positions, &values).unwrap();

        assert_eq!(slab[[0, 0]], 10);
        assert_eq!(slab[[1, 1]], 20);
        // Unfilled cells get min_value (fill value)
        assert_eq!(slab[[0, 1]], i32::MIN);
        assert_eq!(slab[[1, 0]], i32::MIN);
    }

    #[test]
    fn test_fill_primitive_slab_skips_none_positions() {
        let dims = vec![2];
        let positions: Vec<Option<Vec<usize>>> = vec![None, Some(vec![1])];
        let values: arrow::array::ArrayRef = Arc::new(Int32Array::from(vec![Some(100), Some(200)]));

        let slab =
            fill_primitive_slab::<arrow::datatypes::Int32Type>(&dims, &positions, &values).unwrap();

        assert_eq!(slab[[0]], i32::MIN); // skipped
        assert_eq!(slab[[1]], 200);
    }

    #[test]
    fn test_fill_primitive_slab_skips_null_values() {
        let dims = vec![2];
        let positions: Vec<Option<Vec<usize>>> = vec![Some(vec![0]), Some(vec![1])];
        let values: arrow::array::ArrayRef = Arc::new(Int32Array::from(vec![None, Some(42)]));

        let slab =
            fill_primitive_slab::<arrow::datatypes::Int32Type>(&dims, &positions, &values).unwrap();

        assert_eq!(slab[[0]], i32::MIN);
        assert_eq!(slab[[1]], 42);
    }

    #[test]
    fn test_fill_primitive_slab_out_of_bounds() {
        let dims = vec![2];
        let positions: Vec<Option<Vec<usize>>> = vec![Some(vec![5])]; // out of range
        let values: arrow::array::ArrayRef = Arc::new(Int32Array::from(vec![10]));

        let err = fill_primitive_slab::<arrow::datatypes::Int32Type>(&dims, &positions, &values)
            .unwrap_err();
        assert!(err.to_string().contains("out of bounds"));
    }

    #[test]
    fn test_fill_primitive_slab_float64() {
        let dims = vec![3];
        let positions: Vec<Option<Vec<usize>>> = vec![Some(vec![0]), Some(vec![1]), Some(vec![2])];
        let values: arrow::array::ArrayRef = Arc::new(Float64Array::from(vec![1.5, 2.5, 3.5]));

        let slab = fill_primitive_slab::<arrow::datatypes::Float64Type>(&dims, &positions, &values)
            .unwrap();

        assert!((slab[[0]] - 1.5).abs() < f64::EPSILON);
        assert!((slab[[1]] - 2.5).abs() < f64::EPSILON);
        assert!((slab[[2]] - 3.5).abs() < f64::EPSILON);
    }

    // ── dimension_slab_positions ───────────────────────────────────────

    #[test]
    fn test_dimension_slab_positions_basic() {
        let (unique_map, fields) = two_i32_dims(vec![10, 20, 30], vec![100, 200]);

        let schema = Arc::new(Schema::new(
            fields
                .iter()
                .map(|f| f.as_ref().clone())
                .collect::<Vec<_>>(),
        ));
        let dim_batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![10, 20, 30])),
                Arc::new(Int32Array::from(vec![100, 200, 100])),
            ],
        )
        .unwrap();

        let positions = dimension_slab_positions(dim_batch, unique_map).unwrap();
        assert_eq!(positions[0], Some(vec![0, 0])); // lat=10 → 0, lon=100 → 0
        assert_eq!(positions[1], Some(vec![1, 1])); // lat=20 → 1, lon=200 → 1
        assert_eq!(positions[2], Some(vec![2, 0])); // lat=30 → 2, lon=100 → 0
    }

    #[test]
    fn test_dimension_slab_positions_null_gives_none() {
        let lat_field: FieldRef = Arc::new(Field::new("lat", DataType::Int32, true));
        let lon_field: FieldRef = Arc::new(Field::new("lon", DataType::Int32, true));

        let mut map = ColumnValueMap::new();
        map.insert(
            lat_field.clone(),
            Box::new(UniqueColumnValues::<i32> {
                values: vec![10, 20],
            }),
        );
        map.insert(
            lon_field.clone(),
            Box::new(UniqueColumnValues::<i32> {
                values: vec![100, 200],
            }),
        );
        let unique_map = Arc::new(map);
        let fields = [lat_field, lon_field];

        let schema = Arc::new(Schema::new(
            fields
                .iter()
                .map(|f| f.as_ref().clone())
                .collect::<Vec<_>>(),
        ));
        let dim_batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![Some(10), None])),
                Arc::new(Int32Array::from(vec![Some(100), Some(200)])),
            ],
        )
        .unwrap();

        let positions = dimension_slab_positions(dim_batch, unique_map).unwrap();
        assert_eq!(positions[0], Some(vec![0, 0]));
        assert_eq!(positions[1], None); // lat was null → incomplete → None
    }

    #[test]
    fn test_dimension_slab_positions_missing_value_gives_none() {
        // The value 99 is not in the unique set [10, 20]
        let (unique_map, fields) = two_i32_dims(vec![10, 20], vec![100]);

        let schema = Arc::new(Schema::new(
            fields
                .iter()
                .map(|f| f.as_ref().clone())
                .collect::<Vec<_>>(),
        ));
        let dim_batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![99])),
                Arc::new(Int32Array::from(vec![100])),
            ],
        )
        .unwrap();

        let positions = dimension_slab_positions(dim_batch, unique_map).unwrap();
        assert_eq!(positions[0], None); // 99 not found → incomplete
    }

    // ── dimension_slab_positions with floats ───────────────────────────

    #[test]
    fn test_dimension_slab_positions_float64() {
        let lat_field: FieldRef = Arc::new(Field::new("lat", DataType::Float64, false));
        let lon_field: FieldRef = Arc::new(Field::new("lon", DataType::Float64, false));

        let mut map = ColumnValueMap::new();
        map.insert(
            lat_field.clone(),
            Box::new(UniqueColumnValues::<OrderedFloat<f64>> {
                values: vec![OrderedFloat(1.0), OrderedFloat(2.0)],
            }),
        );
        map.insert(
            lon_field.clone(),
            Box::new(UniqueColumnValues::<OrderedFloat<f64>> {
                values: vec![OrderedFloat(10.0), OrderedFloat(20.0)],
            }),
        );
        let unique_map = Arc::new(map);

        let schema = Arc::new(Schema::new(vec![
            lat_field.as_ref().clone(),
            lon_field.as_ref().clone(),
        ]));
        let dim_batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Float64Array::from(vec![2.0, 1.0])),
                Arc::new(Float64Array::from(vec![10.0, 20.0])),
            ],
        )
        .unwrap();

        let positions = dimension_slab_positions(dim_batch, unique_map).unwrap();
        assert_eq!(positions[0], Some(vec![1, 0])); // lat=2.0→1, lon=10.0→0
        assert_eq!(positions[1], Some(vec![0, 1])); // lat=1.0→0, lon=20.0→1
    }

    // ── typed_values ───────────────────────────────────────────────────

    #[test]
    fn test_typed_values_correct_type() {
        let erased: ErasedColumnValues = Box::new(UniqueColumnValues::<i32> {
            values: vec![1, 2, 3],
        });
        let result = typed_values::<i32>("test_col", &erased).unwrap();
        assert_eq!(result, vec![1, 2, 3]);
    }

    #[test]
    fn test_typed_values_wrong_type() {
        let erased: ErasedColumnValues = Box::new(UniqueColumnValues::<i32> {
            values: vec![1, 2, 3],
        });
        let err = typed_values::<i64>("test_col", &erased).unwrap_err();
        assert!(err.to_string().contains("unexpected type"));
    }

    // ── write_ndarray_slab row-count mismatch ──────────────────────────

    #[test]
    fn test_write_ndarray_slab_row_count_mismatch() {
        // We can test the row-count validation without a real NetCDF file
        // by calling write_ndarray_slab with mismatched sizes.
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let path = tmp.path().with_extension("nc");
        let mut nc = netcdf::create(&path).unwrap();

        nc.add_dimension("x", 2).unwrap();
        nc.add_variable::<i32>("var", &["x"]).unwrap();

        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let dim_batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1, 2]))]).unwrap();

        let value_col: arrow::array::ArrayRef = Arc::new(Int32Array::from(vec![10, 20, 30])); // 3 != 2

        let (unique_map, _) = two_i32_dims(vec![1, 2], vec![]);

        let mut variable = nc.variable_mut("var").unwrap();
        let err = write_ndarray_slab(&mut variable, unique_map, dim_batch, value_col).unwrap_err();
        assert!(err.to_string().contains("Row count mismatch"));
    }

    // ── NetCDFNdSink::new rejects non-primitive schemas ────────────────

    #[test]
    fn test_nd_sink_rejects_string_columns() {
        let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, false)]));

        let sink_config = test_sink_config(schema);

        let collection = UniqueValuesHandleCollection::new();
        let err = NetCDFNdSink::new(sink_config, 1, collection).unwrap_err();
        assert!(err.to_string().contains("only supports primitive"));
        assert!(err.to_string().contains("name"));
    }

    // ── DisplayAs impls ────────────────────────────────────────────────

    /// Wrapper to test [`DisplayAs`] implementations.
    struct DisplayAsWrapper<'a, T: DisplayAs>(&'a T);
    impl<T: DisplayAs> std::fmt::Display for DisplayAsWrapper<'_, T> {
        fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
            self.0.fmt_as(DisplayFormatType::Default, f)
        }
    }

    #[test]
    fn test_display_as_flat() {
        let schema = Arc::new(Schema::new(vec![Field::new("val", DataType::Int32, false)]));
        let sink_config = test_sink_config(schema);
        let sink = NetCDFSink::new(sink_config);
        let display = format!("{}", DisplayAsWrapper(&sink));
        assert!(display.contains("NetCDFSink"));
    }

    #[test]
    fn test_display_as_nd() {
        let schema = Arc::new(Schema::new(vec![Field::new("val", DataType::Int32, false)]));
        let sink_config = test_sink_config(schema);
        let collection = UniqueValuesHandleCollection::new();
        let sink = NetCDFNdSink::new(sink_config, 0, collection).unwrap();
        let display = format!("{}", DisplayAsWrapper(&sink));
        assert!(display.contains("NetCDFNdSink"));
    }

    // ── end-to-end flat write via NetCDFSink ───────────────────────────

    #[tokio::test]
    async fn test_flat_sink_round_trip() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("temperature", DataType::Float64, true),
            Field::new("pressure", DataType::Int64, true),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Float64Array::from(vec![15.0, 20.5, 30.1])),
                Arc::new(Int64Array::from(vec![1013, 1010, 1005])),
            ],
        )
        .unwrap();

        // Write to a temp NetCDF via the flat sink
        let tmp_dir = tempfile::tempdir().unwrap();
        let nc_path = tmp_dir.path().join("test_flat.nc");

        // Create an ArrowRecordBatchWriter and write directly to verify
        // the encoder pathway works
        let mut writer =
            ArrowRecordBatchWriter::<DefaultEncoder>::new(&nc_path, schema.clone()).unwrap();
        writer.write_record_batch(batch.clone()).unwrap();
        writer.finish().unwrap();

        // Read the file back with netcdf and verify contents
        let nc_file = netcdf::open(&nc_path).unwrap();

        let temp_var = nc_file
            .variable("temperature")
            .expect("temperature variable");
        let temp_vals: Vec<f64> = temp_var.get_values(..).unwrap();
        assert_eq!(temp_vals.len(), 3);
        assert!((temp_vals[0] - 15.0).abs() < f64::EPSILON);
        assert!((temp_vals[1] - 20.5).abs() < f64::EPSILON);
        assert!((temp_vals[2] - 30.1).abs() < f64::EPSILON);

        let press_var = nc_file.variable("pressure").expect("pressure variable");
        let press_vals: Vec<i64> = press_var.get_values(..).unwrap();
        assert_eq!(press_vals, vec![1013i64, 1010, 1005]);
    }

    // ── end-to-end nd write ────────────────────────────────────────────

    #[test]
    fn test_nd_slab_write_round_trip() {
        // Create a 2×3 grid: lat = {10, 20}, lon = {100, 200, 300}
        let lat_field: FieldRef = Arc::new(Field::new("lat", DataType::Int32, false));
        let lon_field: FieldRef = Arc::new(Field::new("lon", DataType::Int32, false));

        let mut uv_map = ColumnValueMap::new();
        uv_map.insert(
            lat_field.clone(),
            Box::new(UniqueColumnValues::<i32> {
                values: vec![10, 20],
            }),
        );
        uv_map.insert(
            lon_field.clone(),
            Box::new(UniqueColumnValues::<i32> {
                values: vec![100, 200, 300],
            }),
        );
        let uv_map = Arc::new(uv_map);

        // Build flat batch with 4 rows (not all cells filled)
        let dim_schema = Arc::new(Schema::new(vec![
            lat_field.as_ref().clone(),
            lon_field.as_ref().clone(),
        ]));
        let dim_batch = RecordBatch::try_new(
            dim_schema,
            vec![
                Arc::new(Int32Array::from(vec![10, 10, 20, 20])),
                Arc::new(Int32Array::from(vec![100, 200, 200, 300])),
            ],
        )
        .unwrap();

        let value_col: arrow::array::ArrayRef =
            Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0]));

        // Write to a temp NetCDF
        let tmp_dir = tempfile::tempdir().unwrap();
        let nc_path = tmp_dir.path().join("test_nd.nc");
        let mut nc_file = netcdf::create(&nc_path).unwrap();

        // Define dimensions and variables
        nc_file.add_dimension("lat", 2).unwrap();
        nc_file.add_dimension("lon", 3).unwrap();

        // Dimension coordinate variables
        let mut lat_var = nc_file.add_variable::<i32>("lat", &["lat"]).unwrap();
        lat_var
            .put_values::<i32, _>(&[10i32, 20], vec![0..2])
            .unwrap();

        let mut lon_var = nc_file.add_variable::<i32>("lon", &["lon"]).unwrap();
        lon_var
            .put_values::<i32, _>(&[100i32, 200, 300], vec![0..3])
            .unwrap();

        // Data variable
        nc_file
            .add_variable::<f64>("temperature", &["lat", "lon"])
            .unwrap();

        let mut temp_var = nc_file.variable_mut("temperature").unwrap();
        write_ndarray_slab(&mut temp_var, uv_map, dim_batch, value_col).unwrap();
        nc_file.close().unwrap();

        // Read back and verify
        let nc_read = netcdf::open(&nc_path).unwrap();
        let temp_read = nc_read.variable("temperature").unwrap();
        let data = temp_read.get::<f64, _>(..).unwrap();

        // Shape should be [2, 3]
        assert_eq!(data.shape(), &[2, 3]);

        // Filled cells
        assert!((data[[0, 0]] - 1.0).abs() < f64::EPSILON); // lat=10, lon=100
        assert!((data[[0, 1]] - 2.0).abs() < f64::EPSILON); // lat=10, lon=200
        assert!((data[[1, 1]] - 3.0).abs() < f64::EPSILON); // lat=20, lon=200
        assert!((data[[1, 2]] - 4.0).abs() < f64::EPSILON); // lat=20, lon=300

        // Unfilled cells get fill value (f64::MIN)
        assert_eq!(data[[0, 2]], f64::MIN); // lat=10, lon=300 not written
        assert_eq!(data[[1, 0]], f64::MIN); // lat=20, lon=100 not written
    }
}
