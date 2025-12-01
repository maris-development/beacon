use std::{any::Any, fmt::Formatter, sync::Arc};

use arrow::{
    array::{Array, ArrowPrimitiveType, PrimitiveArray, RecordBatch},
    datatypes::{DataType, FieldRef, SchemaRef},
};
use beacon_arrow_netcdf::{
    NcString,
    encoders::default::DefaultEncoder,
    netcdf::{Extents, FileMut, NcTypeDescriptor, VariableMut},
    writer::ArrowRecordBatchWriter,
};
use datafusion::{
    datasource::{physical_plan::FileSinkConfig, sink::DataSink},
    error::DataFusionError,
    execution::{SendableRecordBatchStream, TaskContext},
    physical_plan::{DisplayAs, DisplayFormatType},
};
use futures::StreamExt;
use ndarray::ArrayBase;
use ordered_float::OrderedFloat;
use tracing::warn;

use crate::netcdf::{
    execution::unique_values::{
        ColumnValueMap, ErasedColumnValues, UniqueColumnValues, UniqueValuesHandleCollection,
    },
    object_resolver::NetCDFSinkResolver,
};

#[derive(Debug, Clone)]
pub struct NetCDFSink {
    sink_config: FileSinkConfig,
    path_resolver: NetCDFSinkResolver,
}

impl NetCDFSink {
    pub fn new(path_resolver: Arc<NetCDFSinkResolver>, sink_config: FileSinkConfig) -> Self {
        Self {
            sink_config,
            path_resolver: (*path_resolver).clone(),
        }
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
        let output_path = self
            .path_resolver
            .resolve_output_path(&self.sink_config.table_paths[0].prefix());

        let mut rows_written: u64 = 0;
        let mut nc_writer =
            ArrowRecordBatchWriter::<DefaultEncoder>::new(output_path, arrow_schema).map_err(
                |e| {
                    datafusion::error::DataFusionError::Execution(format!(
                        "Failed to create NetCDF ArrowRecordBatchWriter: {}",
                        e
                    ))
                },
            )?;

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

#[derive(Debug, Clone)]
pub struct NetCDFNdSink {
    sink_config: FileSinkConfig,
    path_resolver: NetCDFSinkResolver,
    ndims: usize,
    unique_values: UniqueValuesHandleCollection,
}

impl NetCDFNdSink {
    pub fn new(
        path_resolver: Arc<NetCDFSinkResolver>,
        sink_config: FileSinkConfig,
        ndims: usize,
        unique_values: UniqueValuesHandleCollection,
    ) -> Result<Self, DataFusionError> {
        // Assert that the sink config only contains primitive types
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
            path_resolver: (*path_resolver).clone(),
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
        let arrow_schema = self.sink_config.output_schema().clone();
        let output_path = self
            .path_resolver
            .resolve_output_path(&self.sink_config.table_paths[0].prefix());

        let mut rows_written: u64 = 0;
        let mut nc_file = beacon_arrow_netcdf::netcdf::create(output_path).map_err(|e| {
            datafusion::error::DataFusionError::Execution(format!(
                "Failed to create NetCDF file: {}",
                e
            ))
        })?;
        let unique_values_collection: ColumnValueMap =
            self.unique_values.unique_values().ok_or_else(|| {
                DataFusionError::Execution(
                    "No dimensions values collected for NetCDFNdSink".to_string(),
                )
            })?;

        create_dimension_variables(&mut nc_file, &unique_values_collection)?;

        // For each field that is not part of the unique values collection. Create a variable that used the unique values fields as dimensions
        for field in self.sink_config.output_schema().fields() {
            if unique_values_collection.contains_key(field) {
                continue;
            }
            let dims: Vec<&str> = unique_values_collection
                .keys()
                .take(self.ndims)
                .map(|f| f.name().as_str())
                .collect();

            create_variable(&mut nc_file, field, &dims)?; // Create variable
        }

        // let mut pinned_steam = std::pin::pin!(data);

        // while let Some(batch) = pinned_steam.next().await {
        //     let batch = batch?;
        //     rows_written += batch.num_rows() as u64;
        //     nc_writer.write_record_batch(batch).map_err(|e| {
        //         datafusion::error::DataFusionError::Execution(format!(
        //             "Failed to write record batch to NetCDF: {}",
        //             e
        //         ))
        //     })?;
        // }

        // nc_writer.finish().map_err(|e| {
        //     datafusion::error::DataFusionError::Execution(format!(
        //         "Failed to finish writing NetCDF: {}",
        //         e
        //     ))
        // })?;

        Ok(rows_written)
    }
}

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

fn create_dimension_variables(
    file: &mut FileMut,
    unique_values: &ColumnValueMap,
) -> datafusion::error::Result<()> {
    for (field_ref, erased_values) in unique_values {
        create_dimension_for_field(file, field_ref, erased_values)?;
    }

    Ok(())
}

fn create_dimension_for_field(
    file: &mut FileMut,
    field: &FieldRef,
    erased_values: &ErasedColumnValues,
) -> datafusion::error::Result<()> {
    let dim_name = field.name().to_string();
    // Ensure dimension exists
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

fn df_err(field: &str, action: &str, err: impl std::fmt::Display) -> DataFusionError {
    DataFusionError::Execution(format!(
        "Failed to {} for column '{}': {}",
        action, field, err
    ))
}

fn typed_ndarray_slab<T: NcTypeDescriptor + Clone>(
    dims: &[usize],
    fill_value: T,
) -> ArrayBase<ndarray::OwnedRepr<T>, ndarray::Dim<ndarray::IxDynImpl>> {
    ArrayBase::from_elem(ndarray::IxDyn(dims), fill_value)
}

fn write_ndarray_slab(
    variable: &mut VariableMut,
    unique_values: Arc<ColumnValueMap>,
    dimension_columns: RecordBatch,
    value_column: arrow::array::ArrayRef,
) -> datafusion::error::Result<()> {
    assert_eq!(dimension_columns.num_rows(), value_column.len());

    let slab_positions = dimension_slab_positions(dimension_columns, unique_values);
    let extents = infer_extents(&slab_positions)?;
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
            );

            variable.put(nc_extents, slab.view()).map_err(|e| {
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
            );

            variable.put(nc_extents, slab.view()).map_err(|e| {
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
            );

            variable.put(nc_extents, slab.view()).map_err(|e| {
                DataFusionError::Execution(format!(
                    "Failed to write slab Int32 data to variable '{}': {}",
                    variable.name(),
                    e
                ))
            })?;
        }
        DataType::Int64 | DataType::Timestamp(_, _) | DataType::Date64 | DataType::Time64(_) => {
            let slab = fill_primitive_slab::<arrow::datatypes::Int64Type>(
                &dims,
                &slab_positions,
                &value_column,
            );

            variable.put(nc_extents, slab.view()).map_err(|e| {
                DataFusionError::Execution(format!(
                    "Failed to write slab Int64 data to variable '{}': {}",
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
            );

            variable.put(nc_extents, slab.view()).map_err(|e| {
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
            );

            variable.put(nc_extents, slab.view()).map_err(|e| {
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
            );

            variable.put(nc_extents, slab.view()).map_err(|e| {
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
            );

            variable.put(nc_extents, slab.view()).map_err(|e| {
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
            );

            variable.put(nc_extents, slab.view()).map_err(|e| {
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
            );

            variable.put(nc_extents, slab.view()).map_err(|e| {
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

fn infer_extents(
    positions: &[Option<Vec<usize>>],
) -> Result<Vec<std::ops::Range<usize>>, DataFusionError> {
    let mut extents = Vec::new();

    for pos in positions.iter().flatten() {
        // Assume all positions are of the same length
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

fn infer_slab_dims(extents: &Vec<std::ops::Range<usize>>) -> Vec<usize> {
    let mut dims = Vec::new();

    for extent in extents {
        dims.push(extent.end - extent.start);
    }

    dims
}

fn fill_primitive_slab<T>(
    dims: &[usize],
    slab_positions: &[Option<Vec<usize>>],
    value_column: &arrow::array::ArrayRef,
) -> ArrayBase<
    ndarray::OwnedRepr<<T as ArrowPrimitiveType>::Native>,
    ndarray::Dim<ndarray::IxDynImpl>,
>
where
    T: ArrowPrimitiveType,
    T::Native: NcTypeDescriptor + Clone + Default,
    T::Native: num_traits::Bounded,
{
    use num_traits::bounds::LowerBounded;
    let array = value_column
        .as_ref()
        .as_any()
        .downcast_ref::<PrimitiveArray<T>>()
        .expect("value column type mismatch");

    let mut base = typed_ndarray_slab(dims, T::Native::min_value());

    for (maybe_position, maybe_value) in slab_positions.iter().zip(array.iter()) {
        let Some(position) = maybe_position else {
            continue;
        };
        let Some(value) = maybe_value else {
            continue;
        };

        let ix = ndarray::IxDyn(position.as_slice());
        base[ix] = value;
    }

    base
}

fn dimension_slab_positions(
    dimension_columns: RecordBatch,
    unique_values: Arc<ColumnValueMap>,
) -> Vec<Option<Vec<usize>>> {
    let mut slab_positions = vec![
        Vec::<usize>::with_capacity(dimension_columns.num_columns());
        dimension_columns.num_rows()
    ];

    let schema = dimension_columns.schema();
    let num_rows = dimension_columns.num_rows();

    for column_index in 0..dimension_columns.num_columns() {
        let field = schema.field(column_index);
        let array = dimension_columns.column(column_index);
        let erased_values = unique_values
            .get(field)
            .expect("missing unique values for dimension column");

        macro_rules! push_primitive_positions {
            ($arrow_ty:ty, $native_ty:ty) => {{
                let typed_values = typed_values::<$native_ty>(field.name(), erased_values)
                    .expect("unique values type mismatch for primitive dimension");
                let primitive_array = array
                    .as_any()
                    .downcast_ref::<PrimitiveArray<$arrow_ty>>()
                    .expect("dimension column type mismatch");
                for row_index in 0..num_rows {
                    if primitive_array.is_null(row_index) {
                        continue;
                    }
                    let value = primitive_array.value(row_index);
                    if let Ok(pos) = typed_values.binary_search(&value) {
                        slab_positions[row_index].push(pos);
                    }
                }
            }};
        }

        macro_rules! push_float_positions {
            ($arrow_ty:ty, $native_ty:ty) => {{
                let typed_values =
                    typed_values::<OrderedFloat<$native_ty>>(field.name(), erased_values)
                        .expect("unique values type mismatch for float dimension");
                let primitive_array = array
                    .as_any()
                    .downcast_ref::<PrimitiveArray<$arrow_ty>>()
                    .expect("dimension column type mismatch");
                for row_index in 0..num_rows {
                    if primitive_array.is_null(row_index) {
                        continue;
                    }
                    let ordered_value =
                        OrderedFloat::<$native_ty>(primitive_array.value(row_index));
                    if let Ok(pos) = typed_values.binary_search(&ordered_value) {
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
            | DataType::Timestamp(_, _)
            | DataType::Duration(_)
            | DataType::Interval(_) => {
                push_primitive_positions!(arrow::datatypes::Int64Type, i64);
            }
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
                warn!(
                    "Skipping slab position inference for unsupported data type {:?} (column '{}')",
                    unsupported,
                    field.name()
                );
            }
        }
    }
    slab_positions
        .iter()
        .map(|slab_pos| {
            if slab_pos.len() == dimension_columns.num_columns() {
                Some(slab_pos.clone())
            } else {
                None
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use std::sync::Arc;

    #[test]
    fn infer_extents_returns_expected_ranges() {
        let positions = vec![Some(vec![0, 2]), Some(vec![3, 1]), None, Some(vec![1, 5])];

        let extents = infer_extents(&positions).expect("extents should be inferred");

        assert_eq!(extents.len(), 2);
        assert_eq!(extents[0], 0..4);
        assert_eq!(extents[1], 1..6);
    }

    #[test]
    fn infer_extents_errors_when_all_positions_missing() {
        let positions = vec![None, None];

        let err = infer_extents(&positions).expect_err("expected inference failure");
        match err {
            DataFusionError::Execution(message) => {
                assert!(message.contains("Cannot infer extents"));
            }
            other => panic!("unexpected error: {:?}", other),
        }
    }

    #[test]
    fn infer_slab_dims_returns_axis_lengths() {
        let extents = vec![2..5, 10..11, 4..9];

        let dims = infer_slab_dims(&extents);

        assert_eq!(dims, vec![3, 1, 5]);
    }

    #[test]
    fn fill_primitive_slab_populates_only_valid_positions() {
        let dims = vec![3, 2];
        let slab_positions = vec![Some(vec![0, 0]), Some(vec![1, 1]), Some(vec![2, 1]), None];
        let value_column: arrow::array::ArrayRef =
            Arc::new(Int32Array::from(vec![Some(10), None, Some(30), Some(40)]));

        let slab = fill_primitive_slab::<arrow::datatypes::Int32Type>(
            &dims,
            &slab_positions,
            &value_column,
        );

        assert_eq!(slab.shape(), &[3, 2]);
        assert_eq!(slab[[0, 0]], 10);
        assert_eq!(slab[[1, 1]], i32::MIN);
        assert_eq!(slab[[2, 1]], 30);
        assert_eq!(slab[[2, 0]], i32::MIN);
    }
}
