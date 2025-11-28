use std::{any::Any, fmt::Formatter, sync::Arc};

use arrow::datatypes::{DataType, FieldRef, SchemaRef};
use beacon_arrow_netcdf::{
    NcString,
    encoders::default::DefaultEncoder,
    netcdf::{FileMut, NcTypeDescriptor},
    writer::ArrowRecordBatchWriter,
};
use datafusion::{
    datasource::{physical_plan::FileSinkConfig, sink::DataSink},
    error::DataFusionError,
    execution::{SendableRecordBatchStream, TaskContext},
    physical_plan::{DisplayAs, DisplayFormatType},
};
use futures::StreamExt;
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

fn create_dimension_variables(
    file: &mut FileMut,
    unique_values: Vec<ColumnValueMap>,
) -> datafusion::error::Result<()> {
    for column_values in unique_values {
        for (field_ref, erased_values) in column_values {
            create_dimension_for_field(file, &field_ref, &erased_values)?;
        }
    }

    Ok(())
}

fn create_dimension_for_field(
    file: &mut FileMut,
    field: &FieldRef,
    erased_values: &ErasedColumnValues,
) -> datafusion::error::Result<()> {
    let dim_name = field.name().clone();

    match field.data_type() {
        DataType::Utf8 | DataType::LargeUtf8 => {
            let values = typed_values::<String>(&dim_name, erased_values)?;
            write_string_dimension(file, &dim_name, &values)?;
        }
        DataType::Boolean => {
            let values = typed_values::<bool>(&dim_name, erased_values)?;
            let encoded: Vec<u8> = values.into_iter().map(|v| if v { 1 } else { 0 }).collect();
            write_numeric_dimension::<u8>(file, &dim_name, &encoded)?;
        }
        DataType::Int8 => {
            let values = typed_values::<i8>(&dim_name, erased_values)?;
            write_numeric_dimension::<i8>(file, &dim_name, &values)?;
        }
        DataType::Int16 => {
            let values = typed_values::<i16>(&dim_name, erased_values)?;
            write_numeric_dimension::<i16>(file, &dim_name, &values)?;
        }
        DataType::Int32 | DataType::Date32 | DataType::Time32(_) => {
            let values = typed_values::<i32>(&dim_name, erased_values)?;
            write_numeric_dimension::<i32>(file, &dim_name, &values)?;
        }
        DataType::Int64
        | DataType::Date64
        | DataType::Time64(_)
        | DataType::Timestamp(_, _)
        | DataType::Duration(_)
        | DataType::Interval(_) => {
            let values = typed_values::<i64>(&dim_name, erased_values)?;
            write_numeric_dimension::<i64>(file, &dim_name, &values)?;
        }
        DataType::UInt8 => {
            let values = typed_values::<u8>(&dim_name, erased_values)?;
            write_numeric_dimension::<u8>(file, &dim_name, &values)?;
        }
        DataType::UInt16 => {
            let values = typed_values::<u16>(&dim_name, erased_values)?;
            write_numeric_dimension::<u16>(file, &dim_name, &values)?;
        }
        DataType::UInt32 => {
            let values = typed_values::<u32>(&dim_name, erased_values)?;
            write_numeric_dimension::<u32>(file, &dim_name, &values)?;
        }
        DataType::UInt64 => {
            let values = typed_values::<u64>(&dim_name, erased_values)?;
            write_numeric_dimension::<u64>(file, &dim_name, &values)?;
        }
        DataType::Float32 => {
            let ordered = typed_values::<OrderedFloat<f32>>(&dim_name, erased_values)?;
            let floats: Vec<f32> = ordered.into_iter().map(|v| v.into_inner()).collect();
            write_numeric_dimension::<f32>(file, &dim_name, &floats)?;
        }
        DataType::Float64 => {
            let ordered = typed_values::<OrderedFloat<f64>>(&dim_name, erased_values)?;
            let floats: Vec<f64> = ordered.into_iter().map(|v| v.into_inner()).collect();
            write_numeric_dimension::<f64>(file, &dim_name, &floats)?;
        }
        DataType::Decimal128(_, _)
        | DataType::Binary
        | DataType::LargeBinary
        | DataType::FixedSizeBinary(_) => {
            warn!(
                "Skipping dimension creation for unsupported data type {:?} (column '{}')",
                field.data_type(),
                field.name()
            );
        }
        other => {
            warn!(
                "Skipping dimension creation for unsupported data type {:?} (column '{}')",
                other,
                field.name()
            );
        }
    }

    Ok(())
}

fn write_string_dimension(
    file: &mut FileMut,
    name: &str,
    values: &[String],
) -> datafusion::error::Result<()> {
    if values.is_empty() {
        return Ok(());
    }

    ensure_dimension(file, name, values.len())?;
    let dim_refs = vec![name];
    let mut variable = file
        .add_variable::<NcString>(name, &dim_refs)
        .map_err(|e| df_err(name, "create string coordinate", e))?;
    let cstrings: Vec<NcString> = values.iter().map(|value| NcString::new(value)).collect();
    let extents = vec![0..cstrings.len()];
    variable
        .put_values::<NcString, _>(&cstrings, extents)
        .map_err(|e| df_err(name, "write string coordinate", e))?;

    Ok(())
}

fn write_numeric_dimension<T>(
    file: &mut FileMut,
    name: &str,
    values: &[T],
) -> datafusion::error::Result<()>
where
    T: NcTypeDescriptor + Copy,
{
    if values.is_empty() {
        return Ok(());
    }

    ensure_dimension(file, name, values.len())?;
    let dim_refs = vec![name];
    let mut variable = file
        .add_variable::<T>(name, &dim_refs)
        .map_err(|e| df_err(name, "create numeric coordinate", e))?;
    let extents = vec![0..values.len()];
    variable
        .put_values::<T, _>(values, extents)
        .map_err(|e| df_err(name, "write numeric coordinate", e))?;

    Ok(())
}

fn ensure_dimension(file: &mut FileMut, name: &str, len: usize) -> datafusion::error::Result<()> {
    if len == 0 {
        return Ok(());
    }

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
        .as_ref()
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

fn typed_ndarray_slap<T : NcTypeDescriptor>()
