//! griddap decode. The netCDF decoder replaces this stub.

/// Decode a griddap `.nc` response.
pub async fn decode_netcdf(
    _file: tempfile::NamedTempFile,
    _target: arrow::datatypes::SchemaRef,
    _batch_size: usize,
) -> datafusion::error::Result<
    futures::stream::BoxStream<'static, datafusion::error::Result<arrow::array::RecordBatch>>,
> {
    Err(datafusion::error::DataFusionError::NotImplemented(
        "griddap decode".into(),
    ))
}
