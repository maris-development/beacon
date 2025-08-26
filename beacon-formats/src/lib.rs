use std::sync::Arc;

use datafusion::execution::SessionState;

use crate::{
    arrow::ArrowFormatFactory,
    csv::CsvFormatFactory,
    geo_parquet::GeoParquetFormatFactory,
    netcdf::{
        NetcdfFormatFactory, NetcdfOptions,
        object_resolver::{NetCDFObjectResolver, NetCDFSinkResolver},
    },
    parquet::ParquetFormatFactory,
};

pub mod arrow;
pub mod csv;
pub mod geo_parquet;
pub mod netcdf;
pub mod odv_ascii;
pub mod parquet;

/// Register file formats with the session state that can be used for reading
pub fn register_file_formats(
    session_state: &mut SessionState,
    netcdf_object_resolver: Arc<NetCDFObjectResolver>,
    netcdf_sink_resolver: Arc<NetCDFSinkResolver>,
) -> datafusion::error::Result<()> {
    session_state.register_file_format(Arc::new(ParquetFormatFactory), true)?;
    session_state.register_file_format(Arc::new(CsvFormatFactory), true)?;
    session_state.register_file_format(Arc::new(ArrowFormatFactory), true)?;
    session_state.register_file_format(Arc::new(GeoParquetFormatFactory::default()), true)?;
    session_state.register_file_format(
        Arc::new(NetcdfFormatFactory::new(
            NetcdfOptions::default(),
            netcdf_object_resolver,
            netcdf_sink_resolver,
        )),
        true,
    )?;
    Ok(())
}
