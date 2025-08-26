use std::sync::Arc;

use datafusion::execution::SessionState;

use crate::{
    arrow::ArrowFormatFactory,
    csv::CsvFormatFactory,
    geo_parquet::GeoParquetFormatFactory,
    netcdf::{NetcdfFormatFactory, NetcdfOptions, object_resolver::NetCDFObjectResolver},
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
) -> datafusion::error::Result<()> {
    session_state.register_file_format(Arc::new(ParquetFormatFactory), true)?;
    session_state.register_file_format(Arc::new(CsvFormatFactory), true)?;
    session_state.register_file_format(Arc::new(ArrowFormatFactory), true)?;
    session_state.register_file_format(Arc::new(GeoParquetFormatFactory::default()), true)?;
    session_state.register_file_format(
        Arc::new(NetcdfFormatFactory::new(
            NetcdfOptions::default(),
            netcdf_object_resolver,
        )),
        true,
    )?;
    Ok(())
}
