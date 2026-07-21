//! Registration of the file formats Beacon can read.
//!
//! This pulls together the individual `beacon-arrow-*` format crates and
//! registers their factories with a DataFusion session.

use std::sync::Arc;

use beacon_arrow_atlas::datafusion::{AtlasFormatFactory, options::AtlasOptions};
use beacon_arrow_bbf::datafusion::BBFFormatFactory;
use beacon_arrow_csv::datafusion::CsvFormatFactory;
use beacon_arrow_geoparquet::datafusion::GeoParquetFormatFactory;
use beacon_arrow_ipc::datafusion::ArrowFormatFactory;
use beacon_arrow_netcdf::datafusion::{NetCDFFormatFactory, options::NetcdfOptions};
use beacon_arrow_parquet::datafusion::ParquetFormatFactory;
use beacon_arrow_tiff::datafusion::TiffFormatFactory;
use beacon_arrow_zarr::datafusion::{ZarrFormatFactory, ZarrOptions};
use beacon_datafusion_ext::format_ext::FileFormatFactoryExt;
use beacon_object_storage::DatasetsStore;
use datafusion::prelude::SessionContext;

/// Register file formats with the session state that can be used for reading
pub fn file_formats(
    session_context: Arc<SessionContext>,
    datasets_object_store: Arc<DatasetsStore>,
    config: &beacon_config::Config,
) -> datafusion::error::Result<Vec<Arc<dyn FileFormatFactoryExt>>> {
    let state_ref = session_context.state_ref();
    let mut state = state_ref.write();

    // The per-format config types are the same types the factories take, so the
    // runtime's config is handed straight through.
    let formats: Vec<Arc<dyn FileFormatFactoryExt>> = vec![
        Arc::new(ParquetFormatFactory),
        Arc::new(CsvFormatFactory),
        Arc::new(ArrowFormatFactory),
        Arc::new(NetCDFFormatFactory::new(
            datasets_object_store.clone(),
            NetcdfOptions::default(),
            config.netcdf.clone(),
        )),
        Arc::new(AtlasFormatFactory::new(
            datasets_object_store.clone(),
            AtlasOptions::default(),
            config.atlas.clone(),
        )),
        Arc::new(TiffFormatFactory::new(Default::default())),
        // Writable: `COPY TO ... STORED AS ZARR` builds a directory store under
        // the datasets store's tmp root, the same way NetCDF does.
        Arc::new(ZarrFormatFactory::new_for_write(
            datasets_object_store.clone(),
            ZarrOptions::default(),
        )),
        Arc::new(BBFFormatFactory::new(config.bbf.clone())),
        Arc::new(GeoParquetFormatFactory::default()),
    ];

    for format in formats.iter() {
        state.register_file_format(format.clone(), true)?;
    }

    Ok(formats)
}
