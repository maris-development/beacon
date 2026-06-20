//! Registration of the file formats Beacon can read.
//!
//! This pulls together the individual `beacon-arrow-*` format crates and
//! registers their factories with a DataFusion session.

use std::sync::Arc;

use beacon_arrow_atlas::datafusion::{AtlasConfig, AtlasFormatFactory, options::AtlasOptions};
use beacon_arrow_bbf::datafusion::{BBFFormatFactory, BbfConfig};
use beacon_arrow_csv::datafusion::CsvFormatFactory;
use beacon_arrow_geoparquet::datafusion::GeoParquetFormatFactory;
use beacon_arrow_ipc::datafusion::ArrowFormatFactory;
use beacon_arrow_netcdf::datafusion::{NetCDFFormatFactory, NetcdfConfig, options::NetcdfOptions};
use beacon_arrow_parquet::datafusion::ParquetFormatFactory;
use beacon_arrow_tiff::datafusion::TiffFormatFactory;
use beacon_arrow_zarr::datafusion::ZarrFormatFactory;
use beacon_datafusion_ext::format_ext::FileFormatFactoryExt;
use beacon_object_storage::DatasetsStore;
use datafusion::prelude::SessionContext;

/// Register file formats with the session state that can be used for reading
pub fn file_formats(
    session_context: Arc<SessionContext>,
    datasets_object_store: Arc<DatasetsStore>,
) -> datafusion::error::Result<Vec<Arc<dyn FileFormatFactoryExt>>> {
    let state_ref = session_context.state_ref();
    let mut state = state_ref.write();

    // Bridge: build the per-format config (owned by each format crate) from the
    // process-global config. A later change threads an owned config in instead.
    let netcdf_config = NetcdfConfig {
        use_reader_cache: beacon_config::CONFIG.netcdf.use_reader_cache,
        reader_cache_size: beacon_config::CONFIG.netcdf.reader_cache_size,
        enable_statistics: beacon_config::CONFIG.netcdf.enable_statistics,
    };
    let atlas_config = AtlasConfig {
        use_reader_cache: beacon_config::CONFIG.atlas.use_reader_cache,
        reader_cache_size: beacon_config::CONFIG.atlas.reader_cache_size,
    };
    let bbf_config = BbfConfig {
        split_streams_slice: beacon_config::CONFIG.runtime.bbf_split_streams_slice,
    };

    let formats: Vec<Arc<dyn FileFormatFactoryExt>> = vec![
        Arc::new(ParquetFormatFactory),
        Arc::new(CsvFormatFactory),
        Arc::new(ArrowFormatFactory),
        Arc::new(NetCDFFormatFactory::new(
            datasets_object_store.clone(),
            NetcdfOptions::default(),
            netcdf_config,
        )),
        Arc::new(AtlasFormatFactory::new(
            datasets_object_store.clone(),
            AtlasOptions::default(),
            atlas_config,
        )),
        Arc::new(TiffFormatFactory::new(Default::default())),
        Arc::new(ZarrFormatFactory),
        Arc::new(BBFFormatFactory::new(bbf_config)),
        Arc::new(GeoParquetFormatFactory::default()),
    ];

    for format in formats.iter() {
        state.register_file_format(format.clone(), true)?;
    }

    Ok(formats)
}
