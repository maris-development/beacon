use std::sync::Arc;

use beacon_arrow_atlas::datafusion::{AtlasFormatFactory, options::AtlasOptions};
use beacon_arrow_netcdf::datafusion::{NetCDFFormatFactory, options::NetcdfOptions};
use beacon_arrow_tiff::datafusion::TiffFormatFactory;
use beacon_datafusion_ext::format_ext::FileFormatFactoryExt;
use beacon_object_storage::DatasetsStore;
use datafusion::prelude::SessionContext;

use crate::{
    arrow::ArrowFormatFactory, csv::CsvFormatFactory, parquet::ParquetFormatFactory,
    zarr::ZarrFormatFactory,
};

pub mod arrow;
pub mod csv;
pub mod parquet;

/// Re-export of the BBF DataFusion integration, which now lives in the
/// `beacon-arrow-bbf` crate. Kept here so existing `beacon_formats::bbf::*`
/// references keep resolving.
pub mod bbf {
    pub use beacon_arrow_bbf::datafusion::{BBFFormat, BBFFormatFactory};
}

/// Re-export of the GeoParquet DataFusion integration, which now lives in the
/// `beacon-arrow-geoparquet` crate. Kept here so existing
/// `beacon_formats::geo_parquet::*` references keep resolving.
pub mod geo_parquet {
    pub use beacon_arrow_geoparquet::datafusion::{
        GeoParquetFormat, GeoParquetFormatFactory, GeoParquetOptions,
    };
}

/// Re-export of the Zarr DataFusion integration, which now lives in the
/// `beacon-arrow-zarr` crate alongside the other N-D formats. Kept here so
/// existing `beacon_formats::zarr::ZarrFormat` references keep resolving.
pub mod zarr {
    pub use beacon_arrow_zarr::datafusion::{ZarrFormat, ZarrFormatFactory};
}

/// Register file formats with the session state that can be used for reading
pub fn file_formats(
    session_context: Arc<SessionContext>,
    datasets_object_store: Arc<DatasetsStore>,
) -> datafusion::error::Result<Vec<Arc<dyn FileFormatFactoryExt>>> {
    let state_ref = session_context.state_ref();
    let mut state = state_ref.write();

    let formats: Vec<Arc<dyn FileFormatFactoryExt>> = vec![
        Arc::new(ParquetFormatFactory),
        Arc::new(CsvFormatFactory),
        Arc::new(ArrowFormatFactory),
        Arc::new(NetCDFFormatFactory::new(
            datasets_object_store.clone(),
            NetcdfOptions::default(),
        )),
        Arc::new(AtlasFormatFactory::new(
            datasets_object_store.clone(),
            AtlasOptions::default(),
        )),
        Arc::new(TiffFormatFactory::new(Default::default())),
        Arc::new(ZarrFormatFactory),
        Arc::new(bbf::BBFFormatFactory),
    ];

    for format in formats.iter() {
        state.register_file_format(format.clone(), true)?;
    }

    Ok(formats)
}
