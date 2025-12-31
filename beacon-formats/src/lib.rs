use std::sync::Arc;

use beacon_object_storage::DatasetsStore;
use datafusion::prelude::SessionContext;
use object_store::ObjectMeta;

use crate::{
    arrow::ArrowFormatFactory,
    csv::CsvFormatFactory,
    netcdf::{
        NetCDFFormatFactory, NetcdfOptions,
        object_resolver::{NetCDFObjectResolver, NetCDFSinkResolver},
    },
    parquet::ParquetFormatFactory,
    zarr::ZarrFormatFactory,
};

pub mod arrow;
pub mod csv;
pub mod geo_parquet;
pub mod netcdf;
pub mod odv_ascii;
pub mod parquet;
pub mod zarr;

pub trait FileFormatFactoryExt:
    datafusion::datasource::file_format::FileFormatFactory + Send + Sync
{
    fn discover_datasets(&self, objects: &[ObjectMeta]) -> datafusion::error::Result<Vec<Dataset>>;
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Dataset {
    pub file_path: String,
    pub format: DatasetFormat,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum DatasetFormat {
    Parquet,
    Csv,
    Arrow,
    NetCDF,
    Zarr,
}

impl Dataset {
    pub fn update_file_path(&mut self, new_path: String) {
        self.file_path = new_path;
    }

    pub fn file_path(&self) -> &str {
        &self.file_path
    }
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
        Arc::new(ZarrFormatFactory),
    ];

    for format in formats.iter() {
        state.register_file_format(format.clone(), true)?;
    }

    Ok(formats)
}
