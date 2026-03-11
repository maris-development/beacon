use std::sync::Arc;

use beacon_object_storage::DatasetsStore;
use datafusion::prelude::SessionContext;
use object_store::ObjectMeta;

use crate::{
    arrow::ArrowFormatFactory,
    csv::CsvFormatFactory,
    netcdf::{NetCDFFormatFactory, NetcdfOptions},
    parquet::ParquetFormatFactory,
    zarr::ZarrFormatFactory,
};

pub mod arrow;
pub mod bbf;
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
    BBF,
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
        Arc::new(bbf::BBFFormatFactory),
    ];

    for format in formats.iter() {
        state.register_file_format(format.clone(), true)?;
    }

    Ok(formats)
}

/// Get the maximum number of open file descriptors allowed by the system.
/// On Unix systems, this is determined by the NOFILE rlimit. On other systems, it defaults to u64::MAX (running through docker should default to unix though).
pub fn max_open_fd() -> u64 {
    #[cfg(unix)]
    {
        use rlimit::{Resource, getrlimit};
        if let Ok((soft_limit, _)) = getrlimit(Resource::NOFILE) {
            tracing::debug!(
                "Max open file descriptors (NOFILE soft limit): {}",
                soft_limit
            );
            soft_limit
        } else {
            tracing::warn!("Failed to get NOFILE rlimit, defaulting to 1024");
            1024
        }
    }
    #[cfg(not(unix))]
    u64::MAX
}

pub fn file_open_parallelism() -> usize {
    let max = max_open_fd() as usize / 2; // use half of the available file descriptors for parallelism to be safe
    //Make sure max is at least 1 to avoid zero parallelism
    std::cmp::max(max, 1)
}
