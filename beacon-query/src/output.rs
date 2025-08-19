use tempfile::NamedTempFile;
use utoipa::ToSchema;

#[derive(Debug, serde::Serialize, serde::Deserialize, ToSchema)]
pub struct Output {
    pub format: OutputFormat,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum OutputFormat {
    Csv,
    #[serde(alias = "arrow")]
    Ipc,
    Parquet,
    Json,
    // Odv(OdvOptions),
    NetCDF,
    GeoParquet {
        longitude_column: String,
        latitude_column: String,
    },
}

#[derive(Debug)]
pub enum QueryOutputFile {
    Csv(NamedTempFile),
    Ipc(NamedTempFile),
    Json(NamedTempFile),
    Parquet(NamedTempFile),
    NetCDF(NamedTempFile),
    Odv(NamedTempFile),
    GeoParquet(NamedTempFile),
}

impl QueryOutputFile {
    pub fn size(&self) -> anyhow::Result<u64> {
        match self {
            QueryOutputFile::Csv(file) => Ok(file.path().metadata()?.len()),
            QueryOutputFile::Ipc(file) => Ok(file.path().metadata()?.len()),
            QueryOutputFile::Json(file) => Ok(file.path().metadata()?.len()),
            QueryOutputFile::Parquet(file) => Ok(file.path().metadata()?.len()),
            QueryOutputFile::NetCDF(file) => Ok(file.path().metadata()?.len()),
            QueryOutputFile::Odv(file) => Ok(file.path().metadata()?.len()),
            QueryOutputFile::GeoParquet(file) => Ok(file.path().metadata()?.len()),
        }
    }
}
