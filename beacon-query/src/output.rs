use tempfile::NamedTempFile;

#[derive(Debug)]
pub enum QueryOutputFile {
    Csv(NamedTempFile),
    Ipc(NamedTempFile),
    Json(NamedTempFile),
    Parquet(NamedTempFile),
    NetCDF(NamedTempFile),
    Odv(NamedTempFile),
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
        }
    }
}
