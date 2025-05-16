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
