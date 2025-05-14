use tempfile::NamedTempFile;

#[derive(Debug)]
pub enum QueryOutputBuffer {
    Csv(NamedTempFile),
    Ipc(NamedTempFile),
    Json(NamedTempFile),
    Parquet(NamedTempFile),
    NetCDF(NamedTempFile),
    Odv(NamedTempFile),
}
