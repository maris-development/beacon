use std::path::PathBuf;

pub struct IndexEngine {}

pub enum IndexUpdate {
    FileChanged { path: PathBuf },
    FileRemoved { path: PathBuf },
    FileAdded { path: PathBuf },
}
