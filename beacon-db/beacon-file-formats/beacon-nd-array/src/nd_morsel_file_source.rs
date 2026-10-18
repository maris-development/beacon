use datafusion::datasource::physical_plan::FileSource;

use crate::nd_file_opener::NdFileOpener;
use std::sync::Arc;

pub struct NdMorselFileSource {
    opener: Arc<dyn NdFileOpener>,
}

impl NdMorselFileSource {
    pub fn new(opener: Arc<dyn NdFileOpener>) -> Self {
        Self { opener }
    }
}

impl FileSource for NdMorselFileSource {}
