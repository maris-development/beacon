use std::sync::Arc;

use datafusion::{
    catalog::TableProvider, datasource::file_format::FileFormat, prelude::SessionContext,
};
use serde::{Deserialize, Serialize};

use super::IndexProvider;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlobIndex {
    pub glob_pattern: String,
    pub file_format: IndexFileFormat,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IndexFileFormat {
    ArrowIpc,
    Parquet,
    CSV,
    NetCDF,
}

impl IndexFileFormat {
    pub fn file_format(&self) -> Arc<dyn FileFormat> {
        match self {
            IndexFileFormat::ArrowIpc => {
                todo!()
            }
            IndexFileFormat::Parquet => {
                todo!()
            }
            IndexFileFormat::CSV => {
                todo!()
            }
            IndexFileFormat::NetCDF => {
                todo!()
            }
        }
    }
}

#[typetag::serde]
impl IndexProvider for GlobIndex {
    fn as_table(&self, session_ctx: SessionContext) -> Arc<dyn TableProvider> {
        todo!()
    }

    fn update(&self, update: super::IndexUpdate) -> anyhow::Result<()> {
        Ok(())
    }
}
