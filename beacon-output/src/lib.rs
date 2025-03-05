use std::{fmt::Debug, path::PathBuf, pin::Pin, sync::Arc};

use arrow::datatypes::SchemaRef;
use bytes::Bytes;
use datafusion::{
    execution::SendableRecordBatchStream,
    prelude::{DataFrame, SessionContext},
};
use futures::TryStream;
use tempfile::NamedTempFile;
use utoipa::ToSchema;

mod csv;
mod ipc;
mod json;
mod netcdf;
mod odv;
mod parquet;

pub struct Output {
    pub output_method: OutputMethod,
    pub content_type: String,
    pub content_disposition: String,
}

pub enum OutputMethod {
    Stream(
        Pin<
            Box<
                dyn TryStream<
                        Error = anyhow::Error,
                        Ok = Bytes,
                        Item = Result<Bytes, anyhow::Error>,
                    > + Send,
            >,
        >,
    ),
    File(NamedTempFile),
}

#[derive(Debug, serde::Serialize, serde::Deserialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum OutputFormat {
    Csv,
    Ipc,
    Parquet,
    Json,
    Odv,
    NetCDF,
}

impl OutputFormat {
    pub async fn output(&self, ctx: Arc<SessionContext>, df: DataFrame) -> anyhow::Result<Output> {
        match self {
            OutputFormat::Csv => csv::output(df).await,
            OutputFormat::Ipc => ipc::output(ctx, df).await,
            OutputFormat::Parquet => parquet::output(df).await,
            OutputFormat::Json => json::output(df).await,
            OutputFormat::Odv => odv::output(ctx.clone(), df).await,
            OutputFormat::NetCDF => netcdf::output(ctx.clone(), df).await,
        }
    }
}

pub struct TempOutputFile {
    pub tmp_dir_path: PathBuf,
    pub file: NamedTempFile,
}

impl TempOutputFile {
    pub fn new(prefix: &str, suffix: &str) -> anyhow::Result<Self> {
        //Create the temp dir if it doesn't exist
        let tmp_dir_path = beacon_config::DATA_DIR.join("tmp");
        std::fs::create_dir_all(tmp_dir_path.as_path())?;

        Ok(Self {
            file: tempfile::Builder::new()
                .prefix(prefix)
                .suffix(suffix)
                .tempfile_in(tmp_dir_path.as_path())?,
            tmp_dir_path,
        })
    }

    pub fn path(&self) -> PathBuf {
        self.file.path().to_path_buf()
    }

    pub fn object_store_path(&self) -> String {
        format!(
            "/tmp/{}",
            self.file
                .path()
                .file_name()
                .unwrap()
                .to_string_lossy()
                .to_string()
        )
    }
}
