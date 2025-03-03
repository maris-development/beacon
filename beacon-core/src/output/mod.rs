use std::{fmt::Debug, pin::Pin, sync::Arc};

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
}

impl OutputFormat {
    pub async fn output(&self, ctx: Arc<SessionContext>, df: DataFrame) -> anyhow::Result<Output> {
        match self {
            OutputFormat::Csv => csv::output(df).await,
            OutputFormat::Ipc => ipc::output(ctx, df).await,
            OutputFormat::Parquet => parquet::output(df).await,
            OutputFormat::Json => json::output(df).await,
            OutputFormat::Odv => odv::output(ctx.clone(), df).await,
        }
    }
}

pub fn create_temp_file(prefix: &str, suffix: &str) -> anyhow::Result<NamedTempFile> {
    let temp_dir = beacon_config::DATA_DIR.join(beacon_config::TMP_DIR_PATH.as_path());
    println!("temp_dir: {:?}", temp_dir);
    tempfile::Builder::new()
        .prefix(prefix)
        .suffix(suffix)
        .tempfile_in(temp_dir)
        .map_err(Into::into)
}
