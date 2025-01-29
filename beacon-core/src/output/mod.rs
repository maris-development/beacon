use std::{fmt::Debug, pin::Pin};

use arrow::datatypes::SchemaRef;
use bytes::Bytes;
use datafusion::{
    execution::SendableRecordBatchStream,
    prelude::{DataFrame, SessionContext},
};
use futures::TryStream;
use tempfile::NamedTempFile;

mod csv;
mod ipc;
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

#[typetag::serde]
#[async_trait::async_trait]
pub trait OutputFormat: Debug {
    async fn output(&self, ctx: SessionContext, df: DataFrame) -> anyhow::Result<Output>;
}
