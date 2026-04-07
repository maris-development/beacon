use async_trait::async_trait;
use beacon_atlas::prelude::Dataset;
use datafusion::{execution::SendableRecordBatchStream, prelude::SQLOptions};
use futures::stream::BoxStream;

use crate::statement_handlers::{context::HandlerContext, payload::StatementPayload};

#[async_trait]
pub(crate) trait StatementHandler: Send + Sync {
    fn kind(&self) -> crate::statement_handlers::payload::StatementKind;

    async fn execute(
        &self,
        payload: StatementPayload,
        context: &HandlerContext,
        sql_options: &SQLOptions,
    ) -> anyhow::Result<SendableRecordBatchStream>;
}

#[async_trait]
pub(crate) trait IngestFormatLoader: Send + Sync {
    fn format_name(&self) -> &'static str;

    async fn load(
        &self,
        context: &HandlerContext,
        glob_pattern: &str,
    ) -> anyhow::Result<BoxStream<'static, anyhow::Result<Dataset>>>;
}