//! `ErddapExec`: one tabledap `.parquet` request per partition.

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use datafusion::error::Result;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};

use crate::client::ErddapClient;

/// Runs one ERDDAP request per partition and decodes the parquet response.
#[derive(Debug)]
pub struct ErddapExec {
    client: Arc<ErddapClient>,
    urls: Vec<String>,
    schema: SchemaRef,
    limit: Option<usize>,
    properties: Arc<PlanProperties>,
}

impl ErddapExec {
    /// Build the plan. Each response is conformed to `schema`, the output schema.
    pub fn new(
        client: Arc<ErddapClient>,
        urls: Vec<String>,
        schema: SchemaRef,
        limit: Option<usize>,
    ) -> Self {
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(urls.len().max(1)),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            client,
            urls,
            schema,
            limit,
            properties,
        }
    }

    /// The request URL of each partition.
    pub fn urls(&self) -> &[String] {
        &self.urls
    }
}

impl DisplayAs for ErddapExec {
    fn fmt_as(&self, _format: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "ErddapExec: partitions={}, urls=[{}]",
            self.urls.len(),
            self.urls.join(", ")
        )
    }
}

impl ExecutionPlan for ErddapExec {
    fn name(&self) -> &str {
        "ErddapExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let schema = self.schema.clone();
        let Some(url) = self.urls.get(partition).cloned() else {
            return Ok(Box::pin(RecordBatchStreamAdapter::new(
                schema,
                futures::stream::empty(),
            )));
        };
        let client = self.client.clone();
        let batch_size = context.session_config().batch_size();
        let target = schema.clone();
        let batches = futures::stream::once(async move {
            match client.download(&url, ".parquet").await? {
                // No matching rows is an empty partition.
                None => Ok(futures::stream::empty().boxed()),
                Some(file) => crate::tabledap::decode_parquet(file, target, batch_size).await,
            }
        })
        .try_flatten();
        let batches = match self.limit {
            Some(limit) => truncate(batches.boxed(), limit),
            None => batches.boxed(),
        };
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, batches)))
    }
}

/// Stop after `limit` rows, slicing the last batch.
fn truncate(
    stream: BoxStream<'static, Result<RecordBatch>>,
    limit: usize,
) -> BoxStream<'static, Result<RecordBatch>> {
    stream
        .scan(limit, |left, batch| {
            if *left == 0 {
                return futures::future::ready(None);
            }
            let out = batch.map(|b| {
                let take = b.num_rows().min(*left);
                *left -= take;
                b.slice(0, take)
            });
            futures::future::ready(Some(out))
        })
        .boxed()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixture::{FixtureServer, Route};
    use datafusion::prelude::SessionContext;
    use futures::TryStreamExt;

    #[tokio::test]
    async fn no_results_is_an_empty_partition_and_limit_truncates() {
        let server = FixtureServer::start(vec![
            Route::status(
                "/erddap/tabledap/t.parquet",
                404,
                crate::fixture::test_file("no_results.txt"),
            )
            .with_query("empty"),
            Route::file("/erddap/tabledap/t.parquet", "tabledap.parquet"),
        ])
        .await;
        let client = Arc::new(ErddapClient::new(std::time::Duration::from_secs(5)).unwrap());
        let ctx = SessionContext::new();
        let empty = ErddapExec::new(
            client.clone(),
            vec![format!("{}/tabledap/t.parquet?empty", server.erddap_url())],
            Arc::new(arrow::datatypes::Schema::empty()),
            None,
        );
        let batches: Vec<_> = empty
            .execute(0, ctx.task_ctx())
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert_eq!(
            batches
                .iter()
                .map(|b: &RecordBatch| b.num_rows())
                .sum::<usize>(),
            0
        );

        let limited = ErddapExec::new(
            client,
            vec![format!("{}/tabledap/t.parquet?all", server.erddap_url())],
            Arc::new(arrow::datatypes::Schema::empty()),
            Some(3),
        );
        let batches: Vec<_> = limited
            .execute(0, ctx.task_ctx())
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert_eq!(
            batches
                .iter()
                .map(|b: &RecordBatch| b.num_rows())
                .sum::<usize>(),
            3
        );
    }
}
