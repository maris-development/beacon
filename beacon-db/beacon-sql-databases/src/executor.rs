//! A [`SQLExecutor`] wrapper that applies the filters the federated scan receives.
//!
//! `datafusion-federation` reports every filter from physical filter pushdown as handled, so
//! DataFusion drops the `FilterExec` above the federated scan. The `datafusion-table-providers`
//! executors ignore those filters, and the query then returns rows that do not match its
//! `WHERE` clause. [`FilteringSqlExecutor`] delegates to the engine's executor and applies the
//! filters to each batch, like the remote-Beacon executor does.

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use beacon_datafusion_ext::remote::apply_pushed_filters;
use datafusion::common::Statistics;
use datafusion::error::Result;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::{PhysicalExpr, SendableRecordBatchStream};
use datafusion::sql::unparser::dialect::Dialect;
use datafusion_federation::sql::{
    AstAnalyzer, LogicalOptimizer, SQLExecutor, SQLFederationProvider,
};

/// Build a federation provider that runs `provider`'s executor through [`FilteringSqlExecutor`].
///
/// The provider's optimizer holds the executor that plans the federated scan, so a new provider
/// is necessary. Name and compute context do not change, so tables on one database still
/// federate together.
pub(crate) fn filtering_federation(provider: &SQLFederationProvider) -> Arc<SQLFederationProvider> {
    let executor = FilteringSqlExecutor::new(Arc::clone(&provider.executor));
    Arc::new(SQLFederationProvider::new(Arc::new(executor)))
}

/// Delegates to the engine's [`SQLExecutor`] and applies the pushed-down filters to its stream.
pub struct FilteringSqlExecutor {
    inner: Arc<dyn SQLExecutor>,
}

impl FilteringSqlExecutor {
    pub fn new(inner: Arc<dyn SQLExecutor>) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl SQLExecutor for FilteringSqlExecutor {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn compute_context(&self) -> Option<String> {
        self.inner.compute_context()
    }

    fn dialect(&self) -> Arc<dyn Dialect> {
        self.inner.dialect()
    }

    fn logical_optimizer(&self) -> Option<LogicalOptimizer> {
        self.inner.logical_optimizer()
    }

    fn ast_analyzer(&self) -> Option<AstAnalyzer> {
        self.inner.ast_analyzer()
    }

    fn execute(
        &self,
        query: &str,
        schema: SchemaRef,
        filters: &[Arc<dyn PhysicalExpr>],
    ) -> Result<SendableRecordBatchStream> {
        let stream = self.inner.execute(query, Arc::clone(&schema), filters)?;
        Ok(apply_pushed_filters(stream, schema, filters))
    }

    async fn statistics(&self, plan: &LogicalPlan) -> Result<Statistics> {
        self.inner.statistics(plan).await
    }

    async fn table_names(&self) -> Result<Vec<String>> {
        self.inner.table_names().await
    }

    async fn get_table_schema(&self, table_name: &str) -> Result<SchemaRef> {
        self.inner.get_table_schema(table_name).await
    }

    fn metrics(&self) -> Option<MetricsSet> {
        self.inner.metrics()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::tests::definition;
    use crate::source::BeaconSqlTable;
    use arrow::array::{Int64Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::catalog::TableProvider;
    use datafusion::datasource::MemTable;
    use datafusion::execution::SessionStateBuilder;
    use datafusion::optimizer::OptimizerRule;
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use datafusion::prelude::SessionContext;
    use datafusion::sql::unparser::dialect::DefaultDialect;
    use datafusion::sql::TableReference;
    use datafusion_federation::sql::{RemoteTable, SQLTableSource};
    use datafusion_federation::{FederatedQueryPlanner, FederatedTableProviderAdaptor};
    use futures::TryStreamExt;

    /// A database that runs the federated SQL on its own context. Like the
    /// `datafusion-table-providers` executors, it ignores `filters`.
    struct InMemoryDatabase {
        context: SessionContext,
    }

    #[async_trait]
    impl SQLExecutor for InMemoryDatabase {
        fn name(&self) -> &str {
            "in_memory_database"
        }

        fn compute_context(&self) -> Option<String> {
            Some("in_memory_database".to_string())
        }

        fn dialect(&self) -> Arc<dyn Dialect> {
            Arc::new(DefaultDialect {})
        }

        fn execute(
            &self,
            query: &str,
            schema: SchemaRef,
            _filters: &[Arc<dyn PhysicalExpr>],
        ) -> Result<SendableRecordBatchStream> {
            let context = self.context.clone();
            let query = query.to_string();
            let stream =
                futures::stream::once(
                    async move { context.sql(&query).await?.execute_stream().await },
                )
                .try_flatten();
            Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
        }

        async fn table_names(&self) -> Result<Vec<String>> {
            Ok(vec![])
        }

        async fn get_table_schema(&self, table_name: &str) -> Result<SchemaRef> {
            Ok(self.context.table_provider(table_name).await?.schema())
        }
    }

    fn table(column: &str, values: Vec<i64>) -> Arc<MemTable> {
        let schema = Arc::new(Schema::new(vec![Field::new(column, DataType::Int64, true)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(values))],
        )
        .unwrap();
        Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap())
    }

    /// A federated table `obs` (`depth` 0 to 9) in the shape `build_provider` registers.
    fn database_table(filtering: bool) -> Arc<dyn TableProvider> {
        let obs = table("depth", (0..10).collect());
        let database = SessionContext::new();
        database.register_table("obs", obs.clone()).unwrap();

        let provider = SQLFederationProvider::new(Arc::new(InMemoryDatabase { context: database }));
        let provider = if filtering {
            filtering_federation(&provider)
        } else {
            Arc::new(provider)
        };
        let remote = RemoteTable::new(TableReference::parse_str("obs").into(), obs.schema());
        let table = BeaconSqlTable::new(Arc::new(remote), definition(obs.schema()));
        let source = SQLTableSource::new_with_table(provider, Arc::new(table));
        Arc::new(FederatedTableProviderAdaptor::new(Arc::new(source)))
    }

    /// A local context with the federated table `pg_obs` and a one-row local table `one`.
    fn local_context(
        rules: Vec<Arc<dyn OptimizerRule + Send + Sync>>,
        filtering: bool,
    ) -> SessionContext {
        let state = SessionStateBuilder::new()
            .with_optimizer_rules(rules)
            .with_query_planner(Arc::new(FederatedQueryPlanner::new()))
            .with_default_features()
            .build();
        let context = SessionContext::new_with_state(state);
        context
            .register_table("pg_obs", database_table(filtering))
            .unwrap();
        context.register_table("one", table("k", vec![1])).unwrap();
        context
    }

    async fn count(context: &SessionContext, sql: &str) -> i64 {
        let batches = context.sql(sql).await.unwrap().collect().await.unwrap();
        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("count is Int64")
            .value(0)
    }

    const JOINED: &str = "SELECT count(*) FROM pg_obs o CROSS JOIN one WHERE o.depth < 2";

    /// The upstream rule federates before `push_down_filter`, so the filter stays above the
    /// federated scan. Physical filter pushdown then moves it into the scan.
    #[tokio::test]
    async fn a_filter_pushed_into_the_federated_scan_holds() {
        let rules = datafusion_federation::default_optimizer_rules();
        assert_eq!(
            count(&local_context(rules.clone(), false), JOINED).await,
            10,
            "the engine executor alone loses the filter; drop the wrapper once it does not"
        );
        assert_eq!(count(&local_context(rules, true), JOINED).await, 2);
    }

    /// Beacon's own rule set keeps the result right, with or without a join.
    #[tokio::test]
    async fn filters_hold_with_the_beacon_federation_rules() {
        let rules = beacon_datafusion_ext::remote::federation_optimizer_rules();
        let context = local_context(rules, true);
        assert_eq!(count(&context, JOINED).await, 2);
        assert_eq!(
            count(&context, "SELECT count(*) FROM pg_obs WHERE depth < 2").await,
            2
        );
        assert_eq!(count(&context, "SELECT count(*) FROM pg_obs").await, 10);
    }
}
