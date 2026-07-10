//! [`LanceTable`]: beacon's `TableProvider` for a managed Lance dataset.
//!
//! Mirrors the `IcebergTable` wrapper: it holds its own serializable definition
//! so beacon's schema-persistence layer can downcast to it and recover the
//! `table.json`. Reads delegate to Lance's `LanceTableProvider`; the dataset is
//! reopened at the **latest version** on every scan so prior inserts/replaces are
//! visible. Writes go through a [`LanceDataSink`] (Lance's provider is read-only).

use std::any::Any;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::TableType;
use datafusion::datasource::sink::DataSinkExec;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use lance::datafusion::LanceTableProvider;
use lance::dataset::builder::DatasetBuilder;
use lance::session::Session as LanceSession;

use crate::definition::LanceTableDefinition;
use crate::io::WriteKind;
use crate::sink::LanceDataSink;
use crate::warehouse::LanceWarehouse;

/// A beacon-managed Lance table provider.
#[derive(Debug, Clone)]
pub struct LanceTable {
    definition: LanceTableDefinition,
    schema: SchemaRef,
    /// The runtime-scoped warehouse, used to serialize writes (via the sink).
    warehouse: Arc<LanceWarehouse>,
}

impl LanceTable {
    pub fn new(
        definition: LanceTableDefinition,
        schema: SchemaRef,
        warehouse: Arc<LanceWarehouse>,
    ) -> Self {
        Self {
            definition,
            schema,
            warehouse,
        }
    }

    /// Open the dataset at the definition's location, caching its Arrow schema.
    pub async fn open(
        definition: LanceTableDefinition,
        warehouse: Arc<LanceWarehouse>,
    ) -> anyhow::Result<Self> {
        let provider = open_read_provider(&definition.location, warehouse.session())
            .await
            .map_err(|e| {
                anyhow::anyhow!("Failed to open Lance table '{}': {e}", definition.name)
            })?;
        Ok(Self::new(definition, provider.schema(), warehouse))
    }

    /// The serializable definition used to persist and rebuild this table.
    pub fn definition(&self) -> &LanceTableDefinition {
        &self.definition
    }
}

/// Open the latest dataset version at `uri` (resolved through `session`'s
/// object-store registry) as a Lance read provider.
async fn open_read_provider(
    uri: &str,
    session: Arc<LanceSession>,
) -> DataFusionResult<LanceTableProvider> {
    let dataset = DatasetBuilder::from_uri(uri)
        .with_session(session)
        .load()
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    Ok(LanceTableProvider::new(Arc::new(dataset), false, false))
}

#[async_trait]
impl TableProvider for LanceTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        // Reopen to the latest version so scans observe prior inserts/replaces.
        let provider =
            open_read_provider(&self.definition.location, self.warehouse.session()).await?;
        provider.scan(state, projection, filters, limit).await
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        // Report Inexact: Lance may use the predicate to prune, and DataFusion
        // re-applies it for correctness.
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }

    async fn insert_into(
        &self,
        _state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let kind = match insert_op {
            InsertOp::Append => WriteKind::Append,
            InsertOp::Overwrite | InsertOp::Replace => WriteKind::Overwrite,
        };
        let sink = Arc::new(LanceDataSink::new(
            self.definition.location.clone(),
            self.schema.clone(),
            kind,
            self.warehouse.clone(),
        ));
        // The sink writes a single input stream, but `DataSinkExec` only consumes
        // partition 0 of its input (it expects the optimizer's EnforceDistribution
        // to coalesce, which beacon's hand-built physical plans do not run). Merge
        // multi-partition inputs (e.g. a multi-file `read_parquet` scan feeding a
        // CTAS / INSERT ... SELECT) so every row is written, not just the first
        // partition's.
        let input = if input.output_partitioning().partition_count() > 1 {
            Arc::new(CoalescePartitionsExec::new(input)) as Arc<dyn ExecutionPlan>
        } else {
            input
        };
        Ok(Arc::new(DataSinkExec::new(input, sink, None)))
    }
}
