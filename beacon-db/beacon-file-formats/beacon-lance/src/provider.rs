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
use datafusion::datasource::sink::DataSinkExec;
use datafusion::datasource::TableType;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use lance::dataset::builder::DatasetBuilder;
use lance::dataset::Dataset;
use lance::datafusion::LanceTableProvider;
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
            .map_err(|e| anyhow::anyhow!("Failed to open Lance table '{}': {e}", definition.name))?;
        Ok(Self::new(definition, provider.schema(), warehouse))
    }

    /// The serializable definition used to persist and rebuild this table.
    pub fn definition(&self) -> &LanceTableDefinition {
        &self.definition
    }
}

/// Open the latest dataset version at `uri` (resolved through `session`'s
/// object-store registry).
async fn open_dataset(uri: &str, session: Arc<LanceSession>) -> DataFusionResult<Arc<Dataset>> {
    let dataset = DatasetBuilder::from_uri(uri)
        .with_session(session)
        .load()
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    Ok(Arc::new(dataset))
}

/// Open the latest dataset version at `uri` as a Lance read provider.
async fn open_read_provider(
    uri: &str,
    session: Arc<LanceSession>,
) -> DataFusionResult<LanceTableProvider> {
    let dataset = open_dataset(uri, session).await?;
    Ok(LanceTableProvider::new(dataset, false, false))
}

/// Build a scan plan restricted to `fragments`.
///
/// Lance's own `LanceTableProvider` scans every fragment through one scanner,
/// which yields a single DataFusion partition; its internal reader concurrency
/// then caps the whole scan at ~4 fragments in flight. Building one plan per
/// fragment group and unioning them gives DataFusion real partitions, so the
/// read and decode fan out across the runtime's threads.
async fn scan_fragment_group(
    dataset: &Arc<Dataset>,
    schema: &SchemaRef,
    range: std::ops::Range<usize>,
    projection: Option<&Vec<usize>>,
    filters: &[Expr],
) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
    let mut scan = dataset.scan();
    // Slice here so the concrete `Fragment` type never appears in the signature
    // (it lives in lance-table, which beacon-lance does not depend on directly).
    scan.with_fragments(dataset.fragments()[range].to_vec());

    match projection {
        Some(p) if p.is_empty() => {
            scan.empty_project().map_err(DataFusionError::from)?;
        }
        Some(p) => {
            let columns: Vec<String> = p
                .iter()
                .map(|i| schema.field(*i).name().clone())
                .collect();
            scan.project(&columns).map_err(DataFusionError::from)?;
        }
        None => {}
    }

    if let Some((first, rest)) = filters.split_first() {
        let mut expr = first.clone();
        for f in rest {
            expr = Expr::and(expr, f.clone());
        }
        scan.filter_expr(expr);
    }

    // Row order across the union is not meaningful anyway, and an unordered scan
    // lets Lance read within the group concurrently.
    scan.scan_in_order(false);
    scan.create_plan().await.map_err(DataFusionError::from)
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
        let dataset = open_dataset(&self.definition.location, self.warehouse.session()).await?;

        let target = state.config_options().execution.target_partitions;
        let n_frags = dataset.fragments().len();

        // A LIMIT must not be applied per group (each would return `limit` rows),
        // and splitting is pointless below two fragments or one target partition.
        if limit.is_some() || target <= 1 || n_frags < 2 {
            let provider = LanceTableProvider::new(dataset, false, false);
            return provider.scan(state, projection, filters, limit).await;
        }

        // Spread fragments over at most `target` groups.
        let groups = target.min(n_frags);
        let per = n_frags.div_ceil(groups);
        let mut plans = Vec::with_capacity(groups);
        let mut start = 0;
        while start < n_frags {
            let end = (start + per).min(n_frags);
            plans.push(
                scan_fragment_group(&dataset, &self.schema, start..end, projection, filters).await?,
            );
            start = end;
        }

        // `try_new` returns the sole input unchanged when there is only one.
        UnionExec::try_new(plans)
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        // Lance applies the predicate exactly (its own provider reports Exact for
        // the same reason), so DataFusion does not need to re-apply it above the
        // scan. Reporting Inexact here added a redundant FilterExec over every row.
        Ok(vec![TableProviderFilterPushDown::Exact; filters.len()])
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
