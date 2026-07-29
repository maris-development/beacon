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
use lance::dataset::scanner::MaterializationStyle;
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
    let projected_columns = projection.map_or_else(|| schema.fields().len(), |p| p.len());
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

        // Only meaningful alongside a filter: materialization style decides which
        // columns are read in full and filtered in memory ("early") versus read
        // only at the matching row offsets ("late").
        //
        // Lance's heuristic decides per column: local storage calls a column early
        // when its fixed byte width is under 10. It never considers how *many*
        // columns are projected, so a wide projection makes almost every numeric
        // column early and the scan reads the whole table no matter how few rows
        // match. Measured on the 100M-row, 105-column ClickBench table, selecting
        // all columns under a filter matching 0.016% of rows: 16.4s early vs 11.5s
        // late; at 0.74% (738k rows) it is 3.4s vs 1.0s.
        //
        // Late is not free: it fetches matching rows individually, so it loses when
        // the filter matches most of the table. Measured crossover, by fraction of
        // rows matched:
        //     0.016% ->  late 1.43x faster      1.6%  -> late 1.80x faster
        //     0.74%  ->  late 3.28x faster       38%  -> late 1.09x slower
        //     100%   ->  late 1.26x slower
        // So the rule is width, not selectivity (which we cannot know at plan
        // time): above `LATE_MATERIALIZATION_MIN_COLUMNS` the downside is a bounded
        // ~10-25% on unselective filters and the upside is several fold, while
        // narrow projections keep Lance's heuristic, where early costs little.
        match lance_materialization_style() {
            Some(style) => scan.materialization_style(style),
            None if projected_columns > LATE_MATERIALIZATION_MIN_COLUMNS => {
                scan.materialization_style(MaterializationStyle::AllLate)
            }
            None => &mut scan,
        };
    }

    // Scan each group in fragment order. Groups cover contiguous fragment ranges
    // and land on output partitions in the same order, so concatenating the
    // partitions in index order reproduces an unsplit scan exactly.
    scan.scan_in_order(true);
    scan.create_plan().await.map_err(DataFusionError::from)
}

/// Projection width above which a filtered scan switches to late materialization.
/// See the crossover measurements in `scan_fragment_group`.
const LATE_MATERIALIZATION_MIN_COLUMNS: usize = 16;

/// Override for Lance's column materialization heuristic.
///
/// `BEACON_LANCE_MATERIALIZATION=late|early` forces all columns one way; unset
/// keeps Lance's per-column heuristic.
fn lance_materialization_style() -> Option<MaterializationStyle> {
    match std::env::var("BEACON_LANCE_MATERIALIZATION")
        .ok()
        .as_deref()
        .map(str::trim)
    {
        Some("late") => Some(MaterializationStyle::AllLate),
        Some("early") => Some(MaterializationStyle::AllEarly),
        _ => None,
    }
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

        // Plain UnionExec: it maps child `i` onto output partition `i`, so the scan
        // exposes one partition per fragment group and downstream operators (hash
        // repartition, aggregation) fan out over them exactly as they do over a
        // multi-file parquet scan.
        //
        // This does not cost determinism. UnionExec's child-to-partition mapping is
        // itself deterministic; the run-to-run row shuffling comes from *merging*
        // partitions in completion order, which happens in `CoalescePartitionsExec`.
        // `OrderedCoalesce` rewrites every such node into an `OrderedUnionExec`, and
        // `execute_stream_ordered` does the same for the final collection, so order
        // is restored at the points where partitions actually converge.
        //
        // Collapsing to a single partition here instead (which is what this used to
        // do) made the scan a serialization point: DataFusion put a
        // `RepartitionExec: RoundRobinBatch(N)` directly above it to fan the rows
        // back out, so every row crossed one channel and was then copied again.
        Ok(Arc::new(UnionExec::new(plans)))
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
