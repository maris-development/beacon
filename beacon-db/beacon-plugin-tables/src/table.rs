//! The table of a plugin table type.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use async_trait::async_trait;
use beacon_datafusion_ext::nd::exec::{NdBroadcastExec, NdSourceExec};
use beacon_datafusion_ext::nd::{is_nd_encoded, logical_schema};
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{Statistics, plan_err};
use datafusion::datasource::TableType;
use datafusion::error::Result;
use datafusion::execution::SessionState;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::{CreateExternalTable, Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::projection::{ProjectionExec, ProjectionExpr};
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};

use crate::definition::PluginTableDefinition;
use crate::filters::pushable;
use crate::install::InstalledTableTypes;

/// A table that a plugin factory made. It keeps its definition, so Beacon can persist it.
///
/// When every column of the plugin provider is `beacon.nd`-encoded, the table is an nd
/// table: its schema is the logical schema, and its scan broadcasts each nd batch to rows.
#[derive(Debug)]
pub struct PluginTable {
    definition: PluginTableDefinition,
    plugin: String,
    installed: Arc<AtomicBool>,
    inner: Arc<dyn TableProvider>,
    /// The logical schema of an nd table. `None` for a flat table.
    nd_schema: Option<SchemaRef>,
}

impl PluginTable {
    /// The persisted definition.
    pub fn definition(&self) -> &PluginTableDefinition {
        &self.definition
    }

    /// The name of the plugin that owns the table type.
    pub fn plugin(&self) -> &str {
        &self.plugin
    }

    /// True when the plugin provider returns `beacon.nd`-encoded batches.
    pub fn is_nd(&self) -> bool {
        self.nd_schema.is_some()
    }

    /// The nd scan: the plugin batches go through `NdSourceExec` and `NdBroadcastExec`.
    async fn scan_nd(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let count_only = projection.is_some_and(Vec::is_empty);
        // An nd batch of no column has no grid, so `COUNT(*)` reads every column.
        let projection = if count_only { None } else { projection };
        // A limit counts nd batches in the plugin, not rows, so it stays with DataFusion.
        let plan = self.inner.scan(state, projection, filters, None).await?;
        let source = Arc::new(NdSourceExec::try_new(plan)?);
        let broadcast: Arc<dyn ExecutionPlan> = Arc::new(NdBroadcastExec::try_new(source)?);
        if !count_only {
            return Ok(broadcast);
        }
        // No column, and one row per grid cell.
        Ok(Arc::new(ProjectionExec::try_new(
            Vec::<ProjectionExpr>::new(),
            broadcast,
        )?))
    }

    fn check_installed(&self) -> Result<()> {
        if self.installed.load(Ordering::Acquire) {
            return Ok(());
        }
        plan_err!(
            "table '{}' needs plugin '{}', and the plugin is uninstalled",
            self.definition.name,
            self.plugin
        )
    }
}

/// The logical schema of `schema` when every field is `beacon.nd`-encoded, `None` when
/// no field is. A mix of encoded and plain fields is an error.
fn nd_logical_schema(table: &str, schema: &Schema) -> Result<Option<SchemaRef>> {
    let encoded = schema
        .fields()
        .iter()
        .filter(|field| is_nd_encoded(field))
        .count();
    if encoded == 0 {
        return Ok(None);
    }
    if encoded < schema.fields().len() {
        let plain: Vec<&str> = schema
            .fields()
            .iter()
            .filter(|field| !is_nd_encoded(field))
            .map(|field| field.name().as_str())
            .collect();
        return plan_err!(
            "table '{table}': the plugin schema mixes beacon.nd columns and plain columns ({}); \
             make all columns beacon.nd or none",
            plain.join(", ")
        );
    }
    logical_schema(schema).map(Some)
}

/// Creates the table of `cmd` with a plugin table type. Returns `None` when no plugin
/// type has the name `cmd.file_type`.
pub async fn create_plugin_table(
    state: &SessionState,
    installed: &InstalledTableTypes,
    cmd: &CreateExternalTable,
) -> anyhow::Result<Option<Arc<dyn TableProvider>>> {
    let Some((plugin, table_type)) = installed.find(&cmd.file_type) else {
        return Ok(None);
    };
    let inner = table_type.factory().create(state, cmd).await?;
    let nd_schema = nd_logical_schema(&cmd.name.to_string(), &inner.schema())?;
    Ok(Some(Arc::new(PluginTable {
        definition: PluginTableDefinition::from_command(cmd, table_type.name()),
        plugin,
        installed: table_type.installed_flag(),
        inner,
        nd_schema,
    })))
}

#[async_trait]
impl TableProvider for PluginTable {
    fn schema(&self) -> SchemaRef {
        match &self.nd_schema {
            Some(schema) => Arc::clone(schema),
            None => self.inner.schema(),
        }
    }

    fn table_type(&self) -> TableType {
        self.inner.table_type()
    }

    fn statistics(&self) -> Option<Statistics> {
        // The plugin counts nd batches, not rows.
        match self.is_nd() {
            true => None,
            false => self.inner.statistics(),
        }
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        let sent: Vec<&Expr> = filters.iter().copied().filter(|f| pushable(f)).collect();
        let answers = match self.inner.supports_filters_pushdown(&sent) {
            Ok(answers) => answers,
            Err(error) => {
                // A filter that cannot cross the boundary stays with DataFusion.
                tracing::debug!(
                    "plugin table '{}' takes no filters: {error}",
                    self.definition.name
                );
                vec![TableProviderFilterPushDown::Unsupported; sent.len()]
            }
        };
        let mut answers = answers.into_iter();
        // Put the plugin answers back in the input order.
        Ok(filters
            .iter()
            .map(|filter| match pushable(filter) {
                true => answers
                    .next()
                    .unwrap_or(TableProviderFilterPushDown::Unsupported),
                false => TableProviderFilterPushDown::Unsupported,
            })
            // An nd plugin filters grids, not rows, so DataFusion filters the rows again.
            .map(|answer| match answer {
                TableProviderFilterPushDown::Exact if self.is_nd() => {
                    TableProviderFilterPushDown::Inexact
                }
                answer => answer,
            })
            .collect())
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.check_installed()?;
        // DataFusion sends only filters that `supports_filters_pushdown` accepted.
        let sent: Vec<Expr> = filters.iter().filter(|f| pushable(f)).cloned().collect();
        if self.is_nd() {
            return self.scan_nd(state, projection, &sent).await;
        }
        self.inner.scan(state, projection, &sent, limit).await
    }

    async fn insert_into(
        &self,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.check_installed()?;
        // A plugin sink can read only partition 0, and Beacon runs no optimizer that merges them.
        let input = if input.output_partitioning().partition_count() > 1 {
            Arc::new(CoalescePartitionsExec::new(input)) as Arc<dyn ExecutionPlan>
        } else {
            input
        };
        self.inner.insert_into(state, input, insert_op).await
    }
}
