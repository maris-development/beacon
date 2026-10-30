//! The table of a plugin table type.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{Statistics, plan_err};
use datafusion::datasource::TableType;
use datafusion::error::Result;
use datafusion::execution::SessionState;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::{CreateExternalTable, Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;

use crate::definition::PluginTableDefinition;
use crate::filters::pushable;
use crate::install::InstalledTableTypes;

/// A table that a plugin factory made. It keeps its definition, so Beacon can persist it.
#[derive(Debug)]
pub struct PluginTable {
    definition: PluginTableDefinition,
    plugin: String,
    installed: Arc<AtomicBool>,
    inner: Arc<dyn TableProvider>,
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
    Ok(Some(Arc::new(PluginTable {
        definition: PluginTableDefinition::from_command(cmd, table_type.name()),
        plugin,
        installed: table_type.installed_flag(),
        inner,
    })))
}

#[async_trait]
impl TableProvider for PluginTable {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn table_type(&self) -> TableType {
        self.inner.table_type()
    }

    fn statistics(&self) -> Option<Statistics> {
        self.inner.statistics()
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        let sent: Vec<&Expr> = filters.iter().copied().filter(|f| pushable(f)).collect();
        let mut answers = self.inner.supports_filters_pushdown(&sent)?.into_iter();
        // Put the plugin answers back in the input order.
        Ok(filters
            .iter()
            .map(|filter| match pushable(filter) {
                true => answers
                    .next()
                    .unwrap_or(TableProviderFilterPushDown::Unsupported),
                false => TableProviderFilterPushDown::Unsupported,
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
        self.inner.scan(state, projection, &sent, limit).await
    }

    async fn insert_into(
        &self,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.check_installed()?;
        self.inner.insert_into(state, input, insert_op).await
    }
}
