//! The persisted `CREATE EXTERNAL TABLE ... STORED AS <plugin type>` definition.

use std::collections::HashMap;
use std::sync::Arc;

use beacon_datafusion_ext::table_ext::TableDefinition;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::TableProvider;
use datafusion::common::DFSchema;
use datafusion::logical_expr::CreateExternalTable;
use datafusion::prelude::SessionContext;

use crate::install::InstalledTableTypes;
use crate::table::create_plugin_table;

/// The persisted configuration of a plugin table.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct PluginTableDefinition {
    /// The logical table name.
    pub name: String,
    /// The `STORED AS` name of the plugin table type.
    pub table_type: String,
    /// `LOCATION`, as the user gave it.
    pub location: String,
    /// The table `OPTIONS`, as DataFusion passes them.
    pub options: HashMap<String, String>,
    /// The user column list. It is empty when the plugin finds the columns.
    pub columns: SchemaRef,
    /// `PARTITIONED BY`.
    pub partition_cols: Vec<String>,
    /// Original `CREATE EXTERNAL TABLE` SQL, if available.
    pub definition: Option<String>,
}

impl PluginTableDefinition {
    /// The definition of `cmd`, for the type `table_type`.
    pub fn from_command(cmd: &CreateExternalTable, table_type: &str) -> Self {
        Self {
            name: cmd.name.to_string(),
            table_type: table_type.to_string(),
            location: cmd.location.clone(),
            options: cmd.options.clone(),
            columns: Arc::clone(cmd.schema.inner()),
            partition_cols: cmd.table_partition_cols.clone(),
            definition: cmd.definition.clone(),
        }
    }

    /// The command that creates the table again.
    pub fn to_command(&self) -> anyhow::Result<CreateExternalTable> {
        let schema = DFSchema::try_from(self.columns.as_ref().clone())?;
        Ok(CreateExternalTable::builder(
            self.name.as_str(),
            self.location.clone(),
            self.table_type.clone(),
            Arc::new(schema),
        )
        .with_partition_cols(self.partition_cols.clone())
        .with_options(self.options.clone())
        .with_definition(self.definition.clone())
        .build())
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "plugin_table")]
impl TableDefinition for PluginTableDefinition {
    async fn build_provider(
        &self,
        context: Arc<SessionContext>,
    ) -> anyhow::Result<Arc<dyn TableProvider>> {
        let state = context.state();
        let installed = state
            .config()
            .get_extension::<InstalledTableTypes>()
            .ok_or_else(|| anyhow::anyhow!("plugin table types are not set up in this session"))?;
        let cmd = self.to_command()?;
        create_plugin_table(&state, &installed, &cmd)
            .await?
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "table type '{}' is not loaded: install the plugin that has it",
                    self.table_type
                )
            })
    }

    fn table_name(&self) -> &str {
        &self.name
    }
}
