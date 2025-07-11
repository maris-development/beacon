use std::{any::Any, collections::HashMap, path::PathBuf, sync::Arc};

use arrow::datatypes::SchemaRef;
use datafusion::{
    catalog::{Session, TableProvider},
    common::DFSchema,
    execution::context::ExecutionProps,
    logical_expr::TableProviderFilterPushDown,
    physical_expr::create_physical_expr,
    physical_plan::{projection::ProjectionExec, ExecutionPlan},
    prelude::{col, Expr, SessionContext},
};

use crate::{error::TableError, table::TableType, util::remap_filter};

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
#[serde(untagged)]
pub enum PresetColumnMapping {
    ColumnName(String),
    ColumnAlias { column_name: String, alias: String },
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PresetTable {
    #[serde(flatten)]
    pub table_engine: Arc<TableType>,
    pub preset_filter_columns: Vec<String>,
    pub data_columns: Vec<PresetColumnMapping>,
    pub metadata_columns: Vec<PresetColumnMapping>,
}

impl PresetTable {
    pub async fn create(
        &self,
        table_directory: PathBuf,
        session_ctx: Arc<SessionContext>,
    ) -> Result<(), TableError> {
        match self.table_engine.as_ref() {
            TableType::Logical(logical_table) => {
                Box::pin(logical_table.create(table_directory, session_ctx)).await?
            }
            TableType::Physical(physical_table) => {
                Box::pin(physical_table.create(table_directory, session_ctx)).await?
            }
            TableType::PresetTable(preset_table) => {
                Box::pin(preset_table.create(table_directory, session_ctx)).await?
            }
        }
        Ok(())
    }

    pub async fn table_provider(
        &self,
        table_directory: PathBuf,
        session_ctx: Arc<SessionContext>,
    ) -> Result<Arc<dyn TableProvider>, TableError> {
        let current_provider = match self.table_engine.as_ref() {
            TableType::Logical(logical_table) => {
                Box::pin(logical_table.table_provider(session_ctx)).await?
            }
            TableType::Physical(physical_table) => {
                Box::pin(physical_table.table_provider(table_directory, session_ctx)).await?
            }
            TableType::PresetTable(preset_table) => {
                Box::pin(preset_table.table_provider(table_directory, session_ctx)).await?
            }
        };

        let current_schema = current_provider.schema();

        let mut exposed_fields = Vec::new();
        let mut renames = HashMap::new();

        for column in self.data_columns.iter() {
            match column {
                PresetColumnMapping::ColumnName(column_name) => {
                    if let Some(field) = current_schema.field_with_name(column_name).ok() {
                        exposed_fields.push(field.clone());
                    }
                }
                PresetColumnMapping::ColumnAlias { column_name, alias } => {
                    if let Some(field) = current_schema.field_with_name(column_name).ok() {
                        exposed_fields.push(field.clone().with_name(alias));
                        renames.insert(column_name.clone(), alias.clone());
                    }
                }
            }
        }

        for column in self.metadata_columns.iter() {
            match column {
                PresetColumnMapping::ColumnName(column_name) => {
                    if let Some(field) = current_schema.field_with_name(column_name).ok() {
                        exposed_fields.push(field.clone());
                    }
                }
                PresetColumnMapping::ColumnAlias { column_name, alias } => {
                    if let Some(field) = current_schema.field_with_name(column_name).ok() {
                        exposed_fields.push(field.clone().with_name(alias));
                        renames.insert(column_name.clone(), alias.clone());
                    }
                }
            }
        }

        let schema = SchemaRef::new(arrow::datatypes::Schema::new(exposed_fields));

        let preset_table_provider =
            PresetTableProvider::new(current_provider, schema.clone(), renames);

        Ok(Arc::new(preset_table_provider))
    }
}

#[derive(Debug)]
struct PresetTableProvider {
    inner: Arc<dyn TableProvider>,
    exposed_schema: SchemaRef,
    renames: HashMap<String, String>,
}

impl PresetTableProvider {
    pub fn new(
        inner: Arc<dyn TableProvider>,
        exposed_schema: SchemaRef,
        renames: HashMap<String, String>,
    ) -> Self {
        Self {
            inner,
            exposed_schema,
            renames,
        }
    }
}

#[async_trait::async_trait]
impl TableProvider for PresetTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Get a reference to the schema for this table
    fn schema(&self) -> SchemaRef {
        self.exposed_schema.clone()
    }

    fn table_type(&self) -> datafusion::datasource::TableType {
        datafusion::datasource::TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let alias_exprs = filters
            .iter()
            .map(|e| remap_filter(e.clone(), &self.renames))
            .collect::<Result<Vec<_>, _>>()?;

        let scan = self
            .inner
            .scan(state, projection, &alias_exprs, limit)
            .await?;

        let df_schema = DFSchema::try_from(scan.schema().as_ref().clone())?;
        let props = ExecutionProps::new();

        let mut proj_exprs = Vec::with_capacity(self.renames.len());
        for (alias, real_name) in &self.renames {
            // make a logical Expr::Column against the real name
            let log_expr: Expr = col(real_name);
            // plan it into a PhysicalExpr
            let phys_expr = create_physical_expr(&log_expr, &df_schema, &props)?;
            // now alias it in the ProjectionExec
            proj_exprs.push((phys_expr, alias.clone()));
        }

        let with_aliases = ProjectionExec::try_new(proj_exprs, scan)?;
        Ok(Arc::new(with_aliases))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::error::Result<Vec<TableProviderFilterPushDown>> {
        self.inner.supports_filters_pushdown(filters)
    }
}
