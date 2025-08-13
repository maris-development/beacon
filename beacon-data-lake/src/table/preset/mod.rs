use std::{any::Any, collections::HashMap, path::PathBuf, sync::Arc};

use arrow::datatypes::SchemaRef;
use chrono::NaiveDateTime;
use datafusion::{
    catalog::{Session, TableProvider},
    common::{Column, DFSchema},
    execution::context::ExecutionProps,
    logical_expr::TableProviderFilterPushDown,
    physical_expr::create_physical_expr,
    physical_plan::{ExecutionPlan, projection::ProjectionExec},
    prelude::{Expr, SessionContext},
};
use indexmap::IndexMap;

use crate::{error::TableError, table::TableType, util::remap_filter};

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct PresetColumnMapping {
    column_name: String,
    alias: Option<String>,
    description: Option<String>,
    filter: Option<PresetFilterColumn>,
    #[serde(flatten)]
    #[serde(default)]
    _metadata_fields: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
#[serde(untagged)]
pub enum PresetFilterColumn {
    Exact {
        options: Vec<Exact>,
    },
    Range {
        #[serde(flatten)]
        range: Range,
    },
    Any {},
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(untagged)]
pub enum Range {
    Number {
        min: f64,
        max: f64,
    },
    Text {
        min: String,
        max: String,
    },
    Timestamp {
        min: NaiveDateTime,
        max: NaiveDateTime,
    },
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
#[serde(untagged)]
pub enum Exact {
    String(String),
    Number(f64),
    Timestamp(NaiveDateTime),
    Boolean(bool),
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PresetTable {
    #[serde(flatten)]
    pub table_engine: Arc<TableType>,
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
        let mut renames = IndexMap::new();

        for column in self.data_columns.iter() {
            if let Some(field) = current_schema.field_with_name(&column.column_name).ok() {
                if let Some(alias) = column.alias.as_ref() {
                    exposed_fields.push(field.clone().with_name(alias));
                    renames.insert(column.column_name.clone(), alias.clone());
                } else {
                    exposed_fields.push(field.clone());
                }
            } else {
                return Err(TableError::TableError(format!(
                    "Data column '{}' not found in the current schema",
                    column.column_name
                )));
            }
        }

        for column in self.metadata_columns.iter() {
            if let Some(field) = current_schema.field_with_name(&column.column_name).ok() {
                if let Some(alias) = column.alias.as_ref() {
                    exposed_fields.push(field.clone().with_name(alias));
                    renames.insert(column.column_name.clone(), alias.clone());
                } else {
                    exposed_fields.push(field.clone());
                }
            } else {
                return Err(TableError::TableError(format!(
                    "Metadata column '{}' not found in the current schema",
                    column.column_name
                )));
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
    renames: IndexMap<String, String>,
}

impl PresetTableProvider {
    pub fn new(
        inner: Arc<dyn TableProvider>,
        exposed_schema: SchemaRef,
        renames: IndexMap<String, String>,
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
        let inverted_renames: HashMap<_, _> = self
            .renames
            .iter()
            .map(|(k, v)| (v.clone(), k.clone()))
            .collect();

        let alias_exprs = filters
            .iter()
            .map(|e| remap_filter(e.clone(), &inverted_renames))
            .collect::<Result<Vec<_>, _>>()?;

        let projection = projection
            .cloned()
            .unwrap_or_else(|| (0..self.exposed_schema.fields().len()).collect::<Vec<_>>());

        let mut source_projection = Vec::with_capacity(projection.len());

        // Translate the projection indices to the actual column names
        let source_schema = self.inner.schema();

        for column_index in projection {
            let exposed_column_name = self.exposed_schema.field(column_index).name();
            let source_name = inverted_renames
                .get(exposed_column_name)
                .unwrap_or(&exposed_column_name);
            if let Ok(source_column_index) = source_schema.index_of(&source_name) {
                source_projection.push(source_column_index);
            } else {
                return Err(datafusion::error::DataFusionError::Configuration(format!(
                    "Column '{}' not found in the source schema",
                    exposed_column_name
                )));
            }
        }

        let scan = self
            .inner
            .scan(state, Some(source_projection.as_ref()), &alias_exprs, limit)
            .await?;

        let scan_schema = scan.schema();
        let df_schema = DFSchema::try_from(scan_schema.clone()).unwrap();

        let props = state.execution_props();

        let mut proj_exprs = Vec::with_capacity(self.renames.len());

        for field in df_schema.fields() {
            // Check if the field is in the renames map
            if let Some(alias) = self.renames.get(field.name()) {
                // Make a logical Expr::Column against the real name
                let log_expr: Expr = Expr::Column(Column::new_unqualified(field.name().clone()));
                // Plan it into a PhysicalExpr
                let phys_expr = create_physical_expr(&log_expr, &df_schema, &props).unwrap();
                // Now alias it in the ProjectionExec
                proj_exprs.push((phys_expr, alias.clone()));

                tracing::debug!(
                    "Adding projection for column '{}' with alias '{}'",
                    field.name(),
                    alias
                );
            }
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
