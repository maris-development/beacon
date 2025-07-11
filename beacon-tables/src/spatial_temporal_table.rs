use std::{any::Any, path::PathBuf, sync::Arc};

use arrow::datatypes::SchemaRef;
use datafusion::{
    catalog::{Session, TableProvider},
    logical_expr::TableProviderFilterPushDown,
    physical_plan::ExecutionPlan,
    prelude::{Expr, SessionContext},
};

use crate::{error::TableError, table::TableType};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SpatialTemporalTable {
    #[serde(flatten)]
    pub table_engine: Arc<TableType>,
    pub longitude_column: String,
    pub latitude_column: String,
    pub time_column: String,
    pub depth_column: Option<String>,
    pub data_columns: Vec<String>,
    pub metadata_columns: Vec<String>,
}

impl SpatialTemporalTable {
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

        for column in self.data_columns.iter() {
            if let Some(field) = current_schema.field_with_name(column).ok() {
                exposed_fields.push(field.clone());
            } else {
                panic!("Data column '{}' not found in the current schema", column);
            }
        }

        for column in self.metadata_columns.iter() {
            if let Some(field) = current_schema.field_with_name(column).ok() {
                exposed_fields.push(field.clone());
            } else {
                panic!(
                    "Metadata column '{}' not found in the current schema",
                    column
                );
            }
        }

        let schema = SchemaRef::new(arrow::datatypes::Schema::new(exposed_fields));

        let preset_table_provider = PresetTableProvider::new(current_provider, schema.clone());

        Ok(Arc::new(preset_table_provider))
    }
}

#[derive(Debug)]
struct PresetTableProvider {
    inner: Arc<dyn TableProvider>,
    exposed_schema: SchemaRef,
}

impl PresetTableProvider {
    pub fn new(inner: Arc<dyn TableProvider>, exposed_schema: SchemaRef) -> Self {
        Self {
            inner,
            exposed_schema,
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
        self.inner.scan(state, projection, filters, limit).await
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::error::Result<Vec<TableProviderFilterPushDown>> {
        self.inner.supports_filters_pushdown(filters)
    }
}
