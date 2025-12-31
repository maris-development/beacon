use std::sync::Arc;

use arrow::datatypes::Schema;
use datafusion::{
    catalog::{Session, TableProvider},
    execution::object_store::ObjectStoreUrl,
    logical_expr::TableProviderFilterPushDown,
    physical_plan::ExecutionPlan,
    prelude::{Expr, SessionContext},
};

use crate::table::{_type::TableType, error::TableError};

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct GeoSpatialTable {
    #[serde(flatten)]
    table_engine: Arc<TableType>,
    #[serde(alias = "longitude_column")]
    x_column: String,
    #[serde(alias = "latitude_column")]
    y_column: String,
    crs: Option<String>,
}

impl GeoSpatialTable {
    pub async fn create(
        &self,
        table_directory: object_store::path::Path,
        session_ctx: Arc<SessionContext>,
    ) -> Result<(), TableError> {
        match self.table_engine.as_ref() {
            TableType::Logical(_) => {}
            TableType::Preset(preset_table) => {
                Box::pin(preset_table.create(table_directory, session_ctx)).await?
            }
            TableType::GeoSpatial(geo_spatial_table) => {
                Box::pin(geo_spatial_table.create(table_directory, session_ctx)).await?
            }
            TableType::Empty(default_table) => {
                Box::pin(default_table.create(table_directory, session_ctx)).await?
            }
        }
        Ok(())
    }

    pub(crate) async fn table_provider(
        &self,
        table_directory_store_url: ObjectStoreUrl,
        data_directory_store_url: ObjectStoreUrl,
        session_ctx: Arc<SessionContext>,
    ) -> Result<Arc<dyn TableProvider>, TableError> {
        self.table_engine
            .table_provider(
                session_ctx,
                table_directory_store_url,
                data_directory_store_url,
            )
            .await
    }
}

#[derive(Debug)]
pub struct GeoSpatialTableProvider {
    inner: Arc<dyn TableProvider>,
}

#[async_trait::async_trait]
impl TableProvider for GeoSpatialTableProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> Arc<Schema> {
        self.inner.schema()
    }

    fn table_type(&self) -> datafusion::datasource::TableType {
        self.inner.table_type()
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
