use std::sync::Arc;

use beacon_atlas::datafusion::table::AtlasTable;
use beacon_datafusion_ext::table_ext::{ExternalTable, TableDefinition, ViewTableDefinition};
use datafusion::{
    catalog::TableProvider, datasource::ViewTable, error::DataFusionError,
    execution::object_store::ObjectStoreUrl, prelude::SessionContext,
};
use futures::StreamExt;

#[derive(Clone)]
pub struct SchemaPersistenceService {
    session_context: Arc<SessionContext>,
    table_directory_store_url: ObjectStoreUrl,
}

impl SchemaPersistenceService {
    pub fn new(
        session_context: Arc<SessionContext>,
        table_directory_store_url: ObjectStoreUrl,
    ) -> Self {
        Self {
            session_context,
            table_directory_store_url,
        }
    }

    pub async fn persist_provider_definition(
        &self,
        table_name: &str,
        table: &dyn TableProvider,
    ) -> datafusion::error::Result<()> {
        let table_json = Self::serialize_table_provider_definition(table_name, table)?;
        self.persist_table_definition_json(table_name, table_json)
            .await
    }

    pub async fn remove_persisted_table(&self, table_name: &str) -> datafusion::error::Result<()> {
        let path = object_store::path::Path::from(table_name);
        let table_object_store = self
            .session_context
            .runtime_env()
            .object_store(&self.table_directory_store_url)
            .map_err(|error| {
                DataFusionError::Plan(format!(
                    "Failed to get table object store for deregistering table {}: {}",
                    table_name, error
                ))
            })?;

        let mut removable_files = table_object_store.list(Some(&path));
        while let Some(file) = removable_files.next().await {
            match file {
                Ok(file) => {
                    if let Err(error) = table_object_store.delete(&file.location).await {
                        tracing::error!(
                            "Failed to delete file {:?} for table {}: {}",
                            file.location,
                            table_name,
                            error
                        );
                    }
                }
                Err(error) => {
                    tracing::error!(
                        "Failed to list files for table {} during deregistration: {}",
                        table_name,
                        error
                    );
                }
            }
        }

        Ok(())
    }

    fn serialize_table_provider_definition(
        table_name: &str,
        table: &dyn TableProvider,
    ) -> datafusion::error::Result<String> {
        let definition: Arc<dyn TableDefinition> =
            if let Some(table) = table.as_any().downcast_ref::<AtlasTable>() {
                let definition = table.definition();
                Arc::new(definition)
            } else if let Some(table) = table.as_any().downcast_ref::<ExternalTable>() {
                let definition = table.definition();
                Arc::new(definition.clone())
            } else if let Some(table) = table.as_any().downcast_ref::<ViewTable>() {
                let definition = ViewTableDefinition::try_from_view(table).map_err(|error| {
                    DataFusionError::Plan(format!(
                        "Failed to create ViewTableDefinition for table {}: {}",
                        table_name, error
                    ))
                })?;
                Arc::new(definition)
            } else {
                return Err(DataFusionError::Plan(format!(
                    "Unsupported table provider type for table {}",
                    table_name
                )));
            };

        let json = serde_json::to_string_pretty(&definition).map_err(|error| {
            DataFusionError::Plan(format!(
                "Failed to serialize table definition for table {}: {}",
                table_name, error
            ))
        })?;

        Ok(json)
    }

    async fn persist_table_definition_json(
        &self,
        table_name: &str,
        table_json: String,
    ) -> datafusion::error::Result<()> {
        let path = object_store::path::Path::from(format!("{}/table.json", table_name));
        let table_object_store = self
            .session_context
            .runtime_env()
            .object_store(&self.table_directory_store_url)
            .map_err(|error| {
                DataFusionError::Plan(format!(
                    "Failed to get table object store for registering table {}: {}",
                    table_name, error
                ))
            })?;

        table_object_store
            .put(&path, table_json.into_bytes().into())
            .await
            .map_err(|error| {
                DataFusionError::Plan(format!(
                    "Failed to store table definition for table {}: {}",
                    table_name, error
                ))
            })?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::SchemaPersistenceService;
    use beacon_datafusion_ext::table_ext::ViewTableDefinition;
    use datafusion::{
        datasource::ViewTable, execution::object_store::ObjectStoreUrl, prelude::SessionContext,
    };
    use futures::StreamExt;
    use object_store::{ObjectStore, memory::InMemory, path::Path};
    use std::sync::Arc;
    use url::Url;

    fn test_service() -> (
        SchemaPersistenceService,
        Arc<SessionContext>,
        Arc<InMemory>,
        ObjectStoreUrl,
    ) {
        let session_context = Arc::new(SessionContext::new());
        let table_store = Arc::new(InMemory::new());
        let tables_url = ObjectStoreUrl::parse("tables://").expect("tables url should parse");

        session_context.register_object_store(
            &Url::parse(tables_url.as_str()).expect("tables url should be valid"),
            table_store.clone(),
        );

        (
            SchemaPersistenceService::new(session_context.clone(), tables_url.clone()),
            session_context,
            table_store,
            tables_url,
        )
    }

    #[tokio::test]
    async fn persist_and_remove_view_definition_json() {
        let (service, session_context, table_store, tables_url) = test_service();
        let sql = "SELECT 1 AS x";
        let plan = session_context
            .state()
            .create_logical_plan(sql)
            .await
            .expect("logical plan should be created");
        let view = ViewTable::new(plan, Some(sql.to_string()));

        service
            .persist_provider_definition("saved_view", &view)
            .await
            .expect("view definition should be persisted");

        let table_json_path = Path::from("saved_view/table.json");
        let payload = table_store
            .get(&table_json_path)
            .await
            .expect("table.json should be present")
            .bytes()
            .await
            .expect("table.json bytes should be readable");

        let persisted: ViewTableDefinition =
            serde_json::from_slice(&payload).expect("persisted JSON should deserialize");
        assert_eq!(persisted.definition, sql);

        service
            .remove_persisted_table("saved_view")
            .await
            .expect("table directory should be removed");

        let object_store = session_context
            .runtime_env()
            .object_store(&tables_url)
            .expect("tables object store should be available");
        let mut remaining = object_store.list(Some(&Path::from("saved_view")));
        assert!(remaining.next().await.is_none());
    }

    #[tokio::test]
    async fn persist_view_definition_requires_sql_text() {
        let (service, session_context, _, _) = test_service();
        let plan = session_context
            .state()
            .create_logical_plan("SELECT 1 AS x")
            .await
            .expect("logical plan should be created");
        let view = ViewTable::new(plan, None);

        let err = service
            .persist_provider_definition("view_without_sql", &view)
            .await
            .expect_err("view without SQL definition should fail");

        assert!(err.to_string().contains("requires a SQL definition"));
    }
}
