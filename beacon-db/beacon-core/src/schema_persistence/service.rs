use std::sync::Arc;
use object_store::ObjectStoreExt;

use beacon_datafusion_ext::table_ext::{
    ExternalTable, MaterializedView, TableDefinition, ViewTableDefinition,
};
use datafusion::{
    catalog::TableProvider, datasource::ViewTable, error::DataFusionError,
    execution::object_store::ObjectStoreUrl, prelude::SessionContext,
};
use futures::StreamExt;

/// The file name of the comments sidecar in a table directory.
const COMMENTS_FILE: &str = "comments.json";

fn comments_path(table_name: &str) -> object_store::path::Path {
    object_store::path::Path::from(format!("{table_name}/{COMMENTS_FILE}"))
}

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

    /// Persist a table's comments sidecar to `db://<name>/comments.json`.
    ///
    /// Comments are stored separately from `table.json` so they apply to every
    /// table type and can change without a rebuild of the provider.
    pub async fn persist_table_comments_json(
        &self,
        table_name: &str,
        comments_json: String,
    ) -> datafusion::error::Result<()> {
        let table_object_store = self.table_object_store(table_name)?;
        table_object_store
            .put(&comments_path(table_name), comments_json.into_bytes().into())
            .await
            .map_err(|error| {
                DataFusionError::Plan(format!(
                    "Failed to store comments for table {}: {}",
                    table_name, error
                ))
            })?;
        Ok(())
    }

    /// Load a table's comments sidecar, or `None` if it has none.
    pub async fn load_table_comments_json(
        &self,
        table_name: &str,
    ) -> datafusion::error::Result<Option<String>> {
        let table_object_store = self.table_object_store(table_name)?;
        match table_object_store.get(&comments_path(table_name)).await {
            Ok(result) => {
                let bytes = result.bytes().await.map_err(|error| {
                    DataFusionError::Plan(format!(
                        "Failed to read comments for table {}: {}",
                        table_name, error
                    ))
                })?;
                let text = String::from_utf8(bytes.to_vec()).map_err(|error| {
                    DataFusionError::Plan(format!(
                        "Comments for table {} are not valid UTF-8: {}",
                        table_name, error
                    ))
                })?;
                Ok(Some(text))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(error) => Err(DataFusionError::Plan(format!(
                "Failed to load comments for table {}: {}",
                table_name, error
            ))),
        }
    }

    /// Remove a table's comments sidecar. A missing sidecar is not an error.
    pub async fn remove_table_comments_json(
        &self,
        table_name: &str,
    ) -> datafusion::error::Result<()> {
        let table_object_store = self.table_object_store(table_name)?;
        match table_object_store.delete(&comments_path(table_name)).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(error) => Err(DataFusionError::Plan(format!(
                "Failed to remove comments for table {}: {}",
                table_name, error
            ))),
        }
    }

    /// Every comments sidecar in the store, as `(table name, JSON)` pairs.
    pub async fn list_table_comments_json(
        &self,
    ) -> datafusion::error::Result<Vec<(String, String)>> {
        let store = self.table_object_store(COMMENTS_FILE)?;
        // Table folders are top-level, so one level avoids a walk of all data files.
        let folders = store.list_with_delimiter(None).await.map_err(|error| {
            DataFusionError::Plan(format!("Failed to list table comments: {error}"))
        })?;
        let mut sidecars = Vec::new();
        for folder in folders.common_prefixes {
            let table_name = folder.as_ref().to_string();
            if let Some(json) = self.load_table_comments_json(&table_name).await? {
                sidecars.push((table_name, json));
            }
        }
        Ok(sidecars)
    }

    /// Resolve the object store backing `db://` table definitions.
    fn table_object_store(
        &self,
        table_name: &str,
    ) -> datafusion::error::Result<Arc<dyn object_store::ObjectStore>> {
        self.session_context
            .runtime_env()
            .object_store(&self.table_directory_store_url)
            .map_err(|error| {
                DataFusionError::Plan(format!(
                    "Failed to get table object store for table {}: {}",
                    table_name, error
                ))
            })
    }

    pub async fn remove_persisted_table(&self, table_name: &str) -> datafusion::error::Result<()> {
        let path = object_store::path::Path::from(table_name);
        let table_object_store = self.table_object_store(table_name)?;

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
        let definition = definition_from_provider(table_name, table)?;

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
        let table_object_store = self.table_object_store(table_name)?;

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

/// Reconstruct a serializable [`TableDefinition`] from a live table provider by
/// downcasting it to one of beacon's managed provider types.
///
/// This is the inverse of building a provider from a definition: it lets the
/// catalog recover a table's persisted spec (used both to persist the table and
/// to surface its configuration) without keeping a parallel registry of
/// definitions alongside the providers.
pub fn definition_from_provider(
    table_name: &str,
    table: &dyn TableProvider,
) -> datafusion::error::Result<Arc<dyn TableDefinition>> {
    if let Some(table) = table.as_any().downcast_ref::<beacon_lance::LanceTable>() {
        Ok(Arc::new(table.definition().clone()))
    } else if let Some(table) = table.as_any().downcast_ref::<ExternalTable>() {
        Ok(Arc::new(table.definition().clone()))
    } else if let Some(table) = table.as_any().downcast_ref::<MaterializedView>() {
        Ok(Arc::new(table.definition().clone()))
    } else if let Some(definition) = beacon_datafusion_ext::remote::remote_table_definition(table) {
        Ok(Arc::new(definition))
    } else if let Some(table) = table.as_any().downcast_ref::<beacon_delta::BeaconDeltaTable>() {
        Ok(Arc::new(table.definition().clone()))
    } else if let Some(definition) = beacon_iceberg::iceberg_table_definition(table) {
        Ok(Arc::new(definition))
    } else if let Some(table) = table.as_any().downcast_ref::<beacon_icechunk::IcechunkTable>() {
        Ok(Arc::new(table.definition().clone()))
    } else if let Some(definition) = beacon_sql_databases::sql_database_table_definition(table) {
        Ok(Arc::new(definition))
    } else if let Some(table) = table.as_any().downcast_ref::<ViewTable>() {
        let definition =
            ViewTableDefinition::try_from_view(table_name, table).map_err(|error| {
                DataFusionError::Plan(format!(
                    "Failed to create ViewTableDefinition for table {}: {}",
                    table_name, error
                ))
            })?;
        Ok(Arc::new(definition))
    } else {
        Err(DataFusionError::Plan(format!(
            "Unsupported table provider type for table {}",
            table_name
        )))
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
    use object_store::{ObjectStore, ObjectStoreExt, memory::InMemory, path::Path};
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
        let tables_url = ObjectStoreUrl::parse("db://").expect("db url should parse");

        session_context.register_object_store(
            &Url::parse(tables_url.as_str()).expect("db url should be valid"),
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

    #[tokio::test]
    async fn comments_sidecar_round_trip_and_cleanup() {
        let (service, _ctx, table_store, _url) = test_service();

        // Missing sidecar reads as None.
        assert!(service
            .load_table_comments_json("obs")
            .await
            .expect("load should succeed")
            .is_none());

        // Persist then load returns the stored JSON.
        let payload = r#"{"version":1,"table":"t"}"#.to_string();
        service
            .persist_table_comments_json("obs", payload.clone())
            .await
            .expect("persist should succeed");
        assert_eq!(
            service
                .load_table_comments_json("obs")
                .await
                .expect("load should succeed")
                .as_deref(),
            Some(payload.as_str())
        );
        assert!(table_store
            .get(&Path::from("obs/comments.json"))
            .await
            .is_ok());
        assert_eq!(
            service
                .list_table_comments_json()
                .await
                .expect("list should succeed"),
            vec![("obs".to_string(), payload.clone())]
        );

        // Explicit removal clears it (and is a no-op when already absent).
        service
            .remove_table_comments_json("obs")
            .await
            .expect("remove should succeed");
        assert!(service
            .load_table_comments_json("obs")
            .await
            .expect("load should succeed")
            .is_none());
        service
            .remove_table_comments_json("obs")
            .await
            .expect("removing an absent sidecar is not an error");

        // Dropping the whole table directory removes the sidecar too.
        service
            .persist_table_comments_json("obs", payload)
            .await
            .expect("persist should succeed");
        service
            .remove_persisted_table("obs")
            .await
            .expect("table removal should succeed");
        assert!(service
            .load_table_comments_json("obs")
            .await
            .expect("load should succeed")
            .is_none());
    }
}
