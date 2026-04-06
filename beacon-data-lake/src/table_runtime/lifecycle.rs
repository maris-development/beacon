use std::sync::Arc;

use datafusion::{
    catalog::TableProvider,
    execution::object_store::ObjectStoreUrl,
    prelude::SessionContext,
};

use crate::table::TableFormat;

use super::provider_factory::TableProviderFactory;

#[derive(Clone)]
pub struct TableLifecycleService {
    provider_factory: TableProviderFactory,
    session_context: Arc<SessionContext>,
    data_directory_store_url: ObjectStoreUrl,
}

impl TableLifecycleService {
    pub fn new(
        session_context: Arc<SessionContext>,
        data_directory_store_url: ObjectStoreUrl,
        table_directory_store_url: ObjectStoreUrl,
    ) -> Self {
        Self {
            provider_factory: TableProviderFactory::new(
                session_context.clone(),
                data_directory_store_url.clone(),
                table_directory_store_url,
            ),
            session_context,
            data_directory_store_url,
        }
    }

    pub async fn refresh_provider(&self, table: &TableFormat) -> anyhow::Result<Arc<dyn TableProvider>> {
        let (_, provider) = self.provider_factory.build(table).await?;
        Ok(provider)
    }

    pub async fn apply_operation(
        &self,
        table: &TableFormat,
        op: serde_json::Value,
    ) -> anyhow::Result<()> {
        match table {
            TableFormat::Legacy(legacy) => {
                let result = legacy
                    .table_type
                    .apply_operation(op, self.session_context.clone(), &self.data_directory_store_url)
                    .await;
                result.map_err(|e| anyhow::anyhow!("Failed to apply operation: {}", e))
            }
            TableFormat::DefinitionBased(definition) => Err(anyhow::anyhow!(
                "Table operations are not implemented yet for definition-based (SQL) tables: {}",
                definition.table_name()
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::TableLifecycleService;
    use crate::table::{Table, TableFormat, _type::TableType, empty::EmptyTable};
    use beacon_datafusion_ext::table_ext::ViewTableDefinition;
    use datafusion::{
        datasource::{TableType as ProviderTableType, ViewTable},
        execution::object_store::ObjectStoreUrl,
        prelude::SessionContext,
    };
    use serde_json::json;
    use std::sync::Arc;

    fn lifecycle_service() -> TableLifecycleService {
        TableLifecycleService::new(
            Arc::new(SessionContext::new()),
            ObjectStoreUrl::parse("datasets://").expect("datasets url should parse"),
            ObjectStoreUrl::parse("tables://").expect("tables url should parse"),
        )
    }

    #[tokio::test]
    async fn refresh_provider_builds_definition_based_provider() {
        let service = lifecycle_service();
        let table = TableFormat::DefinitionBased(Arc::new(ViewTableDefinition {
            name: "runtime_view".to_string(),
            definition: "SELECT 7 AS value".to_string(),
            dependencies: vec![],
        }));

        let provider = service
            .refresh_provider(&table)
            .await
            .expect("definition-based provider should be refreshed");

        assert_eq!(provider.table_type(), ProviderTableType::View);
        assert!(provider.as_any().downcast_ref::<ViewTable>().is_some());
    }

    #[tokio::test]
    async fn apply_operation_for_definition_based_tables_is_not_supported() {
        let service = lifecycle_service();
        let table = TableFormat::DefinitionBased(Arc::new(ViewTableDefinition {
            name: "runtime_view".to_string(),
            definition: "SELECT 7 AS value".to_string(),
            dependencies: vec![],
        }));

        let err = service
            .apply_operation(&table, json!({"op": "noop"}))
            .await
            .expect_err("definition-based operation should not be supported yet");

        assert!(err.to_string().contains("not implemented"));
        assert!(err.to_string().contains("runtime_view"));
    }

    #[tokio::test]
    async fn apply_operation_legacy_empty_table_returns_expected_error() {
        let service = lifecycle_service();
        let table = TableFormat::Legacy(Table {
            table_directory: vec![],
            table_name: "legacy_empty".to_string(),
            table_type: TableType::Empty(EmptyTable::new()),
            description: Some("lifecycle test".to_string()),
        });

        let err = service
            .apply_operation(&table, json!({"op": "noop"}))
            .await
            .expect_err("empty legacy table should reject operations");

        assert!(err.to_string().contains("No operations supported for table type"));
    }
}
