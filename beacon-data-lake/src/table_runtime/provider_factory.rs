use std::sync::Arc;

use beacon_datafusion_ext::table_ext::TableDefinition;
use datafusion::{
    catalog::TableProvider, execution::object_store::ObjectStoreUrl, prelude::SessionContext,
};

#[derive(Clone)]
pub struct TableProviderFactory {
    session_context: Arc<SessionContext>,
    data_directory_store_url: ObjectStoreUrl,
}

impl TableProviderFactory {
    pub fn new(
        session_context: Arc<SessionContext>,
        data_directory_store_url: ObjectStoreUrl,
    ) -> Self {
        Self {
            session_context,
            data_directory_store_url,
        }
    }

    pub async fn build(
        &self,
        definition: &Arc<dyn TableDefinition>,
    ) -> anyhow::Result<(String, Arc<dyn TableProvider>)> {
        let provider = definition
            .build_provider(self.session_context.clone(), &self.data_directory_store_url)
            .await?;
        Ok((definition.table_name().to_string(), provider))
    }
}

#[cfg(test)]
mod tests {
    use super::TableProviderFactory;
    use beacon_datafusion_ext::table_ext::{TableDefinition, ViewTableDefinition};
    use datafusion::{
        datasource::{TableType as ProviderTableType, ViewTable},
        execution::object_store::ObjectStoreUrl,
        prelude::SessionContext,
    };
    use std::sync::Arc;

    #[tokio::test]
    async fn build_definition_table_returns_view_provider() {
        let factory = TableProviderFactory::new(
            Arc::new(SessionContext::new()),
            ObjectStoreUrl::parse("datasets://").expect("datasets url should parse"),
        );

        let definition = ViewTableDefinition {
            name: "view_definition".to_string(),
            definition: "SELECT 1 AS col".to_string(),
            dependencies: vec![],
        };
        let table: Arc<dyn TableDefinition> = Arc::new(definition.clone());

        let (name, provider) = factory
            .build(&table)
            .await
            .expect("definition provider should build");

        assert_eq!(name, definition.table_name());
        assert_eq!(provider.table_type(), ProviderTableType::View);
        assert!(provider.as_any().downcast_ref::<ViewTable>().is_some());
    }
}
