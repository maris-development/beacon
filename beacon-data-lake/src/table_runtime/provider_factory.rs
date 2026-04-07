use std::sync::Arc;

use datafusion::{
    catalog::TableProvider, execution::object_store::ObjectStoreUrl, prelude::SessionContext,
};

use crate::table::TableFormat;

#[derive(Clone)]
pub struct TableProviderFactory {
    session_context: Arc<SessionContext>,
    data_directory_store_url: ObjectStoreUrl,
    table_directory_store_url: ObjectStoreUrl,
}

impl TableProviderFactory {
    pub fn new(
        session_context: Arc<SessionContext>,
        data_directory_store_url: ObjectStoreUrl,
        table_directory_store_url: ObjectStoreUrl,
    ) -> Self {
        Self {
            session_context,
            data_directory_store_url,
            table_directory_store_url,
        }
    }

    pub async fn build(
        &self,
        table: &TableFormat,
    ) -> anyhow::Result<(String, Arc<dyn TableProvider>)> {
        match table {
            TableFormat::Legacy(legacy) => {
                let provider = legacy
                    .table_provider(
                        self.session_context.clone(),
                        self.data_directory_store_url.clone(),
                        self.table_directory_store_url.clone(),
                    )
                    .await?;
                Ok((legacy.table_name.clone(), provider))
            }
            TableFormat::DefinitionBased(definition) => {
                let provider = definition
                    .build_provider(self.session_context.clone(), &self.data_directory_store_url)
                    .await?;
                Ok((definition.table_name().to_string(), provider))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::TableProviderFactory;
    use crate::table::{_type::TableType, Table, TableFormat, empty::EmptyTable};
    use beacon_datafusion_ext::table_ext::{TableDefinition, ViewTableDefinition};
    use datafusion::{
        datasource::{TableType as ProviderTableType, ViewTable},
        execution::object_store::ObjectStoreUrl,
        prelude::SessionContext,
    };
    use std::sync::Arc;

    #[tokio::test]
    async fn build_legacy_table_returns_table_name_and_provider() {
        let factory = TableProviderFactory::new(
            Arc::new(SessionContext::new()),
            ObjectStoreUrl::parse("datasets://").expect("datasets url should parse"),
            ObjectStoreUrl::parse("tables://").expect("tables url should parse"),
        );

        let table = TableFormat::Legacy(Table {
            table_directory: vec![],
            table_name: "legacy_empty".to_string(),
            table_type: TableType::Empty(EmptyTable::new()),
            description: Some("provider factory legacy".to_string()),
        });

        let (name, provider) = factory
            .build(&table)
            .await
            .expect("legacy provider should build");

        assert_eq!(name, "legacy_empty");
        assert_eq!(provider.table_type(), ProviderTableType::Base);
    }

    #[tokio::test]
    async fn build_definition_table_returns_view_provider() {
        let factory = TableProviderFactory::new(
            Arc::new(SessionContext::new()),
            ObjectStoreUrl::parse("datasets://").expect("datasets url should parse"),
            ObjectStoreUrl::parse("tables://").expect("tables url should parse"),
        );

        let definition = ViewTableDefinition {
            name: "view_definition".to_string(),
            definition: "SELECT 1 AS col".to_string(),
            dependencies: vec![],
        };
        let table = TableFormat::DefinitionBased(Arc::new(definition.clone()));

        let (name, provider) = factory
            .build(&table)
            .await
            .expect("definition provider should build");

        assert_eq!(name, definition.table_name());
        assert_eq!(provider.table_type(), ProviderTableType::View);
        assert!(provider.as_any().downcast_ref::<ViewTable>().is_some());
    }
}
