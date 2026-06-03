//! The serializable [`TableDefinition`] trait shared by all persisted table kinds.

use std::fmt::Debug;
use std::sync::Arc;

use datafusion::catalog::TableProvider;
use datafusion::datasource::TableType;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::prelude::SessionContext;

#[typetag::serde(tag = "definition_type")]
#[async_trait::async_trait]
/// A serializable table definition that can materialize a DataFusion provider.
pub trait TableDefinition: Debug + Send + Sync {
    /// Builds a concrete [`TableProvider`] from this definition.
    ///
    /// Implementations use the provided session context and store URL to resolve
    /// formats, schemas, and physical locations.
    async fn build_provider(
        &self,
        context: Arc<SessionContext>,
        data_store_url: &ObjectStoreUrl,
    ) -> anyhow::Result<Arc<dyn TableProvider>>;

    fn depends_on(&self) -> Vec<String> {
        Vec::new()
    }

    fn table_name(&self) -> &str;

    fn table_type(&self) -> TableType {
        TableType::Base
    }
}

#[cfg(test)]
mod tests {
    use crate::table_ext::{ExternalTableDefinition, TableDefinition, ViewTableDefinition};
    use datafusion::arrow::datatypes::Schema;
    use datafusion::datasource::TableType;
    use std::collections::HashMap;
    use std::sync::Arc;

    #[test]
    /// Verifies trait-level table_type defaults to Base unless overridden.
    fn table_definition_table_type_defaults_and_overrides() {
        let external = ExternalTableDefinition {
            name: "ext_table".to_string(),
            location: "tmp/path".to_string(),
            file_type: "parquet".to_string(),
            schema: Arc::new(Schema::empty()),
            definition: None,
            partition_cols: vec![],
            options: HashMap::new(),
            if_not_exists: false,
        };
        let view = ViewTableDefinition {
            name: "view_table".to_string(),
            definition: "SELECT 1".to_string(),
            dependencies: vec![],
        };

        assert_eq!(external.table_type(), TableType::Base);
        assert_eq!(view.table_type(), TableType::View);
    }
}
