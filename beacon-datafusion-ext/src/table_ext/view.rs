//! SQL-defined [`ViewTableDefinition`] and its provider construction.

use std::sync::Arc;

use datafusion::datasource::{TableType, ViewTable};
use datafusion::logical_expr::{DdlStatement, LogicalPlan};
use datafusion::prelude::{SQLOptions, SessionContext};

use super::definition::TableDefinition;

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
/// Persisted configuration for a SQL-defined [`ViewTable`].
pub struct ViewTableDefinition {
    /// Logical view name.
    pub name: String,
    /// SQL query used to create the view.
    pub definition: String,
    /// Dependencies on other tables, used to determine rebuild order.
    #[serde(default)]
    pub dependencies: Vec<String>,
}

impl ViewTableDefinition {
    /// Builds a serializable view definition from an existing [`ViewTable`].
    ///
    /// Returns an error when the input view has no persisted SQL definition.
    pub fn try_from_view(view_name: &str, table: &ViewTable) -> anyhow::Result<Self> {
        let plan = table.logical_plan();
        let mut dependencies = Vec::new();
        Self::traverse_logical_plan_for_dependencies(plan, &mut dependencies);

        match table.definition() {
            Some(def) => Ok(Self {
                name: view_name.to_string(),
                definition: def.clone(),
                dependencies,
            }),
            None => Err(anyhow::anyhow!(
                "ViewTableDefinition requires a SQL definition to be created from a ViewTable without a definition"
            )),
        }
    }

    fn traverse_logical_plan_for_dependencies(plan: &LogicalPlan, dependencies: &mut Vec<String>) {
        let _ = plan.apply_with_subqueries(|node| {
            if let LogicalPlan::TableScan(table_scan) = node {
                let table_name = table_scan.table_name.to_string();
                if !dependencies.contains(&table_name) {
                    dependencies.push(table_name);
                }
            }

            Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
        });

        dependencies.sort();
    }

    pub async fn into_view_table(self, context: Arc<SessionContext>) -> anyhow::Result<ViewTable> {
        let options = SQLOptions::new().with_allow_ddl(true);
        let df = context.sql_with_options(&self.definition, options).await?;
        let cloned_plan = df.logical_plan();
        if let LogicalPlan::Ddl(DdlStatement::CreateView(plan)) = cloned_plan {
            Ok(ViewTable::new(
                plan.input.as_ref().clone(),
                Some(self.definition),
            ))
        } else {
            // This should never happen because the definition must have come from a ViewTable, but we defensively handle it just in case.
            anyhow::bail!(
                "Expected logical plan for view '{}' to be a CreateView DDL, but got: {:?}",
                self.name,
                cloned_plan
            );
        }
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "view_table")]
impl TableDefinition for ViewTableDefinition {
    /// Compiles the stored SQL and returns a DataFusion [`ViewTable`] provider.
    async fn build_provider(
        &self,
        context: Arc<SessionContext>,
        _data_store_url: &datafusion::execution::object_store::ObjectStoreUrl,
    ) -> anyhow::Result<Arc<dyn datafusion::catalog::TableProvider>> {
        // Compile the SQL definition into a DataFusion logical plan and use it to create a ViewTable provider.
        let state = context.state();
        let plan = state.create_logical_plan(&self.definition).await?;

        if let LogicalPlan::Ddl(DdlStatement::CreateView(plan)) = plan {
            Ok(Arc::new(ViewTable::new(
                plan.input.as_ref().clone(),
                Some(self.definition.clone()),
            )))
        } else {
            // This should never happen because the definition must have come from a ViewTable, but we defensively handle it just in case.
            anyhow::bail!(
                "Expected logical plan for view '{}' to be a CreateView DDL, but got: {:?}",
                self.name,
                plan
            );
        }
    }

    fn table_name(&self) -> &str {
        &self.name
    }

    fn depends_on(&self) -> Vec<String> {
        self.dependencies.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }
}

#[cfg(test)]
mod tests {
    use super::ViewTableDefinition;
    use crate::table_ext::TableDefinition;
    use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::datasource::{MemTable, TableType, ViewTable};
    use datafusion::execution::object_store::ObjectStoreUrl;
    use datafusion::prelude::SessionContext;
    use std::sync::Arc;

    #[tokio::test]
    /// Verifies extracting a definition from a view succeeds when SQL is present.
    async fn view_table_definition_try_from_view_reads_definition() {
        let context = SessionContext::new();
        let plan = context
            .state()
            .create_logical_plan("SELECT 1 AS x")
            .await
            .unwrap();
        let view = ViewTable::new(plan, Some("SELECT 1 AS x".to_string()));

        let definition = ViewTableDefinition::try_from_view("view", &view).unwrap();
        assert_eq!(definition.name, "view");
        assert_eq!(definition.definition, "SELECT 1 AS x");
    }

    #[tokio::test]
    /// Verifies extracting a definition from a view fails when SQL is absent.
    async fn view_table_definition_try_from_view_requires_definition() {
        let context = SessionContext::new();
        let plan = context
            .state()
            .create_logical_plan("SELECT 1 AS x")
            .await
            .unwrap();
        let view = ViewTable::new(plan, None);

        let err =
            ViewTableDefinition::try_from_view("view", &view).expect_err("missing definition");
        assert!(err.to_string().contains("requires a SQL definition"));
    }

    #[tokio::test]
    /// Verifies building a view definition yields a downcastable [`ViewTable`].
    async fn view_table_definition_build_provider_creates_view_table() {
        let definition = ViewTableDefinition {
            name: "my_view".to_string(),
            definition: "SELECT 42 AS answer".to_string(),
            dependencies: vec![],
        };

        let context = Arc::new(SessionContext::new());
        let store_url = ObjectStoreUrl::parse("file://").unwrap();

        let provider = definition
            .build_provider(context, &store_url)
            .await
            .expect("view provider should be built");

        let view = provider
            .as_any()
            .downcast_ref::<ViewTable>()
            .expect("provider should be a ViewTable");
        assert_eq!(
            view.definition().cloned(),
            Some("SELECT 42 AS answer".to_string())
        );
    }

    #[test]
    /// Verifies legacy view JSON without dependencies remains deserializable.
    fn view_table_definition_deserializes_without_dependencies_for_compatibility() {
        let legacy_json = r#"{
  "type": "view_table",
  "name": "legacy_view",
  "definition": "SELECT 1 AS x"
}"#;

        let definition: Arc<dyn TableDefinition> =
            serde_json::from_str(legacy_json).expect("legacy view JSON should deserialize");

        assert_eq!(definition.table_name(), "legacy_view");
        assert_eq!(definition.table_type(), TableType::View);
        assert!(definition.depends_on().is_empty());
    }

    #[tokio::test]
    /// Verifies dependency extraction from direct table scans is stable and deduplicated.
    async fn view_table_dependency_traversal_collects_direct_scans() {
        let context = SessionContext::new();
        let schema: SchemaRef =
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, true)]));
        let batch = RecordBatch::new_empty(schema.clone());

        for table_name in ["t1", "t2"] {
            let table = MemTable::try_new(schema.clone(), vec![vec![batch.clone()]])
                .expect("mem table should be created");
            context
                .register_table(table_name, Arc::new(table))
                .expect("table should be registered");
        }

        let plan = context
            .state()
            .create_logical_plan(
                "SELECT t1.id FROM t1 JOIN t2 ON t1.id = t2.id WHERE t1.id IN (SELECT id FROM t1)",
            )
            .await
            .expect("logical plan should be created");

        let mut dependencies = Vec::new();
        ViewTableDefinition::traverse_logical_plan_for_dependencies(&plan, &mut dependencies);

        assert_eq!(dependencies, vec!["t1".to_string(), "t2".to_string()]);
    }

    #[tokio::test]
    /// Verifies dependency extraction traverses nested subquery plans embedded in expressions.
    async fn view_table_dependency_traversal_collects_nested_subqueries() {
        let context = SessionContext::new();
        let schema: SchemaRef =
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, true)]));
        let batch = RecordBatch::new_empty(schema.clone());

        for table_name in ["t1", "t2", "t3"] {
            let table = MemTable::try_new(schema.clone(), vec![vec![batch.clone()]])
                .expect("mem table should be created");
            context
                .register_table(table_name, Arc::new(table))
                .expect("table should be registered");
        }

        let plan = context
            .state()
            .create_logical_plan(
                "SELECT id FROM t1 WHERE id IN (SELECT id FROM t2 WHERE EXISTS (SELECT 1 FROM t3 WHERE t3.id = t2.id))",
            )
            .await
            .expect("logical plan should be created");

        let mut dependencies = Vec::new();
        ViewTableDefinition::traverse_logical_plan_for_dependencies(&plan, &mut dependencies);

        assert_eq!(
            dependencies,
            vec!["t1".to_string(), "t2".to_string(), "t3".to_string()]
        );
    }
}
