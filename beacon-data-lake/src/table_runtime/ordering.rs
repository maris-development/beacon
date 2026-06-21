use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use beacon_datafusion_ext::table_ext::TableDefinition;
use datafusion::datasource::TableType as DefinitionTableType;

#[derive(Clone)]
struct ViewOrderEntry {
    name: String,
    table: Arc<dyn TableDefinition>,
    dependencies: Vec<String>,
}

fn sort_named_tables(tables: &mut [(String, Arc<dyn TableDefinition>)]) {
    tables.sort_by(|a, b| a.0.cmp(&b.0));
}

fn normalize_relation_name(relation: &str) -> String {
    relation
        .trim_matches('"')
        .trim_matches('`')
        .trim_end_matches('.')
        .rsplit('.')
        .next()
        .unwrap_or(relation)
        .trim_matches('"')
        .trim_matches('`')
        .to_string()
}

fn normalize_dependency_set(dependencies: Vec<String>) -> HashSet<String> {
    dependencies
        .into_iter()
        .map(|name| normalize_relation_name(&name))
        .collect()
}

fn is_temporary_definition(definition: &serde_json::Value) -> bool {
    let bool_flags = ["temporary", "is_temporary", "temp", "is_temp"];
    if bool_flags.iter().any(|field| {
        definition
            .get(*field)
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false)
    }) {
        return true;
    }

    if definition
        .get("location")
        .and_then(serde_json::Value::as_str)
        .map(|location| {
            location.starts_with("tmp://")
                || location.starts_with("tmp/")
                || location.starts_with("/tmp/")
        })
        .unwrap_or(false)
    {
        return true;
    }

    definition
        .get("definition")
        .and_then(serde_json::Value::as_str)
        .map(|sql| {
            let lowered = sql.to_ascii_lowercase();
            lowered.contains("create temporary") || lowered.contains("create temp")
        })
        .unwrap_or(false)
}

async fn order_view_tables(view_tables: Vec<ViewOrderEntry>) -> Vec<Arc<dyn TableDefinition>> {
    let mut by_name = HashMap::new();
    let mut persisted_dependencies = HashMap::new();

    for entry in view_tables {
        by_name.insert(entry.name.clone(), entry.table);
        persisted_dependencies.insert(entry.name, entry.dependencies);
    }

    let view_names = by_name.keys().cloned().collect::<HashSet<_>>();
    let mut dependencies = HashMap::<String, HashSet<String>>::new();
    let mut reverse_dependencies = HashMap::<String, HashSet<String>>::new();

    for view_name in &view_names {
        let mut deps = normalize_dependency_set(
            persisted_dependencies
                .get(view_name)
                .cloned()
                .unwrap_or_default(),
        );

        deps.retain(|dep| dep != view_name && view_names.contains(dep));

        for dep in &deps {
            reverse_dependencies
                .entry(dep.clone())
                .or_default()
                .insert(view_name.clone());
        }

        dependencies.insert(view_name.clone(), deps);
    }

    let mut ready = BTreeSet::new();
    for (name, deps) in &dependencies {
        if deps.is_empty() {
            ready.insert(name.clone());
        }
    }

    let mut ordered_names = Vec::new();
    let mut processed = HashSet::new();

    while let Some(name) = ready.iter().next().cloned() {
        ready.remove(&name);
        processed.insert(name.clone());
        ordered_names.push(name.clone());

        if let Some(dependents) = reverse_dependencies.get(&name) {
            for dependent in dependents {
                if let Some(dep_set) = dependencies.get_mut(dependent) {
                    dep_set.remove(&name);
                    if dep_set.is_empty() && !processed.contains(dependent) {
                        ready.insert(dependent.clone());
                    }
                }
            }
        }
    }

    if ordered_names.len() < by_name.len() {
        let mut remaining = by_name
            .keys()
            .filter(|name| !processed.contains(*name))
            .cloned()
            .collect::<Vec<_>>();
        remaining.sort();
        ordered_names.extend(remaining);
    }

    let mut ordered_tables = Vec::new();
    for name in ordered_names {
        if let Some(table) = by_name.get(&name) {
            ordered_tables.push(table.clone());
        }
    }

    ordered_tables
}

/// Orders table definitions so that providers are registered after the tables
/// they depend on: base tables first, then temporary definitions, then views in
/// dependency order.
pub async fn order_tables(
    tables: &HashMap<String, Arc<dyn TableDefinition>>,
) -> Vec<Arc<dyn TableDefinition>> {
    let mut definition_base = Vec::new();
    let mut definition_temp = Vec::new();
    let mut definition_views = Vec::new();

    for (name, definition) in tables {
        let serialized = serde_json::to_value(definition.as_ref()).unwrap_or_default();

        if definition.table_type() == DefinitionTableType::View {
            let dependencies = definition.depends_on();
            definition_views.push(ViewOrderEntry {
                name: name.clone(),
                table: definition.clone(),
                dependencies,
            });
        } else if is_temporary_definition(&serialized) {
            definition_temp.push((name.clone(), definition.clone()));
        } else {
            definition_base.push((name.clone(), definition.clone()));
        }
    }

    sort_named_tables(&mut definition_base);
    sort_named_tables(&mut definition_temp);

    let mut ordered = Vec::with_capacity(tables.len());
    ordered.extend(definition_base.into_iter().map(|(_, table)| table));
    ordered.extend(definition_temp.into_iter().map(|(_, table)| table));
    ordered.extend(order_view_tables(definition_views).await);

    ordered
}

#[cfg(test)]
mod tests {
    use super::order_tables;
    use beacon_datafusion_ext::table_ext::TableDefinition;
    use datafusion::arrow::datatypes::Schema;
    use datafusion::catalog::TableProvider;
    use datafusion::datasource::{TableType, empty::EmptyTable as EmptyProvider};
    use datafusion::execution::object_store::ObjectStoreUrl;
    use datafusion::prelude::SessionContext;
    use std::collections::HashMap;
    use std::sync::Arc;

    #[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
    struct TestBaseDefinition {
        name: String,
        temporary: bool,
    }

    #[async_trait::async_trait]
    #[typetag::serde(name = "ordering_test_base_definition")]
    impl TableDefinition for TestBaseDefinition {
        async fn build_provider(
            &self,
            _context: Arc<SessionContext>,
            _data_store_url: &ObjectStoreUrl,
        ) -> anyhow::Result<Arc<dyn TableProvider>> {
            Ok(Arc::new(EmptyProvider::new(Arc::new(Schema::empty()))))
        }

        fn table_name(&self) -> &str {
            &self.name
        }
    }

    #[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
    struct TestViewDefinition {
        name: String,
        dependencies: Vec<String>,
    }

    #[async_trait::async_trait]
    #[typetag::serde(name = "ordering_test_view_definition")]
    impl TableDefinition for TestViewDefinition {
        async fn build_provider(
            &self,
            _context: Arc<SessionContext>,
            _data_store_url: &ObjectStoreUrl,
        ) -> anyhow::Result<Arc<dyn TableProvider>> {
            Ok(Arc::new(EmptyProvider::new(Arc::new(Schema::empty()))))
        }

        fn depends_on(&self) -> Vec<String> {
            self.dependencies.clone()
        }

        fn table_name(&self) -> &str {
            &self.name
        }

        fn table_type(&self) -> TableType {
            TableType::View
        }
    }

    fn position(order: &[Arc<dyn TableDefinition>], name: &str) -> usize {
        order
            .iter()
            .position(|table| table.table_name() == name)
            .expect("table should be present in order")
    }

    #[tokio::test]
    async fn order_tables_keeps_category_boundaries_and_view_dependencies() {
        let mut tables: HashMap<String, Arc<dyn TableDefinition>> = HashMap::new();

        tables.insert(
            "definition_base".to_string(),
            Arc::new(TestBaseDefinition {
                name: "definition_base".to_string(),
                temporary: false,
            }),
        );
        tables.insert(
            "definition_temp".to_string(),
            Arc::new(TestBaseDefinition {
                name: "definition_temp".to_string(),
                temporary: true,
            }),
        );
        tables.insert(
            "view_parent".to_string(),
            Arc::new(TestViewDefinition {
                name: "view_parent".to_string(),
                dependencies: vec![],
            }),
        );
        tables.insert(
            "view_child".to_string(),
            Arc::new(TestViewDefinition {
                name: "view_child".to_string(),
                dependencies: vec!["view_parent".to_string()],
            }),
        );

        let order = order_tables(&tables).await;

        assert!(position(&order, "definition_base") < position(&order, "definition_temp"));
        assert!(position(&order, "definition_temp") < position(&order, "view_parent"));
        assert!(position(&order, "view_parent") < position(&order, "view_child"));
    }

    #[tokio::test]
    async fn ordered_definition_views_follow_table_scan_dependencies() {
        use beacon_datafusion_ext::table_ext::{ExternalTableDefinition, ViewTableDefinition};

        let mut tables: HashMap<String, Arc<dyn TableDefinition>> = HashMap::new();

        let base = ExternalTableDefinition {
            name: "base_table".to_string(),
            location: "dataset/base_table/*.parquet".to_string(),
            file_type: "parquet".to_string(),
            schema: Arc::new(Schema::empty()),
            definition: None,
            partition_cols: vec![],
            options: HashMap::new(),
            if_not_exists: false,
        };

        let view_a = ViewTableDefinition {
            name: "view_a".to_string(),
            definition: "SELECT * FROM base_table".to_string(),
            dependencies: vec!["base_table".to_string()],
        };

        let view_b = ViewTableDefinition {
            name: "view_b".to_string(),
            definition: "SELECT * FROM view_a".to_string(),
            dependencies: vec!["view_a".to_string()],
        };

        tables.insert(base.name.clone(), Arc::new(base));
        tables.insert(view_a.name.clone(), Arc::new(view_a));
        tables.insert(view_b.name.clone(), Arc::new(view_b));

        let order = order_tables(&tables).await;

        assert!(position(&order, "base_table") < position(&order, "view_a"));
        assert!(position(&order, "view_a") < position(&order, "view_b"));
    }

    #[tokio::test]
    async fn order_tables_uses_table_type_for_view_classification() {
        let mut tables: HashMap<String, Arc<dyn TableDefinition>> = HashMap::new();

        tables.insert(
            "def_temp".to_string(),
            Arc::new(TestBaseDefinition {
                name: "def_temp".to_string(),
                temporary: true,
            }),
        );
        tables.insert(
            "z_parent".to_string(),
            Arc::new(TestViewDefinition {
                name: "z_parent".to_string(),
                dependencies: vec![],
            }),
        );
        tables.insert(
            "a_child".to_string(),
            Arc::new(TestViewDefinition {
                name: "a_child".to_string(),
                dependencies: vec!["z_parent".to_string()],
            }),
        );

        let order = order_tables(&tables).await;

        assert!(position(&order, "def_temp") < position(&order, "z_parent"));
        assert!(position(&order, "z_parent") < position(&order, "a_child"));
    }
}
