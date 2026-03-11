use std::{
    any::Any,
    collections::{HashMap, HashSet},
    sync::Arc,
};

use arrow::datatypes::SchemaRef;
use chrono::NaiveDateTime;
use datafusion::{
    catalog::{Session, TableProvider, memory::DataSourceExec},
    common::{Column, DFSchema},
    execution::object_store::ObjectStoreUrl,
    logical_expr::TableProviderFilterPushDown,
    physical_expr::{create_physical_expr, create_physical_exprs},
    physical_plan::{ExecutionPlan, projection::ProjectionExec},
    prelude::{Expr, SessionContext},
};
use indexmap::IndexMap;

use crate::{
    table::{TableType, error::TableError},
    util::remap_filter,
};

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct PresetColumnMapping {
    column_name: String,
    alias: Option<String>,
    description: Option<String>,
    filter: Option<PresetFilterColumn>,
    column_metadata_columns: Option<Vec<PresetColumnMapping>>,
    #[serde(flatten)]
    #[serde(default)]
    _metadata_fields: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
#[serde(untagged)]
pub enum PresetFilterColumn {
    Exact {
        options: Vec<Exact>,
    },
    Range {
        #[serde(flatten)]
        range: Range,
    },
    Any {},
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(untagged)]
pub enum Range {
    Number {
        min: f64,
        max: f64,
    },
    Text {
        min: String,
        max: String,
    },
    Timestamp {
        min: NaiveDateTime,
        max: NaiveDateTime,
    },
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
#[serde(untagged)]
pub enum Exact {
    String(String),
    Number(f64),
    Timestamp(NaiveDateTime),
    Boolean(bool),
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PresetTable {
    #[serde(flatten)]
    pub table_engine: Arc<TableType>,
    pub data_columns: Vec<PresetColumnMapping>,
    pub metadata_columns: Vec<PresetColumnMapping>,
}

impl PresetTable {
    fn insert_exposed_field(
        exposed_fields: &mut Vec<arrow::datatypes::Field>,
        exposed_names: &mut HashSet<String>,
        renames: &mut IndexMap<String, String>,
        field: &arrow::datatypes::Field,
        column_name: &str,
        alias: Option<&String>,
    ) -> Result<(), TableError> {
        let exposed_name = alias.cloned().unwrap_or_else(|| field.name().clone());

        if !exposed_names.insert(exposed_name.clone()) {
            return Err(TableError::GenericTableError(format!(
                "Preset table exposes duplicate column name '{}'",
                exposed_name
            )));
        }

        if let Some(alias) = alias {
            exposed_fields.push(field.clone().with_name(alias));
            renames.insert(column_name.to_string(), alias.clone());
        } else {
            exposed_fields.push(field.clone());
        }

        Ok(())
    }

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
            TableType::Merged(merged_table) => {
                Box::pin(merged_table.create(table_directory, session_ctx)).await?
            }
            TableType::Empty(default_table) => {
                Box::pin(default_table.create(table_directory, session_ctx)).await?
            }
        }
        Ok(())
    }

    pub async fn table_provider(
        &self,
        table_directory_store_url: ObjectStoreUrl,
        data_directory_store_url: ObjectStoreUrl,
        session_ctx: Arc<SessionContext>,
    ) -> Result<Arc<dyn TableProvider>, TableError> {
        let current_provider = self
            .table_engine
            .table_provider(
                session_ctx,
                table_directory_store_url,
                data_directory_store_url,
            )
            .await?;

        let current_schema = current_provider.schema();

        let mut exposed_fields = Vec::new();
        let mut exposed_names = HashSet::new();
        let mut renames = IndexMap::new();

        for column in self.data_columns.iter() {
            if let Ok(field) = current_schema.field_with_name(&column.column_name) {
                Self::insert_exposed_field(
                    &mut exposed_fields,
                    &mut exposed_names,
                    &mut renames,
                    field,
                    &column.column_name,
                    column.alias.as_ref(),
                )?;

                if let Some(nested_metadata_columns) = column.column_metadata_columns.as_ref() {
                    for nested_column in nested_metadata_columns.iter() {
                        if let Ok(nested_field) =
                            current_schema.field_with_name(&nested_column.column_name)
                        {
                            Self::insert_exposed_field(
                                &mut exposed_fields,
                                &mut exposed_names,
                                &mut renames,
                                nested_field,
                                &nested_column.column_name,
                                nested_column.alias.as_ref(),
                            )?;
                        } else {
                            return Err(TableError::GenericTableError(format!(
                                "Nested metadata column '{}' not found in the current schema",
                                nested_column.column_name
                            )));
                        }
                    }
                }
            } else {
                return Err(TableError::GenericTableError(format!(
                    "Data column '{}' not found in the current schema",
                    column.column_name
                )));
            }
        }

        for column in self.metadata_columns.iter() {
            if let Ok(field) = current_schema.field_with_name(&column.column_name) {
                Self::insert_exposed_field(
                    &mut exposed_fields,
                    &mut exposed_names,
                    &mut renames,
                    field,
                    &column.column_name,
                    column.alias.as_ref(),
                )?;
            } else {
                return Err(TableError::GenericTableError(format!(
                    "Metadata column '{}' not found in the current schema",
                    column.column_name
                )));
            }
        }

        let schema = SchemaRef::new(arrow::datatypes::Schema::new(exposed_fields));

        let preset_table_provider =
            PresetTableProvider::new(current_provider, schema.clone(), renames);

        Ok(Arc::new(preset_table_provider))
    }
}

#[derive(Debug)]
struct PresetTableProvider {
    inner: Arc<dyn TableProvider>,
    exposed_schema: SchemaRef,
    renames: IndexMap<String, String>,
}

impl PresetTableProvider {
    pub fn new(
        inner: Arc<dyn TableProvider>,
        exposed_schema: SchemaRef,
        renames: IndexMap<String, String>,
    ) -> Self {
        Self {
            inner,
            exposed_schema,
            renames,
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
        let inverted_renames: HashMap<_, _> = self
            .renames
            .iter()
            .map(|(k, v)| (v.clone(), k.clone()))
            .collect();

        let alias_exprs = filters
            .iter()
            .map(|e| remap_filter(e.clone(), &inverted_renames))
            .collect::<Result<Vec<_>, _>>()?;

        let projection = projection
            .cloned()
            .unwrap_or_else(|| (0..self.exposed_schema.fields().len()).collect::<Vec<_>>());

        let mut source_projection = Vec::with_capacity(projection.len());

        // Translate the projection indices to the actual column names
        let source_schema = self.inner.schema();

        for column_index in projection {
            let exposed_column_name = self.exposed_schema.field(column_index).name();
            let source_name = inverted_renames
                .get(exposed_column_name)
                .unwrap_or(exposed_column_name);
            if let Ok(source_column_index) = source_schema.index_of(source_name) {
                source_projection.push(source_column_index);
            } else {
                return Err(datafusion::error::DataFusionError::Configuration(format!(
                    "Column '{}' not found in the source schema",
                    exposed_column_name
                )));
            }
        }

        let scan = self
            .inner
            .scan(state, Some(source_projection.as_ref()), &alias_exprs, limit)
            .await?;

        let scan_schema = scan.schema();
        let df_schema = DFSchema::try_from(scan_schema.clone()).unwrap();

        let props = state.execution_props();

        let mut proj_exprs = Vec::with_capacity(self.renames.len());

        for field in df_schema.fields() {
            // Check if the field is in the renames map
            if let Some(alias) = self.renames.get(field.name()) {
                // Make a logical Expr::Column against the real name
                let log_expr: Expr = Expr::Column(Column::new_unqualified(field.name().clone()));
                // Plan it into a PhysicalExpr
                let phys_expr = create_physical_expr(&log_expr, &df_schema, props).unwrap();
                // Now alias it in the ProjectionExec
                proj_exprs.push((phys_expr, alias.clone()));

                tracing::debug!(
                    "Adding projection for column '{}' with alias '{}'",
                    field.name(),
                    alias
                );
            }
        }

        let phys_pushdown_filters = create_physical_exprs(alias_exprs.iter(), &df_schema, props);

        match phys_pushdown_filters {
            Ok(exprs) if !exprs.is_empty() => {
                tracing::debug!("Pushdown filters: {:?}", exprs);

                if let Some(dse) = scan.as_any().downcast_ref::<DataSourceExec>() {
                    let file_source = dse.data_source().clone();
                    match file_source.try_pushdown_filters(exprs, state.config_options()) {
                        Ok(pushdown_res) => {
                            if let Some(updated_node) = pushdown_res.updated_node {
                                tracing::debug!(
                                    "Pushdown filters updated node: {:?}",
                                    updated_node
                                );
                                return Ok(Arc::new(ProjectionExec::try_new(
                                    proj_exprs,
                                    Arc::new(dse.clone().with_data_source(updated_node)),
                                )?));
                            }
                            tracing::debug!("Pushdown filters did not update the node");
                        }
                        Err(e) => {
                            tracing::warn!("Error during pushdown filter: {}", e);
                        }
                    }
                }

                Ok(Arc::new(ProjectionExec::try_new(proj_exprs, scan)?))
            }
            Ok(_) | Err(_) => {
                if let Err(e) = &phys_pushdown_filters {
                    tracing::warn!(
                        "Error creating physical expressions for pushdown filters: {}",
                        e
                    );
                }
                Ok(Arc::new(ProjectionExec::try_new(proj_exprs, scan)?))
            }
        }
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::error::Result<Vec<TableProviderFilterPushDown>> {
        self.inner.supports_filters_pushdown(filters)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::{
        array::{ArrayRef, Int32Array},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use datafusion::{catalog::MemTable, prelude::SessionContext};
    use futures::StreamExt;

    use crate::{DataLake, TABLES_OBJECT_STORE_URL, table::Table};

    fn mapping(column_name: &str, alias: Option<&str>) -> PresetColumnMapping {
        PresetColumnMapping {
            column_name: column_name.to_string(),
            alias: alias.map(str::to_string),
            description: None,
            filter: None,
            column_metadata_columns: None,
            _metadata_fields: HashMap::new(),
        }
    }

    async fn preset_provider_result(
        preset: PresetTable,
    ) -> Result<Arc<dyn TableProvider>, TableError> {
        let ctx = Arc::new(SessionContext::new());
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1])) as ArrayRef,
                Arc::new(Int32Array::from(vec![2])) as ArrayRef,
            ],
        )
        .unwrap();
        let source = Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap());
        ctx.register_table("source", source).unwrap();

        preset
            .table_provider(
                ObjectStoreUrl::parse("file://").unwrap(),
                ObjectStoreUrl::parse("file://").unwrap(),
                ctx,
            )
            .await
    }

    fn preset_table(
        data_columns: Vec<PresetColumnMapping>,
        metadata_columns: Vec<PresetColumnMapping>,
    ) -> PresetTable {
        PresetTable {
            table_engine: Arc::new(TableType::Merged(crate::table::merged::MergedTable {
                table_names: vec!["source".to_string()],
            })),
            data_columns,
            metadata_columns,
        }
    }

    #[tokio::test]
    async fn preset_table_provider_rejects_duplicate_aliases() {
        let preset = preset_table(
            vec![mapping("a", Some("dup")), mapping("b", Some("dup"))],
            vec![],
        );

        let result = preset_provider_result(preset).await;

        match result {
            Err(TableError::GenericTableError(message)) => {
                assert_eq!(message, "Preset table exposes duplicate column name 'dup'");
            }
            other => panic!("expected duplicate-column error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn preset_table_provider_rejects_alias_colliding_with_existing_name() {
        let preset = preset_table(vec![mapping("a", Some("b")), mapping("b", None)], vec![]);

        let result = preset_provider_result(preset).await;

        match result {
            Err(TableError::GenericTableError(message)) => {
                assert_eq!(message, "Preset table exposes duplicate column name 'b'");
            }
            other => panic!("expected duplicate-column error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn create_table_rejects_duplicate_preset_output_columns_without_persisting() {
        let ctx = Arc::new(SessionContext::new());
        let data_lake = Arc::new(DataLake::new(ctx.clone()).await);

        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1])) as ArrayRef,
                Arc::new(Int32Array::from(vec![2])) as ArrayRef,
            ],
        )
        .unwrap();
        let source = Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap());
        ctx.register_table("source", source).unwrap();

        let table_name = format!(
            "invalid-preset-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );

        let table = Table {
            table_directory: vec![],
            table_name: table_name.clone(),
            table_type: TableType::Preset(preset_table(
                vec![mapping("a", Some("dup")), mapping("b", Some("dup"))],
                vec![],
            )),
            description: None,
        };

        let result = data_lake.create_table(table).await;

        match result {
            Err(TableError::GenericTableError(message)) => {
                assert_eq!(message, "Preset table exposes duplicate column name 'dup'");
            }
            other => panic!("expected duplicate-column error, got {other:?}"),
        }

        assert!(data_lake.list_table(&table_name).is_none());

        let object_store = ctx
            .runtime_env()
            .object_store(&*TABLES_OBJECT_STORE_URL)
            .unwrap();
        let mut entries = object_store.list(Some(&object_store::path::Path::from(table_name)));
        assert!(entries.next().await.is_none());
    }
}
