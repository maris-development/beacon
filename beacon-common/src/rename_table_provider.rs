use std::{any::Any, borrow::Cow, ops::DerefMut, sync::Arc};

use arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::{
    catalog::{Session, TableProvider},
    common::{
        error::GenericError,
        tree_node::{Transformed, TreeNode},
        Constraints, Statistics,
    },
    datasource::TableType,
    logical_expr::{LogicalPlan, TableProviderFilterPushDown},
    physical_plan::ExecutionPlan,
    prelude::Expr,
};

use crate::rename_exec_plan::RenameExec;

#[derive(Debug)]
pub struct RenameTableProvider {
    input_table_provider: Arc<dyn TableProvider>,
    renamed_schema: SchemaRef,
}

impl RenameTableProvider {
    pub fn new(
        input_table_provider: Arc<dyn TableProvider>,
        renamed_schema: SchemaRef,
    ) -> anyhow::Result<Self> {
        //Validate that the renamed schema has the same number of columns and data types as the input plan
        let input_schema = input_table_provider.schema();
        if input_schema.fields().len() != renamed_schema.fields().len() {
            return Err(anyhow::anyhow!(
                "Input table provider and renamed schema have different number of columns"
            ));
        }

        for (i, field) in input_schema.fields().iter().enumerate() {
            if field.data_type() != renamed_schema.field(i).data_type() {
                return Err(anyhow::anyhow!(
                    "Input table provider and renamed schema have different data types for column {}",
                    field.name()
                ));
            }
        }

        Ok(Self {
            input_table_provider,
            renamed_schema,
        })
    }

    fn find_non_renamed_column(&self, column_name: &str) -> Option<String> {
        let index = self.schema().index_of(column_name).ok()?;

        Some(
            self.input_table_provider
                .schema()
                .field(index)
                .name()
                .to_string(),
        )
    }

    pub fn rename_fields(
        schema: &Schema,
        rename_map: &[(String, String)],
    ) -> Result<Schema, GenericError> {
        let mut index_map_fields = indexmap::IndexMap::new();
        for field in schema.fields() {
            index_map_fields.insert(field.name().to_string(), field.clone());
        }

        for (old_name, new_name) in rename_map {
            //Ensure the field exists in the schema
            if index_map_fields.get(old_name).is_none() {
                return Err(GenericError::from(format!(
                    "Field {} not found in schema",
                    old_name
                )));
            }

            //Rename the field
            if let Some(field) = index_map_fields.get_mut(old_name) {
                *field = Arc::new(field.as_ref().clone().with_name(new_name));
            }
        }

        Ok(Schema::new(
            index_map_fields
                .into_iter()
                .map(|(_, field)| field)
                .collect::<Vec<_>>(),
        ))
    }

    pub fn rename_field(
        schema: &Schema,
        old_name: &str,
        new_name: &str,
    ) -> Result<Schema, GenericError> {
        //Ensure the field exists in the schema
        if schema.index_of(old_name).is_err() {
            return Err(GenericError::from(format!(
                "Field {} not found in schema",
                old_name
            )));
        }

        let new_fields: Vec<Arc<Field>> = schema
            .fields()
            .iter()
            .map(|field| {
                if field.name() == old_name {
                    Arc::new(Field::new(
                        new_name,
                        field.data_type().clone(),
                        field.is_nullable(),
                    ))
                } else {
                    field.clone()
                }
            })
            .collect();

        Ok(Schema::new(new_fields))
    }
}

#[async_trait::async_trait]
impl TableProvider for RenameTableProvider {
    fn as_any(&self) -> &dyn Any {
        self.input_table_provider.as_any()
    }

    fn schema(&self) -> SchemaRef {
        self.renamed_schema.clone()
    }

    fn constraints(&self) -> Option<&Constraints> {
        self.input_table_provider.constraints()
    }

    fn table_type(&self) -> TableType {
        self.input_table_provider.table_type()
    }

    fn get_table_definition(&self) -> Option<&str> {
        self.input_table_provider.get_table_definition()
    }

    fn get_logical_plan(&self) -> Option<Cow<LogicalPlan>> {
        self.input_table_provider.get_logical_plan()
    }

    fn get_column_default(&self, _column: &str) -> Option<&Expr> {
        self.input_table_provider.get_column_default(_column)
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        //Traverse filter columns and rename columns from sanitized to original
        let transformed_filters: Vec<Expr> = filters
            .iter()
            .cloned()
            .map(|expr| {
                expr.transform_up(|e| match e {
                    Expr::Column(mut col) => {
                        let non_renamed_column = self.find_non_renamed_column(&col.name).unwrap();
                        col.name = non_renamed_column;
                        Ok(Transformed::new_transformed(Expr::Column(col), true))
                    }
                    _ => Ok(Transformed::new_transformed(e, false)),
                })
                .map(|e| e.data)
            })
            .collect::<Result<Vec<_>, _>>()?;

        let plan = self
            .input_table_provider
            .scan(state, projection, transformed_filters.as_slice(), limit)
            .await?;

        let projected_renamed_schema = projection
            .map(|proj| self.renamed_schema.project(&proj).map(Arc::new))
            .unwrap_or(Ok(self.renamed_schema.clone()))?;

        let renamed_exec =
            RenameExec::new(plan, projected_renamed_schema).map_err(|e| GenericError::from(e))?;
        return Ok(Arc::new(renamed_exec));
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::error::Result<Vec<TableProviderFilterPushDown>> {
        self.input_table_provider.supports_filters_pushdown(filters)
    }

    fn statistics(&self) -> Option<Statistics> {
        None
    }
}
