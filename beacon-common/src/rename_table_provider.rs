use std::{any::Any, borrow::Cow, sync::Arc};

use arrow::datatypes::SchemaRef;
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
