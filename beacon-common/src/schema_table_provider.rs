use std::sync::Arc;

use arrow::{
    array::{BooleanArray, RecordBatch, StringArray},
    datatypes::{Field, Schema},
};
use datafusion::{
    catalog::{memory::MemorySourceConfig, Session, TableProvider},
    physical_plan::ExecutionPlan,
    prelude::Expr,
};

use crate::super_table::SuperListingTable;

#[derive(Debug)]
pub struct SchemaTableProvider {
    super_listing_table: SuperListingTable,
}

impl SchemaTableProvider {
    pub fn new(super_listing_table: SuperListingTable) -> Self {
        Self {
            super_listing_table,
        }
    }
}

#[async_trait::async_trait]
impl TableProvider for SchemaTableProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> datafusion::arrow::datatypes::SchemaRef {
        let fields: Vec<Field> = vec![
            Field::new("column_name", arrow::datatypes::DataType::Utf8, false),
            Field::new("data_type", arrow::datatypes::DataType::Utf8, false),
            Field::new("is_nullable", arrow::datatypes::DataType::Boolean, false),
        ];

        Arc::new(Schema::new(fields))
    }

    fn table_type(&self) -> datafusion::datasource::TableType {
        datafusion::datasource::TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let schema = self.super_listing_table.schema();
        let column_names = schema
            .fields()
            .iter()
            .map(|f| Some(f.name().clone()))
            .collect::<StringArray>();
        let column_datatypes = schema
            .fields()
            .iter()
            .map(|f| Some(f.data_type().to_string()))
            .collect::<StringArray>();
        let column_nullables = schema
            .fields()
            .iter()
            .map(|f| Some(f.is_nullable()))
            .collect::<BooleanArray>();

        let batch = RecordBatch::try_new(
            self.schema(),
            vec![
                Arc::new(column_names),
                Arc::new(column_datatypes),
                Arc::new(column_nullables),
            ],
        )?;

        let exec =
            MemorySourceConfig::try_new_exec(&[vec![batch]], self.schema(), projection.cloned())?;

        Ok(exec)
    }
}
