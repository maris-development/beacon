//! [`DatasetsTable`]: the table `list_datasets` returns.
//!
//! `scan` resolves the path and builds the plan. The walk itself runs when
//! the plan executes. See [`super::exec`].

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use beacon_datafusion_ext::format_ext::FileFormatFactoryExt;
use beacon_datafusion_ext::listing_factory::ListingFactory;
use datafusion::{
    catalog::{Session, TableProvider},
    datasource::TableType,
    error::DataFusionError,
    physical_expr::PhysicalExpr,
    physical_plan::{ExecutionPlan, expressions::Column, projection::ProjectionExec},
    prelude::Expr,
};

use super::classify::classify;
use super::exec::{DatasetsExec, RowStreamFactory};

/// The full `DatasetMetadata` shape.
pub fn list_datasets_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("file_name", DataType::Utf8, false),
        Field::new("file_format", DataType::Utf8, false),
        Field::new("can_inspect", DataType::Boolean, false),
        Field::new("can_partial_explore", DataType::Boolean, false),
        // Null when the size or timestamp could not be resolved.
        Field::new("size", DataType::UInt64, true),
        Field::new("last_modified", DataType::Utf8, true),
    ]))
}

/// The datasets of the store that match a glob, as a table.
#[derive(Debug)]
pub struct DatasetsTable {
    pattern: String,
    offset: usize,
    limit: Option<usize>,
    file_formats: Vec<Arc<dyn FileFormatFactoryExt>>,
    schema: SchemaRef,
}

impl DatasetsTable {
    pub fn new(
        pattern: String,
        offset: usize,
        limit: Option<usize>,
        file_formats: Vec<Arc<dyn FileFormatFactoryExt>>,
    ) -> Self {
        Self {
            pattern,
            offset,
            limit,
            file_formats,
            schema: list_datasets_schema(),
        }
    }
}

/// The tighter of the function's own limit and the planner push-down.
fn effective_limit(declared: Option<usize>, pushed: Option<usize>) -> Option<usize> {
    match (declared, pushed) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (a, b) => a.or(b),
    }
}

#[async_trait::async_trait]
impl TableProvider for DatasetsTable {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let schema = self.schema();
        let factory = state
            .config()
            .get_extension::<ListingFactory>()
            .ok_or_else(|| {
                DataFusionError::Execution(
                    "list_datasets: the listing factory is not registered on the session"
                        .to_string(),
                )
            })?;

        // The listing holds no session, so each execute rebuilds the walk from it.
        let listing = factory.listing(state, &self.pattern)?;
        let formats = self.file_formats.clone();
        let rows: RowStreamFactory =
            Arc::new(move || Ok(classify(formats.clone(), listing.stream())));

        let plan = Arc::new(DatasetsExec::new(
            Arc::clone(&schema),
            rows,
            self.offset,
            effective_limit(self.limit, limit),
            format!("glob={}", self.pattern),
        ));

        // The node produces the full row shape, so a projection sits above it.
        match projection {
            Some(projection) => {
                let exprs: Vec<(Arc<dyn PhysicalExpr>, String)> = projection
                    .iter()
                    .map(|index| {
                        let field = schema.field(*index);
                        (
                            Arc::new(Column::new(field.name(), *index)) as Arc<dyn PhysicalExpr>,
                            field.name().to_string(),
                        )
                    })
                    .collect();
                Ok(Arc::new(ProjectionExec::try_new(exprs, plan)?))
            }
            None => Ok(plan),
        }
    }
}
