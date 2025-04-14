use std::{any::Any, borrow::Cow, sync::Arc};

use arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::{
    catalog::{Session, TableProvider},
    common::{Constraints, Statistics},
    datasource::{
        file_format::FileFormat,
        listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl},
        TableType,
    },
    execution::SessionState,
    logical_expr::{dml::InsertOp, LogicalPlan, TableProviderFilterPushDown},
    physical_expr::EquivalenceProperties,
    physical_plan::{DisplayAs, ExecutionPlan, PlanProperties},
    prelude::Expr,
};

use beacon_common::super_typing::super_type_schema;
use futures::StreamExt;
use regex::Regex;

pub mod arrow_format;
pub mod csv_format;
pub mod formats_factory;
pub mod netcdf_format;
pub mod odv_format;
pub mod parquet_format;
pub mod sql;

#[derive(Debug)]
pub struct DataSource {
    inner_table: ListingTable,
    sanitized_schema: Option<SchemaRef>,
}

impl DataSource {
    pub async fn new(
        session_state: &SessionState,
        file_format: Arc<dyn FileFormat>,
        table_urls: Vec<ListingTableUrl>,
    ) -> anyhow::Result<Self> {
        //Set file extension to empty string to avoid file extension check
        let listing_options = ListingOptions::new(file_format).with_file_extension("");

        let mut schemas = vec![];
        let mut sanitized_schemas = vec![];

        for table_url in &table_urls {
            tracing::debug!("Infer schema for table: {}", table_url);
            let schema = listing_options
                .infer_schema(&session_state, table_url)
                .await?;

            schemas.push(schema.clone());
            sanitized_schemas.push(Arc::new(Self::sanitize_schema(&schema)));
        }

        let super_schema = Arc::new(
            super_type_schema(&schemas)
                .map_err(|e| anyhow::anyhow!("Failed to super type schema: {}", e))?,
        );

        //Sanitize the schema
        let sanitized_schema = Arc::new(super_type_schema(&sanitized_schemas).unwrap());

        let config = ListingTableConfig::new_with_multi_paths(table_urls)
            .with_listing_options(listing_options)
            .with_schema(super_schema);

        let table = ListingTable::try_new(config)?;
        Ok(Self {
            inner_table: table,
            sanitized_schema: Some(sanitized_schema),
        })
    }

    fn sanitize_column_name(name: &str) -> String {
        let name = name.to_lowercase();
        let re = Regex::new(r"[^\w]").unwrap(); // Matches anything except [a-zA-Z0-9_]
        let mut sanitized = re.replace_all(&name, "_").to_string();

        // If name starts with a digit, prefix it with an underscore
        if sanitized
            .chars()
            .next()
            .map(|c| c.is_ascii_digit())
            .unwrap_or(false)
        {
            sanitized = format!("_{}", sanitized);
        }

        sanitized
    }

    fn sanitize_schema(schema: &Schema) -> Schema {
        let fields: Vec<Field> = schema
            .fields()
            .iter()
            .map(|field| {
                let sanitized_name = Self::sanitize_column_name(field.name());
                Field::new(
                    &sanitized_name,
                    field.data_type().clone(),
                    field.is_nullable(),
                )
            })
            .collect();

        Schema::new(fields)
    }
}

#[async_trait::async_trait]
impl TableProvider for DataSource {
    fn as_any(&self) -> &dyn Any {
        self.inner_table.as_any()
    }

    fn schema(&self) -> SchemaRef {
        self.sanitized_schema.as_ref().unwrap().clone()
    }

    fn constraints(&self) -> Option<&Constraints> {
        self.inner_table.constraints()
    }

    fn table_type(&self) -> TableType {
        self.inner_table.table_type()
    }

    fn get_table_definition(&self) -> Option<&str> {
        self.inner_table.get_table_definition()
    }

    fn get_logical_plan(&self) -> Option<Cow<LogicalPlan>> {
        self.inner_table.get_logical_plan()
    }

    fn get_column_default(&self, _column: &str) -> Option<&Expr> {
        self.inner_table.get_column_default(_column)
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let plan = self
            .inner_table
            .scan(state, projection, filters, limit)
            .await?;

        Ok(plan)
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::error::Result<Vec<TableProviderFilterPushDown>> {
        self.inner_table.supports_filters_pushdown(filters)
    }

    fn statistics(&self) -> Option<Statistics> {
        self.inner_table.statistics()
    }

    async fn insert_into(
        &self,
        _state: &dyn Session,
        _input: Arc<dyn ExecutionPlan>,
        _insert_op: InsertOp,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        self.inner_table
            .insert_into(_state, _input, _insert_op)
            .await
    }
}

#[derive(Debug)]
struct SanitationPlan {
    unsanitized_schema: SchemaRef,
    sanitized_schema: SchemaRef,
    sub_plan: Arc<dyn ExecutionPlan>,
    plan_properties: PlanProperties,
}

impl SanitationPlan {
    fn new(
        unsanitized_schema: SchemaRef,
        sanitized_schema: SchemaRef,
        sub_plan: Arc<dyn ExecutionPlan>,
    ) -> Self {
        let original_plan_props = sub_plan.properties().clone();
        let eq_props = original_plan_props
            .eq_properties
            .clone()
            .with_new_schema(sanitized_schema.clone())
            .unwrap();
        let plan_properties = original_plan_props.with_eq_properties(eq_props);

        Self {
            unsanitized_schema,
            sanitized_schema,
            sub_plan,
            plan_properties,
        }
    }
}

impl DisplayAs for SanitationPlan {
    fn fmt_as(
        &self,
        t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            datafusion::physical_plan::DisplayFormatType::Default => {
                write!(f, "SanitationPlan: {}", self.unsanitized_schema)
            }
            datafusion::physical_plan::DisplayFormatType::Verbose => {
                write!(
                    f,
                    "SanitationPlan: unsanitized schema: {}, sanitized schema: {}",
                    self.unsanitized_schema, self.sanitized_schema
                )
            }
        }
    }
}

#[async_trait::async_trait]
impl ExecutionPlan for SanitationPlan {
    fn name(&self) -> &str {
        "SanitationPlan"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &datafusion::physical_plan::PlanProperties {
        &self.plan_properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self.clone())
        } else {
            Err(datafusion::error::DataFusionError::Internal(
                "SanitationPlan does not have children".to_string(),
            ))
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> datafusion::error::Result<datafusion::execution::SendableRecordBatchStream> {
        let stream = self.sub_plan.execute(partition, context.clone())?;

        let schema = self.sanitized_schema.clone();
        let stream = async_stream::stream! {
            while let Some(batch) = stream.next().await {
                let batch = batch.map_err(|e| {
                    datafusion::error::DataFusionError::Execution(format!("Failed to read batch: {}", e))
                })?;
                yield Ok(batch);
            }
        };

        Ok(Box::pin(stream))
    }
}
