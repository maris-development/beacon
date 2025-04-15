use std::{any::Any, borrow::Cow, sync::Arc};

use arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::{
    catalog::{Session, TableProvider},
    common::{
        tree_node::{Transformed, TreeNode},
        Constraints, Statistics,
    },
    datasource::{
        file_format::FileFormat,
        listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl},
        TableType,
    },
    execution::SessionState,
    logical_expr::{dml::InsertOp, LogicalPlan, TableProviderFilterPushDown},
    physical_plan::{stream::RecordBatchStreamAdapter, DisplayAs, ExecutionPlan, PlanProperties},
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

        //Sanitize the schema if enabled in the config
        let sanitized_schema = if beacon_config::CONFIG.sanitize_schema {
            let sanitized_schema = Arc::new(super_type_schema(&sanitized_schemas).unwrap());
            Some(sanitized_schema)
        } else {
            None
        };

        let config = ListingTableConfig::new_with_multi_paths(table_urls)
            .with_listing_options(listing_options)
            .with_schema(super_schema);

        let table = ListingTable::try_new(config)?;

        Ok(Self {
            inner_table: table,
            sanitized_schema: sanitized_schema,
        })
    }

    fn find_non_sanitized_column(&self, column_name: &str) -> Option<String> {
        let index = self
            .sanitized_schema
            .as_ref()
            .and_then(|schema| schema.index_of(column_name).ok());

        index.map(|i| self.inner_table.schema().field(i).name().to_string())
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
        if let Some(sanitized_schema) = &self.sanitized_schema {
            // println!("Sanitized schema: {:?}", sanitized_schema);
            sanitized_schema.clone()
        } else {
            self.inner_table.schema()
        }
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
        // Apply the projection to the current schema and add is_not_null filters for every column

        if let Some(sanitized_schema) = &self.sanitized_schema {
            //Traverse filter columns and rename columns from sanitized to original
            let transformed_filters: Vec<Expr> = filters
                .iter()
                .cloned()
                .map(|expr| {
                    expr.transform_up(|e| match e {
                        Expr::Column(mut col) => {
                            let non_sanitized_name =
                                self.find_non_sanitized_column(&col.name).unwrap();
                            col.name = non_sanitized_name;
                            Ok(Transformed::new_transformed(Expr::Column(col), true))
                        }
                        _ => Ok(Transformed::new_transformed(e, false)),
                    })
                    .map(|e| e.data)
                })
                .collect::<Result<Vec<_>, _>>()?;

            let plan = self
                .inner_table
                .scan(state, projection, transformed_filters.as_slice(), limit)
                .await?;

            let sanitized_plan = SanitizedPlan::new(sanitized_schema.clone(), plan.clone());
            return Ok(Arc::new(sanitized_plan));
        } else {
            let plan = self
                .inner_table
                .scan(state, projection, filters, limit)
                .await?;
            Ok(plan)
        }
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
struct SanitizedPlan {
    sub_plan: Arc<dyn ExecutionPlan>,
    plan_properties: PlanProperties,
}

impl SanitizedPlan {
    fn new(sanitized_schema: SchemaRef, sub_plan: Arc<dyn ExecutionPlan>) -> Self {
        let original_plan_props = sub_plan.properties().clone();

        let eq_props = original_plan_props
            .eq_properties
            .clone()
            .with_new_schema(sanitized_schema.clone())
            .unwrap();
        let plan_properties = original_plan_props.with_eq_properties(eq_props);

        Self {
            sub_plan,
            plan_properties,
        }
    }
}

impl DisplayAs for SanitizedPlan {
    fn fmt_as(
        &self,
        t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            datafusion::physical_plan::DisplayFormatType::Default => {
                write!(f, "SanitizedPlan: {}", self.sub_plan.name())
            }
            datafusion::physical_plan::DisplayFormatType::Verbose => {
                write!(
                    f,
                    "SanitizedPlan: {}\nSubplan: {}",
                    self.sub_plan.name(),
                    self.sub_plan
                        .as_any()
                        .downcast_ref::<SanitizedPlan>()
                        .unwrap()
                        .sub_plan
                        .name()
                )
            }
        }
    }
}

#[async_trait::async_trait]
impl ExecutionPlan for SanitizedPlan {
    fn name(&self) -> &str {
        "SanitizedPlan"
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
                "SanitizedPlan does not have children".to_string(),
            ))
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> datafusion::error::Result<datafusion::execution::SendableRecordBatchStream> {
        let mut stream = self.sub_plan.execute(partition, context.clone())?;

        let schema = self.plan_properties.eq_properties.schema().clone();
        let stream = async_stream::try_stream! {
            while let Some(batch) = stream.next().await {
                let batch = batch?;
                let columns = batch.columns().to_vec();
                let sanitized_batch = arrow::record_batch::RecordBatch::try_new(
                    schema.clone(),
                    columns,
                )?;
                yield sanitized_batch;
            }
        };

        let adapter = RecordBatchStreamAdapter::new(
            self.plan_properties.eq_properties.schema().clone(),
            stream,
        );

        Ok(Box::pin(adapter))
    }
}
