use std::{any::Any, sync::Arc};

use arrow::datatypes::SchemaRef;
use datafusion::{
    catalog::{Session, TableProvider},
    common::{Column, DFSchema},
    datasource::TableType as DataFusionTableType,
    error::DataFusionError,
    execution::object_store::ObjectStoreUrl,
    logical_expr::{Expr, ExprSchemable, TableProviderFilterPushDown},
    physical_expr::create_physical_expr,
    physical_plan::{
        ExecutionPlan, empty::EmptyExec, filter::FilterExec, projection::ProjectionExec,
        union::UnionExec,
    },
    prelude::{SessionContext, lit},
    scalar::ScalarValue,
};

use crate::{table::error::TableError, util::super_type_schema};

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct MergedTable {
    #[serde(alias = "tables")]
    pub table_names: Vec<String>,
}

impl MergedTable {
    pub async fn create(
        &self,
        _table_directory: object_store::path::Path,
        _session_ctx: Arc<SessionContext>,
    ) -> Result<(), TableError> {
        Ok(())
    }

    pub async fn table_provider(
        &self,
        _table_directory_store_url: ObjectStoreUrl,
        _data_directory_store_url: ObjectStoreUrl,
        session_ctx: Arc<SessionContext>,
    ) -> Result<Arc<dyn TableProvider>, TableError> {
        let mut providers = Vec::with_capacity(self.table_names.len());
        for table_name in &self.table_names {
            let provider = session_ctx
                .table_provider(table_name)
                .await
                .map_err(|_| TableError::TableNotFound(table_name.clone()))?;
            providers.push(provider);
        }

        let merged = MergedTableProvider::try_new(providers)
            .map_err(|e| TableError::GenericTableError(e.to_string()))?;

        Ok(Arc::new(merged))
    }
}

#[derive(Debug)]
struct MergedTableProvider {
    providers: Vec<Arc<dyn TableProvider>>,
    merged_schema: SchemaRef,
}

impl MergedTableProvider {
    fn try_new(providers: Vec<Arc<dyn TableProvider>>) -> Result<Self, DataFusionError> {
        if providers.is_empty() {
            return Ok(Self {
                providers,
                merged_schema: Arc::new(arrow::datatypes::Schema::empty()),
            });
        }

        let schemas: Vec<_> = providers.iter().map(|provider| provider.schema()).collect();
        let merged_schema =
            Arc::new(super_type_schema(&schemas).map_err(|e| {
                DataFusionError::Execution(format!("Failed to merge schemas: {e}"))
            })?);

        Ok(Self {
            providers,
            merged_schema,
        })
    }

    fn aligned_projection_exec(
        &self,
        state: &dyn Session,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let input_schema = plan.schema();
        let input_df_schema = DFSchema::try_from(input_schema.clone())?;
        let execution_props = state.execution_props();

        let mut projection_exprs = Vec::with_capacity(self.merged_schema.fields().len());
        for field in self.merged_schema.fields() {
            let field_name = field.name();
            let target_type = field.data_type().clone();

            let logical_expr = match input_schema.field_with_name(field_name) {
                Ok(input_field) => {
                    if input_field.data_type() == &target_type {
                        Expr::Column(Column::new_unqualified(field_name.clone()))
                    } else {
                        Expr::Column(Column::new_unqualified(field_name.clone()))
                            .cast_to(&target_type, &input_df_schema)?
                    }
                }
                Err(_) => lit(ScalarValue::Null).cast_to(&target_type, &input_df_schema)?,
            };

            let physical_expr =
                create_physical_expr(&logical_expr, &input_df_schema, execution_props)?;

            projection_exprs.push((physical_expr, field_name.clone()));
        }

        Ok(Arc::new(ProjectionExec::try_new(projection_exprs, plan)?))
    }

    fn filter_applicable_to_schema(filter: &Expr, schema: &SchemaRef) -> bool {
        let referenced_columns = filter.column_refs();
        referenced_columns
            .iter()
            .all(|column| schema.field_with_name(column.name()).is_ok())
    }
}

#[async_trait::async_trait]
impl TableProvider for MergedTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.merged_schema.clone()
    }

    fn table_type(&self) -> DataFusionTableType {
        DataFusionTableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        if self.providers.is_empty() {
            return Ok(Arc::new(EmptyExec::new(self.merged_schema.clone())));
        }

        let mut plans = Vec::with_capacity(self.providers.len());
        for provider in &self.providers {
            let child_schema = provider.schema();
            let child_filters = filters
                .iter()
                .filter(|filter| Self::filter_applicable_to_schema(filter, &child_schema))
                .cloned()
                .collect::<Vec<_>>();

            let scan = provider.scan(state, None, &child_filters, None).await?;
            let aligned = self.aligned_projection_exec(state, scan)?;
            plans.push(aligned);
        }

        let mut merged_plan: Arc<dyn ExecutionPlan> = Arc::new(UnionExec::new(plans));

        if !filters.is_empty() {
            let merged_df_schema = DFSchema::try_from(merged_plan.schema())?;
            let execution_props = state.execution_props();

            for filter in filters {
                let physical_filter =
                    create_physical_expr(filter, &merged_df_schema, execution_props)?;
                merged_plan = Arc::new(FilterExec::try_new(physical_filter, merged_plan)?);
            }
        }

        if let Some(projection_indices) = projection {
            let merged_df_schema = DFSchema::try_from(merged_plan.schema())?;
            let execution_props = state.execution_props();
            let projection_exprs = projection_indices
                .iter()
                .map(|index| {
                    let field = self.merged_schema.field(*index);
                    let expr = Expr::Column(Column::new_unqualified(field.name().clone()));
                    let physical_expr =
                        create_physical_expr(&expr, &merged_df_schema, execution_props)?;
                    Ok((physical_expr, field.name().clone()))
                })
                .collect::<Result<Vec<_>, DataFusionError>>()?;

            merged_plan = Arc::new(ProjectionExec::try_new(projection_exprs, merged_plan)?);
        }

        Ok(merged_plan)
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::error::Result<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use arrow::{
        array::{Array, ArrayRef, Float64Array, Int32Array},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use datafusion::catalog::MemTable;
    use datafusion::catalog::Session;
    use datafusion::execution::object_store::ObjectStoreUrl;
    use datafusion::physical_plan::collect;
    use datafusion::prelude::col;

    fn f64_values(column: &ArrayRef) -> Vec<Option<f64>> {
        let col = column
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("expected Float64Array");
        (0..col.len())
            .map(|idx| {
                if col.is_null(idx) {
                    None
                } else {
                    Some(col.value(idx))
                }
            })
            .collect()
    }

    fn i32_values(column: &ArrayRef) -> Vec<Option<i32>> {
        let col = column
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("expected Int32Array");
        (0..col.len())
            .map(|idx| {
                if col.is_null(idx) {
                    None
                } else {
                    Some(col.value(idx))
                }
            })
            .collect()
    }

    #[derive(Debug)]
    struct RecordingProvider {
        schema: SchemaRef,
        scan_calls: Arc<AtomicUsize>,
        pushed_filters: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl TableProvider for RecordingProvider {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn schema(&self) -> SchemaRef {
            self.schema.clone()
        }

        fn table_type(&self) -> DataFusionTableType {
            DataFusionTableType::Base
        }

        async fn scan(
            &self,
            _state: &dyn Session,
            _projection: Option<&Vec<usize>>,
            filters: &[Expr],
            _limit: Option<usize>,
        ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
            self.scan_calls.fetch_add(1, Ordering::SeqCst);
            self.pushed_filters
                .fetch_add(filters.len(), Ordering::SeqCst);
            Ok(Arc::new(EmptyExec::new(self.schema.clone())))
        }

        fn supports_filters_pushdown(
            &self,
            filters: &[&Expr],
        ) -> datafusion::error::Result<Vec<TableProviderFilterPushDown>> {
            Ok(vec![TableProviderFilterPushDown::Exact; filters.len()])
        }
    }

    #[tokio::test]
    async fn merged_table_provider_merges_schema_using_super_type() {
        let schema_a = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, true)]));
        let schema_b = Arc::new(Schema::new(vec![Field::new("v", DataType::Float64, true)]));

        let batch_a = RecordBatch::try_new(
            schema_a.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef],
        )
        .unwrap();
        let batch_b = RecordBatch::try_new(
            schema_b.clone(),
            vec![Arc::new(Float64Array::from(vec![3.5])) as ArrayRef],
        )
        .unwrap();

        let table_a = Arc::new(MemTable::try_new(schema_a, vec![vec![batch_a]]).unwrap())
            as Arc<dyn TableProvider>;
        let table_b = Arc::new(MemTable::try_new(schema_b, vec![vec![batch_b]]).unwrap())
            as Arc<dyn TableProvider>;

        let provider = MergedTableProvider::try_new(vec![table_a, table_b]).unwrap();
        assert_eq!(provider.schema().field(0).data_type(), &DataType::Float64);
    }

    #[tokio::test]
    async fn merged_table_provider_scans_with_aligned_schema() {
        let schema_a = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("shared", DataType::Int32, true),
        ]));
        let schema_b = Arc::new(Schema::new(vec![
            Field::new("b", DataType::Int32, true),
            Field::new("shared", DataType::Float64, true),
        ]));

        let batch_a = RecordBatch::try_new(
            schema_a.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef,
                Arc::new(Int32Array::from(vec![10, 20])) as ArrayRef,
            ],
        )
        .unwrap();

        let batch_b = RecordBatch::try_new(
            schema_b.clone(),
            vec![
                Arc::new(Int32Array::from(vec![7])) as ArrayRef,
                Arc::new(Float64Array::from(vec![3.5])) as ArrayRef,
            ],
        )
        .unwrap();

        let table_a = Arc::new(MemTable::try_new(schema_a, vec![vec![batch_a]]).unwrap())
            as Arc<dyn TableProvider>;
        let table_b = Arc::new(MemTable::try_new(schema_b, vec![vec![batch_b]]).unwrap())
            as Arc<dyn TableProvider>;

        let provider = Arc::new(MergedTableProvider::try_new(vec![table_a, table_b]).unwrap());

        let ctx = SessionContext::new();
        let state = ctx.state();
        let plan = provider.scan(&state, None, &[], None).await.unwrap();
        let task_ctx = ctx.task_ctx();
        let batches = collect(plan, task_ctx).await.unwrap();

        let mut total_rows = 0usize;
        let schema = provider.schema();

        let a_idx = schema.index_of("a").unwrap();
        let b_idx = schema.index_of("b").unwrap();
        let shared_idx = schema.index_of("shared").unwrap();

        let mut a_values = Vec::new();
        let mut b_values = Vec::new();
        let mut shared_values = Vec::new();

        for batch in &batches {
            total_rows += batch.num_rows();
            a_values.extend(i32_values(batch.column(a_idx)));
            b_values.extend(i32_values(batch.column(b_idx)));
            shared_values.extend(f64_values(batch.column(shared_idx)));
        }

        assert_eq!(total_rows, 3);
        assert_eq!(a_values, vec![Some(1), Some(2), None]);
        assert_eq!(b_values, vec![None, None, Some(7)]);
        assert_eq!(shared_values, vec![Some(10.0), Some(20.0), Some(3.5)]);
    }

    #[tokio::test]
    async fn merged_table_provider_pushes_filters_to_matching_children() {
        let schema_a = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
        let schema_b = Arc::new(Schema::new(vec![Field::new("b", DataType::Int32, true)]));

        let a_scan_calls = Arc::new(AtomicUsize::new(0));
        let b_scan_calls = Arc::new(AtomicUsize::new(0));
        let a_pushed_filters = Arc::new(AtomicUsize::new(0));
        let b_pushed_filters = Arc::new(AtomicUsize::new(0));

        let table_a = Arc::new(RecordingProvider {
            schema: schema_a,
            scan_calls: a_scan_calls.clone(),
            pushed_filters: a_pushed_filters.clone(),
        }) as Arc<dyn TableProvider>;

        let table_b = Arc::new(RecordingProvider {
            schema: schema_b,
            scan_calls: b_scan_calls.clone(),
            pushed_filters: b_pushed_filters.clone(),
        }) as Arc<dyn TableProvider>;

        let provider = MergedTableProvider::try_new(vec![table_a, table_b]).unwrap();
        let ctx = SessionContext::new();
        let state = ctx.state();

        let filter_a = col("a").gt(lit(10));
        let _plan = provider
            .scan(&state, None, &[filter_a], None)
            .await
            .expect("scan should succeed");

        assert_eq!(a_scan_calls.load(Ordering::SeqCst), 1);
        assert_eq!(b_scan_calls.load(Ordering::SeqCst), 1);
        assert_eq!(a_pushed_filters.load(Ordering::SeqCst), 1);
        assert_eq!(b_pushed_filters.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn merged_table_resolves_providers_by_table_name() {
        let ctx = Arc::new(SessionContext::new());

        let schema_a = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
        let schema_b = Arc::new(Schema::new(vec![Field::new("b", DataType::Int32, true)]));

        let batch_a = RecordBatch::try_new(
            schema_a.clone(),
            vec![Arc::new(Int32Array::from(vec![1])) as ArrayRef],
        )
        .unwrap();
        let batch_b = RecordBatch::try_new(
            schema_b.clone(),
            vec![Arc::new(Int32Array::from(vec![2])) as ArrayRef],
        )
        .unwrap();

        let table_a = Arc::new(MemTable::try_new(schema_a, vec![vec![batch_a]]).unwrap());
        let table_b = Arc::new(MemTable::try_new(schema_b, vec![vec![batch_b]]).unwrap());

        ctx.register_table("t_a", table_a).unwrap();
        ctx.register_table("t_b", table_b).unwrap();

        let merged = MergedTable {
            table_names: vec!["t_a".to_string(), "t_b".to_string()],
        };

        let provider = merged
            .table_provider(
                ObjectStoreUrl::parse("file://").unwrap(),
                ObjectStoreUrl::parse("file://").unwrap(),
                ctx,
            )
            .await
            .unwrap();

        assert!(provider.schema().field_with_name("a").is_ok());
        assert!(provider.schema().field_with_name("b").is_ok());
    }

    #[tokio::test]
    async fn merged_table_returns_not_found_for_missing_table_name() {
        let ctx = Arc::new(SessionContext::new());
        let merged = MergedTable {
            table_names: vec!["missing_table".to_string()],
        };

        let result = merged
            .table_provider(
                ObjectStoreUrl::parse("file://").unwrap(),
                ObjectStoreUrl::parse("file://").unwrap(),
                ctx,
            )
            .await;

        match result {
            Err(TableError::TableNotFound(name)) => assert_eq!(name, "missing_table"),
            _ => panic!("Expected TableError::TableNotFound"),
        }
    }
}
