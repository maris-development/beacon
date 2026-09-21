use std::{any::Any, collections::HashMap, sync::Arc};

use arrow::datatypes::{Field, Schema, SchemaRef};
use beacon_datafusion_ext::nd::{encoding::nd_value_type, is_nd_encoded};
use beacon_datafusion_ext::scan_adapt::AdaptingOpener;
use beacon_datafusion_ext::type_widening::{ArrowTypeWideningStrategy, DefaultArrowTypeWidening};
use datafusion::physical_expr::expressions::Column;
use datafusion::{
    common::plan_err,
    config::ConfigOptions,
    datasource::{
        physical_plan::{FileOpener, FileScanConfig, FileSource},
        table_schema::TableSchema,
    },
    physical_expr::{conjunction, projection::ProjectionExprs},
    physical_optimizer::pruning::PruningPredicate,
    physical_plan::{
        PhysicalExpr,
        filter_pushdown::{FilterPushdownPropagation, PushedDown},
        metrics::ExecutionPlanMetricsSet,
    },
};
use datafusion_datasource::projection::SplitProjection;
use object_store::ObjectStore;
use parking_lot::Mutex;
use tokio_util::sync::CancellationToken;

use crate::datafusion::{metrics::BBFGlobalMetrics, opener::BBFOpener, stream_share::StreamShare};

#[derive(Clone, Debug)]
pub struct BBFSource {
    /// The table schema (file schema + partition columns).
    table_schema: TableSchema,
    /// Execution plan metrics.
    execution_plan_metrics: ExecutionPlanMetricsSet,
    /// Pruning Predicate
    predicate: Option<Arc<dyn PhysicalExpr>>,
    /// File Tracer
    file_tracer: Arc<Mutex<Arc<Mutex<Vec<String>>>>>,
    /// The token that stops every stream this source opens.
    cancellation_token: Arc<Mutex<CancellationToken>>,
    /// Stream Partition Share
    stream_partition_shares: Arc<Mutex<HashMap<object_store::path::Path, Arc<StreamShare>>>>,
    /// Global Metrics
    global_metrics: BBFGlobalMetrics,
    /// The projection the scan pushed down, split into the file columns to
    /// read and the expressions `ProjectionOpener` applies on top of them.
    projection: Option<SplitProjection>,
    /// The rule that merged the table schema. It decides which casts read
    /// null. The format sets it from the session when it plans.
    type_widening: Arc<dyn ArrowTypeWideningStrategy>,
}

impl BBFSource {
    pub fn new(table_schema: TableSchema) -> Self {
        let base_metrics = ExecutionPlanMetricsSet::new();
        let global_metrics = BBFGlobalMetrics::new(base_metrics.clone());
        Self {
            table_schema,
            execution_plan_metrics: base_metrics,
            predicate: None,
            file_tracer: Arc::new(Mutex::new(Arc::new(Mutex::new(vec![])))),
            cancellation_token: Arc::new(Mutex::new(CancellationToken::new())),
            stream_partition_shares: Arc::new(Mutex::new(HashMap::new())),
            global_metrics,
            projection: None,
            type_widening: Arc::new(DefaultArrowTypeWidening::new()),
        }
    }

    /// The same source, with the merge rule of the session.
    pub fn with_type_widening(mut self, strategy: Arc<dyn ArrowTypeWideningStrategy>) -> Self {
        self.type_widening = strategy;
        self
    }

    /// Returns a copy of this source carrying the given projection. Used to
    /// preserve a pushed-down projection when the format rebuilds the source
    /// in `create_physical_plan`.
    pub fn with_projection(mut self, projection: Option<ProjectionExprs>) -> Self {
        self.projection = projection
            .map(|projection| SplitProjection::new(self.table_schema.file_schema(), &projection));
        self
    }

    pub fn set_file_tracer(&self, tracer: Arc<Mutex<Vec<String>>>) {
        let mut file_tracer = self.file_tracer.lock();
        *file_tracer = tracer;
    }

    /// Sets the token that stops every stream this source opens.
    ///
    /// A cancel ends each open stream with one error item. Read tasks that
    /// the BBF reader already spawned finish in the background.
    pub fn set_cancellation_token(&self, token: CancellationToken) {
        *self.cancellation_token.lock() = token;
    }

    /// The token that stops every stream this source opens.
    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancellation_token.lock().clone()
    }

    /// Refuses a scan that does not select a subset of the table columns.
    ///
    /// The reader flattens each nd column on the dimensions of the selected
    /// columns. A scan of every column flattens on every dimension.
    ///
    /// # Errors
    ///
    /// Returns a plan error when the source has no projection, when the
    /// projection names no column, or when it names every column of the table.
    pub fn require_projection(&self) -> datafusion::error::Result<()> {
        let Some(projection) = &self.projection else {
            return plan_err!("{PROJECTION_REQUIRED}");
        };
        // `file_indices` holds each file column once, in table order.
        let selected = projection.file_indices.len();
        if selected == 0 || selected >= self.table_schema.file_schema().fields().len() {
            return plan_err!("{PROJECTION_REQUIRED}");
        }
        Ok(())
    }
}

const PROJECTION_REQUIRED: &str = "BBF scan needs a column list. SELECT * and count(*) are not \
    allowed. The reader flattens n-dimensional columns on the dimensions of the selected columns.";

/// `schema` with every `beacon.nd` field unwrapped to its value type.
///
/// The format plans over the encoded schema. A source built without the
/// format, as the unit tests do, holds value types already, and passes through.
fn value_schema(schema: &Schema) -> datafusion::error::Result<Schema> {
    let fields = schema
        .fields()
        .iter()
        .map(|field| {
            if is_nd_encoded(field) {
                Ok(Field::new(
                    field.name(),
                    nd_value_type(field.data_type())?,
                    field.is_nullable(),
                ))
            } else {
                Ok(field.as_ref().clone())
            }
        })
        .collect::<datafusion::error::Result<Vec<_>>>()?;
    Ok(Schema::new_with_metadata(fields, schema.metadata().clone()))
}

impl FileSource for BBFSource {
    /// Creates a `dyn FileOpener` based on given parameters
    fn create_file_opener(
        &self,
        object_store: Arc<dyn ObjectStore>,
        _base_config: &FileScanConfig,
        _partition: usize,
    ) -> datafusion::error::Result<Arc<dyn FileOpener>> {
        self.require_projection()?;
        let projection = self
            .projection
            .clone()
            .expect("require_projection passed, so the projection is set");
        let file_schema = self.table_schema.file_schema();
        // The scan schema, in projection order. The projection holds columns
        // only, so this is what the scan reports and what the adapter targets.
        let read_schema: SchemaRef = Arc::new(projection.source.project_schema(file_schema)?);
        // The predicate and the pruning index speak the value types, not the
        // encoded structs the scan carries.
        let value_schema = Arc::new(value_schema(file_schema)?);
        let pruning_predicate = self
            .predicate
            .clone()
            .map(|p| PruningPredicate::try_new(p, Arc::clone(&value_schema)))
            .transpose()?;
        let inner: Arc<dyn FileOpener> = Arc::new(BBFOpener::new(
            Arc::clone(&read_schema),
            pruning_predicate,
            object_store,
            value_schema,
            self.file_tracer.lock().clone(),
            self.stream_partition_shares.clone(),
            self.global_metrics.clone(),
            self.cancellation_token(),
        ));
        Ok(AdaptingOpener::wrap(
            inner,
            read_schema,
            Arc::clone(&self.type_widening),
        ))
    }

    /// Any
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_schema(&self) -> &TableSchema {
        &self.table_schema
    }

    /// The scan emits one encoded row per entry, so the batch size has no
    /// hold on it.
    fn with_batch_size(&self, _batch_size: usize) -> Arc<dyn FileSource> {
        Arc::new(self.clone())
    }
    /// Return execution plan metrics
    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.execution_plan_metrics
    }

    fn projection(&self) -> Option<&ProjectionExprs> {
        self.projection
            .as_ref()
            .map(|projection| &projection.source)
    }

    fn try_pushdown_projection(
        &self,
        projection: &ProjectionExprs,
    ) -> datafusion::error::Result<Option<Arc<dyn FileSource>>> {
        // The scan carries encoded nd columns, so only a column can be read
        // off it. An expression stays above the broadcast, where the nd
        // optimizer can sink it below the materialization.
        let columns_only = projection
            .iter()
            .all(|expr| expr.expr.as_any().downcast_ref::<Column>().is_some());
        if !columns_only {
            return Ok(None);
        }
        let merged = match &self.projection {
            Some(existing) => existing.source.try_merge(projection)?,
            None => projection.clone(),
        };
        Ok(Some(Arc::new(self.clone().with_projection(Some(merged)))))
    }

    fn try_pushdown_filters(
        &self,
        filters: Vec<Arc<dyn PhysicalExpr>>,
        _config: &ConfigOptions,
    ) -> datafusion::error::Result<FilterPushdownPropagation<Arc<dyn FileSource>>> {
        let predicate = match self.predicate.clone() {
            Some(predicate) => conjunction(std::iter::once(predicate).chain(filters.clone())),
            None => conjunction(filters.clone()),
        };

        let source = Self {
            predicate: Some(predicate),
            ..self.clone()
        };

        Ok(FilterPushdownPropagation::with_parent_pushdown_result(vec![
            PushedDown::No;
            filters.len()
        ])
        .with_updated_node(Arc::new(source)))
    }

    /// String representation of file source such as "csv", "json", "parquet"
    fn file_type(&self) -> &str {
        "bbf"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_expr::expressions::{col, lit};
    use datafusion::physical_expr::projection::ProjectionExprs;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
            Field::new("c", DataType::Utf8, true),
        ]))
    }

    fn source() -> BBFSource {
        BBFSource::new(TableSchema::from_file_schema(schema()))
    }

    fn downcast(source: &Arc<dyn FileSource>) -> &BBFSource {
        source
            .as_any()
            .downcast_ref::<BBFSource>()
            .expect("should still be a BBFSource")
    }

    /// A fresh source must not prune or project anything; those only appear
    /// once the optimizer pushes them down.
    #[test]
    fn new_source_starts_without_predicate_or_projection() {
        let source = source();
        assert!(source.predicate.is_none());
        assert!(source.projection().is_none());
        assert_eq!(source.file_type(), "bbf");
    }

    /// `with_batch_size` is called by the execution layer after the format built
    /// the source, so it must keep the schema.
    #[test]
    fn with_batch_size_keeps_the_schema() {
        let resized = source().with_batch_size(64);
        assert_eq!(downcast(&resized).table_schema().file_schema(), &schema());
    }

    /// The first projection pushdown is adopted verbatim; the source must report it
    /// back so `create_physical_plan` can carry it across a source rebuild.
    #[test]
    fn try_pushdown_projection_adopts_first_projection() {
        let projection = ProjectionExprs::from_indices(&[0, 2], &schema());
        let pushed = source()
            .try_pushdown_projection(&projection)
            .expect("pushdown should succeed")
            .expect("BBF supports projection pushdown");
        assert_eq!(
            downcast(&pushed)
                .projection()
                .expect("projection recorded")
                .column_indices(),
            vec![0, 2]
        );
    }

    /// A second pushdown composes with the first (the new projection indexes into
    /// the already-projected schema), rather than replacing it.
    #[test]
    fn try_pushdown_projection_merges_with_existing_projection() {
        let first = ProjectionExprs::from_indices(&[0, 2], &schema());
        let projected_schema = first.project_schema(&schema()).expect("project schema");
        let second = ProjectionExprs::from_indices(&[1], &projected_schema);

        let source = source().with_projection(Some(first));
        let pushed = source
            .try_pushdown_projection(&second)
            .expect("pushdown should succeed")
            .expect("BBF supports projection pushdown");
        // Column 1 of (a, c) is `c`, i.e. index 2 of the original schema.
        assert_eq!(
            downcast(&pushed)
                .projection()
                .expect("projection recorded")
                .column_indices(),
            vec![2]
        );
    }

    /// Filters are kept for BBF's own container pruning but must still be reported
    /// as `PushedDown::No`, because pruning is best-effort and the parent operator
    /// has to re-apply the filter for correctness.
    #[test]
    fn try_pushdown_filters_keeps_predicate_but_does_not_claim_it() {
        let schema = schema();
        let filter: Arc<dyn PhysicalExpr> =
            Arc::new(datafusion::physical_expr::expressions::BinaryExpr::new(
                col("a", &schema).unwrap(),
                datafusion::logical_expr::Operator::Gt,
                lit(1i32),
            ));
        let result = source()
            .try_pushdown_filters(vec![filter.clone()], &ConfigOptions::default())
            .expect("filter pushdown should succeed");

        assert_eq!(result.filters.len(), 1);
        assert!(
            matches!(result.filters[0], PushedDown::No),
            "BBF pruning is best-effort, so the filter must not be claimed"
        );
        let updated = result.updated_node.expect("source should be updated");
        assert!(downcast(&updated).predicate.is_some());
    }

    /// Successive filter pushdowns must accumulate into a conjunction instead of
    /// the later one dropping the earlier predicate.
    #[test]
    fn try_pushdown_filters_conjoins_successive_predicates() {
        let schema = schema();
        let f1: Arc<dyn PhysicalExpr> =
            Arc::new(datafusion::physical_expr::expressions::BinaryExpr::new(
                col("a", &schema).unwrap(),
                datafusion::logical_expr::Operator::Gt,
                lit(1i32),
            ));
        let f2: Arc<dyn PhysicalExpr> =
            Arc::new(datafusion::physical_expr::expressions::BinaryExpr::new(
                col("b", &schema).unwrap(),
                datafusion::logical_expr::Operator::Lt,
                lit(9i32),
            ));

        let first = source()
            .try_pushdown_filters(vec![f1], &ConfigOptions::default())
            .unwrap()
            .updated_node
            .expect("updated source");
        let second = downcast(&first)
            .try_pushdown_filters(vec![f2], &ConfigOptions::default())
            .unwrap()
            .updated_node
            .expect("updated source");

        let predicate = downcast(&second)
            .predicate
            .clone()
            .expect("predicate recorded");
        let rendered = format!("{predicate}");
        assert!(rendered.contains("a@0 > 1"), "predicate was {rendered}");
        assert!(rendered.contains("b@1 < 9"), "predicate was {rendered}");
        assert!(rendered.contains("AND"), "predicate was {rendered}");
    }

    /// The file tracer is shared by reference: replacing it on the source must be
    /// visible to openers created afterwards, which is how scanned entries are
    /// reported back to the query layer.
    #[test]
    fn set_file_tracer_swaps_the_shared_tracer() {
        let source = source();
        let tracer = Arc::new(Mutex::new(vec!["seed".to_string()]));
        source.set_file_tracer(tracer.clone());
        assert_eq!(source.file_tracer.lock().lock().as_slice(), ["seed"]);
        tracer.lock().push("more".to_string());
        assert_eq!(source.file_tracer.lock().lock().len(), 2);
    }

    fn open_with(source: BBFSource) -> datafusion::error::Result<()> {
        use datafusion::datasource::physical_plan::FileScanConfigBuilder;
        use datafusion::execution::object_store::ObjectStoreUrl;
        let conf = FileScanConfigBuilder::new(
            ObjectStoreUrl::parse("file://").expect("url"),
            Arc::new(source.clone()) as Arc<dyn FileSource>,
        )
        .build();
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        source.create_file_opener(store, &conf, 0).map(|_| ())
    }

    /// The reader flattens nd columns on the dimensions of the selected columns,
    /// so a scan must name its columns. No projection at all is refused.
    #[test]
    fn create_file_opener_refuses_a_scan_without_projection() {
        let err = open_with(source()).expect_err("no projection must fail");
        assert!(
            matches!(err, datafusion::error::DataFusionError::Plan(_)),
            "{err}"
        );
        assert!(err.to_string().contains("column list"), "{err}");
    }

    /// `SELECT *` arrives as a projection over every table column. It is refused
    /// for the same reason as a missing projection.
    #[test]
    fn create_file_opener_refuses_a_projection_of_every_column() {
        let all = ProjectionExprs::from_indices(&[0, 1, 2], &schema());
        let err =
            open_with(source().with_projection(Some(all))).expect_err("full projection must fail");
        assert!(
            matches!(err, datafusion::error::DataFusionError::Plan(_)),
            "{err}"
        );
        assert!(err.to_string().contains("SELECT *"), "{err}");
    }

    /// The scan reads encoded nd columns, so an expression cannot run in the
    /// opener. The source declines it and DataFusion keeps the `ProjectionExec`
    /// above the broadcast.
    #[test]
    fn try_pushdown_projection_declines_an_expression() {
        use datafusion::physical_expr::expressions::BinaryExpr;
        use datafusion::physical_expr::projection::ProjectionExpr;

        let schema = schema();
        let plus_one = BinaryExpr::new(
            col("a", &schema).unwrap(),
            datafusion::logical_expr::Operator::Plus,
            lit(1i32),
        );
        let projection = ProjectionExprs::new([ProjectionExpr::new(Arc::new(plus_one), "x")]);
        let pushed = source()
            .try_pushdown_projection(&projection)
            .expect("pushdown must not fail");
        assert!(pushed.is_none(), "an expression must stay above the scan");
    }

    /// `count(*)` arrives as a projection of no column. It is refused too.
    #[test]
    fn create_file_opener_refuses_a_projection_of_no_column() {
        let none = ProjectionExprs::from_indices(&[], &schema());
        let err = open_with(source().with_projection(Some(none)))
            .expect_err("empty projection must fail");
        assert!(
            matches!(err, datafusion::error::DataFusionError::Plan(_)),
            "{err}"
        );
        assert!(err.to_string().contains("count(*)"), "{err}");
    }

    /// A projection that leaves out at least one column passes.
    #[test]
    fn create_file_opener_accepts_a_projection_of_some_columns() {
        let some = ProjectionExprs::from_indices(&[2, 0], &schema());
        open_with(source().with_projection(Some(some))).expect("subset projection is fine");
    }
}
