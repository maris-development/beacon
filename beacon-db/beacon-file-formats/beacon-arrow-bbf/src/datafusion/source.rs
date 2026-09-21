use std::{
    any::Any,
    collections::{HashMap, HashSet},
    sync::Arc,
};

use beacon_datafusion_ext::type_widening::{ArrowTypeWideningStrategy, DefaultArrowTypeWidening};
use datafusion::{
    common::plan_err,
    config::ConfigOptions,
    datasource::{
        physical_plan::{FileOpener, FileScanConfig, FileSource},
        schema_adapter::SchemaAdapterFactory,
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
use object_store::ObjectStore;
use parking_lot::Mutex;
use tokio_util::sync::CancellationToken;

use crate::datafusion::{metrics::BBFGlobalMetrics, opener::BBFOpener, stream_share::StreamShare};

#[derive(Clone, Debug)]
pub struct BBFSource {
    /// Optional schema adapter factory.
    schema_adapter_factory: Option<Arc<dyn SchemaAdapterFactory>>, //ToDo: Remove this once we have removed the deprecated schema adapter factory.
    /// The table schema (file schema + partition columns).
    table_schema: TableSchema,
    /// Execution plan metrics.
    execution_plan_metrics: ExecutionPlanMetricsSet,
    /// Batch Size.
    batch_size: usize,
    /// Whether to split each record batch into `batch_size`-row slices.
    split_streams_slice: bool,
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
    /// Projection pushed down by the scan, applied on top of the table schema.
    projection: Option<ProjectionExprs>,
    /// The rule that merged the table schema. It decides which casts read
    /// null. The format sets it from the session when it plans.
    type_widening: Arc<dyn ArrowTypeWideningStrategy>,
}

impl BBFSource {
    pub fn new(table_schema: TableSchema) -> Self {
        let base_metrics = ExecutionPlanMetricsSet::new();
        let global_metrics = BBFGlobalMetrics::new(base_metrics.clone());
        Self {
            schema_adapter_factory: None,
            table_schema,
            execution_plan_metrics: base_metrics,
            batch_size: 32 * 1024,
            split_streams_slice: false,
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

    /// Returns a copy of this source that splits each record batch into
    /// `batch_size`-row slices when `split` is set.
    pub fn with_split_streams_slice(mut self, split: bool) -> Self {
        self.split_streams_slice = split;
        self
    }

    /// Returns a copy of this source carrying the given projection. Used to
    /// preserve a pushed-down projection when the format rebuilds the source
    /// in `create_physical_plan`.
    pub fn with_projection(mut self, projection: Option<ProjectionExprs>) -> Self {
        self.projection = projection;
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
        let selected: HashSet<usize> = projection.column_indices().into_iter().collect();
        if selected.is_empty() || selected.len() >= self.table_schema.table_schema().fields().len()
        {
            return plan_err!("{PROJECTION_REQUIRED}");
        }
        Ok(())
    }
}

const PROJECTION_REQUIRED: &str = "BBF scan needs a column list. SELECT * and count(*) are not \
    allowed. The reader flattens n-dimensional columns on the dimensions of the selected columns.";

impl FileSource for BBFSource {
    /// Creates a `dyn FileOpener` based on given parameters
    fn create_file_opener(
        &self,
        object_store: Arc<dyn ObjectStore>,
        base_config: &FileScanConfig,
        _partition: usize,
    ) -> datafusion::error::Result<Arc<dyn FileOpener>> {
        self.require_projection()?;
        let table_schema = self.table_schema.file_schema().clone();
        let projected_schema = base_config.projected_schema()?;
        let pruning_predicate = self
            .predicate
            .clone()
            .map(|p| PruningPredicate::try_new(p, table_schema.clone()))
            .transpose()?;
        Ok(Arc::new(BBFOpener::new(
            projected_schema,
            pruning_predicate,
            object_store,
            table_schema,
            self.file_tracer.lock().clone(),
            self.stream_partition_shares.clone(),
            self.global_metrics.clone(),
            self.split_streams_slice,
            self.batch_size,
            Arc::clone(&self.type_widening),
            self.cancellation_token(),
        )))
    }

    /// Any
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_schema(&self) -> &TableSchema {
        &self.table_schema
    }

    /// Initialize new type with batch size configuration
    fn with_batch_size(&self, batch_size: usize) -> Arc<dyn FileSource> {
        Arc::new(BBFSource {
            batch_size,
            ..self.clone()
        })
    }
    /// Return execution plan metrics
    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.execution_plan_metrics
    }

    fn schema_adapter_factory(&self) -> Option<Arc<dyn SchemaAdapterFactory>> {
        self.schema_adapter_factory.clone()
    }

    fn projection(&self) -> Option<&ProjectionExprs> {
        self.projection.as_ref()
    }

    fn try_pushdown_projection(
        &self,
        projection: &ProjectionExprs,
    ) -> datafusion::error::Result<Option<Arc<dyn FileSource>>> {
        let merged = match &self.projection {
            Some(existing) => existing.try_merge(projection)?,
            None => projection.clone(),
        };
        let source = BBFSource {
            projection: Some(merged),
            ..self.clone()
        };
        Ok(Some(Arc::new(source)))
    }

    fn with_schema_adapter_factory(
        &self,
        _factory: Arc<dyn SchemaAdapterFactory>,
    ) -> datafusion::error::Result<Arc<dyn FileSource>> {
        Ok(Arc::new(BBFSource {
            schema_adapter_factory: Some(_factory),
            ..self.clone()
        }))
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
    use crate::datafusion::BBFFormat;
    use arrow::datatypes::SchemaRef;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::file_format::FileFormat;
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

    /// A fresh source must not prune, project or slice anything; those only appear
    /// once the optimizer pushes them down.
    #[test]
    fn new_source_starts_without_predicate_or_projection() {
        let source = source();
        assert!(source.predicate.is_none());
        assert!(source.projection().is_none());
        assert!(!source.split_streams_slice);
        assert_eq!(source.file_type(), "bbf");
    }

    /// `with_batch_size` is called by the execution layer after the format built
    /// the source, so it must not reset the slicing configuration or the schema.
    #[test]
    fn with_batch_size_preserves_other_settings() {
        let source = source().with_split_streams_slice(true);
        let resized = source.with_batch_size(64);
        let resized = downcast(&resized);
        assert_eq!(resized.batch_size, 64);
        assert!(resized.split_streams_slice);
        assert_eq!(resized.table_schema().file_schema(), &schema());
    }

    /// The format must propagate its configured slicing default into the source it
    /// builds, otherwise `split_streams_slice = true` would have no effect.
    #[test]
    fn file_source_built_by_format_inherits_split_setting() {
        let format = BBFFormat {
            split_streams_slice: true,
        };
        let source = format.file_source(TableSchema::from_file_schema(schema()));
        assert!(downcast(&source).split_streams_slice);

        let source = BBFFormat::default().file_source(TableSchema::from_file_schema(schema()));
        assert!(!downcast(&source).split_streams_slice);
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
