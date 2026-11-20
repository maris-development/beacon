//! [`FastObjectTable`]: the `TableProvider`.
//!
//! The type wraps a `ListingTable`. See the [module docs](super).

use std::borrow::Cow;
use std::sync::Arc;

use datafusion::{
    arrow::datatypes::{Schema, SchemaRef},
    catalog::{Session, TableProvider},
    common::{Constraints, Statistics, plan_datafusion_err},
    datasource::{
        TableType,
        file_format::FileFormat,
        listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl},
    },
    error::DataFusionError,
    execution::SessionState,
    logical_expr::{LogicalPlan, TableProviderFilterPushDown, dml::InsertOp},
    physical_plan::ExecutionPlan,
    prelude::Expr,
};

use crate::type_widening::{ArrowTypeWideningStrategy, session_widening};

/// A table over objects: a listing table whose schema merges the schemas of its URLs.
///
/// A clone costs what a listing table clone costs. It copies one configuration
/// and some `Arc` values, and it touches no file. `MaterializedView` holds one
/// table by value.
#[derive(Clone, Debug)]
pub struct FastObjectTable {
    inner: ListingTable,
}

impl FastObjectTable {
    /// Build a table over `urls`. Read the schema of each URL and merge them.
    ///
    /// The session decides the result for a column that two files describe
    /// differently. A caller that needs one rule for every session uses
    /// [`try_new_with_widening`](Self::try_new_with_widening).
    pub async fn try_new(
        state: &SessionState,
        format: Arc<dyn FileFormat>,
        urls: Vec<ListingTableUrl>,
    ) -> Result<Self, DataFusionError> {
        let widening = session_widening(state);
        Self::try_new_with_widening(state, format, urls, widening.strategy.as_ref()).await
    }

    /// The same, with the merge rule named rather than taken from the session.
    ///
    /// A caller names a rule here for two reasons. It needs one rule whatever a
    /// deployment registered. Or it runs outside a configured session. A `read_*`
    /// takes the rule of the session through [`try_new`](Self::try_new). Both
    /// callers reach this method.
    pub async fn try_new_with_widening(
        state: &SessionState,
        format: Arc<dyn FileFormat>,
        urls: Vec<ListingTableUrl>,
        widening: &dyn ArrowTypeWideningStrategy,
    ) -> Result<Self, DataFusionError> {
        let options = ListingOptions::new(format)
            // The format finds its own files. A suffix here must also match the
            // marker of a directory format.
            .with_file_extension("")
            .with_target_partitions(state.config_options().execution.target_partitions)
            // Per-file statistics at plan time cost minutes over a large collection.
            .with_collect_stat(false);

        // One schema per URL, each named after its URL.
        let schemas = super::schema::infer_url_schemas(state, &options, &urls).await?;

        let schema = widening
            .merge_schemas(&schemas)
            .map_err(|e| plan_datafusion_err!("Failed to merge schemas for object table: {}", e))?;

        let config = ListingTableConfig::new_with_multi_paths(urls)
            .with_listing_options(options)
            .with_schema(schema);
        Ok(Self {
            inner: ListingTable::try_new(config)?,
        })
    }

    /// Wrap a listing table that the caller configured.
    ///
    /// `try_new` covers a `read_*` scan. Such a scan knows one format and some
    /// URLs. A `CREATE EXTERNAL TABLE` knows more. It states a schema, partition
    /// columns, a sort order, constraints, column defaults and a statistics
    /// cache, and it builds its own listing table for them. This method keeps
    /// each part of that table.
    pub fn from_listing_table(inner: ListingTable) -> Self {
        Self { inner }
    }

    /// The URLs behind this table, with every glob.
    ///
    /// Query-time authorization reads them. It then resolves the dataset paths of
    /// a `read_*` scan.
    pub fn table_paths(&self) -> &[ListingTableUrl] {
        self.inner.table_paths()
    }

    /// The listing table below. For diagnostics and tests.
    pub fn inner(&self) -> &ListingTable {
        &self.inner
    }

    /// The same table with a schema that holds only `projection`.
    ///
    /// The JSON query API names its columns first. A narrow schema then keeps the
    /// unwanted columns of a wide collection out of the plan. A name outside the
    /// schema has no effect. A projection that selects nothing leaves the schema
    /// as it is, because a table with no column reads nothing.
    ///
    /// The method rebuilds from the paths and the options. It therefore suits a
    /// table from [`try_new`](Self::try_new). A table from
    /// [`from_listing_table`](Self::from_listing_table) loses the constraints and
    /// the column defaults of its caller.
    pub fn with_pushdown_projection(
        &self,
        projection: Vec<String>,
    ) -> Result<Self, DataFusionError> {
        let mut schema = self.inner.schema();
        if !projection.is_empty() {
            let kept: Vec<_> = schema
                .fields()
                .iter()
                .filter(|field| projection.contains(field.name()))
                .map(|field| field.as_ref().clone())
                .collect();
            if !kept.is_empty() {
                schema = Arc::new(Schema::new(kept));
            }
        }

        let config = ListingTableConfig::new_with_multi_paths(self.inner.table_paths().to_vec())
            .with_listing_options(self.inner.options().clone())
            .with_schema(schema);
        Ok(Self {
            inner: ListingTable::try_new(config)?,
        })
    }
}

#[async_trait::async_trait]
impl TableProvider for FastObjectTable {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn table_type(&self) -> TableType {
        self.inner.table_type()
    }

    fn constraints(&self) -> Option<&Constraints> {
        self.inner.constraints()
    }

    fn get_table_definition(&self) -> Option<&str> {
        self.inner.get_table_definition()
    }

    fn get_logical_plan(&self) -> Option<Cow<'_, LogicalPlan>> {
        self.inner.get_logical_plan()
    }

    fn get_column_default(&self, column: &str) -> Option<&Expr> {
        self.inner.get_column_default(column)
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        // The listing table plans the scan. A second implementation here must
        // repeat every decision that it makes. Those decisions cover five points.
        // Which filters prune partition directories. The order that a
        // `WITH ORDER` table promises. The split of groups by statistics, which
        // keeps that order. The expression adapter. The predicate that a format
        // pushes into its own reader.
        self.inner.scan(state, projection, filters, limit).await
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::error::Result<Vec<TableProviderFilterPushDown>> {
        // The answer of the listing table, unchanged. It reports `Exact` for a
        // filter that partition directories settle. It reports `Inexact` for every
        // other filter.
        self.inner.supports_filters_pushdown(filters)
    }

    fn statistics(&self) -> Option<Statistics> {
        self.inner.statistics()
    }

    /// A write goes straight to the listing table.
    ///
    /// A prune is a read concern. It decides which files a scan opens. It says
    /// nothing about the place for new rows. The listing table knows how its
    /// format writes.
    async fn insert_into(
        &self,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        self.inner.insert_into(state, input, insert_op).await
    }
}
