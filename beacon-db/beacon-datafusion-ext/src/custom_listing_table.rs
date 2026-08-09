//! Beacon's listing table: super-typed schemas, registry-planned scans.
//!
//! The provider behind every SQL `read_*` function. It differs from a plain
//! `ListingTable` twice, once at each end of its life:
//!
//! - **At creation** the schemas of its URLs are merged through the session's
//!   [`ArrowTypeWidening`] strategy, so files that disagree on a column's
//!   width still form one table. Beacon provides the default strategy —
//!   super typing, where `Int32` + `Int64` reads as `Int64` — and a deployment
//!   may register its own rules through the same extension.
//! - **At scan** the file list is planned from the file-statistics registry
//!   when it can be ([`registry_listing`](crate::registry_listing)), which
//!   lists no store and opens no file. When it cannot, the inner
//!   `ListingTable` plans as always and
//!   [`prune_scan`](beacon_file_stats::prune_scan) drops the files whose
//!   recorded ranges cannot match.
//!
//! `beacon-common`'s `SuperListingTable` is this provider's predecessor with
//! the widening hard-coded and no registry path; it remains for callers that
//! cannot reach this crate.

use std::{any::Any, borrow::Cow, sync::Arc};

use datafusion::{
    arrow::datatypes::SchemaRef,
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

use crate::registry_listing;
use crate::type_widening::{ArrowTypeWidening, SuperTypeWidening};

#[derive(Debug)]
pub struct CustomListingTable {
    inner_table: ListingTable,
}

impl CustomListingTable {
    pub async fn new(
        session_state: &SessionState,
        file_format: Arc<dyn FileFormat>,
        table_urls: Vec<ListingTableUrl>,
    ) -> Result<Self, DataFusionError> {
        let listing_options = ListingOptions::new(file_format)
            .with_file_extension("")
            .with_target_partitions(session_state.config_options().execution.target_partitions)
            .with_collect_stat(true);

        let mut schemas = vec![];
        for table_url in &table_urls {
            tracing::debug!("Infer schema for table/file url: {}", table_url);
            let schema = listing_options
                .infer_schema(session_state, table_url)
                .await?;
            schemas.push(schema);
        }

        // The session decides how a column's diverging types merge. Beacon
        // registers super typing at startup, keeping the behaviour this
        // provider's predecessor hard-coded; a session without the extension
        // (a test, an embedded use) gets the same strategy here rather than a
        // different behaviour.
        let widening = session_state
            .config()
            .get_extension::<ArrowTypeWidening>()
            .unwrap_or_else(|| Arc::new(ArrowTypeWidening::new(Arc::new(SuperTypeWidening))));
        let merged_schema = widening.merge_schemas(&schemas).map_err(|e| {
            plan_datafusion_err!("Failed to merge schemas for listing table: {}", e)
        })?;

        let config = ListingTableConfig::new_with_multi_paths(table_urls)
            .with_listing_options(listing_options)
            .with_schema(merged_schema);
        let table = ListingTable::try_new(config)?;

        Ok(Self { inner_table: table })
    }

    /// The listing URLs (including any globs) backing this table.
    ///
    /// Used by query-time authorization to resolve the dataset paths a
    /// `read_*` scan reads.
    pub fn table_paths(&self) -> &[ListingTableUrl] {
        self.inner_table.table_paths()
    }
}

#[async_trait::async_trait]
impl TableProvider for CustomListingTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.inner_table.schema()
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

    fn get_logical_plan(&'_ self) -> Option<Cow<'_, LogicalPlan>> {
        self.inner_table.get_logical_plan()
    }

    fn get_column_default(&self, column: &str) -> Option<&Expr> {
        self.inner_table.get_column_default(column)
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        // The registry path lists no store and opens no file, and it prunes
        // before the file list exists. `None` means it cannot serve this
        // table, and the listing path below is exactly what would have run.
        if let Some(plan) = registry_listing::try_scan_from_registry(
            state,
            &self.inner_table,
            projection,
            filters,
            limit,
        )
        .await
        {
            return Ok(plan);
        }

        let plan = self
            .inner_table
            .scan(state, projection, filters, limit)
            .await?;

        // Drop the files whose recorded ranges say they cannot match. Done on
        // the built plan, so all of `ListingTable`'s listing, partition and
        // ordering logic still runs and only the file list changes.
        Ok(beacon_file_stats::prune_scan(state, plan, filters, self.schema()).await)
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
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        self.inner_table.insert_into(state, input, insert_op).await
    }
}
