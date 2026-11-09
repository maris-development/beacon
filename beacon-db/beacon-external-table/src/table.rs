//! [`ExternalTable`]: the `TableProvider`.

use std::{borrow::Cow, fmt, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    arrow::datatypes::SchemaRef,
    catalog::{Session, TableProvider},
    common::{Constraints, Result, Statistics, not_impl_err},
    datasource::{
        TableType,
        listing::{ListingTable, ListingTableUrl},
    },
    logical_expr::{Expr, LogicalPlan, TableProviderFilterPushDown, dml::InsertOp},
    physical_plan::ExecutionPlan,
};
use object_store::path::Path as ObjectPath;
use parking_lot::RwLock;

use crate::{
    ExternalTableDefinition, ExternalTableSpec,
    scan::{invalidate_table_files, scan_with_statistics},
    statistics_cache::ExternalTableStatisticsProvider,
};

/// An external table over a set of files.
///
/// It holds a listing table over the files of its last build or refresh. A
/// scan lists the files of that listing table, prunes them with the
/// statistics provider, and plans the format over the kept files.
///
/// The table never holds a session and never refreshes itself. The table lives
/// in the session catalog, so a held session forms a cycle. That cycle keeps the
/// object store registry, and the redb lock on `beacon.db`, for the process
/// lifetime. A `REFRESH` and the storage event refresher give a session to
/// [`refresh`](Self::refresh).
#[derive(Clone)]
pub struct ExternalTable {
    spec: Arc<ExternalTableSpec>,
    // The definition and the listing table change together on a refresh.
    state: Arc<RwLock<State>>,
    /// The statistics that prune the files of a scan. Without a provider, a scan reads all files.
    statistics_provider: Option<Arc<dyn ExternalTableStatisticsProvider>>,
}

/// The definition, which holds the one table schema, and the listing table built from it.
struct State {
    definition: ExternalTableDefinition,
    table: Arc<ListingTable>,
}

impl fmt::Debug for ExternalTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExternalTable")
            .field("definition", &self.state.read().definition)
            .field("url", &self.spec.url)
            .finish_non_exhaustive()
    }
}

impl ExternalTable {
    pub(crate) fn new(
        definition: ExternalTableDefinition,
        spec: ExternalTableSpec,
        table: ListingTable,
        statistics_provider: Option<Arc<dyn ExternalTableStatisticsProvider>>,
    ) -> Self {
        Self {
            spec: Arc::new(spec),
            state: Arc::new(RwLock::new(State {
                definition,
                table: Arc::new(table),
            })),
            statistics_provider,
        }
    }

    /// The statistics provider of the table, if set.
    pub fn statistics_provider(&self) -> Option<&Arc<dyn ExternalTableStatisticsProvider>> {
        self.statistics_provider.as_ref()
    }

    /// The current definition. Its schema is the table schema of the last build or refresh.
    pub fn definition(&self) -> ExternalTableDefinition {
        self.state.read().definition.clone()
    }

    /// The resolved inputs that a refresh builds from.
    pub fn spec(&self) -> &ExternalTableSpec {
        &self.spec
    }

    /// A snapshot of the current listing table.
    pub fn listing_table(&self) -> Arc<ListingTable> {
        Arc::clone(&self.state.read().table)
    }

    /// The table locations, with their globs.
    pub fn table_paths(&self) -> Vec<ListingTableUrl> {
        self.listing_table().table_paths().clone()
    }

    /// The storage prefixes that the table lists.
    pub fn prefixes(&self) -> Vec<ObjectPath> {
        self.table_paths()
            .iter()
            .map(|url| url.prefix().clone())
            .collect()
    }

    /// Whether `path` is below one of the table prefixes. A change to such a
    /// path makes the file list of the table stale.
    pub fn covers(&self, path: &ObjectPath) -> bool {
        self.prefixes()
            .iter()
            .any(|prefix| path_under_prefix(prefix, path))
    }

    /// Lists the files again, infers an inferred schema again and swaps in the new state.
    ///
    /// The caller stores [`definition`](Self::definition) after this call, so
    /// that the next startup uses the new schema.
    pub async fn refresh(&self, session: &dyn Session) -> anyhow::Result<()> {
        // Drop the cached file list of this table, so that the build lists the current files.
        if let Some(cache) = session.runtime_env().cache_manager.get_list_files_cache() {
            cache.drop_table_entries(self.spec.url.get_table_ref())?;
        }
        let mut definition = self.definition();
        let infer = definition.schema_inferred();
        let (table, schema) = self.spec.build_table(session, &definition, infer).await?;
        definition.schema = schema;
        let prune = definition.use_statistics_pruning;
        let table = Arc::new(table);
        *self.state.write() = State {
            definition,
            table: Arc::clone(&table),
        };
        // The provider queues new and changed files, so the next scans can prune them.
        if let Some(provider) = self.statistics_provider.as_ref().filter(|_| prune)
            && let Err(error) = invalidate_table_files(session, &table, provider.as_ref()).await
        {
            tracing::warn!(%error, "the statistics provider did not get the refreshed files");
        }
        Ok(())
    }
}

/// Whether `path` is strictly below `prefix`.
///
/// The match compares whole segments, so `data/example` does not match
/// `data/example_2/...`. An empty prefix (the store root) matches all non-empty paths.
fn path_under_prefix(prefix: &ObjectPath, path: &ObjectPath) -> bool {
    let prefix_parts: Vec<_> = prefix.parts().collect();
    let path_parts: Vec<_> = path.parts().collect();
    path_parts.len() > prefix_parts.len()
        && prefix_parts.iter().zip(&path_parts).all(|(a, b)| a == b)
}

#[async_trait]
impl TableProvider for ExternalTable {
    fn schema(&self) -> SchemaRef {
        self.state.read().table.schema()
    }

    fn constraints(&self) -> Option<&Constraints> {
        (!self.spec.constraints.is_empty()).then_some(&self.spec.constraints)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn get_table_definition(&self) -> Option<&str> {
        self.spec.definition_sql.as_deref()
    }

    fn get_logical_plan(&self) -> Option<Cow<'_, LogicalPlan>> {
        None
    }

    fn get_column_default(&self, column: &str) -> Option<&Expr> {
        self.spec.column_defaults.get(column)
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let (table, prune, type_widening, skip_errors) = {
            let current = self.state.read();
            (
                Arc::clone(&current.table),
                current.definition.use_statistics_pruning,
                Arc::clone(&current.definition.type_widening),
                current.definition.skip_invalid_files_on_scan,
            )
        };
        let provider = self.statistics_provider.as_ref().filter(|_| prune);
        scan_with_statistics(
            state,
            &table,
            provider,
            type_widening,
            skip_errors,
            projection,
            filters,
            limit,
        )
        .await
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        // A prune removes whole files, never rows, so the listing table answer stays correct.
        self.state.read().table.supports_filters_pushdown(filters)
    }

    fn statistics(&self) -> Option<Statistics> {
        self.state.read().table.statistics()
    }

    async fn insert_into(
        &self,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.listing_table()
            .insert_into(state, input, insert_op)
            .await
    }

    async fn delete_from(
        &self,
        _state: &dyn Session,
        _filters: Vec<Expr>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // ToDo: Implement proper delete handling for external tables. Currently not supported.
        not_impl_err!("DELETE not supported for {} table", self.table_type())
    }

    async fn update(
        &self,
        _state: &dyn Session,
        _assignments: Vec<(String, Expr)>,
        _filters: Vec<Expr>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // ToDo: Implement proper update handling for external tables. Currently not supported.
        not_impl_err!("UPDATE not supported for {} table", self.table_type())
    }

    async fn truncate(&self, _state: &dyn Session) -> Result<Arc<dyn ExecutionPlan>> {
        // ToDo: Implement proper truncate handling for external tables. Currently not supported.
        not_impl_err!("TRUNCATE not supported for {} table", self.table_type())
    }
}

#[cfg(test)]
mod tests {
    use super::path_under_prefix;
    use object_store::path::Path as ObjectPath;

    #[test]
    fn path_under_prefix_is_segment_aware() {
        let prefix = ObjectPath::from("data/example");
        assert!(path_under_prefix(
            &prefix,
            &ObjectPath::from("data/example/a.nc")
        ));
        assert!(path_under_prefix(
            &prefix,
            &ObjectPath::from("data/example/x/a.nc")
        ));
        assert!(!path_under_prefix(
            &prefix,
            &ObjectPath::from("data/example_2/a.nc")
        ));
    }

    #[test]
    fn path_under_prefix_requires_a_strictly_deeper_path() {
        let prefix = ObjectPath::from("data/example");
        assert!(!path_under_prefix(
            &prefix,
            &ObjectPath::from("data/example")
        ));
        assert!(!path_under_prefix(&prefix, &ObjectPath::from("data")));
    }

    #[test]
    fn empty_prefix_matches_any_non_empty_path() {
        let root = ObjectPath::from("");
        assert!(path_under_prefix(&root, &ObjectPath::from("a.nc")));
        assert!(!path_under_prefix(&root, &ObjectPath::from("")));
    }
}
