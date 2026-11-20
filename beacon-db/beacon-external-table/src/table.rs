//! [`ExternalTable`]: the `TableProvider`.

use std::{borrow::Cow, collections::HashSet, fmt, sync::Arc};

use anyhow::Context;
use async_trait::async_trait;
use datafusion::{
    arrow::datatypes::{FieldRef, Schema, SchemaRef},
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
    error::ErrorContext,
    scan::scan_with_statistics,
    scan_errors::{OnScanError, ScanErrorPolicy},
    statistics_cache::{ExternalTableStatisticsProvider, StatisticsRegistration},
    statistics_columns_from_options, statistics_refresh_interval_from_options,
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
        *self.state.write() = State {
            definition,
            table: Arc::new(table),
        };
        // The registration carries the new schema, and makes the provider refresh now.
        self.register_statistics()
            .await
            .context("the table is refreshed, but its statistics registration failed")
    }

    /// The registration of the table with a statistics provider, from the current state.
    ///
    /// The schema leaves out the partition columns: a file holds no statistics for them.
    ///
    /// # Errors
    ///
    /// Returns an error when the statistics options of the table are not valid.
    pub fn statistics_registration(&self) -> Result<StatisticsRegistration> {
        let current = self.state.read();
        let definition = &current.definition;
        let partition_columns: HashSet<&str> = current
            .table
            .options()
            .table_partition_cols
            .iter()
            .map(|(name, _)| name.as_str())
            .collect();
        let table_schema = current.table.schema();
        let fields: Vec<FieldRef> = table_schema
            .fields()
            .iter()
            .filter(|field| !partition_columns.contains(field.name().as_str()))
            .cloned()
            .collect();
        Ok(StatisticsRegistration {
            table: definition.name.clone(),
            store_url: self.spec.url.object_store(),
            urls: current.table.table_paths().clone(),
            file_type: definition.file_type.to_lowercase(),
            format_options: definition.format_options(),
            schema: Arc::new(Schema::new_with_metadata(
                fields,
                table_schema.metadata().clone(),
            )),
            columns: statistics_columns_from_options(&definition.options)?,
            refresh_interval: statistics_refresh_interval_from_options(&definition.options)?,
            type_widening: Arc::clone(&definition.type_widening),
        })
    }

    /// Registers the table with its statistics provider, when the table prunes with statistics.
    ///
    /// # Errors
    ///
    /// Returns an error when the statistics options are not valid, or the provider refuses the table.
    pub(crate) async fn register_statistics(&self) -> anyhow::Result<()> {
        let Some(provider) = self.pruning_provider() else {
            return Ok(());
        };
        provider
            .register_table(self.statistics_registration()?)
            .await
    }

    /// Stops the statistics refresh of the table, and deletes its statistics.
    ///
    /// A drop of the table calls this. A table without statistics pruning does nothing.
    ///
    /// # Errors
    ///
    /// Returns an error when the provider cannot delete the statistics.
    pub async fn drop_statistics(&self) -> anyhow::Result<()> {
        let Some(provider) = self.pruning_provider() else {
            return Ok(());
        };
        // The name is read before the await, so no lock is held across it.
        let name = self.state.read().definition.name.clone();
        provider.drop_table(&name).await
    }

    /// The statistics provider, when the table prunes with statistics.
    fn pruning_provider(&self) -> Option<Arc<dyn ExternalTableStatisticsProvider>> {
        let prune = self.state.read().definition.use_statistics_pruning;
        self.statistics_provider.clone().filter(|_| prune)
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
        let (table, name, type_widening, errors) = {
            let current = self.state.read();
            let definition = &current.definition;
            let on_error = if definition.skip_invalid_files_on_scan {
                OnScanError::Skip
            } else {
                OnScanError::Fail
            };
            let context = ErrorContext::new(&definition.name, &definition.file_type.to_lowercase());
            (
                Arc::clone(&current.table),
                definition.name.clone(),
                Arc::clone(&definition.type_widening),
                ScanErrorPolicy::new(context, on_error),
            )
        };
        let provider = self.pruning_provider();
        let statistics = provider
            .as_deref()
            .map(|provider| (provider, name.as_str()));
        scan_with_statistics(
            state,
            &table,
            statistics,
            type_widening,
            errors,
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
