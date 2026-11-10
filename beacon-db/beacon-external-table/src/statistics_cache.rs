//! The statistics provider interface of external tables.
//!
//! A table that sets `use_statistics_pruning` registers itself with the
//! provider when it is built and when it is refreshed. The provider keeps the
//! statistics of the files of that table. A scan then asks the provider for
//! the statistics of the files it listed.

use std::{collections::HashMap, sync::Arc, time::Duration};

use beacon_datafusion_ext::type_widening::ArrowTypeWideningStrategy;
use datafusion::{
    arrow::datatypes::SchemaRef, common::pruning::PruningStatistics,
    datasource::listing::ListingTableUrl, execution::object_store::ObjectStoreUrl,
};
use futures::future::BoxFuture;
use object_store::ObjectMeta;

/// Statistics that a `PruningPredicate` reads directly.
pub type SharedPruningStatistics = Arc<dyn PruningStatistics + Send + Sync>;

/// What a provider needs to keep the statistics of one external table.
#[derive(Debug, Clone)]
pub struct StatisticsRegistration {
    /// The table name, which identifies the statistics of the table.
    pub table: String,
    /// The store that holds the files.
    pub store_url: ObjectStoreUrl,
    /// The table locations, with their globs. The provider lists them again on each refresh.
    pub urls: Vec<ListingTableUrl>,
    /// The file format identifier (`STORED AS`), for example `parquet`.
    pub file_type: String,
    /// The format options, without the Beacon table options.
    pub format_options: HashMap<String, String>,
    /// The columns of the files, without the partition columns.
    pub schema: SchemaRef,
    /// The columns to keep statistics for. `None` means each column that can hold a range.
    pub columns: Option<Vec<String>>,
    /// The time between two refreshes of the statistics.
    pub refresh_interval: Duration,
    /// The type widening strategy of the table, which decides how a file type casts to the table type.
    pub type_widening: Arc<dyn ArrowTypeWideningStrategy>,
}

/// A source of per-file statistics that an external table scan prunes its files with.
///
/// The scan fails open: an error, or an object without statistics, keeps the files.
pub trait ExternalTableStatisticsProvider: Send + Sync {
    /// Starts to keep the statistics of a table, or updates the registration of
    /// the table, and refreshes the statistics now.
    ///
    /// # Errors
    ///
    /// Returns an error when the registration is not valid, for example when
    /// the table has more statistics columns than the provider allows.
    fn register_table(
        &self,
        registration: StatisticsRegistration,
    ) -> BoxFuture<'static, anyhow::Result<()>>;

    /// Stops the refresh of `table`, and deletes its statistics.
    ///
    /// # Errors
    ///
    /// Returns an error when the statistics cannot be deleted.
    fn drop_table(&self, table: &str) -> BoxFuture<'static, anyhow::Result<()>>;

    /// The statistics of `objects` of `table`, for the columns of `schema`.
    ///
    /// Container `i` of the statistics describes `objects[i]`. An object that the
    /// provider has no statistics for reads as null, so the scan keeps it.
    /// `schema` holds only the columns that the scan needs statistics for.
    ///
    /// # Errors
    ///
    /// Returns an error when the table is not registered, or the read fails.
    fn fetch_statistics(
        &self,
        table: &str,
        objects: &[&ObjectMeta],
        schema: SchemaRef,
    ) -> BoxFuture<'static, anyhow::Result<SharedPruningStatistics>>;
}

/// The session extension that holds the default statistics provider of external tables.
///
/// A table that the builder gets no provider for takes this one.
#[derive(Clone)]
pub struct ExternalTableStatistics(pub Arc<dyn ExternalTableStatisticsProvider>);

impl std::fmt::Debug for ExternalTableStatistics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("ExternalTableStatistics")
            .finish_non_exhaustive()
    }
}
