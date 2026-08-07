//! A [`TableProvider`] wrapper that carries the [`IcebergTableDefinition`] and
//! keeps the registered table current.
//!
//! It does two jobs.
//!
//! **It carries the definition.** `CREATE EXTERNAL TABLE … STORED AS ICEBERG`
//! registers the provider with Beacon's `TableManager`, which persists
//! `table.json` by downcasting the registered provider back to a known
//! definition (see `serialize_table_provider_definition`). An
//! `IcebergStaticTableProvider` cannot carry that, so it is wrapped — the same
//! shape `beacon-delta` and the federated remote table use.
//!
//! **It follows the table.** An Iceberg provider is pinned to the metadata file
//! it was built from, so a table another system keeps writing would freeze at
//! whatever it looked like when Beacon started. The wrapper re-resolves the
//! current metadata file in [`TableProvider::schema`] — the first thing the
//! planner asks for — and every [`TableProvider::scan`] then reads that exact
//! metadata file. So a new snapshot *or a new column* shows up on the next
//! query, with no restart, and a plan never disagrees with the data its scan
//! returns.

use std::any::Any;
use std::sync::{Arc, RwLock};

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{not_impl_err, Statistics};
use datafusion::datasource::TableType;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::TableProviderFilterPushDown;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::Expr;
use object_store::ObjectStore;
use tokio::runtime::{Handle, RuntimeFlavor};

use crate::definition::IcebergTableDefinition;
use crate::provider::{open_iceberg_table, resolve_metadata_location, OpenedTable};

/// The table as one metadata file describes it.
struct CurrentTable {
    /// Path of the metadata file, relative to the table directory.
    metadata_location: String,
    schema: SchemaRef,
    provider: Arc<dyn TableProvider>,
}

/// Wraps an Iceberg [`TableProvider`], pairing it with the Beacon definition
/// that produced it so the catalog can persist and reload the table.
pub struct BeaconIcebergTable {
    definition: IcebergTableDefinition,
    /// The pinned snapshot for time travel, if the table OPTIONS named one.
    snapshot_id: Option<i64>,
    /// Beacon's datasets store, resolved when the table was registered. It comes
    /// from the runtime's object-store registry, which outlives any one session.
    store: Arc<dyn ObjectStore>,
    /// Path of the table directory inside `store`.
    prefix: String,
    current: RwLock<CurrentTable>,
    /// The runtime the refresh runs on. `TableProvider::schema` is synchronous,
    /// and reading a metadata file is not.
    runtime: Handle,
}

impl std::fmt::Debug for CurrentTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CurrentTable")
            .field("metadata_location", &self.metadata_location)
            .finish_non_exhaustive()
    }
}

impl BeaconIcebergTable {
    pub fn new(
        store: Arc<dyn ObjectStore>,
        prefix: String,
        definition: IcebergTableDefinition,
        snapshot_id: Option<i64>,
        opened: OpenedTable,
        runtime: Handle,
    ) -> Self {
        Self {
            definition,
            snapshot_id,
            store,
            prefix,
            current: RwLock::new(CurrentTable {
                metadata_location: opened.metadata_location,
                schema: opened.schema,
                provider: opened.provider,
            }),
            runtime,
        }
    }

    pub fn definition(&self) -> &IcebergTableDefinition {
        &self.definition
    }

    fn current(&self) -> std::sync::RwLockReadGuard<'_, CurrentTable> {
        self.current
            .read()
            .expect("iceberg table state is not poisoned")
    }

    /// Re-resolve the current metadata file and, if it moved, rebuild the
    /// provider from it.
    async fn refresh(&self) -> anyhow::Result<()> {
        let metadata_location = resolve_metadata_location(&self.store, &self.prefix).await?;
        if metadata_location == self.current().metadata_location {
            return Ok(());
        }

        let opened = open_iceberg_table(
            self.store.clone(),
            &self.definition.location,
            Some(&metadata_location),
            self.snapshot_id,
        )
        .await?;

        let mut current = self
            .current
            .write()
            .expect("iceberg table state is not poisoned");
        *current = CurrentTable {
            metadata_location: opened.metadata_location,
            schema: opened.schema,
            provider: opened.provider,
        };
        Ok(())
    }

    /// Refresh from a synchronous caller, on whatever runtime it happens to be.
    ///
    /// A failure here is not fatal: the last known metadata file is still a
    /// valid view of the table, so the query proceeds against it. A
    /// current-thread runtime cannot block at all, and keeps the cached table
    /// too — every Beacon server runs multi-threaded, so this only affects
    /// single-threaded tests.
    fn refresh_blocking(&self) {
        let refreshed = match Handle::try_current() {
            Ok(handle) if handle.runtime_flavor() == RuntimeFlavor::CurrentThread => return,
            Ok(_) => tokio::task::block_in_place(|| self.runtime.block_on(self.refresh())),
            Err(_) => self.runtime.block_on(self.refresh()),
        };
        if let Err(error) = refreshed {
            tracing::warn!(
                table = %self.definition.name,
                location = %self.definition.location,
                "failed to refresh Iceberg table metadata, reading the last known version: {error:#}"
            );
        }
    }
}

#[async_trait::async_trait]
impl TableProvider for BeaconIcebergTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// The schema of the table as of now.
    ///
    /// The planner calls this before it plans, so this is where the table is
    /// re-resolved: a column another system added is selectable on the next
    /// query rather than after a restart.
    fn schema(&self) -> SchemaRef {
        self.refresh_blocking();
        self.current().schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        // Deliberately *not* refreshed: this reads the metadata file the planner
        // resolved in `schema()`, so the projection indexes it computed still
        // name the same columns.
        let provider = self.current().provider.clone();
        provider.scan(state, projection, filters, limit).await
    }

    /// Iceberg prunes data files from the manifests' column statistics, so every
    /// filter is worth pushing down; the scan re-checks the rows it reads.
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::error::Result<Vec<TableProviderFilterPushDown>> {
        self.current().provider.supports_filters_pushdown(filters)
    }

    fn statistics(&self) -> Option<Statistics> {
        self.current().provider.statistics()
    }

    /// Beacon reads Iceberg; it writes none.
    ///
    /// Without this the caller gets DataFusion's default, "Insert into not
    /// implemented for this table", which says nothing about why or what to do
    /// instead. `STORED AS DELTA` right next to it *does* accept `INSERT INTO`,
    /// so the difference is worth naming.
    async fn insert_into(
        &self,
        _state: &dyn Session,
        _input: Arc<dyn ExecutionPlan>,
        _insert_op: InsertOp,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        not_impl_err!(
            "Beacon reads Iceberg table '{}' read-only, so it accepts no INSERT. \
             Write to it with an Iceberg writer such as Spark or PyIceberg, or use \
             a managed table to change rows from Beacon.",
            self.definition.name
        )
    }
}

/// Recover the definition of a registered Iceberg table, if `table` is one.
///
/// Used by the catalog to persist `table.json` and to report the table's
/// configuration.
pub fn iceberg_table_definition(table: &dyn TableProvider) -> Option<IcebergTableDefinition> {
    table
        .as_any()
        .downcast_ref::<BeaconIcebergTable>()
        .map(|table| table.definition().clone())
}

impl std::fmt::Debug for BeaconIcebergTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BeaconIcebergTable")
            .field("name", &self.definition.name)
            .field("location", &self.definition.location)
            .field("snapshot_id", &self.snapshot_id)
            .field("current", &self.current)
            .finish()
    }
}
