//! The runtime [`ExternalTable`] provider and its event-driven self-refresh.

use std::borrow::Cow;
use std::sync::{Arc, Weak};

use beacon_object_storage::event::ObjectEvent;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{Constraints, DataFusionError, Statistics, not_impl_err};
use datafusion::datasource::TableType;
use datafusion::datasource::listing::ListingTable;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::{LogicalPlan, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::{Expr, SessionContext};
use parking_lot::RwLock;
use tokio::sync::broadcast::Receiver;
use tokio::sync::broadcast::error::RecvError;
use tokio::task::JoinHandle;

use super::external_definition::ExternalTableDefinition;
use super::listing::{ExternalTableRebuild, build_listing_table};

/// Aborts the background refresh tasks when the last clone of the owning
/// [`ExternalTable`] is dropped.
#[derive(Debug)]
pub struct RefreshListener {
    handles: Vec<JoinHandle<()>>,
}

impl Drop for RefreshListener {
    fn drop(&mut self) {
        for handle in &self.handles {
            handle.abort();
        }
    }
}

/// Rebuild the inner listing table from `rebuild` and swap it in.
///
/// A no-op (returning `Ok`) when the owning session context has been dropped.
async fn rebuild_into(
    inner: &Arc<RwLock<Arc<ListingTable>>>,
    rebuild: &ExternalTableRebuild,
    ctx: &Weak<SessionContext>,
) -> anyhow::Result<()> {
    let Some(context) = ctx.upgrade() else {
        return Ok(());
    };
    let state = context.state();
    let table = build_listing_table(&state, rebuild).await?;
    *inner.write() = Arc::new(table);
    Ok(())
}

#[derive(Clone, Debug)]
pub struct ExternalTable {
    definition: ExternalTableDefinition,
    inner: Arc<RwLock<Arc<ListingTable>>>,
    rebuild: Arc<ExternalTableRebuild>,
    ctx: Weak<SessionContext>,
    _listener: Option<Arc<RefreshListener>>,
}

impl ExternalTable {
    /// Create a self-refreshing external table.
    ///
    /// `event_receivers` holds one datasets-store subscription per storage prefix
    /// (already filtered to that prefix by the store, so no further filtering is
    /// done here). On any event the table re-infers its schema and re-lists files.
    /// An empty `event_receivers` (e.g. a store that emits no events, or the
    /// `file://` store used in tests) leaves the table to update only on manual
    /// `REFRESH` or the periodic table rescan.
    ///
    /// Events from all receivers are funneled through a single coalescing channel
    /// into one rebuild loop, so refreshes are serialized and bursts collapse to a
    /// single rebuild.
    pub fn new_self_refreshing(
        definition: ExternalTableDefinition,
        initial: ListingTable,
        rebuild: ExternalTableRebuild,
        ctx: Weak<SessionContext>,
        event_receivers: Vec<Receiver<ObjectEvent>>,
    ) -> Self {
        let inner = Arc::new(RwLock::new(Arc::new(initial)));
        let rebuild = Arc::new(rebuild);

        let listener = if event_receivers.is_empty() {
            None
        } else {
            let table_name = definition.name.clone();
            // Capacity-1 coalescing channel: a queued refresh already covers any
            // newer event, so `try_send` may drop when the slot is occupied.
            let (refresh_tx, mut refresh_rx) = tokio::sync::mpsc::channel::<()>(1);
            let mut handles = Vec::with_capacity(event_receivers.len() + 1);

            // One forwarder per prefix receiver: any store-filtered event (or a
            // lag, which may have dropped changes) requests a refresh.
            for mut rx in event_receivers {
                let refresh_tx = refresh_tx.clone();
                let table_name = table_name.clone();
                handles.push(tokio::spawn(async move {
                    loop {
                        match rx.recv().await {
                            Ok(_) => {
                                let _ = refresh_tx.try_send(());
                            }
                            Err(RecvError::Lagged(skipped)) => {
                                tracing::warn!(
                                    table = %table_name,
                                    skipped,
                                    "external table event subscriber lagged; requesting a full refresh"
                                );
                                let _ = refresh_tx.try_send(());
                            }
                            Err(RecvError::Closed) => break,
                        }
                    }
                }));
            }
            // The forwarders hold their own sender clones; drop this one so the
            // consumer ends once every forwarder has stopped.
            drop(refresh_tx);

            // Single consumer = the sole rebuild loop, so rebuilds never race.
            let inner = inner.clone();
            let rebuild = rebuild.clone();
            let ctx = ctx.clone();
            handles.push(tokio::spawn(async move {
                while refresh_rx.recv().await.is_some() {
                    if let Err(error) = rebuild_into(&inner, &rebuild, &ctx).await {
                        tracing::warn!(table = %table_name, %error, "external table self-refresh failed");
                    } else {
                        tracing::info!(table = %table_name, "external table refreshed after storage event");
                    }
                }
            }));

            Some(Arc::new(RefreshListener { handles }))
        };

        Self {
            definition,
            inner,
            rebuild,
            ctx,
            _listener: listener,
        }
    }

    pub fn definition(&self) -> &ExternalTableDefinition {
        &self.definition
    }

    /// A snapshot of the current inner listing table.
    pub fn inner(&self) -> Arc<ListingTable> {
        self.inner.read().clone()
    }

    /// Re-infer the schema over all current objects and swap in a fresh listing.
    pub async fn refresh(&self) -> anyhow::Result<()> {
        rebuild_into(&self.inner, &self.rebuild, &self.ctx).await
    }
}

#[async_trait::async_trait]
impl TableProvider for ExternalTable {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.inner.read().schema()
    }

    fn constraints(&self) -> Option<&Constraints> {
        (!self.rebuild.constraints.is_empty()).then_some(&self.rebuild.constraints)
    }

    /// Get the type of this table for metadata/catalog purposes.
    fn table_type(&self) -> TableType {
        self.inner.read().table_type()
    }

    /// Get the create statement used to create this table, if available.
    fn get_table_definition(&self) -> Option<&str> {
        self.definition.definition.as_deref()
    }

    /// Get the [`LogicalPlan`] of this table, if available.
    ///
    /// Listing-backed external tables have no logical plan.
    fn get_logical_plan(&'_ self) -> Option<Cow<'_, LogicalPlan>> {
        None
    }

    /// Get the default value for a column, if available.
    fn get_column_default(&self, column: &str) -> Option<&Expr> {
        self.rebuild.column_defaults.get(column)
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let inner = self.inner.read().clone();
        inner
            .scan(state, projection, filters, limit)
            .await
            .map_err(|e| DataFusionError::Execution(format!("ExternalTable scan error: {e}")))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::error::Result<Vec<TableProviderFilterPushDown>> {
        self.inner.read().supports_filters_pushdown(filters)
    }

    fn statistics(&self) -> Option<Statistics> {
        self.inner.read().statistics()
    }

    async fn insert_into(
        &self,
        _state: &dyn Session,
        _input: Arc<dyn ExecutionPlan>,
        _insert_op: InsertOp,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        not_impl_err!("Insert into ExternalTable is not supported")
    }
}

#[cfg(test)]
/// Tests for self-refreshing external tables (manual `REFRESH` and event-driven).
mod self_refresh_tests {
    use super::super::events::DATASETS_STORE_URL;
    use super::ExternalTable;
    use crate::table_ext::{ExternalTableDefinition, ExternalTableRebuild, build_listing_table};
    use beacon_common::listing_url::parse_listing_table_url;
    use beacon_object_storage::event::ObjectEvent;
    use datafusion::arrow::array::{Array, Int64Array};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::catalog::TableProvider;
    use datafusion::common::Constraints;
    use datafusion::datasource::file_format::parquet::ParquetFormat;
    use datafusion::datasource::listing::ListingOptions;
    use datafusion::execution::object_store::ObjectStoreUrl;
    use datafusion::parquet::arrow::ArrowWriter;
    use datafusion::prelude::SessionContext;
    use object_store::local::LocalFileSystem;
    use object_store::path::Path as ObjectPath;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::broadcast;
    use tokio::sync::broadcast::Receiver;

    fn write_parquet_i64(disk_path: &std::path::Path, values: &[i64]) {
        std::fs::create_dir_all(disk_path.parent().expect("path has parent"))
            .expect("create parent dirs");
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(values.to_vec()))],
        )
        .expect("build batch");
        let file = std::fs::File::create(disk_path).expect("create file");
        let mut writer = ArrowWriter::try_new(file, schema, None).expect("arrow writer");
        writer.write(&batch).expect("write batch");
        writer.close().expect("close writer");
    }

    async fn count_rows(ctx: &SessionContext) -> i64 {
        let df = ctx.sql("SELECT count(*) FROM obs").await.expect("plan");
        let batches = df.collect().await.expect("collect");
        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64 count")
            .value(0)
    }

    fn ctx_with_datasets(dir: &std::path::Path) -> Arc<SessionContext> {
        let ctx = Arc::new(SessionContext::new());
        let store_url = ObjectStoreUrl::parse(DATASETS_STORE_URL).unwrap();
        let store = Arc::new(LocalFileSystem::new_with_prefix(dir).expect("local store"));
        ctx.register_object_store(store_url.as_ref(), store);
        ctx
    }

    async fn build_external(
        ctx: &Arc<SessionContext>,
        events: Vec<Receiver<ObjectEvent>>,
    ) -> ExternalTable {
        let store_url = ObjectStoreUrl::parse(DATASETS_STORE_URL).unwrap();
        let listing_table_url =
            parse_listing_table_url(&store_url, "obs/**/*.parquet").expect("listing url");
        let options =
            ListingOptions::new(Arc::new(ParquetFormat::default())).with_file_extension("");
        let rebuild = ExternalTableRebuild {
            listing_table_url,
            options,
            provided_schema: None,
            constraints: Constraints::default(),
            column_defaults: HashMap::new(),
            definition_sql: None,
        };
        let state = ctx.state();
        let initial = build_listing_table(&state, &rebuild)
            .await
            .expect("initial listing table");
        let definition = ExternalTableDefinition {
            name: "obs".to_string(),
            location: "obs/**/*.parquet".to_string(),
            file_type: "parquet".to_string(),
            schema: Arc::new(Schema::empty()),
            definition: None,
            partition_cols: vec![],
            options: HashMap::new(),
            if_not_exists: false,
        };
        ExternalTable::new_self_refreshing(
            definition,
            initial,
            rebuild,
            Arc::downgrade(ctx),
            events,
        )
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn manual_refresh_reinfers_and_empties() {
        let dir = tempfile::tempdir().unwrap();
        write_parquet_i64(&dir.path().join("obs/a.parquet"), &[1]);
        let ctx = ctx_with_datasets(dir.path());

        let external = build_external(&ctx, vec![]).await;
        ctx.register_table("obs", Arc::new(external.clone()))
            .unwrap();
        assert_eq!(count_rows(&ctx).await, 1);

        // A new file appears; manual refresh re-lists and picks it up.
        write_parquet_i64(&dir.path().join("obs/b.parquet"), &[2, 3]);
        external.refresh().await.unwrap();
        assert_eq!(count_rows(&ctx).await, 3);

        // All files removed; schema falls back to empty.
        std::fs::remove_file(dir.path().join("obs/a.parquet")).unwrap();
        std::fs::remove_file(dir.path().join("obs/b.parquet")).unwrap();
        external.refresh().await.unwrap();
        assert!(external.schema().fields().is_empty());
        assert_eq!(count_rows(&ctx).await, 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn storage_event_triggers_self_refresh() {
        let dir = tempfile::tempdir().unwrap();
        write_parquet_i64(&dir.path().join("obs/a.parquet"), &[1]);
        write_parquet_i64(&dir.path().join("obs/b.parquet"), &[2, 3]);
        let ctx = ctx_with_datasets(dir.path());

        // The injected receiver simulates the store's already-prefix-filtered
        // stream for this table; prefix filtering itself is the store's job
        // (covered by beacon-object-storage's subscription tests).
        let (tx, rx) = broadcast::channel::<ObjectEvent>(16);
        let external = build_external(&ctx, vec![rx]).await;
        ctx.register_table("obs", Arc::new(external)).unwrap();
        assert_eq!(count_rows(&ctx).await, 3);

        // A file under the table prefix is removed; the table's own listener
        // refreshes it without any table-manager involvement.
        std::fs::remove_file(dir.path().join("obs/b.parquet")).unwrap();
        tx.send(ObjectEvent::Deleted(ObjectPath::from("obs/b.parquet")))
            .unwrap();

        let mut count = count_rows(&ctx).await;
        for _ in 0..50 {
            if count == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
            count = count_rows(&ctx).await;
        }
        assert_eq!(
            count, 1,
            "event under the table prefix should refresh the table"
        );
    }
}
