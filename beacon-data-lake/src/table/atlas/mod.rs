use std::{
    any::Any,
    collections::HashSet,
    sync::{Arc, atomic::AtomicI64},
};

use arrow::datatypes::{Schema, SchemaRef};
use beacon_atlas::collection::AtlasCollection;
use beacon_atlas::datafusion::AtlasFormat;
use beacon_atlas::partition::ops::delete::DeleteDatasetsPartition;
use beacon_atlas::schema::AtlasSuperTypingMode;
use beacon_common::listing_url::parse_listing_table_url;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::Statistics;
use datafusion::datasource::TableType as DataFusionTableType;
use datafusion::datasource::listing::{ListingOptions, ListingTable, ListingTableConfig};
use datafusion::error::DataFusionError;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::{TableProviderFilterPushDown, dml::InsertOp};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::Expr;
use datafusion::prelude::SessionContext;
use futures::StreamExt;
use object_store::ObjectStore;

pub mod feed;

#[derive(serde::Deserialize, serde::Serialize)]
pub struct AtlasTable {
    pub feeds: Vec<Box<dyn feed::AtlasFeed + Send + Sync>>,
    #[serde(default)]
    pub resync_interval_seconds: Option<u64>,
    pub sync_on_startup: bool,
    pub last_resync_time: AtomicI64,
    // Directory within the datasets store where the Atlas Collection will be stored. This is used to determine the path for the collection's metadata and partitions.
    pub atlas_directory: String,
}

impl std::fmt::Debug for AtlasTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AtlasTable")
            .field("feeds_len", &self.feeds.len())
            .field("resync_interval_seconds", &self.resync_interval_seconds)
            .field("sync_on_startup", &self.sync_on_startup)
            .field("atlas_directory", &self.atlas_directory)
            .finish()
    }
}

impl AtlasTable {
    pub async fn create<S: ObjectStore + Clone>(
        &self,
        store: &S,
        path: object_store::path::Path,
    ) -> anyhow::Result<()> {
        let metadata_path = path.child("atlas.json");

        match store.get(&metadata_path).await {
            Ok(_) => {
                anyhow::bail!("Atlas collection already exists at '{}'", path);
            }
            Err(object_store::Error::NotFound { .. }) => {}
            Err(error) => {
                return Err(anyhow::anyhow!(
                    "Failed to check Atlas metadata at '{}': {}",
                    metadata_path,
                    error
                ));
            }
        }

        AtlasCollection::create(
            (*store).clone(),
            path,
            self.atlas_directory.clone(),
            None,
            AtlasSuperTypingMode::General,
        )
        .await?;

        Ok(())
    }

    pub async fn resync(&self) -> anyhow::Result<()> {
        todo!()
    }

    pub fn update_last_resync_time(&self) {
        let now = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
        self.last_resync_time
            .store(now, std::sync::atomic::Ordering::SeqCst);
    }

    pub fn last_resync_time(&self) -> Option<chrono::DateTime<chrono::Utc>> {
        let timestamp = self
            .last_resync_time
            .load(std::sync::atomic::Ordering::SeqCst);
        if timestamp == 0 {
            None
        } else {
            let secs = timestamp.div_euclid(1_000_000_000);
            let nanos = timestamp.rem_euclid(1_000_000_000) as u32;
            chrono::DateTime::<chrono::Utc>::from_timestamp(secs, nanos)
        }
    }
}

pub struct AtlasTableProvider {
    atlas_table: Arc<AtlasTable>,
    session_ctx: Arc<SessionContext>,
}

#[async_trait::async_trait]
impl TableProvider for AtlasTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        todo!()
    }

    fn table_type(&self) -> DataFusionTableType {
        DataFusionTableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        todo!()
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::error::Result<Vec<TableProviderFilterPushDown>> {
        // Since the AtlasTable itself doesn't implement filter pushdown, we return "Unsupported" for all filters. The actual filter pushdown capabilities will be determined by the runtime provider during query execution.
        Ok(vec![
            TableProviderFilterPushDown::Unsupported;
            filters.len()
        ])
    }

    fn statistics(&self) -> Option<Statistics> {
        None
    }
}
