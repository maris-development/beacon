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
use datafusion::datasource::{TableType as DataFusionTableType, listing::ListingTableUrl};
use datafusion::datasource::{
    file_format::parquet::ParquetFormatFactory,
    listing::{ListingOptions, ListingTable, ListingTableConfig},
};
use datafusion::error::DataFusionError;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::{TableProviderFilterPushDown, dml::InsertOp};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::Expr;
use datafusion::prelude::SessionContext;
use datafusion::{
    catalog::{Session, TableProvider},
    physical_expr_adapter::DefaultPhysicalExprAdapter,
};
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

    pub async fn resync(&self, session: Arc<dyn Session>) -> anyhow::Result<()> {
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

    pub async fn table_provider(
        &self,
        session: &dyn Session,
        object_store_url: &ObjectStoreUrl,
    ) -> anyhow::Result<Arc<dyn TableProvider>> {
        let atlas_path =
            object_store::path::Path::parse(&self.atlas_directory)?.child("atlas.json");
        let listing_url = parse_listing_table_url(object_store_url, atlas_path.as_ref())?;
        // Listing Table
        let listing_options = ListingOptions::new(Arc::new(AtlasFormat::default()));
        let schema = listing_options.infer_schema(session, &listing_url).await?;
        let config = ListingTableConfig::new(listing_url)
            .with_listing_options(listing_options)
            .with_schema(schema);
        let table = ListingTable::try_new(config)?;

        Ok(Arc::new(table))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use datafusion::assert_batches_eq;
    use datafusion::datasource::listing_table_factory::ListingTableFactory;
    use datafusion::error::Result;
    use datafusion::prelude::SessionContext;
    use tempfile::TempDir;

    #[tokio::test]
    async fn create_external_parquet_table_before_files_exist() -> Result<()> {
        let ctx = SessionContext::new();

        // Create an empty directory. No parquet files exist yet.
        let tmp = TempDir::new()?;
        let table_dir = tmp.path().join("parquet_table");
        std::fs::create_dir_all(&table_dir)?;

        // Important:
        // - use an explicit schema
        // - point LOCATION at the directory, with trailing slash
        let sql = format!(
            "CREATE EXTERNAL TABLE t
            STORED AS PARQUET
            LOCATION '{}'",
            format!("{}", table_dir.display())
        );

        ctx.sql(&sql).await.unwrap().collect().await.unwrap();

        // The table should exist and be queryable even though no parquet files exist yet.
        let df = ctx.sql("DESCRIBE t").await.unwrap();
        let batches = df.collect().await.unwrap();
        println!(
            "{}",
            arrow::util::pretty::pretty_format_batches(&batches).unwrap()
        );

        // // Optional: prove inserts work and create parquet output in that location
        // ctx.sql("INSERT INTO t VALUES (1, 'a'), (2, 'b')")
        //     .await?
        //     .collect()
        //     .await?;

        // let df = ctx.sql("SELECT id, name FROM t ORDER BY id").await?;
        // let batches = df.collect().await?;
        // assert_batches_eq!(
        //     &[
        //         "+----+------+",
        //         "| id | name |",
        //         "+----+------+",
        //         "| 1  | a    |",
        //         "| 2  | b    |",
        //         "+----+------+",
        //     ],
        //     &batches
        // );

        Ok(())
    }
}
