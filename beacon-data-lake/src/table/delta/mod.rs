use std::{collections::HashMap, sync::Arc};

use datafusion::{
    catalog::TableProvider, execution::object_store::ObjectStoreUrl, prelude::SessionContext,
};
use deltalake::{
    DeltaTableConfig,
    delta_datafusion::{DeltaScanConfigBuilder, DeltaTableProvider},
    kernel::EagerSnapshot,
    logstore::{LogStore, StorageConfig, default_logstore},
};
use object_store::path::Path;
use url::Url;

use crate::table::error::TableError;

/// Configuration for mounting an external Delta Lake table.
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct DeltaTable {
    /// Location of the Delta table. Accepts a full URI (e.g. `s3://bucket/path`) or
    /// a path relative to the data lake datasets directory when running on the
    /// local filesystem backend.
    pub location: String,
    /// Optional version to pin the table to.
    #[serde(default)]
    pub version: Option<i64>,
    /// Extra storage options forwarded to the Delta storage backend.
    #[serde(default)]
    pub storage_options: HashMap<String, String>,
}

impl DeltaTable {
    pub async fn create(
        &self,
        _table_directory: object_store::path::Path,
        _session_ctx: Arc<SessionContext>,
    ) -> Result<(), TableError> {
        Ok(())
    }

    fn create_log_store(
        &self,
        store_url: &ObjectStoreUrl,
        data_directory_prefix: &Path,
        session_ctx: Arc<SessionContext>,
    ) -> Result<Arc<dyn LogStore>, TableError> {
        let object_store = session_ctx
            .runtime_env()
            .object_store(store_url.clone())
            .map_err(|e| {
                TableError::GenericTableError(format!("Failed to get object store: {}", e))
            })?;

        let table_path = object_store::path::Path::from(self.location.clone());
        // Merge with data directory prefix if needed
        let mut full_path = data_directory_prefix.clone();
        for part in data_directory_prefix.parts() {
            full_path = full_path.child(part);
        }
        for part in table_path.parts() {
            full_path = full_path.child(part);
        }
        let url = Url::parse(full_path.to_string().as_str()).unwrap();

        let config = StorageConfig::default();
        let prefixed_obj_store =
            config
                .decorate_store(object_store.clone(), &url)
                .map_err(|e| {
                    TableError::GenericTableError(format!("Failed to decorate object store: {}", e))
                })?;

        Ok(default_logstore(
            Arc::new(prefixed_obj_store),
            object_store.clone(),
            &url,
            &config,
        ))
    }

    pub async fn table_provider(
        &self,
        data_directory_store_url: ObjectStoreUrl,
        data_directory_prefix: Path,
        session_ctx: Arc<SessionContext>,
    ) -> Result<Arc<dyn TableProvider>, TableError> {
        let log_store = self.create_log_store(
            &data_directory_store_url,
            &data_directory_prefix,
            session_ctx.clone(),
        )?;

        let is_delta = log_store.is_delta_table_location().await.map_err(|e| {
            TableError::GenericTableError(format!("Failed to verify Delta table location: {}", e))
        })?;
        if !is_delta {
            return Err(TableError::GenericTableError(
                "The specified location is not a valid Delta table".to_string(),
            ));
        }

        let delta_config = DeltaTableConfig::default();
        let eager_snapshot = EagerSnapshot::try_new(&log_store, delta_config.clone(), self.version)
            .await
            .map_err(|e| {
                TableError::GenericTableError(format!(
                    "Failed to create eager snapshot of Delta table: {}",
                    e
                ))
            })?;

        let scan_config = DeltaScanConfigBuilder::default()
            .build(&eager_snapshot)
            .map_err(|e| {
                TableError::GenericTableError(format!("Failed to build Delta scan config: {}", e))
            })?;
        let provider = DeltaTableProvider::try_new(eager_snapshot, log_store, scan_config)
            .map_err(|e| {
                TableError::GenericTableError(format!(
                    "Failed to create Delta table provider: {}",
                    e
                ))
            })?;

        Ok(Arc::new(provider))
    }
}
