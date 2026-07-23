//! Persistence for crawler definitions.
//!
//! Crawler definitions are stored as JSON in the tables object store under a
//! reserved `__crawlers__/` prefix (filename `<name>.json`). This deliberately
//! sidesteps the table loader, which only picks up files named `table.json`, so
//! crawlers and tables never collide. Mirrors [`SchemaPersistenceService`] for tables.

use std::sync::Arc;

use datafusion::{error::DataFusionError, execution::object_store::ObjectStoreUrl};
use futures::StreamExt;
use object_store::{path::Path, ObjectStore, ObjectStoreExt};

use crate::statement_plan::{upgrade_session, SessionCell};

use super::definition::CrawlerDefinition;

/// Reserved prefix for crawler definitions within the tables store.
const CRAWLERS_PREFIX: &str = "__crawlers__";

#[derive(Clone)]
pub struct CrawlerPersistence {
    /// Weak, for the same reason as [`CrawlEngine`](super::engine::CrawlEngine):
    /// the session owns the manager, so the manager must not own the session.
    session: SessionCell,
    db_store_url: ObjectStoreUrl,
}

impl CrawlerPersistence {
    pub(crate) fn new(session: SessionCell, db_store_url: ObjectStoreUrl) -> Self {
        Self {
            session,
            db_store_url,
        }
    }

    fn store(&self) -> Result<Arc<dyn ObjectStore>, DataFusionError> {
        upgrade_session(&self.session, "crawler persistence")
            .map_err(|e| DataFusionError::Plan(e.to_string()))?
            .runtime_env()
            .object_store(&self.db_store_url)
            .map_err(|e| DataFusionError::Plan(format!("crawler store unavailable: {e}")))
    }

    fn path_for(name: &str) -> Path {
        Path::from(format!("{CRAWLERS_PREFIX}/{name}.json"))
    }

    /// Persist (create or overwrite) a crawler definition.
    pub async fn save(&self, def: &CrawlerDefinition) -> anyhow::Result<()> {
        let json = serde_json::to_vec_pretty(def)?;
        self.store()?
            .put(&Self::path_for(&def.name), json.into())
            .await
            .map_err(|e| anyhow::anyhow!("failed to persist crawler '{}': {e}", def.name))?;
        Ok(())
    }

    /// Remove a persisted crawler definition. Missing files are ignored.
    pub async fn delete(&self, name: &str) -> anyhow::Result<()> {
        let store = self.store()?;
        match store.delete(&Self::path_for(name)).await {
            Ok(()) => Ok(()),
            Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(anyhow::anyhow!("failed to delete crawler '{name}': {e}")),
        }
    }

    /// Load every persisted crawler definition. Unreadable entries are logged and skipped.
    pub async fn load_all(&self) -> anyhow::Result<Vec<CrawlerDefinition>> {
        let store = self.store()?;
        let prefix = Path::from(CRAWLERS_PREFIX);

        let locations: Vec<Path> = store
            .list(Some(&prefix))
            .filter_map(|entry| async move {
                match entry {
                    Ok(meta) => Some(meta.location),
                    Err(error) => {
                        tracing::error!("failed to list crawler directory: {error}");
                        None
                    }
                }
            })
            .collect()
            .await;

        let mut crawlers = Vec::new();
        for location in locations {
            if location.extension() != Some("json") {
                continue;
            }
            match store.get(&location).await {
                Ok(payload) => match payload.bytes().await {
                    Ok(bytes) => match serde_json::from_slice::<CrawlerDefinition>(&bytes) {
                        Ok(def) => crawlers.push(def),
                        Err(error) => {
                            tracing::error!("failed to parse crawler {location}: {error}")
                        }
                    },
                    Err(error) => tracing::error!("failed to read crawler {location}: {error}"),
                },
                Err(error) => tracing::error!("failed to fetch crawler {location}: {error}"),
            }
        }
        Ok(crawlers)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;
    use object_store::memory::InMemory;
    use std::collections::HashMap;
    use url::Url;

    /// Returns the session alongside the service: the service holds only a `Weak`,
    /// so the caller must keep the context alive for the duration of the test.
    fn service() -> (CrawlerPersistence, Arc<SessionContext>) {
        let ctx = Arc::new(SessionContext::new());
        let store = Arc::new(InMemory::new());
        let url = ObjectStoreUrl::parse("db://").unwrap();
        ctx.register_object_store(&Url::parse(url.as_str()).unwrap(), store);

        let cell = crate::statement_plan::new_session_cell();
        let _ = cell.set(Arc::downgrade(&ctx));
        (CrawlerPersistence::new(cell, url), ctx)
    }

    fn def(name: &str) -> CrawlerDefinition {
        CrawlerDefinition::from_sql(name, Some("p/".to_string()), &HashMap::new()).unwrap()
    }

    #[tokio::test]
    async fn save_load_delete_roundtrip() {
        let (svc, _ctx) = service();
        svc.save(&def("argo")).await.unwrap();
        svc.save(&def("ctd")).await.unwrap();

        let mut loaded = svc.load_all().await.unwrap();
        loaded.sort_by(|a, b| a.name.cmp(&b.name));
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[0].name, "argo");
        assert_eq!(loaded[1].name, "ctd");

        svc.delete("argo").await.unwrap();
        let after = svc.load_all().await.unwrap();
        assert_eq!(after.len(), 1);
        assert_eq!(after[0].name, "ctd");

        // Deleting a missing crawler is a no-op.
        svc.delete("does-not-exist").await.unwrap();
    }
}
