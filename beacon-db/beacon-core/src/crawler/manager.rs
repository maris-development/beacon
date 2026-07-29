//! Owns crawler definitions and their background triggers.
//!
//! The manager loads persisted crawlers at startup, runs them on demand, and (when
//! enabled) drives scheduled crawls. Background tasks hold a [`Weak`]
//! back-reference so the manager can be dropped; [`Drop`] aborts them.

use std::collections::HashMap;
use std::sync::{Arc, OnceLock, Weak};
use std::time::Duration;

use beacon_common::CrawlerConfig;
use beacon_datafusion_ext::format_ext::FileFormatFactoryExt;
use datafusion::execution::object_store::ObjectStoreUrl;
use parking_lot::Mutex;
use tokio::sync::Mutex as AsyncMutex;
use tokio::task::JoinHandle;

use crate::statement_plan::SessionCell;

use super::definition::CrawlerDefinition;
use super::engine::{CrawlEngine, CrawlReport};
use super::persistence::CrawlerPersistence;

/// Shared, late-filled handle to the manager, registered as a session extension so
/// DDL actions (`CREATE/RUN/DROP CRAWLER`) can reach it. Mirrors the `SessionCell`
/// pattern: the manager is built after the session context, so the cell is
/// registered empty during context init and filled once the manager exists.
///
/// The session owns the manager through this handle. The reverse edge is weak —
/// the manager reaches the session through a [`SessionCell`] — so there is no
/// cycle, and dropping the runtime drops the session, the manager, and the
/// tables-store lock in that order.
pub type CrawlerManagerHandle = Arc<OnceLock<Arc<CrawlerManager>>>;

/// Create an empty manager handle to register as a session extension.
pub fn new_crawler_manager_handle() -> CrawlerManagerHandle {
    Arc::new(OnceLock::new())
}

struct CrawlerEntry {
    def: CrawlerDefinition,
    /// Serializes runs of this crawler so scheduled + manual triggers never race.
    run_lock: Arc<AsyncMutex<()>>,
    /// Background trigger tasks (scheduled).
    tasks: Vec<JoinHandle<()>>,
}

pub struct CrawlerManager {
    engine: CrawlEngine,
    persistence: CrawlerPersistence,
    config: CrawlerConfig,
    crawlers: Mutex<HashMap<String, CrawlerEntry>>,
}

impl CrawlerManager {
    pub(crate) fn new(
        session: SessionCell,
        file_formats: Vec<Arc<dyn FileFormatFactoryExt>>,
        db_store_url: ObjectStoreUrl,
        config: CrawlerConfig,
    ) -> Arc<Self> {
        let engine = CrawlEngine::new(session.clone(), file_formats);
        let persistence = CrawlerPersistence::new(session, db_store_url);
        Arc::new(Self {
            engine,
            persistence,
            config,
            crawlers: Mutex::new(HashMap::new()),
        })
    }

    /// Load persisted crawlers and start their triggers.
    pub async fn init(self: &Arc<Self>) -> anyhow::Result<()> {
        let persisted = self.persistence.load_all().await?;
        for mut def in persisted {
            self.apply_event_driven_fallback(&mut def);
            self.insert_and_start(def);
        }
        tracing::info!(
            "crawler manager initialized with {} crawler(s)",
            self.crawlers.lock().len()
        );
        Ok(())
    }

    /// Define (or replace) a crawler: persist it and (re)start its triggers.
    pub async fn create(self: &Arc<Self>, mut def: CrawlerDefinition) -> anyhow::Result<()> {
        self.apply_event_driven_fallback(&mut def);
        self.persistence.save(&def).await?;
        self.stop_tasks(&def.name);
        self.insert_and_start(def);
        Ok(())
    }

    /// Run a crawler once, on demand.
    pub async fn run(&self, name: &str) -> anyhow::Result<CrawlReport> {
        let (def, run_lock) = {
            let crawlers = self.crawlers.lock();
            let entry = crawlers
                .get(name)
                .ok_or_else(|| anyhow::anyhow!("crawler '{name}' does not exist"))?;
            (entry.def.clone(), entry.run_lock.clone())
        };
        // Serialize concurrent runs of the same crawler.
        let _guard = run_lock.lock().await;
        self.engine.run(&def).await
    }

    /// Remove a crawler definition and stop its triggers. Crawled tables are left
    /// in place (deletion semantics: the catalog keeps reflecting reality).
    pub async fn drop_crawler(&self, name: &str) -> anyhow::Result<()> {
        self.stop_tasks(name);
        let existed = self.crawlers.lock().remove(name).is_some();
        self.persistence.delete(name).await?;
        if !existed {
            return Err(anyhow::anyhow!("crawler '{name}' does not exist"));
        }
        Ok(())
    }

    /// List defined crawlers (for `SHOW CRAWLERS`).
    pub fn list(&self) -> Vec<CrawlerDefinition> {
        let mut out: Vec<_> = self
            .crawlers
            .lock()
            .values()
            .map(|e| e.def.clone())
            .collect();
        out.sort_by(|a, b| a.name.cmp(&b.name));
        out
    }

    /// Event-driven crawling is not currently implemented (`event_driven` is preserved
    /// as a forward-compatibility placeholder — see [`CrawlerDefinition::event_driven`]).
    /// So that an `event_driven` crawler with no explicit schedule is not silently
    /// inert, run it at the default poll interval instead.
    fn apply_event_driven_fallback(&self, def: &mut CrawlerDefinition) {
        if def.event_driven && def.schedule_secs.is_none() {
            tracing::info!(
                "crawler '{}' requests event-driven crawling, which is not currently \
                 implemented; running it at the default {}s poll interval instead",
                def.name,
                self.config.default_interval_secs
            );
            def.schedule_secs = Some(self.config.default_interval_secs);
        }
    }

    /// Insert an entry and spawn its trigger tasks (when the subsystem is enabled).
    fn insert_and_start(self: &Arc<Self>, def: CrawlerDefinition) {
        let mut tasks = Vec::new();
        if self.config.enable {
            if let Some(secs) = def.schedule_secs {
                tasks.push(self.spawn_scheduled(def.name.clone(), secs));
            }
        }
        self.crawlers.lock().insert(
            def.name.clone(),
            CrawlerEntry {
                def,
                run_lock: Arc::new(AsyncMutex::new(())),
                tasks,
            },
        );
    }

    fn spawn_scheduled(self: &Arc<Self>, name: String, secs: u64) -> JoinHandle<()> {
        let weak = Arc::downgrade(self);
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(secs.max(1)));
            ticker.tick().await; // consume the immediate first tick
            loop {
                ticker.tick().await;
                if !run_via_weak(&weak, &name).await {
                    break; // manager dropped
                }
            }
        })
    }

    fn stop_tasks(&self, name: &str) {
        if let Some(entry) = self.crawlers.lock().get_mut(name) {
            for task in entry.tasks.drain(..) {
                task.abort();
            }
        }
    }
}

impl Drop for CrawlerManager {
    fn drop(&mut self) {
        for entry in self.crawlers.lock().values_mut() {
            for task in entry.tasks.drain(..) {
                task.abort();
            }
        }
    }
}

/// Upgrade the weak manager handle and run a crawl. Returns `false` when the
/// manager has been dropped, signalling the trigger task to exit.
async fn run_via_weak(weak: &Weak<CrawlerManager>, name: &str) -> bool {
    let Some(manager) = weak.upgrade() else {
        return false;
    };
    if let Err(error) = manager.run(name).await {
        tracing::warn!("scheduled crawl of '{name}' failed: {error}");
    }
    true
}
