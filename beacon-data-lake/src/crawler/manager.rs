//! Owns crawler definitions and their background triggers.
//!
//! The manager loads persisted crawlers at startup, runs them on demand, and (when
//! enabled) drives scheduled and event-driven crawls. Background tasks hold a
//! [`Weak`] back-reference so the manager can be dropped; [`Drop`] aborts them.

use std::collections::HashMap;
use std::sync::{Arc, OnceLock, Weak};
use std::time::Duration;

use beacon_datafusion_ext::format_ext::FileFormatFactoryExt;
use beacon_object_storage::event::ObjectEvent;
use beacon_object_storage::DatasetsStore;
use datafusion::prelude::SessionContext;
use parking_lot::Mutex;
use tokio::sync::{broadcast, Mutex as AsyncMutex};
use tokio::task::JoinHandle;

use crate::TABLES_OBJECT_STORE_URL;

use super::definition::CrawlerDefinition;
use super::engine::{CrawlEngine, CrawlReport};
use super::persistence::CrawlerPersistence;

/// Shared, late-filled handle to the manager, registered as a session extension so
/// DDL actions (`CREATE/RUN/DROP CRAWLER`) can reach it. Mirrors the `SessionCell`
/// pattern: the manager is built after the session context, so the cell is
/// registered empty during context init and filled once the manager exists.
pub type CrawlerManagerHandle = Arc<OnceLock<Arc<CrawlerManager>>>;

/// Create an empty manager handle to register as a session extension.
pub fn new_crawler_manager_handle() -> CrawlerManagerHandle {
    Arc::new(OnceLock::new())
}

/// Debounce window for coalescing a burst of storage events into one crawl.
const EVENT_DEBOUNCE: Duration = Duration::from_secs(2);

struct CrawlerEntry {
    def: CrawlerDefinition,
    /// Serializes runs of this crawler so scheduled + event triggers never race.
    run_lock: Arc<AsyncMutex<()>>,
    /// Background trigger tasks (scheduled and/or event-driven).
    tasks: Vec<JoinHandle<()>>,
}

pub struct CrawlerManager {
    engine: CrawlEngine,
    persistence: CrawlerPersistence,
    datasets_store: Arc<DatasetsStore>,
    config: beacon_config::CrawlerConfig,
    /// Whether storage change events can actually fire (so `event_driven` is real).
    events_available: bool,
    crawlers: Mutex<HashMap<String, CrawlerEntry>>,
}

impl CrawlerManager {
    pub fn new(
        session_ctx: Arc<SessionContext>,
        file_formats: Vec<Arc<dyn FileFormatFactoryExt>>,
        datasets_store: Arc<DatasetsStore>,
        config: beacon_config::CrawlerConfig,
        events_available: bool,
    ) -> Arc<Self> {
        let engine = CrawlEngine::new(session_ctx.clone(), file_formats);
        let persistence = CrawlerPersistence::new(session_ctx, TABLES_OBJECT_STORE_URL.clone());
        Arc::new(Self {
            engine,
            persistence,
            datasets_store,
            config,
            events_available,
            crawlers: Mutex::new(HashMap::new()),
        })
    }

    /// Load persisted crawlers and start their triggers.
    pub async fn init(self: &Arc<Self>) -> anyhow::Result<()> {
        let persisted = self.persistence.load_all().await?;
        for mut def in persisted {
            self.apply_availability_guard(&mut def);
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
        self.apply_availability_guard(&mut def);
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

    /// If a crawler wants event-driven crawls but events cannot fire and it has no
    /// schedule, fall back to a default poll interval rather than going inert.
    fn apply_availability_guard(&self, def: &mut CrawlerDefinition) {
        if def.event_driven && !self.events_available && def.schedule_secs.is_none() {
            tracing::warn!(
                "crawler '{}' is event-driven but storage events are unavailable and no \
                 schedule is set; falling back to a {}s poll interval",
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
            if def.event_driven && self.events_available {
                tasks.push(self.spawn_event_driven(def.name.clone(), def.target_prefix.clone()));
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

    fn spawn_event_driven(self: &Arc<Self>, name: String, prefix: String) -> JoinHandle<()> {
        let weak = Arc::downgrade(self);
        let mut rx: broadcast::Receiver<ObjectEvent> =
            self.datasets_store.subscribe_events(&prefix);
        tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(_) => {
                        // Debounce: pause, then drain the rest of the burst.
                        tokio::time::sleep(EVENT_DEBOUNCE).await;
                        while rx.try_recv().is_ok() {}
                        if !run_via_weak(&weak, &name).await {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => break,
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
        tracing::warn!("scheduled/event crawl of '{name}' failed: {error}");
    }
    true
}
