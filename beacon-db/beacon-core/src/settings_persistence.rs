//! Persisting `ALTER SYSTEM SET` settings in the database file.
//!
//! A plain `SET` changes the live session and is lost on restart. `ALTER SYSTEM
//! SET` writes the value here as well, into the database's own store (redb for a
//! `beacon.db` file), and the runtime replays it at startup — so an operator can
//! turn a knob on a running server without a redeploy *and* without the change
//! quietly reverting at the next restart.
//!
//! One object per key, mirroring [`secret_persistence`](crate::secret_persistence):
//! a setting is small and rewritten often, which is the case redb reclaims pages
//! for, and a per-key object means two concurrent `ALTER SYSTEM SET`s on
//! different keys cannot clobber each other.
//!
//! Precedence at startup is **persisted > environment > default**: the
//! environment builds the session config, and these are applied over it.
//!
//! An in-memory runtime (no `db_path`) has nowhere to write, and persisting is
//! skipped — the same rule persisted secrets follow.

use std::sync::Arc;

use anyhow::Context as _;
use datafusion::prelude::SessionContext;
use futures::StreamExt as _;
use object_store::{ObjectStore, ObjectStoreExt as _, path::Path};

/// The store persisted settings are written to, published as a session
/// extension so `ALTER SYSTEM` can reach it with only a `SessionContext` in hand.
///
/// `None` for an in-memory database, where persistence has nowhere durable to go
/// — the same rule persisted secrets follow.
#[derive(Debug, Clone, Default)]
pub struct SettingsPersistence(Option<Arc<dyn ObjectStore>>);

impl SettingsPersistence {
    pub(crate) fn new(store: Option<Arc<dyn ObjectStore>>) -> Self {
        Self(store)
    }

    /// The store, or `None` when this runtime persists nothing.
    pub(crate) fn store(&self) -> Option<&Arc<dyn ObjectStore>> {
        self.0.as_ref()
    }

    /// The persistence published on `session_ctx`, or an unavailable one for a
    /// session beacon did not build.
    pub(crate) fn from_session(session_ctx: &SessionContext) -> Self {
        session_ctx
            .state()
            .config()
            .get_extension::<SettingsPersistence>()
            .map(|persistence| (*persistence).clone())
            .unwrap_or_default()
    }
}

/// The store prefix persisted settings live under. Distinct from the table
/// (`.../table.json`) and secret layouts, so the three never collide.
const SETTINGS_PREFIX: &str = "__beacon_settings__";

/// The on-disk form of one persisted setting.
///
/// The key is stored alongside the value rather than only in the object name, so
/// a listing needs no unescaping and a future rename can migrate the file names
/// without losing what each object means.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct PersistedSetting {
    key: String,
    value: String,
}

/// The object a setting is stored at.
///
/// Keys are `[a-z0-9_.]` by construction — they are resolved against
/// [`BeaconOptions`](beacon_datafusion_ext::settings::BeaconOptions) or
/// DataFusion's own option table before they reach here — so the key is a safe
/// path segment as it stands.
fn setting_path(key: &str) -> Path {
    Path::from(format!("{SETTINGS_PREFIX}/{key}.json"))
}

/// Write `key` = `value` into `store`, replacing any earlier value.
pub(crate) async fn persist_setting(
    store: &Arc<dyn ObjectStore>,
    key: &str,
    value: &str,
) -> anyhow::Result<()> {
    let bytes = serde_json::to_vec(&PersistedSetting {
        key: key.to_string(),
        value: value.to_string(),
    })?;
    store
        .put(&setting_path(key), bytes.into())
        .await
        .with_context(|| format!("failed to persist setting '{key}'"))?;
    Ok(())
}

/// Remove a persisted setting. A missing object is not an error: `ALTER SYSTEM
/// RESET` on a key that was never persisted is a no-op, not a failure.
pub(crate) async fn remove_persisted_setting(
    store: &Arc<dyn ObjectStore>,
    key: &str,
) -> anyhow::Result<()> {
    match store.delete(&setting_path(key)).await {
        Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
        Err(error) => Err(anyhow::anyhow!(
            "failed to remove persisted setting '{key}': {error}"
        )),
    }
}

/// Every persisted setting in `store`, as `(key, value)`.
///
/// An entry that fails to parse is logged and skipped: a settings file that a
/// future version wrote differently must not stop the database from opening,
/// since the alternative is a server that cannot start and cannot be fixed
/// through SQL.
pub(crate) async fn load_persisted_settings(
    store: &Arc<dyn ObjectStore>,
) -> anyhow::Result<Vec<(String, String)>> {
    let mut settings = Vec::new();
    let mut listing = store.list(Some(&Path::from(SETTINGS_PREFIX)));
    while let Some(entry) = listing.next().await {
        let location = match entry {
            Ok(meta) => meta.location,
            Err(error) => {
                tracing::error!("failed to list persisted settings: {error}");
                continue;
            }
        };
        match load_one(store, &location).await {
            Ok(setting) => settings.push((setting.key, setting.value)),
            Err(error) => {
                tracing::error!("skipping persisted setting at {location}: {error:#}");
            }
        }
    }
    // Deterministic order, so replaying them logs the same way every boot.
    settings.sort();
    Ok(settings)
}

async fn load_one(
    store: &Arc<dyn ObjectStore>,
    location: &Path,
) -> anyhow::Result<PersistedSetting> {
    let bytes = store.get(location).await?.bytes().await?;
    serde_json::from_slice(&bytes).context("parsing persisted setting")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store() -> Arc<dyn ObjectStore> {
        Arc::new(object_store::memory::InMemory::new())
    }

    #[tokio::test]
    async fn a_persisted_setting_round_trips() {
        let store = store();
        persist_setting(&store, "beacon.default_table", "observations")
            .await
            .unwrap();
        persist_setting(&store, "datafusion.execution.batch_size", "8192")
            .await
            .unwrap();

        assert_eq!(
            load_persisted_settings(&store).await.unwrap(),
            vec![
                (
                    "beacon.default_table".to_string(),
                    "observations".to_string()
                ),
                (
                    "datafusion.execution.batch_size".to_string(),
                    "8192".to_string()
                ),
            ]
        );
    }

    /// Setting the same key twice leaves one value, not two — the second
    /// `ALTER SYSTEM SET` has to win rather than accumulate.
    #[tokio::test]
    async fn re_persisting_a_key_replaces_its_value() {
        let store = store();
        persist_setting(&store, "beacon.default_table", "first")
            .await
            .unwrap();
        persist_setting(&store, "beacon.default_table", "second")
            .await
            .unwrap();

        assert_eq!(
            load_persisted_settings(&store).await.unwrap(),
            vec![("beacon.default_table".to_string(), "second".to_string())]
        );
    }

    /// `ALTER SYSTEM RESET` on a key that was never persisted is a no-op.
    #[tokio::test]
    async fn removing_an_absent_setting_succeeds() {
        let store = store();
        remove_persisted_setting(&store, "beacon.default_table")
            .await
            .unwrap();

        persist_setting(&store, "beacon.default_table", "observations")
            .await
            .unwrap();
        remove_persisted_setting(&store, "beacon.default_table")
            .await
            .unwrap();
        assert!(load_persisted_settings(&store).await.unwrap().is_empty());
    }

    /// One unreadable object must not hide the rest: a server whose settings file
    /// a future version wrote differently still has to start.
    #[tokio::test]
    async fn a_corrupt_entry_is_skipped() {
        let store = store();
        persist_setting(&store, "beacon.default_table", "observations")
            .await
            .unwrap();
        store
            .put(
                &Path::from(format!("{SETTINGS_PREFIX}/broken.json")),
                bytes::Bytes::from_static(b"not json").into(),
            )
            .await
            .unwrap();

        assert_eq!(
            load_persisted_settings(&store).await.unwrap(),
            vec![(
                "beacon.default_table".to_string(),
                "observations".to_string()
            )]
        );
    }
}
