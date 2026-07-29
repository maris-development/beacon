//! Persisting object-store secrets, encrypted, in the database file.
//!
//! A `CREATE PERSISTENT SECRET` writes the secret into the database's own store (redb for a
//! `beacon.db` file), so a copied file carries its own S3/GCS/Azure credentials — the piece that
//! makes a portable single file able to reach the external data it references. The credential
//! *values* are encrypted at rest with the deployment master key ([`EncryptedSecret`]); only the
//! name, backend type, and scope are stored in the clear (so `SHOW SECRETS` can list them without
//! the key). Persistence therefore requires a master key — beacon refuses to write a plaintext
//! secret to disk.
//!
//! This lives in beacon-core (not beside the [`SecretStore`] in beacon-datafusion-ext) because
//! [`EncryptedSecret`] is defined in beacon-sql-databases, which already depends on
//! beacon-datafusion-ext — owning the encryption here avoids a dependency cycle.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Context as _;
use beacon_datafusion_ext::secrets::{Secret, SecretType};
use beacon_sql_databases::EncryptedSecret;
use futures::StreamExt as _;
use object_store::{path::Path, ObjectStore, ObjectStoreExt as _};
use secrecy::ExposeSecret as _;

/// The store prefix persisted secrets live under. Distinct from the table (`.../table.json`)
/// layout so the two never collide in the same object store.
const SECRETS_PREFIX: &str = "__beacon_secrets__";

/// The on-disk form of a persisted secret: metadata in the clear, credential values encrypted.
#[derive(serde::Serialize, serde::Deserialize)]
struct PersistedSecret {
    name: String,
    secret_type: SecretType,
    scope: String,
    /// The options map (credential values) as an encrypted JSON blob.
    options: EncryptedSecret,
}

fn secret_path(name: &str) -> Path {
    Path::from(format!("{SECRETS_PREFIX}/{name}.json"))
}

/// Encrypt and write `secret` into `store`.
pub(crate) async fn persist_secret(
    store: &Arc<dyn ObjectStore>,
    secret: &Secret,
    key: &[u8; 32],
) -> anyhow::Result<()> {
    let options_json = serde_json::to_string(&secret.options)?;
    let persisted = PersistedSecret {
        name: secret.name.clone(),
        secret_type: secret.secret_type,
        scope: secret.scope.clone(),
        options: EncryptedSecret::encrypt(&options_json, key)?,
    };
    let bytes = serde_json::to_vec(&persisted)?;
    store
        .put(&secret_path(&secret.name), bytes.into())
        .await
        .with_context(|| format!("failed to persist secret '{}'", secret.name))?;
    Ok(())
}

/// Remove a persisted secret's object. A missing object is not an error (the secret may have been
/// session-only, or already removed).
pub(crate) async fn remove_persisted_secret(
    store: &Arc<dyn ObjectStore>,
    name: &str,
) -> anyhow::Result<()> {
    match store.delete(&secret_path(name)).await {
        Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
        Err(error) => Err(anyhow::anyhow!(
            "failed to remove persisted secret '{name}': {error}"
        )),
    }
}

/// Load and decrypt every persisted secret from `store`. Each returned [`Secret`] is marked
/// `persistent`. An individual secret that fails to parse or decrypt (e.g. a wrong master key) is
/// logged and skipped, so one bad entry cannot stop the database from opening.
pub(crate) async fn load_persisted_secrets(
    store: &Arc<dyn ObjectStore>,
    key: &[u8; 32],
) -> anyhow::Result<Vec<Secret>> {
    let mut secrets = Vec::new();
    let mut listing = store.list(Some(&Path::from(SECRETS_PREFIX)));
    while let Some(entry) = listing.next().await {
        let location = match entry {
            Ok(meta) => meta.location,
            Err(error) => {
                tracing::error!("failed to list persisted secrets: {error}");
                continue;
            }
        };
        match load_one(store, &location, key).await {
            Ok(secret) => secrets.push(secret),
            Err(error) => {
                tracing::error!("skipping persisted secret at {location}: {error:#}");
            }
        }
    }
    Ok(secrets)
}

async fn load_one(
    store: &Arc<dyn ObjectStore>,
    location: &Path,
    key: &[u8; 32],
) -> anyhow::Result<Secret> {
    let bytes = store.get(location).await?.bytes().await?;
    let persisted: PersistedSecret =
        serde_json::from_slice(&bytes).context("parsing persisted secret")?;
    let options_json = persisted.options.decrypt(key)?;
    let options: HashMap<String, String> =
        serde_json::from_str(options_json.expose_secret()).context("parsing decrypted options")?;
    Ok(Secret {
        name: persisted.name,
        secret_type: persisted.secret_type,
        scope: persisted.scope,
        options,
        persistent: true,
    })
}
