//! Serializable definition for an external SQL-database table.
//!
//! Mirrors `beacon_datafusion_ext::remote::RemoteTableDefinition`: a
//! typetag-serialized definition that builds a DataFusion provider and is
//! persisted as `table.json`. The credential is stored encrypted (see
//! [`crate::secret`]); everything else is plaintext connection metadata.

use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::{anyhow, Context as _};
use arrow::datatypes::{Schema, SchemaRef};
use datafusion::catalog::TableProvider;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::prelude::SessionContext;
use datafusion::sql::TableReference;
use datafusion_federation::{FederatedTableProviderAdaptor, FederatedTableSource};
use secrecy::SecretString;

use beacon_datafusion_ext::table_ext::TableDefinition;

use crate::options::build_pool_params;
use crate::secret::EncryptedSecret;
use crate::source::BeaconSqlFederatedSource;
use crate::SqlEngine;

/// Persisted configuration for an external PostgreSQL/MySQL table.
///
/// Stored as `table.json` and reloaded at startup like every other
/// [`TableDefinition`]. The password (if any) lives in `secret`, encrypted with
/// the deployment master key (`BEACON_SECRETS_KEY`); `options` holds only the
/// non-secret connection parameters.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct SqlDatabaseTableDefinition {
    /// Local logical table name.
    pub name: String,
    /// Which database engine to connect to.
    pub engine: SqlEngine,
    /// Table name on the remote database, e.g. `public.orders` or `orders`.
    pub remote_table: String,
    /// Resolved output schema, pinned when the provider is first built.
    pub schema: SchemaRef,
    /// Non-secret connection options (host, port, user, database, sslmode, …),
    /// keyed by beacon's engine-neutral names.
    pub options: BTreeMap<String, String>,
    /// Encrypted password. `None` for engines/connections without one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secret: Option<EncryptedSecret>,
}

impl SqlDatabaseTableDefinition {
    /// Decrypt the stored credential using the deployment master key from the
    /// session's [`beacon_config::Config`] extension. Errors (fails closed) if a
    /// secret is present but no `BEACON_SECRETS_KEY` is configured.
    fn decrypt_password(
        &self,
        context: &SessionContext,
    ) -> anyhow::Result<Option<SecretString>> {
        let Some(secret) = &self.secret else {
            return Ok(None);
        };
        let config = context
            .state()
            .config()
            .get_extension::<beacon_config::Config>()
            .ok_or_else(|| anyhow!("Beacon configuration is unavailable; cannot decrypt credentials"))?;
        let key = config.secrets.master_key().ok_or_else(|| {
            anyhow!(
                "BEACON_SECRETS_KEY is not set; cannot decrypt stored credentials for table '{}'",
                self.name
            )
        })?;
        Ok(Some(secret.decrypt(key)?))
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "sql_database_table")]
impl TableDefinition for SqlDatabaseTableDefinition {
    async fn build_provider(
        &self,
        context: Arc<SessionContext>,
        _data_store_url: &ObjectStoreUrl,
    ) -> anyhow::Result<Arc<dyn TableProvider>> {
        let password = self.decrypt_password(&context)?;
        let params = build_pool_params(self.engine, &self.options, password);
        let table_ref = TableReference::parse_str(&self.remote_table);

        let provider = self
            .engine
            .build_table_provider(params, table_ref)
            .await
            .with_context(|| {
                format!(
                    "failed to build {} provider for table '{}'",
                    self.engine.as_str(),
                    self.name
                )
            })?;

        // `datafusion-table-providers` returns a `FederatedTableProviderAdaptor`
        // (with the `*-federation` feature). The federation optimizer only
        // recognizes a scan whose registered provider IS that adaptor, so we
        // must keep it bare — wrapping it in another `TableProvider` would
        // silently disable pushdown. Instead, rebuild the adaptor around our own
        // `FederatedTableSource` that carries the definition, keeping the
        // original provider as the non-federated scan fallback.
        let adaptor = provider
            .as_any()
            .downcast_ref::<FederatedTableProviderAdaptor>()
            .ok_or_else(|| {
                anyhow!(
                    "expected a federated provider from datafusion-table-providers for table \
                     '{}'; is the engine's `*-federation` feature enabled?",
                    self.name
                )
            })?;

        // Pin the resolved schema so a catalog round-trip persists the concrete
        // schema rather than whatever placeholder was supplied at creation.
        let mut pinned = self.clone();
        pinned.schema = adaptor.source.schema();

        let wrapped: Arc<dyn FederatedTableSource> = Arc::new(BeaconSqlFederatedSource::new(
            Arc::clone(&adaptor.source),
            pinned,
        ));
        Ok(Arc::new(FederatedTableProviderAdaptor::new_with_provider(
            wrapped,
            Arc::clone(&provider),
        )))
    }

    fn table_name(&self) -> &str {
        &self.name
    }
}

/// An empty schema marker meaning "resolve from the database when the provider
/// is built" — the resolved schema is then pinned into the definition.
pub fn unresolved_schema() -> SchemaRef {
    Arc::new(Schema::empty())
}
