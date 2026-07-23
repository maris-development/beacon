//! Serializable definition for an external SQL-database table.
//!
//! Mirrors `beacon_datafusion_ext::remote::RemoteTableDefinition`: a
//! typetag-serialized definition that builds a DataFusion provider and is
//! persisted as `table.json`. The credential is stored encrypted (see
//! [`crate::secret`]); everything else is plaintext connection metadata.

use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::anyhow;
use arrow::datatypes::{Schema, SchemaRef};
use beacon_datafusion_ext::secrets::SecretStore;
use datafusion::catalog::TableProvider;
use datafusion::prelude::SessionContext;
use datafusion::sql::TableReference;
use datafusion_federation::sql::{SQLTable, SQLTableSource};
use datafusion_federation::FederatedTableProviderAdaptor;
use secrecy::SecretString;

use beacon_datafusion_ext::table_ext::TableDefinition;

use crate::options::build_pool_params;
use crate::secret::EncryptedSecret;
use crate::source::BeaconSqlTable;
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
    /// Decrypt the stored credential using the deployment master key carried by the
    /// session's [`SecretStore`] extension. Errors (fails closed) if a secret is
    /// present but no master key is configured.
    fn decrypt_password(&self, context: &SessionContext) -> anyhow::Result<Option<SecretString>> {
        let Some(secret) = &self.secret else {
            return Ok(None);
        };
        let secret_store = context
            .state()
            .config()
            .get_extension::<SecretStore>()
            .ok_or_else(|| anyhow!("secret store is unavailable; cannot decrypt credentials"))?;
        let key = secret_store.master_key().ok_or_else(|| {
            anyhow!(
                "no secrets encryption key is configured; cannot decrypt stored credentials for \
                 table '{}'",
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
    ) -> anyhow::Result<Arc<dyn TableProvider>> {
        let password = self.decrypt_password(&context)?;
        let params = build_pool_params(self.engine, &self.options, password);
        let table_ref = TableReference::parse_str(&self.remote_table);

        let provider = self
            .engine
            .build_table_provider(params, table_ref)
            .await
            // Surface the full cause chain ({:#}); the underlying connector error
            // (auth/TLS/connection) is otherwise hidden behind this top context.
            .map_err(|e| {
                anyhow::anyhow!(
                    "failed to build {} provider for table '{}': {e:#}",
                    self.engine.as_str(),
                    self.name
                )
            })?;

        // `datafusion-table-providers` returns a `FederatedTableProviderAdaptor`
        // (with the `*-federation` feature) whose `source` is the concrete
        // `SQLTableSource`. The federation optimizer recognizes that exact type,
        // and its `RewriteTableScanAnalyzer` rewrites the local catalog name to
        // the source's `table_reference()` (the remote `LOCATION`) before
        // generating SQL. We must therefore keep the concrete `SQLTableSource` —
        // wrapping the *source* in another type would make that downcast fail and
        // send Beacon's local table name to the remote DB. Instead we wrap the
        // inner `SQLTable` (which still reports the correct remote
        // `table_reference`) so it additionally carries our definition.
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
        let sql_source = adaptor
            .source
            .as_any()
            .downcast_ref::<SQLTableSource>()
            .ok_or_else(|| {
                anyhow!(
                    "expected an SQLTableSource from datafusion-table-providers for table '{}'",
                    self.name
                )
            })?;

        // Pin the resolved schema so a catalog round-trip persists the concrete
        // schema rather than whatever placeholder was supplied at creation.
        let mut pinned = self.clone();
        pinned.schema = sql_source.table.schema();

        // Keep the federation provider/executor; swap the inner table for one that
        // carries our definition while delegating the remote table reference.
        let table: Arc<dyn SQLTable> =
            Arc::new(BeaconSqlTable::new(Arc::clone(&sql_source.table), pinned));
        let source = Arc::new(SQLTableSource::new_with_table(
            Arc::clone(&sql_source.provider),
            table,
        ));
        Ok(Arc::new(FederatedTableProviderAdaptor::new(source)))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::secret::EncryptedSecret;
    use arrow::datatypes::{DataType, Field};
    use datafusion::prelude::SessionConfig;

    fn definition(secret: Option<EncryptedSecret>) -> SqlDatabaseTableDefinition {
        SqlDatabaseTableDefinition {
            name: "pg_companies".to_string(),
            engine: crate::source::tests::engine(),
            remote_table: "public.companies".to_string(),
            schema: Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
            options: BTreeMap::from([
                ("host".to_string(), "db.internal".to_string()),
                ("port".to_string(), "5432".to_string()),
            ]),
            secret,
        }
    }

    /// The definition round-trips through the typetag `TableDefinition` trait,
    /// keeping its `sql_database_table` tag so `table.json` reloads correctly.
    #[test]
    fn definition_serde_round_trip() {
        let definition: Arc<dyn TableDefinition> = Arc::new(definition(None));
        let json = serde_json::to_value(&definition).expect("should serialize");
        assert_eq!(json["definition_type"], "sql_database_table");
        assert_eq!(json["remote_table"], "public.companies");
        assert!(
            json.get("secret").is_none(),
            "an absent secret must be omitted, not written as null: {json}"
        );

        let restored: Arc<dyn TableDefinition> =
            serde_json::from_value(json).expect("should deserialize");
        assert_eq!(restored.table_name(), "pg_companies");
    }

    /// The persisted form never contains the plaintext password.
    #[test]
    fn persisted_form_holds_only_ciphertext() {
        let secret = EncryptedSecret::encrypt("hunter2", &[9u8; 32]).unwrap();
        let json = serde_json::to_string(&definition(Some(secret))).unwrap();
        assert!(!json.contains("hunter2"), "{json}");
        assert!(json.contains("ciphertext"), "{json}");
    }

    /// Fails closed: a stored secret with no secret store in the session must not
    /// silently connect without a password.
    #[test]
    fn decrypt_fails_closed_without_a_secret_store() {
        let secret = EncryptedSecret::encrypt("hunter2", &[9u8; 32]).unwrap();
        let ctx = SessionContext::new();
        let err = definition(Some(secret))
            .decrypt_password(&ctx)
            .expect_err("must not fall back to no password");
        assert!(err.to_string().contains("secret store"), "{err}");
    }

    /// Likewise when a store is present but no master key is configured.
    #[test]
    fn decrypt_fails_closed_without_a_master_key() {
        let secret = EncryptedSecret::encrypt("hunter2", &[9u8; 32]).unwrap();
        let ctx = SessionContext::new_with_config(
            SessionConfig::new().with_extension(Arc::new(SecretStore::new_with_master_key(None))),
        );
        let err = definition(Some(secret))
            .decrypt_password(&ctx)
            .expect_err("must not decrypt without a key");
        assert!(err.to_string().contains("encryption key"), "{err}");
        assert!(err.to_string().contains("pg_companies"), "{err}");
    }

    /// With the right key the credential is recovered; a definition with no
    /// secret yields no password at all.
    #[test]
    fn decrypt_recovers_the_password_with_the_configured_key() {
        use secrecy::ExposeSecret as _;

        let key = [9u8; 32];
        let secret = EncryptedSecret::encrypt("hunter2", &key).unwrap();
        let ctx = SessionContext::new_with_config(
            SessionConfig::new()
                .with_extension(Arc::new(SecretStore::new_with_master_key(Some(key)))),
        );
        let password = definition(Some(secret)).decrypt_password(&ctx).unwrap();
        assert_eq!(password.unwrap().expose_secret(), "hunter2");

        assert!(definition(None).decrypt_password(&ctx).unwrap().is_none());
    }

    /// An unresolved schema is the "resolve from the database" marker, so it must
    /// be empty (and not, say, carry a placeholder column).
    #[test]
    fn unresolved_schema_is_empty() {
        assert_eq!(unresolved_schema().fields().len(), 0);
    }
}
