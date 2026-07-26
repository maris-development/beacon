//! Flight SQL client connection to a remote Beacon instance.

use anyhow::Context as _;
use arrow::record_batch::RecordBatch;
use arrow_flight::sql::client::FlightSqlServiceClient;
use base64::Engine as _;
use futures::TryStreamExt as _;
use tonic::transport::{Channel, Endpoint};

/// How a remote-Beacon connection authenticates to the remote's Flight SQL server.
///
/// The remote accepts either form directly in the `authorization` header: a Beacon-issued bearer
/// token (short-lived), or HTTP Basic username/password (validated statelessly against the remote's
/// auth store — the natural credential for a durable federation link).
#[derive(Clone, PartialEq, Eq, PartialOrd, Hash)]
pub enum RemoteCredential {
    /// No credentials — the remote must permit anonymous Flight SQL access.
    Anonymous,
    /// A Beacon-issued bearer token, sent as `authorization: Bearer <token>`.
    Bearer(String),
    /// Username/password, sent as `authorization: Basic <base64(username:password)>`.
    Basic { username: String, password: String },
}

// Custom, so neither the token nor the password is ever printed (they are credentials).
impl std::fmt::Debug for RemoteCredential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Anonymous => write!(f, "Anonymous"),
            Self::Bearer(_) => write!(f, "Bearer(<redacted>)"),
            Self::Basic { username, .. } => f
                .debug_struct("Basic")
                .field("username", username)
                .field("password", &"<redacted>")
                .finish(),
        }
    }
}

impl RemoteCredential {
    /// Build a credential from a stored `TYPE BEACON` secret's options (`token`, or
    /// `username`/`password`).
    pub fn from_secret(secret: &crate::secrets::Secret) -> anyhow::Result<Self> {
        anyhow::ensure!(
            secret.secret_type == crate::secrets::SecretType::Beacon,
            "secret '{}' is a {} secret, not a beacon secret",
            secret.name,
            secret.secret_type.as_str()
        );
        let option = |key: &str| secret.options.get(key).cloned();
        Self::from_parts(option("token"), option("username"), option("password"))
    }

    /// Build a credential from the individual pieces a user may supply (via keywords or SQL
    /// `WITH (...)`), rejecting ambiguous or incomplete combinations. Shared by the Python `attach`
    /// and the SQL `ATTACH` paths so both validate identically.
    pub fn from_parts(
        token: Option<String>,
        username: Option<String>,
        password: Option<String>,
    ) -> anyhow::Result<Self> {
        match (token, username, password) {
            (Some(_), Some(_), _) | (Some(_), _, Some(_)) => {
                anyhow::bail!("provide either a token or a username/password, not both")
            }
            (Some(token), None, None) => Ok(Self::Bearer(token)),
            (None, Some(username), Some(password)) => Ok(Self::Basic { username, password }),
            (None, Some(_), None) => anyhow::bail!("username given without a password"),
            (None, None, Some(_)) => anyhow::bail!("password given without a username"),
            (None, None, None) => Ok(Self::Anonymous),
        }
    }
}

/// Connection details for a remote Beacon instance's Flight SQL server.
#[derive(Clone, Debug)]
pub struct RemoteConnection {
    /// gRPC endpoint of the remote Flight SQL server, e.g. `http://host:50051`.
    pub url: String,
    /// How the connection authenticates to the remote.
    credential: RemoteCredential,
}

impl RemoteConnection {
    /// An anonymous connection (no credentials).
    pub fn new(url: String) -> Self {
        Self {
            url,
            credential: RemoteCredential::Anonymous,
        }
    }

    /// A connection carrying a specific credential.
    pub fn with_credential(url: String, credential: RemoteCredential) -> Self {
        Self { url, credential }
    }

    /// Open a Flight SQL client to the remote, attaching the credential (if any).
    pub async fn connect(&self) -> anyhow::Result<FlightSqlServiceClient<Channel>> {
        let channel = Endpoint::from_shared(self.url.clone())
            .with_context(|| format!("invalid remote beacon endpoint '{}'", self.url))?
            .connect()
            .await
            .with_context(|| format!("failed to connect to remote beacon at '{}'", self.url))?;

        let mut client = FlightSqlServiceClient::new(channel);
        match &self.credential {
            RemoteCredential::Anonymous => {}
            // Sent as `authorization: Bearer <token>` on every subsequent call.
            RemoteCredential::Bearer(token) => client.set_token(token.clone()),
            // Sent as `authorization: Basic <base64(username:password)>`; the client's Bearer path
            // is inactive (no token), so this custom header is what the remote sees.
            RemoteCredential::Basic { username, password } => {
                let encoded = base64::engine::general_purpose::STANDARD
                    .encode(format!("{username}:{password}"));
                client.set_header("authorization", format!("Basic {encoded}"));
            }
        }
        Ok(client)
    }

    /// Run `sql` on the remote and collect every result batch.
    ///
    /// Used for the small metadata queries that drive catalog enumeration — not for scans, which
    /// go through the federated executor's streaming path.
    pub async fn collect_query(&self, sql: impl Into<String>) -> anyhow::Result<Vec<RecordBatch>> {
        let mut client = self.connect().await?;
        let info = client
            .execute(sql.into(), None)
            .await
            .context("remote beacon rejected the metadata query")?;

        let mut batches = Vec::new();
        for endpoint in info.endpoint {
            let ticket = endpoint
                .ticket
                .context("remote flight endpoint missing ticket")?;
            let mut stream = client.do_get(ticket).await.context("remote beacon do_get failed")?;
            while let Some(batch) = stream.try_next().await.context("remote beacon stream error")? {
                batches.push(batch);
            }
        }
        Ok(batches)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn credential_from_parts_covers_each_combination() {
        let s = |x: &str| Some(x.to_string());

        assert_eq!(
            RemoteCredential::from_parts(None, None, None).unwrap(),
            RemoteCredential::Anonymous
        );
        assert_eq!(
            RemoteCredential::from_parts(s("tok"), None, None).unwrap(),
            RemoteCredential::Bearer("tok".to_string())
        );
        assert_eq!(
            RemoteCredential::from_parts(None, s("u"), s("p")).unwrap(),
            RemoteCredential::Basic {
                username: "u".to_string(),
                password: "p".to_string(),
            }
        );

        // Ambiguous or incomplete combinations are rejected.
        assert!(RemoteCredential::from_parts(s("tok"), s("u"), s("p")).is_err());
        assert!(RemoteCredential::from_parts(s("tok"), None, s("p")).is_err());
        assert!(RemoteCredential::from_parts(None, s("u"), None).is_err());
        assert!(RemoteCredential::from_parts(None, None, s("p")).is_err());
    }

    #[test]
    fn credential_debug_redacts_secrets() {
        let bearer = format!("{:?}", RemoteCredential::Bearer("supersecret".to_string()));
        assert!(!bearer.contains("supersecret"), "{bearer}");

        let basic = format!(
            "{:?}",
            RemoteCredential::Basic {
                username: "alice".to_string(),
                password: "hunter2".to_string(),
            }
        );
        assert!(basic.contains("alice"), "{basic}");
        assert!(!basic.contains("hunter2"), "{basic}");
    }
}
