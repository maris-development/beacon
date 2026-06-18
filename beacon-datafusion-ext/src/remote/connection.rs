//! Flight SQL client connection to a remote Beacon instance.

use anyhow::Context as _;
use arrow_flight::sql::client::FlightSqlServiceClient;
use tonic::transport::{Channel, Endpoint};

/// Connection details for a remote Beacon instance's Flight SQL server.
///
/// Credentials are stored inline (and persisted in `table.json`) by design — see
/// the federated-remote-table plan. Creation is admin-gated DDL.
#[derive(Clone, Debug)]
pub struct RemoteConnection {
    /// gRPC endpoint of the remote Flight SQL server, e.g. `http://host:50051`.
    pub url: String,
    pub username: Option<String>,
    pub password: Option<String>,
}

impl RemoteConnection {
    pub fn new(url: String, username: Option<String>, password: Option<String>) -> Self {
        Self {
            url,
            username,
            password,
        }
    }

    /// Open a Flight SQL client to the remote, performing a Basic-auth handshake
    /// when credentials are configured.
    ///
    /// The handshake mirrors the server side in `beacon-api`'s `do_handshake`: the
    /// client captures the returned Bearer token and reuses it for subsequent
    /// `execute`/`do_get` RPCs.
    pub async fn connect(&self) -> anyhow::Result<FlightSqlServiceClient<Channel>> {
        let channel = Endpoint::from_shared(self.url.clone())
            .with_context(|| format!("invalid remote beacon endpoint '{}'", self.url))?
            .connect()
            .await
            .with_context(|| format!("failed to connect to remote beacon at '{}'", self.url))?;

        let mut client = FlightSqlServiceClient::new(channel);

        if let Some(username) = &self.username {
            let password = self.password.as_deref().unwrap_or_default();
            client
                .handshake(username, password)
                .await
                .with_context(|| format!("Flight SQL handshake with '{}' failed", self.url))?;
        }

        Ok(client)
    }
}
