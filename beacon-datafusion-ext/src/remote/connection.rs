//! Flight SQL client connection to a remote Beacon instance.

use anyhow::Context as _;
use arrow_flight::sql::client::FlightSqlServiceClient;
use tonic::transport::{Channel, Endpoint};

/// Connection details for a remote Beacon instance's Flight SQL server.
///
/// Remote tables connect **anonymously** — no credentials are stored. The remote
/// instance must allow anonymous Flight SQL access for federation to work.
#[derive(Clone, Debug)]
pub struct RemoteConnection {
    /// gRPC endpoint of the remote Flight SQL server, e.g. `http://host:50051`.
    pub url: String,
}

impl RemoteConnection {
    pub fn new(url: String) -> Self {
        Self { url }
    }

    /// Open an anonymous Flight SQL client to the remote.
    ///
    /// No handshake is performed: the client sends no authorization metadata, so
    /// the remote serves it as an anonymous session (and rejects it outright if
    /// anonymous access is disabled there).
    pub async fn connect(&self) -> anyhow::Result<FlightSqlServiceClient<Channel>> {
        let channel = Endpoint::from_shared(self.url.clone())
            .with_context(|| format!("invalid remote beacon endpoint '{}'", self.url))?
            .connect()
            .await
            .with_context(|| format!("failed to connect to remote beacon at '{}'", self.url))?;

        Ok(FlightSqlServiceClient::new(channel))
    }
}
