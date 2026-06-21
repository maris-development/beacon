//! [`SQLExecutor`] backed by an Arrow Flight SQL client to a remote Beacon.
//!
//! `datafusion-federation` hands this executor the unparsed SQL for the largest
//! federatable sub-plan (filters, projection, limit, and whole joins/aggregates),
//! which we run on the remote instance and stream back as Arrow batches.

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::common::Statistics;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{PhysicalExpr, SendableRecordBatchStream};
use datafusion::sql::unparser::dialect::{DefaultDialect, Dialect};
use datafusion_federation::sql::SQLExecutor;
use futures::TryStreamExt;

use super::connection::RemoteConnection;

/// Maps any displayable error into a DataFusion external error.
fn remote_err<E: std::fmt::Display>(error: E) -> DataFusionError {
    DataFusionError::External(format!("remote beacon: {error}").into())
}

/// Executes federated SQL against a remote Beacon over Flight SQL.
#[derive(Debug)]
pub struct BeaconFlightSqlExecutor {
    connection: RemoteConnection,
    /// Stable identity of the remote compute context (its endpoint URL). Tables
    /// sharing a context federate together so joins/aggregates push down to the
    /// remote; distinct URLs never co-federate.
    context: String,
}

impl BeaconFlightSqlExecutor {
    pub fn new(connection: RemoteConnection) -> Self {
        let context = connection.url.clone();
        Self {
            connection,
            context,
        }
    }

    /// Fetch a remote table's schema without transferring data, via a
    /// `LIMIT 0` query whose `FlightInfo` carries the IPC-encoded schema.
    pub async fn fetch_schema(
        connection: &RemoteConnection,
        table: &str,
    ) -> DFResult<SchemaRef> {
        let mut client = connection.connect().await.map_err(remote_err)?;
        let info = client
            .execute(format!("SELECT * FROM {table} LIMIT 0"), None)
            .await
            .map_err(remote_err)?;
        let schema = info.try_decode_schema().map_err(remote_err)?;
        Ok(Arc::new(schema))
    }
}

#[async_trait]
impl SQLExecutor for BeaconFlightSqlExecutor {
    fn name(&self) -> &str {
        "beacon_flight_sql"
    }

    fn compute_context(&self) -> Option<String> {
        Some(self.context.clone())
    }

    fn dialect(&self) -> Arc<dyn Dialect> {
        // The remote is itself a Beacon/DataFusion instance, so DataFusion's own
        // unparser dialect round-trips cleanly through its SQL parser.
        Arc::new(DefaultDialect {})
    }

    fn execute(
        &self,
        query: &str,
        schema: SchemaRef,
        _filters: &[Arc<dyn PhysicalExpr>],
    ) -> DFResult<SendableRecordBatchStream> {
        let connection = self.connection.clone();
        let query = query.to_string();

        // Defer all async Flight work into the stream's first poll so `execute`
        // never blocks and needs no runtime handle — the same async→sync bridge
        // beacon uses elsewhere for streaming exec nodes.
        let stream = futures::stream::once(async move {
            let mut client = connection.connect().await.map_err(remote_err)?;
            let info = client.execute(query, None).await.map_err(remote_err)?;
            let endpoint = info
                .endpoint
                .into_iter()
                .next()
                .ok_or_else(|| remote_err("remote returned no flight endpoints"))?;
            let ticket = endpoint
                .ticket
                .ok_or_else(|| remote_err("remote flight endpoint missing ticket"))?;
            let record_stream = client.do_get(ticket).await.map_err(remote_err)?;
            Ok::<_, DataFusionError>(record_stream.map_err(remote_err))
        })
        .try_flatten();

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }

    async fn table_names(&self) -> DFResult<Vec<String>> {
        // Remote tables are constructed explicitly with a known table reference,
        // so auto-discovery is not used; return empty rather than enumerate.
        Ok(vec![])
    }

    async fn get_table_schema(&self, table_name: &str) -> DFResult<SchemaRef> {
        Self::fetch_schema(&self.connection, table_name).await
    }

    async fn statistics(&self, plan: &LogicalPlan) -> DFResult<Statistics> {
        Ok(Statistics::new_unknown(plan.schema().as_arrow()))
    }
}
