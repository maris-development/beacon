//! Main Arrow Flight SQL service implementation for Beacon.

use std::{
    net::{IpAddr, SocketAddr},
    str::FromStr,
    sync::Arc,
    time::Duration,
};

use anyhow::Context;
use arrow::datatypes::Schema;
use arrow_flight::sql::{
    server::{FlightSqlService, PeekableFlightDataStream},
    ActionClosePreparedStatementRequest, ActionCreatePreparedStatementRequest,
    ActionCreatePreparedStatementResult, CommandGetCatalogs, CommandGetDbSchemas,
    CommandGetSqlInfo, CommandGetTableTypes, CommandGetTables, CommandPreparedStatementQuery,
    CommandStatementQuery, DoPutPreparedStatementResult, ProstMessageExt, SqlInfo,
    TicketStatementQuery,
};
use arrow_flight::{
    encode::FlightDataEncoderBuilder, error::FlightError, flight_service_server::FlightService,
    flight_service_server::FlightServiceServer, Action, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, Ticket,
};
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use tonic::{metadata::MetadataValue, transport::Server, Request, Response, Status, Streaming};
use tracing::info;

use crate::flight_sql::{
    auth::{AuthContext, Authenticator},
    metadata::FlightSqlMetadata,
    storage::SqlHandleStore,
    util::{
        batch_to_response, build_flight_info, encode_schema, to_internal_status, FlightDataStream,
        HandshakeStream,
    },
};

/// Flight SQL service backed by the shared Beacon runtime.
#[derive(Clone)]
pub(crate) struct BeaconFlightSqlService {
    runtime: Arc<beacon_core::runtime::Runtime>,
    authenticator: Authenticator,
    statements: SqlHandleStore,
    prepared_statements: SqlHandleStore,
    metadata: FlightSqlMetadata,
}

impl BeaconFlightSqlService {
    /// Creates a service using the configured anonymous-access policy.
    pub(crate) fn new(runtime: Arc<beacon_core::runtime::Runtime>) -> anyhow::Result<Self> {
        Self::new_with_options(runtime, beacon_config::CONFIG.flight_sql.allow_anonymous)
    }

    /// Creates a service with an explicit anonymous-access setting, used primarily by tests.
    pub(super) fn new_with_options(
        runtime: Arc<beacon_core::runtime::Runtime>,
        allow_anonymous: bool,
    ) -> anyhow::Result<Self> {
        let flight_sql = &beacon_config::CONFIG.flight_sql;

        Ok(Self {
            metadata: FlightSqlMetadata::new(runtime.clone())?,
            runtime,
            authenticator: Authenticator::new(
                allow_anonymous,
                Duration::from_secs(flight_sql.token_ttl_secs),
            ),
            statements: SqlHandleStore::new(Duration::from_secs(flight_sql.statement_ttl_secs)),
            prepared_statements: SqlHandleStore::new(Duration::from_secs(
                flight_sql.prepared_statement_ttl_secs,
            )),
        })
    }

    /// Prepares `FlightInfo` for a one-shot statement execution ticket.
    async fn build_statement_info(
        &self,
        sql: String,
        auth: AuthContext,
        descriptor: FlightDescriptor,
    ) -> Result<FlightInfo, Status> {
        let stream = self
            .runtime
            .run_sql(sql.clone(), auth.is_super_user)
            .await
            .map_err(to_internal_status)?;
        let schema = stream.schema();
        // Statement tickets are one-shot: the SQL is stored once and consumed by `do_get_statement`.
        let handle = self.statements.insert(sql).await;
        let ticket = TicketStatementQuery {
            statement_handle: handle,
        };

        build_flight_info(schema.as_ref(), &descriptor, &ticket.as_any())
    }

    /// Prepares `FlightInfo` for an existing prepared statement handle.
    async fn build_prepared_info(
        &self,
        query: CommandPreparedStatementQuery,
        auth: AuthContext,
        descriptor: FlightDescriptor,
    ) -> Result<FlightInfo, Status> {
        let sql = self
            .get_prepared_statement(&query.prepared_statement_handle)
            .await?;
        let stream = self
            .runtime
            .run_sql(sql, auth.is_super_user)
            .await
            .map_err(to_internal_status)?;

        build_flight_info(stream.schema().as_ref(), &descriptor, &query.as_any())
    }

    /// Executes SQL and converts the resulting Arrow stream into Flight data frames.
    async fn execute_sql_as_flight_stream(
        &self,
        sql: String,
        auth: AuthContext,
    ) -> Result<FlightDataStream, Status> {
        let stream = self
            .runtime
            .run_sql(sql, auth.is_super_user)
            .await
            .map_err(to_internal_status)?;
        let schema = stream.schema();

        let flight_stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(stream.map_err(|error| FlightError::ExternalError(error.into())))
            .map_err(to_internal_status);

        Ok(Box::pin(flight_stream))
    }

    /// Resolves a prepared statement handle to the stored SQL text.
    async fn get_prepared_statement(&self, handle: &Bytes) -> Result<String, Status> {
        self.prepared_statements.get(handle).await
    }
}

#[tonic::async_trait]
impl FlightSqlService for BeaconFlightSqlService {
    type FlightService = Self;

    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {}

    async fn do_handshake(
        &self,
        request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<<Self::FlightService as FlightService>::HandshakeStream>, Status> {
        let auth_value = self.authenticator.authorization_value(&request)?;
        let mut stream = request.into_inner();
        let first_request = stream.next().await.transpose()?;
        let protocol_version = first_request
            .as_ref()
            .map(|request| request.protocol_version)
            .unwrap_or_default();
        let auth = self
            .authenticator
            .authorize_handshake(auth_value.as_deref(), first_request.as_ref())?;
        // Flight SQL clients reuse the bearer token returned by the handshake for subsequent RPCs.
        let token = self.authenticator.issue_token(auth).await;

        let response_stream = futures::stream::once(async move {
            Ok(HandshakeResponse {
                protocol_version,
                payload: Bytes::new(),
            })
        });

        let mut response = Response::new(Box::pin(response_stream) as HandshakeStream);
        let metadata = MetadataValue::try_from(format!("Bearer {token}"))
            .map_err(|_| Status::internal("failed to set bearer token metadata"))?;
        response.metadata_mut().insert("authorization", metadata);

        Ok(response)
    }

    async fn get_flight_info_statement(
        &self,
        query: CommandStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let auth = self.authenticator.authorize_request(&request).await?;
        let descriptor = request.into_inner();
        let info = self
            .build_statement_info(query.query, auth, descriptor)
            .await?;
        Ok(Response::new(info))
    }

    async fn get_flight_info_prepared_statement(
        &self,
        query: CommandPreparedStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let auth = self.authenticator.authorize_request(&request).await?;
        let descriptor = request.into_inner();
        let info = self.build_prepared_info(query, auth, descriptor).await?;
        Ok(Response::new(info))
    }

    async fn get_flight_info_catalogs(
        &self,
        query: CommandGetCatalogs,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        self.authenticator.authorize_request(&request).await?;
        let descriptor = request.into_inner();
        let batch = self.metadata.build_catalogs_batch()?;
        let info = build_flight_info(batch.schema().as_ref(), &descriptor, &query.as_any())?;
        Ok(Response::new(info))
    }

    async fn get_flight_info_schemas(
        &self,
        query: CommandGetDbSchemas,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        self.authenticator.authorize_request(&request).await?;
        let descriptor = request.into_inner();
        let batch = self.metadata.build_schemas_batch(query.clone())?;
        let info = build_flight_info(batch.schema().as_ref(), &descriptor, &query.as_any())?;
        Ok(Response::new(info))
    }

    async fn get_flight_info_tables(
        &self,
        query: CommandGetTables,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        self.authenticator.authorize_request(&request).await?;
        let descriptor = request.into_inner();
        let batch = self.metadata.build_tables_batch(query.clone()).await?;
        let info = build_flight_info(batch.schema().as_ref(), &descriptor, &query.as_any())?;
        Ok(Response::new(info))
    }

    async fn get_flight_info_table_types(
        &self,
        query: CommandGetTableTypes,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        self.authenticator.authorize_request(&request).await?;
        let descriptor = request.into_inner();
        let batch = self.metadata.build_table_types_batch()?;
        let info = build_flight_info(batch.schema().as_ref(), &descriptor, &query.as_any())?;
        Ok(Response::new(info))
    }

    async fn get_flight_info_sql_info(
        &self,
        query: CommandGetSqlInfo,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        self.authenticator.authorize_request(&request).await?;
        let descriptor = request.into_inner();
        let batch = self.metadata.build_sql_info_batch(query.clone())?;
        let info = build_flight_info(batch.schema().as_ref(), &descriptor, &query.as_any())?;
        Ok(Response::new(info))
    }

    async fn do_get_statement(
        &self,
        ticket: TicketStatementQuery,
        request: Request<Ticket>,
    ) -> Result<Response<<Self::FlightService as FlightService>::DoGetStream>, Status> {
        let auth = self.authenticator.authorize_request(&request).await?;
        let sql = self.statements.take(&ticket.statement_handle).await?;
        let stream = self.execute_sql_as_flight_stream(sql, auth).await?;
        Ok(Response::new(stream))
    }

    async fn do_get_prepared_statement(
        &self,
        query: CommandPreparedStatementQuery,
        request: Request<Ticket>,
    ) -> Result<Response<<Self::FlightService as FlightService>::DoGetStream>, Status> {
        let auth = self.authenticator.authorize_request(&request).await?;
        let sql = self
            .get_prepared_statement(&query.prepared_statement_handle)
            .await?;
        let stream = self.execute_sql_as_flight_stream(sql, auth).await?;
        Ok(Response::new(stream))
    }

    async fn do_get_catalogs(
        &self,
        _query: CommandGetCatalogs,
        request: Request<Ticket>,
    ) -> Result<Response<<Self::FlightService as FlightService>::DoGetStream>, Status> {
        self.authenticator.authorize_request(&request).await?;
        Ok(batch_to_response(self.metadata.build_catalogs_batch()?))
    }

    async fn do_get_schemas(
        &self,
        query: CommandGetDbSchemas,
        request: Request<Ticket>,
    ) -> Result<Response<<Self::FlightService as FlightService>::DoGetStream>, Status> {
        self.authenticator.authorize_request(&request).await?;
        Ok(batch_to_response(self.metadata.build_schemas_batch(query)?))
    }

    async fn do_get_tables(
        &self,
        query: CommandGetTables,
        request: Request<Ticket>,
    ) -> Result<Response<<Self::FlightService as FlightService>::DoGetStream>, Status> {
        self.authenticator.authorize_request(&request).await?;
        Ok(batch_to_response(
            self.metadata.build_tables_batch(query).await?,
        ))
    }

    async fn do_get_table_types(
        &self,
        _query: CommandGetTableTypes,
        request: Request<Ticket>,
    ) -> Result<Response<<Self::FlightService as FlightService>::DoGetStream>, Status> {
        self.authenticator.authorize_request(&request).await?;
        Ok(batch_to_response(self.metadata.build_table_types_batch()?))
    }

    async fn do_get_sql_info(
        &self,
        query: CommandGetSqlInfo,
        request: Request<Ticket>,
    ) -> Result<Response<<Self::FlightService as FlightService>::DoGetStream>, Status> {
        self.authenticator.authorize_request(&request).await?;
        Ok(batch_to_response(
            self.metadata.build_sql_info_batch(query)?,
        ))
    }

    async fn do_put_prepared_statement_query(
        &self,
        query: CommandPreparedStatementQuery,
        request: Request<PeekableFlightDataStream>,
    ) -> Result<DoPutPreparedStatementResult, Status> {
        let auth_value = self.authenticator.authorization_value(&request)?;
        self.authenticator.authorize_optional(auth_value).await?;

        Ok(DoPutPreparedStatementResult {
            prepared_statement_handle: Some(query.prepared_statement_handle),
        })
    }

    async fn do_action_create_prepared_statement(
        &self,
        query: ActionCreatePreparedStatementRequest,
        request: Request<Action>,
    ) -> Result<ActionCreatePreparedStatementResult, Status> {
        let auth = self.authenticator.authorize_request(&request).await?;
        let stream = self
            .runtime
            .run_sql(query.query.clone(), auth.is_super_user)
            .await
            .map_err(to_internal_status)?;
        let dataset_schema = encode_schema(stream.schema().as_ref())?;
        let parameter_schema = encode_schema(&Schema::empty())?;
        let prepared_statement_handle = self.prepared_statements.insert(query.query).await;

        Ok(ActionCreatePreparedStatementResult {
            prepared_statement_handle,
            dataset_schema,
            parameter_schema,
        })
    }

    async fn do_action_close_prepared_statement(
        &self,
        query: ActionClosePreparedStatementRequest,
        request: Request<Action>,
    ) -> Result<(), Status> {
        self.authenticator.authorize_request(&request).await?;
        self.prepared_statements
            .close(&query.prepared_statement_handle)
            .await
    }
}

/// Starts the Flight SQL gRPC server on the configured host and port.
pub(crate) async fn serve(runtime: Arc<beacon_core::runtime::Runtime>) -> anyhow::Result<()> {
    let service = BeaconFlightSqlService::new(runtime)?;
    let flight_sql = &beacon_config::CONFIG.flight_sql;
    let addr = SocketAddr::new(
        IpAddr::from_str(&flight_sql.host)
            .map_err(|error| anyhow::anyhow!("failed to parse Flight SQL host: {error}"))?,
        flight_sql.port,
    );

    info!(
        "Flight SQL listening on {addr} (anonymous access: {})",
        flight_sql.allow_anonymous
    );

    Server::builder()
        .add_service(FlightServiceServer::new(service))
        .serve(addr)
        .await
        .context("Flight SQL server failed")?;

    Ok(())
}
