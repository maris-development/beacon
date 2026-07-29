//! Main Arrow Flight SQL service implementation for Beacon.

use std::{
    net::{IpAddr, SocketAddr},
    str::FromStr,
    sync::Arc,
    time::Duration,
};

use crate::flight_sql::{
    auth::{AuthContext, Authenticator},
    metadata::FlightSqlMetadata,
    storage::SqlHandleStore,
    util::{
        batch_to_response, build_flight_info, encode_schema, to_internal_status, FlightDataStream,
        HandshakeStream,
    },
};
use anyhow::Context;
use arrow::datatypes::Schema;
use arrow_flight::sql::{
    server::{FlightSqlService, PeekableFlightDataStream},
    ActionClosePreparedStatementRequest, ActionCreatePreparedStatementRequest,
    ActionCreatePreparedStatementResult, CommandGetCatalogs, CommandGetDbSchemas,
    CommandGetSqlInfo, CommandGetTableTypes, CommandGetTables, CommandPreparedStatementQuery,
    CommandPreparedStatementUpdate, CommandStatementQuery, DoPutPreparedStatementResult,
    ProstMessageExt, SqlInfo, TicketStatementQuery,
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

/// Flight SQL service backed by the shared Beacon runtime.
#[derive(Clone)]
pub(crate) struct BeaconFlightSqlService {
    runtime: Arc<crate::datalake::DataLake>,
    authenticator: Authenticator,
    statements: SqlHandleStore,
    prepared_statements: SqlHandleStore,
    metadata: FlightSqlMetadata,
}

impl BeaconFlightSqlService {
    /// Creates a service using the lake's configured anonymous-access policy.
    pub(crate) fn new(lake: Arc<crate::datalake::DataLake>) -> anyhow::Result<Self> {
        let allow_anonymous = lake.config().flight_sql.allow_anonymous;
        Self::new_with_options(lake, allow_anonymous)
    }

    /// Creates a service with an explicit anonymous-access setting, used primarily by tests.
    pub(super) fn new_with_options(
        lake: Arc<crate::datalake::DataLake>,
        allow_anonymous: bool,
    ) -> anyhow::Result<Self> {
        let flight_sql = lake.config().flight_sql.clone();
        let runtime = lake.clone();

        Ok(Self {
            metadata: FlightSqlMetadata::new(lake.clone())?,
            authenticator: Authenticator::new(
                lake.clone(),
                allow_anonymous,
                Duration::from_secs(flight_sql.token_ttl_secs),
            ),
            runtime,
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
            .runtime()
            .run_query(beacon_core::query::Query::sql(sql.clone()), auth.identity.clone())
            .await
            .map_err(to_internal_status)?
            .into_record_stream()
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
            .runtime()
            .run_query(beacon_core::query::Query::sql(sql), auth.identity.clone())
            .await
            .map_err(to_internal_status)?
            .into_record_stream()
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
            .runtime()
            .run_query(beacon_core::query::Query::sql(sql), auth.identity.clone())
            .await
            .map_err(to_internal_status)?
            .into_record_stream()
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
            .authorize_handshake(auth_value.as_deref(), first_request.as_ref())
            .await?;
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
        let auth = self.authenticator.authorize_request(&request).await?;
        let descriptor = request.into_inner();
        let batch = self.metadata.build_catalogs_batch(auth.identity).await?;
        let info = build_flight_info(batch.schema().as_ref(), &descriptor, &query.as_any())?;
        Ok(Response::new(info))
    }

    async fn get_flight_info_schemas(
        &self,
        query: CommandGetDbSchemas,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let auth = self.authenticator.authorize_request(&request).await?;
        let descriptor = request.into_inner();
        let batch = self
            .metadata
            .build_schemas_batch(query.clone(), auth.identity)
            .await?;
        let info = build_flight_info(batch.schema().as_ref(), &descriptor, &query.as_any())?;
        Ok(Response::new(info))
    }

    async fn get_flight_info_tables(
        &self,
        query: CommandGetTables,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let auth = self.authenticator.authorize_request(&request).await?;
        let descriptor = request.into_inner();
        let batch = self
            .metadata
            .build_tables_batch(query.clone(), auth.identity)
            .await?;
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
        let auth = self.authenticator.authorize_request(&request).await?;
        Ok(batch_to_response(
            self.metadata.build_catalogs_batch(auth.identity).await?,
        ))
    }

    async fn do_get_schemas(
        &self,
        query: CommandGetDbSchemas,
        request: Request<Ticket>,
    ) -> Result<Response<<Self::FlightService as FlightService>::DoGetStream>, Status> {
        let auth = self.authenticator.authorize_request(&request).await?;
        Ok(batch_to_response(
            self.metadata.build_schemas_batch(query, auth.identity).await?,
        ))
    }

    async fn do_get_tables(
        &self,
        query: CommandGetTables,
        request: Request<Ticket>,
    ) -> Result<Response<<Self::FlightService as FlightService>::DoGetStream>, Status> {
        let auth = self.authenticator.authorize_request(&request).await?;
        Ok(batch_to_response(
            self.metadata.build_tables_batch(query, auth.identity).await?,
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

    async fn do_put_prepared_statement_update(
        &self,
        query: CommandPreparedStatementUpdate,
        request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        let auth_value = self.authenticator.authorization_value(&request)?;
        let auth = self.authenticator.authorize_optional(auth_value).await?;

        // For simplicity, we execute the update immediately rather than waiting for the client to signal completion.
        let sql = self
            .get_prepared_statement(&query.prepared_statement_handle)
            .await?;
        let stream = self
            .runtime
            .runtime()
            .run_query(beacon_core::query::Query::sql(sql), auth.identity.clone())
            .await
            .map_err(to_internal_status)?
            .into_record_stream()
            .map_err(to_internal_status)?;

        // We consume the stream to ensure the update is fully executed, but ignore any output batches.
        let row_count = stream
            .map_err(to_internal_status)
            .try_collect::<Vec<_>>()
            .await?
            .into_iter()
            .map(|batch| batch.num_rows() as i64)
            .sum();

        Ok(row_count)
    }

    async fn do_action_create_prepared_statement(
        &self,
        query: ActionCreatePreparedStatementRequest,
        request: Request<Action>,
    ) -> Result<ActionCreatePreparedStatementResult, Status> {
        let auth = self.authenticator.authorize_request(&request).await?;
        let empty_schema = encode_schema(&Schema::empty())?;

        // DDL statements have no result schema and must not be executed here — executing to
        // infer the schema would cause a double-execution when do_put_prepared_statement_update
        // runs the statement for real (e.g. CREATE EXTERNAL TABLE would be created twice).
        let dataset_schema = if is_ddl(&query.query) {
            empty_schema.clone()
        } else {
            let stream = self
                .runtime
                .runtime()
            .run_query(beacon_core::query::Query::sql(query.query.clone()), auth.identity.clone())
            .await
            .map_err(to_internal_status)?
            .into_record_stream()
            .map_err(to_internal_status)?;
            encode_schema(stream.schema().as_ref())?
        };

        let prepared_statement_handle = self.prepared_statements.insert(query.query).await;

        Ok(ActionCreatePreparedStatementResult {
            prepared_statement_handle,
            dataset_schema,
            parameter_schema: empty_schema,
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

fn is_ddl(sql: &str) -> bool {
    let upper = sql.trim_start().to_ascii_uppercase();
    upper.starts_with("CREATE ")
        || upper.starts_with("DROP ")
        || upper.starts_with("ALTER ")
        || upper.starts_with("TRUNCATE ")
}

/// Builds the Flight SQL tonic service for embedding in a caller-managed server
/// — an ephemeral-port test server, a custom shutdown path, a shared tonic
/// router. Production serving goes through [`serve`]. `allow_anonymous` overrides
/// the lake's configured policy, so tests can exercise both postures.
///
/// The concrete service type stays private; it is returned behind `impl
/// FlightService` so callers can only wire it into a server.
pub fn flight_service(
    lake: Arc<crate::datalake::DataLake>,
    allow_anonymous: bool,
) -> anyhow::Result<FlightServiceServer<impl FlightService>> {
    let service = BeaconFlightSqlService::new_with_options(lake, allow_anonymous)?;
    Ok(FlightServiceServer::new(service))
}

/// Starts the Flight SQL gRPC server on the configured host and port.
pub async fn serve(lake: Arc<crate::datalake::DataLake>) -> anyhow::Result<()> {
    let flight_sql = lake.config().flight_sql.clone();
    let service = BeaconFlightSqlService::new(lake)?;
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
