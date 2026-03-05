use std::{net::SocketAddr, num::NonZeroUsize, pin::Pin, sync::Mutex};

use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo,
    HandshakeRequest, HandshakeResponse, IpcMessage, PutResult, SchemaAsIpc, SchemaResult, Ticket,
    encode::FlightDataEncoderBuilder,
    error::FlightError,
    flight_service_server::{FlightService, FlightServiceServer},
};
use beacon_arrow_netcdf::reader::NetCDFArrowReader;
use clap::Parser;
use futures::future;
use futures::{Stream, TryStreamExt};
use lru::LruCache;
use std::sync::Arc;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};

/// Ticket payload for [`FlightService::do_get`].
///
/// Serialised as JSON bytes in the Arrow Flight ticket.
#[derive(serde::Serialize, serde::Deserialize)]
struct DoGetRequest {
    /// Absolute or relative path to the NetCDF file.
    path: String,
    /// Optional zero-based column indices to project.  `None` returns all columns.
    /// Mutually exclusive with `dimensions`; `dimensions` takes priority if both are set.
    #[serde(default)]
    projection: Option<Vec<usize>>,
    /// Optional NetCDF dimension names to project (e.g. `["N_PROF", "N_LEVELS"]`).
    /// Selects all variables that have at least one of the listed dimensions.
    /// Takes priority over `projection` when both are set.
    #[serde(default)]
    dimensions: Option<Vec<String>>,
}

/// Arrow Flight server that streams local Arrow IPC files.
#[derive(Parser, Debug)]
#[command(version, about)]
struct Args {
    /// Base address to listen on.  When `--num-servers` > 1 additional listeners are bound
    /// on consecutive ports (base+1, base+2, …).  All share the same LRU reader cache.
    #[arg(long, default_value = "127.0.0.1:50051")]
    addr: SocketAddr,
    /// Maximum number of open [`NetCDFArrowReader`] instances to keep in the LRU cache.
    #[arg(long, default_value_t = 64)]
    cache_capacity: usize,
    /// Number of Arrow Flight listeners to spawn on consecutive ports.
    #[arg(long, default_value_t = 1)]
    num_servers: usize,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let n = args.num_servers.max(1);

    // All listeners share a single ReaderService (and therefore a single LRU cache).
    let service = ReaderService::new(args.cache_capacity);

    let servers = (0..n).map(|i| {
        let mut addr = args.addr;
        addr.set_port(args.addr.port() + i as u16);
        let svc = service.clone();
        async move {
            Server::builder()
                .add_service(FlightServiceServer::new(svc))
                .serve(addr)
                .await
        }
    });

    future::try_join_all(servers).await?;
    Ok(())
}

type BoxStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

#[derive(Clone)]
pub struct ReaderService {
    cache: Arc<Mutex<LruCache<String, Arc<NetCDFArrowReader>>>>,
}

impl ReaderService {
    fn new(capacity: usize) -> Self {
        let cap = NonZeroUsize::new(capacity.max(1)).expect("capacity >= 1");
        Self {
            cache: Arc::new(Mutex::new(LruCache::new(cap))),
        }
    }

    /// Returns a cached reader for `path`, opening it on first access.
    fn get_or_open(&self, path: &str) -> Result<Arc<NetCDFArrowReader>, Status> {
        let mut cache = self.cache.lock().expect("cache lock poisoned");
        if let Some(reader) = cache.get(path) {
            return Ok(Arc::clone(reader));
        }
        let reader = NetCDFArrowReader::new(path)
            .map_err(|e| Status::not_found(format!("cannot open NetCDF file: {e}")))?;
        let reader = Arc::new(reader);
        cache.put(path.to_owned(), Arc::clone(&reader));
        Ok(reader)
    }
}

#[tonic::async_trait]
impl FlightService for ReaderService {
    type HandshakeStream = BoxStream<HandshakeResponse>;
    type ListFlightsStream = BoxStream<FlightInfo>;
    type DoGetStream = BoxStream<FlightData>;
    type DoPutStream = BoxStream<PutResult>;
    type DoExchangeStream = BoxStream<FlightData>;
    type DoActionStream = BoxStream<arrow_flight::Result>;
    type ListActionsStream = BoxStream<ActionType>;

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("handshake not supported"))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("list_flights not supported"))
    }

    /// Returns a [`FlightInfo`] describing the schema of the NetCDF file at the given path.
    ///
    /// The [`FlightDescriptor`] `cmd` field must be a UTF-8 path to a NetCDF file.
    /// The returned [`FlightInfo`] contains the encoded schema and a [`FlightEndpoint`] whose
    /// ticket is a JSON-encoded [`DoGetRequest`] (with no projection) that can be passed
    /// directly to `do_get` to stream all columns.
    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let descriptor = request.into_inner();
        let path = String::from_utf8(descriptor.cmd.to_vec())
            .map_err(|_| Status::invalid_argument("descriptor cmd must be a UTF-8 file path"))?;

        let reader = self.get_or_open(&path)?;
        let schema = tokio::task::spawn_blocking(move || Ok::<_, Status>(reader.schema()))
            .await
            .map_err(|e| Status::internal(e.to_string()))??;

        // Build a ticket that `do_get` can consume directly.
        let ticket_bytes = serde_json::to_vec(&DoGetRequest {
            path: String::from_utf8(descriptor.cmd.to_vec()).unwrap_or_default(),
            projection: None,
            dimensions: None,
        })
        .map_err(|e| Status::internal(e.to_string()))?;

        let info = FlightInfo {
            flight_descriptor: Some(descriptor.clone()),
            endpoint: vec![FlightEndpoint {
                ticket: Some(Ticket {
                    ticket: ticket_bytes.into(),
                }),
                ..Default::default()
            }],
            total_records: -1,
            total_bytes: -1,
            ..Default::default()
        }
        .try_with_schema(&schema)
        .map_err(|e| Status::internal(format!("error building FlightInfo: {e}")))?;

        Ok(Response::new(info))
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("get_schema not supported"))
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<arrow_flight::PollInfo>, Status> {
        Err(Status::unimplemented("poll_flight_info not supported"))
    }

    /// Streams a NetCDF file as Arrow record batches.
    ///
    /// The ticket payload must be a JSON-encoded [`DoGetRequest`]:
    /// ```json
    /// {"path": "/data/file.nc"}
    /// {"path": "/data/file.nc", "projection": [0, 2, 5]}
    /// {"path": "/data/file.nc", "dimensions": ["N_PROF", "N_LEVELS"]}
    /// ```
    /// `projection` selects columns by zero-based index; `dimensions` selects all variables
    /// that have at least one of the named NetCDF dimensions (e.g. `"N_PROF"`, `"N_LEVELS"`).
    /// `dimensions` takes priority when both are supplied.
    /// Omit both to stream all columns.  Each batch contains at most 65 536 rows.
    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let req: DoGetRequest =
            serde_json::from_slice(&request.into_inner().ticket).map_err(|e| {
                Status::invalid_argument(format!("ticket must be JSON DoGetRequest: {e}"))
            })?;

        let reader = self.get_or_open(&req.path)?;
        let (schema, batch_stream) = tokio::task::spawn_blocking(move || {
            // Dimension projection takes priority over column-index projection.
            let stream = if let Some(dims) = req.dimensions {
                reader
                    .project_with_dimensions(&dims)
                    .map_err(|e| Status::internal(format!("dimension projection error: {e}")))?
                    .read_as_arrow_stream::<&[usize]>(None, 65_536)
                    .map_err(|e| Status::internal(format!("stream error: {e}")))?
            } else {
                reader
                    .read_as_arrow_stream(req.projection.as_deref(), 65_536)
                    .map_err(|e| Status::internal(format!("stream error: {e}")))?
            };
            let schema = stream.schema();
            Ok::<_, Status>((schema, stream))
        })
        .await
        .map_err(|e| Status::internal(e.to_string()))??;

        let flight_stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(batch_stream.map_err(|e| FlightError::ExternalError(e.into())))
            .map_err(|e| Status::internal(e.to_string()));

        Ok(Response::new(Box::pin(flight_stream)))
    }

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("do_put not supported"))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("do_exchange not supported"))
    }

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("do_action not supported"))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("list_actions not supported"))
    }
}
