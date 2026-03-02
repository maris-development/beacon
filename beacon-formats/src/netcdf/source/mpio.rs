//! NetCDF reader client backed by an Arrow Flight gRPC server.
//!
//! This module connects to a running `beacon-arrow-netcdf-mpio` Arrow Flight server
//! and fetches schemas and record-batch streams over gRPC.  The server address is
//! read from `beacon_config::CONFIG.netcdf_flight_addr` (env var
//! `BEACON_NETCDF_FLIGHT_ADDR`, e.g. `http://127.0.0.1:50051`).
//!
//! **Architecture**
//!
//!
//! **Connection caching**
//!
//! The underlying `tonic::transport::Channel` is created once (lazily on the first
//! call) and reused for all subsequent requests.  `Channel` already implements an
//! internal connection pool, so this is safe and efficient.
//! Each public function creates a thin `FlightClient` wrapper around a cheap
//! `.clone()` of the cached channel.

use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use beacon_config::CONFIG;
use beacon_object_storage::DatasetsStore;
use object_store::ObjectMeta;
use tokio::sync::OnceCell;

/// Error type for Arrow Flight NetCDF operations.
#[derive(Debug, thiserror::Error)]
pub enum MpioError {
    /// No `BEACON_NETCDF_FLIGHT_ADDR` configured.
    #[error("No Arrow Flight server address configured (set BEACON_NETCDF_FLIGHT_ADDR)")]
    NoServerAddress,
    #[error("Arrow Flight transport error: {0}")]
    Transport(#[from] tonic::transport::Error),
    #[error("Arrow Flight RPC status: {0}")]
    Status(#[from] tonic::Status),
    #[error("Arrow Flight decode error: {0}")]
    FlightInternal(#[from] arrow_flight::error::FlightError),
    #[error("Arrow IPC schema decode error: {0}")]
    IpcSchema(#[from] arrow::error::ArrowError),
    #[error("Arrow Flight ticket serialization error: {0}")]
    SerdeJson(#[from] serde_json::Error),
    #[error("Arrow Flight returned an empty result")]
    EmptyResult,
    #[error("Failed to translate NetCDF URL path: {0}")]
    UrlTranslation(beacon_object_storage::error::StorageError),
}

/// Backward-compatible type aliases.
pub type FlightError = MpioError;
pub type PoolError = MpioError;

/// JSON ticket payload sent to the `beacon-arrow-netcdf-mpio` Flight server's `DoGet` RPC.
///
/// Must stay in sync with `DoGetRequest` in `beacon-arrow-netcdf-mpio/src/main.rs`.
#[derive(serde::Serialize, serde::Deserialize)]
struct FlightDoGetRequest {
    path: String,
    /// Optional zero-based column indices to project.  Ignored when `dimensions` is set.
    #[serde(default)]
    projection: Option<Vec<usize>>,
    /// Optional NetCDF dimension names.  Takes priority over `projection`.
    #[serde(default)]
    dimensions: Option<Vec<String>>,
}

/// Process-wide cached `tonic` channel.  Initialised on the first call; cloned cheaply
/// afterwards (the channel is a connection-pool internally).
static FLIGHT_CHANNEL: OnceCell<tonic::transport::Channel> = OnceCell::const_new();

/// Returns the configured server address or `MpioError::NoServerAddress`.
fn flight_addr() -> Result<String, MpioError> {
    CONFIG
        .netcdf_flight_addr
        .clone()
        .ok_or(MpioError::NoServerAddress)
}

/// Returns a `FlightClient` backed by the (lazily initialised, cached) channel.
async fn flight_client() -> Result<arrow_flight::FlightClient, MpioError> {
    let addr = flight_addr()?;
    let channel = FLIGHT_CHANNEL
        .get_or_try_init(|| async {
            tracing::debug!(%addr, "connecting to Arrow Flight server");
            tonic::transport::Endpoint::new(addr)
                .map_err(MpioError::Transport)?
                .connect()
                .await
                .map_err(MpioError::Transport)
        })
        .await?
        .clone();
    Ok(arrow_flight::FlightClient::new(channel))
}

/// Decode an Arrow [`Schema`] from the raw IPC bytes stored in [`FlightInfo::schema`].
fn decode_flight_schema(bytes: Vec<u8>) -> Result<SchemaRef, arrow::error::ArrowError> {
    let schema: arrow::datatypes::Schema = arrow_flight::IpcMessage(bytes.into()).try_into()?;
    Ok(Arc::new(schema))
}

/// Fetch the Arrow schema for a NetCDF file via the configured Arrow Flight server.
///
/// Calls `GetFlightInfo` with `descriptor.cmd = <local filesystem path bytes>`.
pub async fn read_schema(
    datasets_object_store: Arc<DatasetsStore>,
    object: ObjectMeta,
) -> Result<SchemaRef, MpioError> {
    let path = datasets_object_store
        .translate_netcdf_url_path(&object.location)
        .map_err(MpioError::UrlTranslation)?;

    let mut client = flight_client().await?;
    let descriptor = arrow_flight::FlightDescriptor::new_cmd(path.into_bytes());
    let info = client.get_flight_info(descriptor).await?;
    let schema = decode_flight_schema(info.schema.to_vec())?;
    Ok(schema)
}

/// Stream a NetCDF file as Arrow [`RecordBatch`]es via the configured Arrow Flight server.
///
/// Calls `DoGet` with a JSON-encoded [`FlightDoGetRequest`] ticket and returns the
/// resulting async stream of [`RecordBatch`]es.
///
/// # Arguments
/// * `projection` â€“ optional column indices (zero-based); ignored when `dimensions` is set
/// * `dimensions` â€“ optional NetCDF dimension names; takes priority over `projection`
pub async fn read_file_as_stream(
    datasets_object_store: Arc<DatasetsStore>,
    object: ObjectMeta,
    projection: Option<Vec<usize>>,
    dimensions: Option<Vec<String>>,
) -> Result<
    impl futures::Stream<Item = Result<RecordBatch, arrow_flight::error::FlightError>>,
    MpioError,
> {
    let path = datasets_object_store
        .translate_netcdf_url_path(&object.location)
        .map_err(MpioError::UrlTranslation)?;

    let ticket_bytes = serde_json::to_vec(&FlightDoGetRequest {
        path,
        projection,
        dimensions,
    })?;
    let ticket = arrow_flight::Ticket {
        ticket: ticket_bytes.into(),
    };

    let mut client = flight_client().await?;
    let stream = client.do_get(ticket).await?;
    Ok(stream)
}

/// Fetch all [`RecordBatch`]es for a NetCDF file via the configured Arrow Flight server
/// and concatenate them into a single batch.
///
/// Internally calls [`read_file_as_stream`] and collects the stream.
///
/// # Arguments
/// * `projection` optional column indices (zero-based); ignored when `dimensions` is set
/// * `dimensions` optional NetCDF dimension names; takes priority over `projection`
pub async fn read_file_as_batch(
    datasets_object_store: Arc<DatasetsStore>,
    object: ObjectMeta,
    projection: Option<Vec<usize>>,
    dimensions: Option<Vec<String>>,
) -> Result<RecordBatch, MpioError> {
    use futures::TryStreamExt as _;

    let stream = read_file_as_stream(datasets_object_store, object, projection, dimensions).await?;

    let batches: Vec<RecordBatch> = stream
        .try_collect()
        .await
        .map_err(MpioError::FlightInternal)?;

    if batches.is_empty() {
        return Err(MpioError::EmptyResult);
    }

    let schema = batches[0].schema();
    let batch = arrow::compute::concat_batches(&schema, &batches)?;
    Ok(batch)
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::net::SocketAddr;

    use arrow::datatypes::{DataType, Field, Schema};
    use arrow_flight::{FlightData, FlightDescriptor, IpcMessage, SchemaAsIpc, Ticket};
    use futures::TryStreamExt;

    // ── pure unit tests ──────────────────────────────────────────────────────

    /// `decode_flight_schema` must round-trip any Arrow schema through IPC bytes.
    #[test]
    fn decode_flight_schema_roundtrip() {
        use prost::Message;
        let schema = Schema::new(vec![
            Field::new("lat", DataType::Float64, true),
            Field::new("lon", DataType::Float64, true),
            Field::new("name", DataType::Utf8, false),
        ]);
        let options = arrow::ipc::writer::IpcWriteOptions::default();
        // let flight_data: FlightData = SchemaAsIpc::new(&schema, &options).into();
        let schema_ipc = SchemaAsIpc::new(&schema, &options);
        let ipc_bytes = IpcMessage::try_from(schema_ipc).unwrap();
        // println!("Length of IPC bytes: {}", ipc_bytes.len());
        // let schema: arrow::datatypes::Schema =
        //     arrow::datatypes::Schema::try_from(&flight_data).expect("try_from");
        // println!("Original schema: {:?}", schema);
        let ipc_vec = ipc_bytes.to_vec();
        let ipc_message = IpcMessage(ipc_vec.into());

        let schema: arrow::datatypes::Schema = ipc_message.try_into().expect("try_into");

        // let decoded = decode_flight_schema(ipc_bytes).expect("decode");
        // assert_eq!(decoded.fields().len(), 3);
        // assert_eq!(decoded.field(0).name(), "lat");
        // assert_eq!(decoded.field(0).data_type(), &DataType::Float64);
        // assert_eq!(decoded.field(2).name(), "name");
        // assert!(!decoded.field(2).is_nullable());
    }

    /// `decode_flight_schema` must return an error on garbage bytes.
    #[test]
    fn decode_flight_schema_rejects_garbage() {
        let result = decode_flight_schema(b"not ipc data".to_vec());
        assert!(result.is_err());
    }

    // ── FlightDoGetRequest serde ─────────────────────────────────────────────

    #[test]
    fn flight_do_get_request_serde_path_only() {
        let req = FlightDoGetRequest {
            path: "/data/file.nc".to_string(),
            projection: None,
            dimensions: None,
        };
        let json = serde_json::to_string(&req).unwrap();
        let decoded: FlightDoGetRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.path, "/data/file.nc");
        assert!(decoded.projection.is_none());
        assert!(decoded.dimensions.is_none());
    }

    #[test]
    fn flight_do_get_request_serde_with_projection() {
        let req = FlightDoGetRequest {
            path: "/data/file.nc".to_string(),
            projection: Some(vec![0, 2, 5]),
            dimensions: None,
        };
        let json = serde_json::to_string(&req).unwrap();
        let decoded: FlightDoGetRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.projection, Some(vec![0, 2, 5]));
        assert!(decoded.dimensions.is_none());
    }

    #[test]
    fn flight_do_get_request_serde_with_dimensions() {
        let req = FlightDoGetRequest {
            path: "/data/file.nc".to_string(),
            projection: None,
            dimensions: Some(vec!["N_PROF".to_string(), "N_LEVELS".to_string()]),
        };
        let json = serde_json::to_string(&req).unwrap();
        let decoded: FlightDoGetRequest = serde_json::from_str(&json).unwrap();
        assert!(decoded.projection.is_none());
        assert_eq!(
            decoded.dimensions,
            Some(vec!["N_PROF".to_string(), "N_LEVELS".to_string()])
        );
    }

    /// Missing optional fields in JSON must deserialize to `None` (via `#[serde(default)]`).
    #[test]
    fn flight_do_get_request_serde_missing_optional_fields() {
        let json = r#"{"path":"/data/file.nc"}"#;
        let decoded: FlightDoGetRequest = serde_json::from_str(json).unwrap();
        assert_eq!(decoded.path, "/data/file.nc");
        assert!(decoded.projection.is_none());
        assert!(decoded.dimensions.is_none());
    }

    // ── flight_addr ──────────────────────────────────────────────────────────

    /// When `BEACON_NETCDF_FLIGHT_ADDR` is unset, `flight_addr()` must return
    /// `MpioError::NoServerAddress`.  When it is set, `flight_addr()` must return `Ok`.
    #[test]
    fn flight_addr_matches_config() {
        if CONFIG.netcdf_flight_addr.is_none() {
            let err = flight_addr().unwrap_err();
            assert!(matches!(err, MpioError::NoServerAddress));
        } else {
            assert!(flight_addr().is_ok());
        }
    }

    // ── integration tests against the real beacon-arrow-netcdf-mpio binary ───
    //
    // These tests spawn the actual `beacon-arrow-netcdf-mpio` process on an
    // ephemeral port and exercise the mpio client against it, bypassing the
    // process-wide `FLIGHT_CHANNEL` cache so each test gets its own server
    // instance.
    //
    // The tests are skipped automatically when the binary has not been built
    // yet (e.g. on a fresh checkout before `cargo build`).  To build it:
    //   cargo build --package beacon-arrow-netcdf-mpio

    /// Returns the path to the `beacon-arrow-netcdf-mpio` binary inside the
    /// workspace `target/debug/` directory, or `None` if it has not been built.
    fn mpio_binary_path() -> Option<std::path::PathBuf> {
        let bin_name = format!("beacon-arrow-netcdf-mpio{}", std::env::consts::EXE_SUFFIX);
        // CARGO_MANIFEST_DIR is beacon-formats/; the workspace root is its parent.
        let workspace_root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .to_path_buf();
        let path = workspace_root.join("target").join("debug").join(&bin_name);
        if path.exists() { Some(path) } else { None }
    }

    /// Path to the shared NetCDF test fixture.
    fn test_nc_path() -> std::path::PathBuf {
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .join("beacon-arrow-netcdf")
            .join("test_files")
            .join("gridded-example.nc")
    }

    /// Creates a `FlightClient` that connects directly to `addr`, bypassing
    /// the process-wide `FLIGHT_CHANNEL` cache.
    async fn direct_flight_client(addr: SocketAddr) -> arrow_flight::FlightClient {
        let channel = tonic::transport::Endpoint::new(format!("http://{addr}"))
            .unwrap()
            .connect()
            .await
            .unwrap();
        arrow_flight::FlightClient::new(channel)
    }

    /// Spawns the real `beacon-arrow-netcdf-mpio` binary on an ephemeral port.
    ///
    /// Returns `None` when the binary has not been built (tests are skipped).
    /// The returned [`std::process::Child`] kills the server when dropped.
    async fn spawn_mpio_server() -> Option<(SocketAddr, std::process::Child)> {
        let binary = mpio_binary_path()?;

        // Reserve a free port by binding to :0, note it, then release so the
        // server process can bind it (small TOCTOU window, acceptable in tests).
        let tmp = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = tmp.local_addr().unwrap().port();
        drop(tmp);

        let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();

        let child = std::process::Command::new(&binary)
            .args(["--addr", &addr.to_string()])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("failed to spawn beacon-arrow-netcdf-mpio");

        // Poll until the server accepts TCP connections (up to 5 s).
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            if std::net::TcpStream::connect(addr).is_ok() {
                break;
            }
            if std::time::Instant::now() > deadline {
                panic!("beacon-arrow-netcdf-mpio did not become ready within 5 s on {addr}");
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }

        Some((addr, child))
    }

    /// `GetFlightInfo` must return a non-empty schema for a valid NetCDF file.
    #[tokio::test]
    async fn get_flight_info_returns_schema() {
        let nc_path = test_nc_path();
        if !nc_path.exists() {
            return;
        }
        let Some((addr, mut _server)) = spawn_mpio_server().await else {
            return;
        };

        let mut client = direct_flight_client(addr).await;
        let descriptor = FlightDescriptor::new_cmd(nc_path.to_string_lossy().as_bytes().to_vec());
        let info = client.get_flight_info(descriptor).await.unwrap();

        assert!(!info.schema.is_empty(), "schema bytes must be non-empty");
    }

    /// `DoGet` without projection must stream all columns.
    #[tokio::test]
    async fn do_get_streams_all_columns() {
        let nc_path = test_nc_path();
        if !nc_path.exists() {
            return;
        }
        let Some((addr, mut _server)) = spawn_mpio_server().await else {
            return;
        };

        let mut client = direct_flight_client(addr).await;

        // Build a ticket for the schema returned by `GetFlightInfo`.  The server expects a JSON-encoded `FlightDoGetRequest`.
        let descriptor = FlightDescriptor::new_cmd(nc_path.to_string_lossy().as_bytes().to_vec());
        let info = client.get_flight_info(descriptor).await.unwrap();

        let schema = info.try_decode_schema().unwrap();
        println!("Got FlightInfo with schema bytes: {:?}", schema);

        let lon_index = schema.column_with_name("lon").unwrap().0;
        let lat_index = schema.column_with_name("lat").unwrap().0;
        let time_index = schema.column_with_name("time").unwrap().0;
        let analysed_sst_index = schema.column_with_name("analysed_sst").unwrap().0;

        let ticket_bytes = serde_json::to_vec(&FlightDoGetRequest {
            path: nc_path.to_string_lossy().into_owned(),
            projection: Some(vec![lon_index, lat_index, time_index, analysed_sst_index]),
            dimensions: None,
        })
        .unwrap();
        let mut stream = client
            .do_get(Ticket {
                ticket: ticket_bytes.into(),
            })
            .await
            .unwrap();
        while let Some(batch) = stream.try_next().await.unwrap() {
            println!("got batch with {} columns", batch.num_columns());
            println!("Rows: {}", batch.num_rows());
            println!("Batch: {:?}", batch);
            break;
        }
        // assert!(!batches.is_empty());
        // assert!(batches[0].num_columns() > 0);
    }

    /// `DoGet` with a two-element projection must return exactly two columns.
    #[tokio::test]
    async fn do_get_respects_column_projection() {
        let nc_path = test_nc_path();
        if !nc_path.exists() {
            return;
        }
        let Some((addr, mut _server)) = spawn_mpio_server().await else {
            return;
        };

        let mut client = direct_flight_client(addr).await;
        let ticket_bytes = serde_json::to_vec(&FlightDoGetRequest {
            path: nc_path.to_string_lossy().into_owned(),
            projection: Some(vec![0, 1]),
            dimensions: None,
        })
        .unwrap();
        let stream = client
            .do_get(Ticket {
                ticket: ticket_bytes.into(),
            })
            .await
            .unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();

        assert!(!batches.is_empty());
        assert_eq!(batches[0].num_columns(), 2);
    }
}
