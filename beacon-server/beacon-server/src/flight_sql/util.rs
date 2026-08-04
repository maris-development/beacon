//! Small protocol helpers shared by the Flight SQL service implementation.

use std::pin::Pin;

use arrow::{datatypes::Schema, record_batch::RecordBatch};
use arrow_flight::{
    encode::FlightDataEncoderBuilder, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo,
    HandshakeResponse, IpcMessage, SchemaAsIpc, Ticket,
};
use arrow_ipc::writer::IpcWriteOptions;
use bytes::Bytes;
use futures::{Stream, TryStreamExt};
use prost::Message;
use tonic::{Response, Status};

/// Boxed handshake response stream returned by the Flight service.
pub(super) type HandshakeStream =
    Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send>>;
/// Boxed `DoGet` stream of encoded Flight data frames.
pub(super) type FlightDataStream = Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send>>;

/// Encodes an Arrow schema into the Flight SQL IPC wire format.
pub(super) fn encode_schema(schema: &Schema) -> Result<Bytes, Status> {
    let options = IpcWriteOptions::default();
    let schema_ipc = SchemaAsIpc::new(schema, &options);
    let ipc_message = IpcMessage::try_from(schema_ipc).map_err(to_internal_status)?;
    Ok(ipc_message.0)
}

/// Builds a `FlightInfo` response for a command-specific descriptor and ticket payload.
pub(super) fn build_flight_info(
    schema: &Schema,
    descriptor: &FlightDescriptor,
    ticket_message: &arrow_flight::sql::Any,
) -> Result<FlightInfo, Status> {
    let mut info = FlightInfo::new()
        .try_with_schema(schema)
        .map_err(to_internal_status)?
        .with_descriptor(descriptor.clone())
        .with_endpoint(FlightEndpoint {
            ticket: Some(Ticket {
                ticket: ticket_message.encode_to_vec().into(),
            }),
            location: vec![],
            expiration_time: None,
            app_metadata: Bytes::new(),
        });
    info.total_records = -1;
    info.total_bytes = -1;
    Ok(info)
}

/// Wraps a single metadata batch into a Flight `DoGet` response stream.
pub(super) fn batch_to_response(batch: RecordBatch) -> Response<FlightDataStream> {
    let schema = batch.schema();
    let stream = futures::stream::once(async move { Ok(batch) });
    let flight_stream = FlightDataEncoderBuilder::new()
        .with_schema(schema)
        .build(stream)
        .map_err(to_internal_status);

    Response::new(Box::pin(flight_stream) as FlightDataStream)
}

/// Maps internal errors onto gRPC internal statuses for Flight responses.
pub(super) fn to_internal_status(error: impl std::fmt::Display) -> Status {
    Status::internal(error.to_string())
}
