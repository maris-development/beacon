use std::{
    collections::HashMap,
    sync::{Mutex, OnceLock},
    time::{Duration, Instant},
};

use anyhow::{anyhow, Context};
use arrow::{
    ipc::{
        writer::{IpcWriteOptions, StreamWriter},
        CompressionType,
    },
    record_batch::RecordBatch,
};
use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Query, State,
    },
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    Json,
};
use beacon_core::runtime::Runtime;
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::auth::verify_basic_auth_header;

static WS_TICKET_STORE: OnceLock<Mutex<HashMap<String, Instant>>> = OnceLock::new();
const WS_SQL_MAX_BINARY_MESSAGE_BYTES: usize = 1024 * 1024;

fn ticket_store() -> &'static Mutex<HashMap<String, Instant>> {
    WS_TICKET_STORE.get_or_init(|| Mutex::new(HashMap::new()))
}

#[derive(Debug, Deserialize)]
pub(crate) struct WsSqlAuthQuery {
    ticket: Option<String>,
}

#[derive(Debug, Serialize)]
pub(crate) struct WsSqlTicketResponse {
    ticket: String,
    expires_in_seconds: u64,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum WsClientMessage {
    RunSql {
        sql: String,
        request_id: Option<String>,
    },
    Cancel {
        request_id: String,
    },
    Ping,
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum WsServerEvent {
    Ready,
    Accepted {
        request_id: String,
    },
    Chunk {
        request_id: String,
        batch_index: usize,
        row_count: usize,
    },
    Done {
        request_id: String,
        total_rows: u64,
    },
    Error {
        request_id: Option<String>,
        message: String,
    },
    Pong,
}

pub(crate) async fn create_ws_sql_ticket(
    headers: HeaderMap,
) -> Result<Json<WsSqlTicketResponse>, StatusCode> {
    ensure_ws_sql_enabled()?;
    verify_basic_auth_header(&headers)?;

    let ttl_secs = beacon_config::CONFIG.ws_sql_ticket_ttl_secs;
    let ticket = Uuid::new_v4().to_string();
    let expires_at = Instant::now() + Duration::from_secs(ttl_secs);

    let mut store = ticket_store()
        .lock()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    store.retain(|_, expiry| *expiry > Instant::now());
    store.insert(ticket.clone(), expires_at);

    Ok(Json(WsSqlTicketResponse {
        ticket,
        expires_in_seconds: ttl_secs,
    }))
}

pub(crate) async fn ws_sql_handler(
    ws: WebSocketUpgrade,
    headers: HeaderMap,
    Query(auth_query): Query<WsSqlAuthQuery>,
    State(state): State<std::sync::Arc<Runtime>>,
) -> Result<impl IntoResponse, StatusCode> {
    ensure_ws_sql_enabled()?;
    authenticate_ws_upgrade(&headers, auth_query.ticket.as_deref())?;

    Ok(ws.on_upgrade(move |socket| async move {
        if let Err(err) = handle_ws_connection(socket, state).await {
            tracing::error!(error = ?err, "websocket sql connection failed");
        }
    }))
}

async fn handle_ws_connection(
    mut socket: WebSocket,
    runtime: std::sync::Arc<Runtime>,
) -> anyhow::Result<()> {
    send_event(&mut socket, WsServerEvent::Ready).await?;

    while let Some(next_msg) = socket.recv().await {
        match next_msg {
            Ok(Message::Text(text)) => {
                let parsed: WsClientMessage = serde_json::from_str(&text)
                    .with_context(|| "invalid websocket JSON message")?;

                match parsed {
                    WsClientMessage::RunSql { sql, request_id } => {
                        let request_id = request_id.unwrap_or_else(|| Uuid::new_v4().to_string());

                        if sql.len() > beacon_config::CONFIG.ws_sql_max_sql_bytes {
                            send_event(
                                &mut socket,
                                WsServerEvent::Error {
                                    request_id: Some(request_id),
                                    message: "SQL payload too large".to_string(),
                                },
                            )
                            .await?;
                            continue;
                        }

                        send_event(
                            &mut socket,
                            WsServerEvent::Accepted {
                                request_id: request_id.clone(),
                            },
                        )
                        .await?;

                        let mut stream = match runtime.run_sql(sql, true).await {
                            Ok(stream) => stream,
                            Err(err) => {
                                send_event(
                                    &mut socket,
                                    WsServerEvent::Error {
                                        request_id: Some(request_id),
                                        message: err.to_string(),
                                    },
                                )
                                .await?;
                                continue;
                            }
                        };

                        let mut total_rows = 0u64;
                        let mut batch_index = 0usize;

                        loop {
                            match stream.try_next().await {
                                Ok(Some(batch)) => {
                                    let payload_chunks =
                                        encode_batch_to_arrow_stream_chunks(
                                            &batch,
                                            WS_SQL_MAX_BINARY_MESSAGE_BYTES,
                                        )
                                        .with_context(|| {
                                            format!(
                                                "failed to encode batch {batch_index} for request {request_id}"
                                            )
                                        })?;

                                    for (row_count, arrow_payload) in payload_chunks {
                                        socket.send(Message::Binary(arrow_payload.into())).await?;

                                        send_event(
                                            &mut socket,
                                            WsServerEvent::Chunk {
                                                request_id: request_id.clone(),
                                                batch_index,
                                                row_count,
                                            },
                                        )
                                        .await?;

                                        total_rows += row_count as u64;
                                        batch_index += 1;
                                    }
                                }
                                Ok(None) => {
                                    send_event(
                                        &mut socket,
                                        WsServerEvent::Done {
                                            request_id,
                                            total_rows,
                                        },
                                    )
                                    .await?;
                                    break;
                                }
                                Err(err) => {
                                    send_event(
                                        &mut socket,
                                        WsServerEvent::Error {
                                            request_id: Some(request_id),
                                            message: err.to_string(),
                                        },
                                    )
                                    .await?;
                                    break;
                                }
                            }
                        }
                    }
                    WsClientMessage::Cancel { request_id } => {
                        send_event(
                            &mut socket,
                            WsServerEvent::Error {
                                request_id: Some(request_id),
                                message: "Cancel is not yet supported on this websocket endpoint"
                                    .to_string(),
                            },
                        )
                        .await?;
                    }
                    WsClientMessage::Ping => {
                        send_event(&mut socket, WsServerEvent::Pong).await?;
                    }
                }
            }
            Ok(Message::Ping(payload)) => {
                socket.send(Message::Pong(payload)).await?;
            }
            Ok(Message::Close(_)) => {
                break;
            }
            Ok(Message::Binary(_)) => {
                send_event(
                    &mut socket,
                    WsServerEvent::Error {
                        request_id: None,
                        message: "Binary client messages are not supported".to_string(),
                    },
                )
                .await?;
            }
            Ok(Message::Pong(_)) => {}
            Err(err) => {
                return Err(err.into());
            }
        }
    }

    Ok(())
}

fn encode_batch_to_arrow_stream(batch: &RecordBatch) -> anyhow::Result<Vec<u8>> {
    let mut payload = Vec::new();
    let options = IpcWriteOptions::default().try_with_compression(Some(CompressionType::ZSTD))?;
    let mut writer =
        StreamWriter::try_new_with_options(&mut payload, batch.schema().as_ref(), options)?;
    writer.write(batch)?;
    writer.finish()?;
    Ok(payload)
}

fn encode_batch_to_arrow_stream_chunks(
    batch: &RecordBatch,
    max_payload_bytes: usize,
) -> anyhow::Result<Vec<(usize, Vec<u8>)>> {
    if max_payload_bytes == 0 {
        return Err(anyhow!("max_payload_bytes must be greater than zero"));
    }

    let full_payload = encode_batch_to_arrow_stream(batch)?;
    if full_payload.len() <= max_payload_bytes {
        return Ok(vec![(batch.num_rows(), full_payload)]);
    }

    let mut chunks = Vec::new();
    let mut offset = 0usize;
    let row_count = batch.num_rows();

    while offset < row_count {
        let mut lo = 1usize;
        let mut hi = row_count - offset;
        let mut best_fit: Option<(usize, Vec<u8>)> = None;

        while lo <= hi {
            let mid = lo + (hi - lo) / 2;
            let payload = encode_batch_to_arrow_stream(&batch.slice(offset, mid))?;

            if payload.len() <= max_payload_bytes {
                best_fit = Some((mid, payload));
                lo = mid + 1;
            } else if mid == 1 {
                hi = 0;
            } else {
                hi = mid - 1;
            }
        }

        let (chunk_rows, chunk_payload) = match best_fit {
            Some(result) => result,
            None => {
                let oversized_payload = encode_batch_to_arrow_stream(&batch.slice(offset, 1))?;
                return Err(anyhow!(
                    "single-row payload of {} bytes exceeds configured websocket max payload size {} bytes",
                    oversized_payload.len(),
                    max_payload_bytes
                ));
            }
        };

        chunks.push((chunk_rows, chunk_payload));
        offset += chunk_rows;
    }

    Ok(chunks)
}

async fn send_event(socket: &mut WebSocket, event: WsServerEvent) -> anyhow::Result<()> {
    let payload = serde_json::to_string(&event)?;
    socket.send(Message::Text(payload.into())).await?;
    Ok(())
}

fn ensure_ws_sql_enabled() -> Result<(), StatusCode> {
    if beacon_config::CONFIG.ws_sql_enable {
        Ok(())
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}

fn authenticate_ws_upgrade(headers: &HeaderMap, ticket: Option<&str>) -> Result<(), StatusCode> {
    if verify_basic_auth_header(headers).is_ok() {
        return Ok(());
    }

    match ticket {
        Some(ticket) => consume_ticket(ticket),
        None => Err(StatusCode::UNAUTHORIZED),
    }
}

fn consume_ticket(ticket: &str) -> Result<(), StatusCode> {
    let mut store = ticket_store()
        .lock()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    store.retain(|_, expiry| *expiry > Instant::now());

    match store.remove(ticket) {
        Some(expiry) if expiry > Instant::now() => Ok(()),
        _ => Err(StatusCode::UNAUTHORIZED),
    }
}

#[cfg(test)]
mod tests {
    use super::{encode_batch_to_arrow_stream, encode_batch_to_arrow_stream_chunks};
    use arrow::{
        array::{Int32Array, StringArray},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use std::sync::Arc;

    fn build_batch(row_count: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("idx", DataType::Int32, false),
            Field::new("payload", DataType::Utf8, false),
        ]));

        let idx_values: Vec<i32> = (0..row_count as i32).collect();
        let payload_values: Vec<String> = (0..row_count)
            .map(|i| format!("row-{i:04}-{}", "x".repeat(128)))
            .collect();

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(idx_values)),
                Arc::new(StringArray::from(payload_values)),
            ],
        )
        .expect("failed to build test batch")
    }

    #[test]
    fn keeps_single_chunk_when_payload_fits_limit() {
        let batch = build_batch(64);
        let full_payload = encode_batch_to_arrow_stream(&batch).expect("encode should succeed");

        let chunks = encode_batch_to_arrow_stream_chunks(&batch, full_payload.len())
            .expect("chunking should succeed");

        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].0, batch.num_rows());
        assert_eq!(chunks[0].1.len(), full_payload.len());
    }

    #[test]
    fn splits_payload_when_batch_exceeds_limit() {
        let batch = build_batch(512);
        let full_payload = encode_batch_to_arrow_stream(&batch).expect("encode should succeed");
        let max_payload_bytes = full_payload.len().saturating_sub(1);

        let chunks = encode_batch_to_arrow_stream_chunks(&batch, max_payload_bytes)
            .expect("chunking should succeed");

        assert!(chunks.len() > 1);
        assert!(chunks
            .iter()
            .all(|(_, payload)| payload.len() <= max_payload_bytes));
        assert_eq!(
            chunks.iter().map(|(rows, _)| *rows).sum::<usize>(),
            batch.num_rows()
        );
    }

    #[test]
    fn errors_when_single_row_exceeds_limit() {
        let batch = build_batch(1);
        let full_payload = encode_batch_to_arrow_stream(&batch).expect("encode should succeed");

        let err = encode_batch_to_arrow_stream_chunks(&batch, full_payload.len().saturating_sub(1))
            .expect_err("single row should not fit reduced limit");

        assert!(
            err.to_string().contains("single-row payload"),
            "unexpected error: {err}"
        );
    }
}
