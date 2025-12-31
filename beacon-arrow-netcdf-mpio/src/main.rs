use std::collections::HashMap;
use std::io::{self, BufReader, BufWriter, Read, Write};

use beacon_arrow_netcdf::mpio_utils::ReadCommand;
use beacon_arrow_netcdf::reader::NetCDFArrowReader;

fn read_exact<R: Read>(r: &mut R, mut buf: &mut [u8]) -> io::Result<()> {
    while !buf.is_empty() {
        let n = r.read(buf)?;
        if n == 0 {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        buf = &mut buf[n..];
    }
    Ok(())
}

fn read_framed_json<R: Read>(stdin: &mut R) -> io::Result<Option<Vec<u8>>> {
    let mut len_buf = [0u8; 4];
    match read_exact(stdin, &mut len_buf) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }

    let json_len = u32::from_le_bytes(len_buf) as usize;
    if json_len == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "zero-length frame",
        ));
    }

    let mut json_buf = vec![0u8; json_len];
    read_exact(stdin, &mut json_buf)?;
    Ok(Some(json_buf))
}

fn main() -> io::Result<()> {
    let mut stdin = BufReader::new(io::stdin());
    let mut stdout = BufWriter::with_capacity(4 * 1024 * 1024, io::stdout());
    let mut reader_cache: HashMap<String, NetCDFArrowReader> = std::collections::HashMap::new();
    let mut log_file = std::fs::File::create("mpio.log")?;

    loop {
        let Some(json_buf) = read_framed_json(&mut stdin)? else {
            break;
        };

        let cmd: ReadCommand = match serde_json::from_slice(&json_buf) {
            Ok(c) => c,
            Err(e) => {
                eprintln!("mpio worker: invalid JSON command: {e}");
                continue;
            }
        };

        match &cmd {
            ReadCommand::ReadArrowSchema { request_id, path } => {
                writeln!(
                    log_file,
                    "mpio worker: received ReadArrowSchema request {} for path {}",
                    request_id, path
                )?;
                let schema = if let Some(reader) = reader_cache.get(path) {
                    // already cached
                    reader.schema().clone()
                } else {
                    let reader = match NetCDFArrowReader::new(path.as_str()) {
                        Ok(r) => r,
                        Err(e) => {
                            respond_error(
                                &mut stdout,
                                *request_id,
                                format!("Failed to open file: {}", e),
                            )?;
                            continue;
                        }
                    };

                    let schema = reader.schema();
                    reader_cache.insert(path.clone(), reader);
                    schema.clone()
                };

                let response = beacon_arrow_netcdf::mpio_utils::CommandResponse::ArrowSchema {
                    request_id: *request_id,
                    schema: schema.as_ref().clone(),
                };
                let response_json = match serde_json::to_vec(&response) {
                    Ok(v) => v,
                    Err(e) => {
                        respond_error(&mut stdout, *request_id, format!("Serialize error: {e}"))?;
                        continue;
                    }
                };
                let response_len = (response_json.len() as u32).to_le_bytes();
                stdout.write_all(&response_len)?;
                stdout.write_all(&response_json)?;
                stdout.flush()?;
                writeln!(
                    log_file,
                    "mpio worker: sent schema response for request {}",
                    request_id
                )?;
            }
            ReadCommand::ReadFile {
                request_id,
                path,
                projection,
                chunk_size: _chunk_size,
                stream_size: _stream_size,
            } => {
                write!(
                    log_file,
                    "mpio worker: received ReadFile request {} for path {}",
                    request_id, path
                )?;
                let reader = if let Some(reader) = reader_cache.get_mut(path) {
                    // already cached
                    reader
                } else {
                    let reader = match NetCDFArrowReader::new(path.as_str()) {
                        Ok(r) => r,
                        Err(e) => {
                            respond_error(
                                &mut stdout,
                                *request_id,
                                format!("Failed to open file: {}", e),
                            )?;
                            continue;
                        }
                    };

                    reader_cache.insert(path.clone(), reader);
                    reader_cache.get_mut(path).unwrap()
                };

                let batch = match reader.read_as_batch(projection.clone()) {
                    Ok(b) => b,
                    Err(e) => {
                        respond_error(
                            &mut stdout,
                            *request_id,
                            format!("Failed to read data: {}", e),
                        )?;
                        continue;
                    }
                };

                // Serialize the batch and send it back
                let buffer = Vec::new();
                let mut arrow_stream_writer =
                    match arrow::ipc::writer::StreamWriter::try_new(buffer, &batch.schema()) {
                        Ok(w) => w,
                        Err(e) => {
                            respond_error(
                                &mut stdout,
                                *request_id,
                                format!("IPC writer init error: {e}"),
                            )?;
                            continue;
                        }
                    };

                if let Err(e) = arrow_stream_writer.write(&batch) {
                    respond_error(&mut stdout, *request_id, format!("IPC write error: {e}"))?;
                    continue;
                }
                if let Err(e) = arrow_stream_writer.finish() {
                    respond_error(&mut stdout, *request_id, format!("IPC finish error: {e}"))?;
                    continue;
                }
                let buffer = match arrow_stream_writer.into_inner() {
                    Ok(b) => b,
                    Err(e) => {
                        respond_error(
                            &mut stdout,
                            *request_id,
                            format!("IPC finalize error: {e}"),
                        )?;
                        continue;
                    }
                };

                let response = beacon_arrow_netcdf::mpio_utils::CommandResponse::BatchesStream {
                    request_id: *request_id,
                    length: buffer.len(),
                    has_more: false,
                };
                let response_json = match serde_json::to_vec(&response) {
                    Ok(v) => v,
                    Err(e) => {
                        respond_error(&mut stdout, *request_id, format!("Serialize error: {e}"))?;
                        continue;
                    }
                };
                let header_response_len = (response_json.len() as u32).to_le_bytes();
                stdout.write_all(&header_response_len)?;
                stdout.write_all(&response_json)?;
                stdout.write_all(&buffer)?;
                stdout.flush()?;
                writeln!(
                    log_file,
                    "mpio worker: sent {} bytes for request {}",
                    buffer.len(),
                    request_id
                )?;
            }
            ReadCommand::Exit => {
                writeln!(log_file, "mpio worker: exiting as requested")?;
                break;
            }
        }
    }
    writeln!(log_file, "mpio worker: shutting down")?;
    Ok(())
}

fn respond_error(
    stdout: &mut BufWriter<io::Stdout>,
    request_id: u32,
    message: String,
) -> io::Result<()> {
    let response = beacon_arrow_netcdf::mpio_utils::CommandResponse::Error {
        request_id,
        message,
    };
    let response_json = serde_json::to_vec(&response).unwrap();
    let response_len = (response_json.len() as u32).to_le_bytes();
    stdout.write_all(&response_len)?;
    stdout.write_all(&response_json)?;
    stdout.flush()?;
    Ok(())
}
