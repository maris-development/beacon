use std::collections::HashMap;
use std::io::{self, BufReader, BufWriter, Read, Write};

use beacon_arrow_netcdf::mpio_utils::Command;
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

fn main() -> io::Result<()> {
    let mut stdin = BufReader::new(io::stdin());
    let mut stdout = BufWriter::with_capacity(4 * 1024 * 1024, io::stdout());
    let mut reader_cache: HashMap<String, NetCDFArrowReader> = std::collections::HashMap::new();

    loop {
        // --- read JSON length ---
        let mut len_buf = [0u8; 4];
        if stdin.read(&mut len_buf)? == 0 {
            break;
        }
        read_exact(&mut stdin, &mut len_buf)?;
        let json_len = u32::from_le_bytes(len_buf) as usize;

        // --- read JSON ---
        let mut json_buf = vec![0u8; json_len];
        read_exact(&mut stdin, &mut json_buf)?;

        let cmd: Command = serde_json::from_slice(&json_buf).unwrap();

        match cmd {
            Command::ReadArrowSchema { request_id, path } => {
                let schema = if let Some(reader) = reader_cache.get(&path) {
                    // already cached
                    reader.schema().clone()
                } else {
                    let reader = match NetCDFArrowReader::new(path.as_str()) {
                        Ok(r) => r,
                        Err(e) => {
                            respond_error(
                                &mut stdout,
                                request_id,
                                format!("Failed to open file: {}", e),
                            )?;
                            continue;
                        }
                    };

                    let schema = reader.schema();
                    reader_cache.insert(path.clone(), reader);
                    schema.clone()
                };

                let response = beacon_arrow_netcdf::mpio_utils::CommandReponse::ArrowSchema {
                    request_id,
                    schema: schema.as_ref().clone(),
                };
                let response_json = serde_json::to_vec(&response)
                    .expect("Incorrect command message. This should never happen.");
                let response_len = (response_json.len() as u32).to_le_bytes();
                stdout.write_all(&response_len)?;
                stdout.write_all(&response_json)?;
                stdout.flush()?;
            }
            Command::ReadFile {
                request_id,
                path,
                projection,
                chunk_size,
                stream_size,
            } => {
                let reader = if let Some(reader) = reader_cache.get_mut(&path) {
                    // already cached
                    reader
                } else {
                    let reader = match NetCDFArrowReader::new(path.as_str()) {
                        Ok(r) => r,
                        Err(e) => {
                            respond_error(
                                &mut stdout,
                                request_id,
                                format!("Failed to open file: {}", e),
                            )?;
                            continue;
                        }
                    };

                    reader_cache.insert(path.clone(), reader);
                    reader_cache.get_mut(&path).unwrap()
                };

                let batch = match reader.read_as_batch(projection) {
                    Ok(b) => b,
                    Err(e) => {
                        respond_error(
                            &mut stdout,
                            request_id,
                            format!("Failed to read data: {}", e),
                        )?;
                        continue;
                    }
                };

                // Serialize the batch and send it back
                let buffer = Vec::new();
                let mut arrow_stream_writer =
                    arrow::ipc::writer::StreamWriter::try_new(buffer, &batch.schema()).unwrap();

                arrow_stream_writer.write(&batch).unwrap();
                arrow_stream_writer.finish().unwrap();
                let buffer = arrow_stream_writer.into_inner().unwrap();

                let response = beacon_arrow_netcdf::mpio_utils::CommandReponse::BatchesStream {
                    request_id,
                    length: buffer.len(),
                    has_more: false,
                };
                let response_json = serde_json::to_vec(&response)
                    .expect("Incorrect command message. This should never happen.");
                let response_len = (response_json.len() as u32).to_le_bytes();
                stdout.write_all(&response_len)?;
                stdout.write_all(&response_json)?;
                stdout.write_all(&buffer)?;
                stdout.flush()?;
            }
            Command::Exit => {
                break;
            }
        }
    }

    Ok(())
}

fn respond_error(
    stdout: &mut BufWriter<io::Stdout>,
    request_id: u32,
    message: String,
) -> io::Result<()> {
    let response = beacon_arrow_netcdf::mpio_utils::CommandReponse::Error {
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
