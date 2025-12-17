use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};

#[derive()]
pub enum MultiPlexerError {

}

#[derive(Debug, Copy, Clone)]
#[repr(u8)]
pub enum MultiPlexerCommand {
    Read,
    Close,
    Exit,
}

pub struct Worker {
    stdin: BufWriter<tokio::process::ChildStdin>,
    stdout: BufReader<tokio::process::ChildStdout>
}

impl Worker {

}

async fn send_read(worker: &mut Worker) -> 