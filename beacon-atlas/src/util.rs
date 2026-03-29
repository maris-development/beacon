use std::fs::File;
use std::io::{Read, Seek, SeekFrom};

use bytes::Bytes;
use object_store::ObjectStore;

/// Streams a file into an object store path using multipart uploads.
///
/// When the file is empty, it writes an empty object instead of completing
/// an empty multipart upload.
pub async fn stream_file_to_store<S: ObjectStore>(
    store: &S,
    path: &object_store::path::Path,
    file: &mut File,
    chunk_size: usize,
) -> anyhow::Result<()> {
    file.seek(SeekFrom::Start(0))?;
    let mut buf = vec![0u8; chunk_size];
    let mut total_read = 0usize;
    let mut uploader = store.put_multipart(path).await?;

    loop {
        let read = file.read(&mut buf)?;
        if read == 0 {
            break;
        }
        total_read += read;
        uploader
            .put_part(Bytes::copy_from_slice(&buf[..read]).into())
            .await?;
    }

    if total_read == 0 {
        store.put(path, Bytes::new().into()).await?;
    } else {
        uploader.complete().await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::stream_file_to_store;
    use bytes::Bytes;
    use object_store::{ObjectStore, memory::InMemory, path::Path};
    use std::io::Write;

    #[tokio::test]
    async fn streams_non_empty_file() -> anyhow::Result<()> {
        let store = InMemory::new();
        let path = Path::from("util/non_empty.bin");
        let mut file = tempfile::tempfile()?;
        file.write_all(b"hello world")?;

        stream_file_to_store(&store, &path, &mut file, 4).await?;

        let bytes = store.get(&path).await?.bytes().await?;
        assert_eq!(bytes, Bytes::from_static(b"hello world"));
        Ok(())
    }

    #[tokio::test]
    async fn streams_empty_file() -> anyhow::Result<()> {
        let store = InMemory::new();
        let path = Path::from("util/empty.bin");
        let mut file = tempfile::tempfile()?;

        stream_file_to_store(&store, &path, &mut file, 8).await?;

        let bytes = store.get(&path).await?.bytes().await?;
        assert!(bytes.is_empty());
        Ok(())
    }
}
