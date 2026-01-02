use std::io::SeekFrom;

use arrow::array::{BooleanArray, RecordBatch};
use arrow::ipc::writer::FileWriter;
use arrow_schema::{DataType, Field, Schema};
use hmac_sha256::Hash;
use tokio::io::{AsyncReadExt, AsyncSeekExt};

use crate::error::{BBFError, BBFReadingError, BBFResult, BBFWritingError};

const FIELD_DELETED: &str = "__deleted";

pub fn deleted_mask_record_batch(deleted: &[bool]) -> BBFResult<RecordBatch> {
    let schema = Schema::new(vec![Field::new(FIELD_DELETED, DataType::Boolean, false)]);
    let array = BooleanArray::from(deleted.to_vec());
    RecordBatch::try_new(std::sync::Arc::new(schema), vec![std::sync::Arc::new(array)]).map_err(
        |e| BBFError::Writing(BBFWritingError::ArrayPartitionFinalizeFailure(Box::new(e))),
    )
}

pub fn decode_deleted_mask(batch: &RecordBatch) -> BBFResult<Vec<bool>> {
    if batch.num_columns() != 1 {
        return Err(BBFReadingError::EntryMaskDecode {
            reason: format!("expected 1 column, got {}", batch.num_columns()),
        }
        .into());
    }

    let col = batch
        .column(0)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| BBFReadingError::EntryMaskDecode {
            reason: "entry mask column is not Boolean".to_string(),
        })?;

    Ok((0..col.len()).map(|i| col.value(i)).collect())
}

/// Encode a deleted-entry mask into an Arrow IPC file stored as a temp file.
///
/// Returns `(file, file_size, content_hash)` where `content_hash` is computed
/// over the encoded file bytes.
pub async fn encode_deleted_mask_to_tempfile(
    deleted: &[bool],
) -> Result<(tokio::fs::File, u64, String), BBFError> {
    let batch = deleted_mask_record_batch(deleted)?;

    let mut tmp = tempfile::tempfile()
        .map_err(|e| BBFWritingError::TempFileCreationFailure(e, "entry_mask".to_string()))?;

    // Arrow IPC encode.
    {
        let mut writer = FileWriter::try_new(&mut tmp, &batch.schema())
            .map_err(|e| BBFWritingError::ArrayPartitionFinalizeFailure(Box::new(e)))?;
        writer
            .write(&batch)
            .map_err(|e| BBFWritingError::ArrayPartitionFinalizeFailure(Box::new(e)))?;
        writer
            .finish()
            .map_err(|e| BBFWritingError::ArrayPartitionFinalizeFailure(Box::new(e)))?;
    }

    let mut tokio_file = tokio::fs::File::from_std(tmp);
    tokio_file
        .seek(SeekFrom::Start(0))
        .await
        .map_err(|e| BBFWritingError::ArrayPartitionFinalizeFailure(Box::new(e)))?;

    let mut hasher = Hash::new();
    let mut buffer = vec![0u8; 1024 * 1024];
    let mut size: u64 = 0;
    loop {
        let n = tokio_file
            .read(&mut buffer)
            .await
            .map_err(|e| BBFWritingError::ArrayPartitionFinalizeFailure(Box::new(e)))?;
        if n == 0 {
            break;
        }
        hasher.update(&buffer[..n]);
        size = size.saturating_add(n as u64);
    }

    tokio_file
        .seek(SeekFrom::Start(0))
        .await
        .map_err(|e| BBFWritingError::ArrayPartitionFinalizeFailure(Box::new(e)))?;

    let hash_bytes = hasher.finalize();
    let hash: String = hash_bytes.iter().map(|b| format!("{:02x}", b)).collect();

    Ok((tokio_file, size, hash))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_ipc::reader::FileReader;
    use std::io::Cursor;

    #[tokio::test]
    async fn encode_and_decode_round_trips() {
        let deleted = vec![false, true, false, true];
        let (mut file, size, hash) = encode_deleted_mask_to_tempfile(&deleted)
            .await
            .expect("encode");

        assert!(size > 0);
        assert_eq!(hash.len(), 64);

        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes).await.expect("read");
        assert_eq!(bytes.len() as u64, size);

        let reader = FileReader::try_new(Cursor::new(bytes), None).expect("ipc reader");
        let batches = reader.collect::<Result<Vec<_>, _>>().expect("read batches");
        assert!(!batches.is_empty());
        let batch = if batches.len() == 1 {
            batches.into_iter().next().expect("batch")
        } else {
            let schema = batches[0].schema();
            arrow::compute::concat_batches(&schema, &batches).expect("concat")
        };

        let decoded = decode_deleted_mask(&batch).expect("decode");
        assert_eq!(decoded, deleted);
    }
}
