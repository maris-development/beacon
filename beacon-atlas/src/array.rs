use std::io::{Read, Seek, SeekFrom};
use std::sync::Arc;

use anyhow::{Result, anyhow};
use arrow::{ipc::writer::FileWriter, record_batch::RecordBatch};
use beacon_nd_arrow::{NdArrowArray, extension::nd_column_field};
use object_store::ObjectStore;
use tempfile::tempfile;

/// Flush to IPC when estimated batch bytes exceed 4 MiB.
const BATCH_SIZE: usize = 4 * 1024 * 1024;

/// Writes ND Arrow arrays to a temporary IPC file and uploads to object storage.
///
/// This writer buffers ND arrays and writes RecordBatches as the estimated
/// batch size grows. Each row is stored as a single ND array value.
pub struct ArrayWriter<S: ObjectStore + Clone> {
    /// Object store used to persist the IPC file.
    pub store: S,
    /// Target object store path.
    pub path: object_store::path::Path,
    /// Storage data type for ND arrays.
    pub data_type: arrow::datatypes::DataType,
    /// Buffered rows (None represents a null row).
    pub arrays: Vec<Option<NdArrowArray>>,
    /// Estimated current batch size in bytes.
    pub current_batch_size: usize,
    /// IPC writer backed by a temporary file.
    pub temp_arrow_file: FileWriter<std::fs::File>,
    /// Schema for the ND column.
    pub schema: Arc<arrow::datatypes::Schema>,
}

impl<S: ObjectStore + Clone> ArrayWriter<S> {
    /// Create a new writer that stores ND arrays with the given storage `data_type`.
    pub fn new(
        store: S,
        path: object_store::path::Path,
        data_type: arrow::datatypes::DataType,
    ) -> Result<Self> {
        let temp_file = tempfile().map_err(|err| anyhow!(err))?;
        let field =
            nd_column_field("nd_arrays", data_type.clone(), true).map_err(|err| anyhow!(err))?;
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![field]));
        let writer = FileWriter::try_new(temp_file, &schema).map_err(|err| anyhow!(err))?;

        Ok(Self {
            store,
            path,
            data_type,
            arrays: Vec::new(),
            current_batch_size: 0,
            temp_arrow_file: writer,
            schema,
        })
    }

    /// Append a null ND array row.
    pub fn append_null(&mut self) -> Result<()> {
        self.arrays.push(None);
        if self.current_batch_size >= BATCH_SIZE {
            self.flush()?;
        }
        Ok(())
    }

    /// Append a single ND array row.
    pub fn append_array(&mut self, array: NdArrowArray) -> Result<()> {
        let array_size = array.storage_array().get_array_memory_size();
        self.current_batch_size = self.current_batch_size.saturating_add(array_size);
        self.arrays.push(Some(array));

        if self.current_batch_size >= BATCH_SIZE {
            self.flush()?;
        }

        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        if self.arrays.is_empty() {
            return Ok(());
        }

        let rows = self
            .arrays
            .drain(..)
            .map(|opt| match opt {
                Some(arr) => Ok(arr),
                None => NdArrowArray::new_null_scalar(Some(self.data_type.clone()))
                    .map_err(|err| anyhow!(err)),
            })
            .collect::<Result<Vec<_>>>()?;

        let column = beacon_nd_arrow::column::NdArrowArrayColumn::from_rows(rows)
            .map_err(|err| anyhow!(err))?;
        let batch = RecordBatch::try_new(self.schema.clone(), vec![column.into_array_ref()])
            .map_err(|err| anyhow!(err))?;

        self.temp_arrow_file
            .write(&batch)
            .map_err(|err| anyhow!(err))?;
        self.current_batch_size = 0;
        Ok(())
    }

    /// Finalize and upload the IPC file to object storage.
    pub async fn finalize(mut self) -> Result<()> {
        self.flush()?;
        self.temp_arrow_file.finish().map_err(|err| anyhow!(err))?;
        let mut inner = self
            .temp_arrow_file
            .into_inner()
            .map_err(|err| anyhow!(err))?;
        inner.seek(SeekFrom::Start(0)).map_err(|err| anyhow!(err))?;
        let mut data = Vec::new();
        inner.read_to_end(&mut data).map_err(|err| anyhow!(err))?;
        self.store
            .put(&self.path, data.into())
            .await
            .map_err(|err| anyhow!(err))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{array::Int32Array, datatypes::DataType};
    use beacon_nd_arrow::dimensions::{Dimension, Dimensions};
    use object_store::{ObjectStore, memory::InMemory, path::Path};

    use super::*;

    #[tokio::test]
    async fn array_writer_writes_ipc() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("nd/arrays.arrow");
        let mut writer = ArrayWriter::new(store.clone(), path.clone(), DataType::Int32).unwrap();

        let dims = Dimensions::new(vec![Dimension::try_new("x", 3).unwrap()]);
        let arr = NdArrowArray::new(Arc::new(Int32Array::from(vec![1, 2, 3])), dims).unwrap();
        writer.append_array(arr).unwrap();
        writer.append_null().unwrap();

        writer.finalize().await.unwrap();
        let bytes = store.get(&path).await.unwrap().bytes().await.unwrap();
        let reader =
            arrow::ipc::reader::FileReader::try_new(std::io::Cursor::new(bytes.to_vec()), None)
                .unwrap();
        let batch = reader.into_iter().next().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 2);
    }
}
