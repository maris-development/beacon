use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use object_store::ObjectStore;

use crate::{
    array::{io_cache::IoCache, reader::ArrayReader},
    consts::{ARRAY_FILE_NAME, LAYOUT_FILE_NAME, STATISTICS_FILE_NAME},
};

pub struct ColumnReader<S: ObjectStore + Clone> {
    // Array Reader
    reader: Arc<ArrayReader<S>>,
}

impl<S: ObjectStore + Clone> ColumnReader<S> {
    pub async fn new(
        object_store: S,
        column_directory: object_store::path::Path,
        io_cache: Arc<IoCache>,
    ) -> anyhow::Result<Self> {
        let array_path = column_directory.child(ARRAY_FILE_NAME);
        let layout_path = column_directory.child(LAYOUT_FILE_NAME);
        let statistics_path = column_directory.child(STATISTICS_FILE_NAME);

        let reader = Arc::new(
            ArrayReader::new_with_cache_and_statistics(
                object_store.clone(),
                layout_path,
                array_path,
                statistics_path,
                io_cache,
            )
            .await?,
        );

        Ok(Self { reader })
    }

    pub fn read_column_array(
        &self,
        dataset_index: u32,
    ) -> Option<anyhow::Result<Arc<dyn beacon_nd_arrow::array::NdArrowArray>>> {
        self.reader.read_dataset_array(dataset_index)
    }

    pub fn read_column_statistics(&self) -> anyhow::Result<Option<RecordBatch>> {
        self.reader.read_statistics_batch()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::UInt32Array;
    use beacon_nd_arrow::array::{NdArrowArray, NdArrowArrayDispatch};
    use object_store::{ObjectStore, memory::InMemory, path::Path};

    use crate::{
        array::{io_cache::IoCache, writer::ArrayWriter},
        consts,
    };

    use super::ColumnReader;

    #[tokio::test]
    async fn read_column_statistics_includes_dataset_index() -> anyhow::Result<()> {
        let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let column_directory = Path::from("column-a");

        let array_path = column_directory.child(consts::ARRAY_FILE_NAME);
        let layout_path = column_directory.child(consts::LAYOUT_FILE_NAME);
        let statistics_path = column_directory.child(consts::STATISTICS_FILE_NAME);

        let first = Arc::new(NdArrowArrayDispatch::new_in_mem(
            vec![1_i32, 3, 9],
            vec![3],
            vec!["x".to_string()],
            None,
        )?) as Arc<dyn NdArrowArray>;
        let second = Arc::new(NdArrowArrayDispatch::new_in_mem(
            vec![2_i32, 4],
            vec![2],
            vec!["x".to_string()],
            None,
        )?) as Arc<dyn NdArrowArray>;

        let mut writer = ArrayWriter::new(
            store.clone(),
            array_path,
            layout_path,
            statistics_path,
            first.data_type(),
            8,
        )?;
        writer.append_array(42, first).await?;
        writer.append_array(99, second).await?;
        writer.finalize().await?;

        let column_reader =
            ColumnReader::new(store, column_directory, Arc::new(IoCache::new(1024 * 1024))).await?;

        let statistics = column_reader
            .read_column_statistics()?
            .expect("statistics should be present");

        let dataset_index = statistics
            .column(statistics.schema().index_of("dataset_index")?)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .expect("dataset_index should be u32");

        assert_eq!(dataset_index.values(), &[42, 99]);

        Ok(())
    }
}
