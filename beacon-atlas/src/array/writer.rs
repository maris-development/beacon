use std::{
    fs::File,
    io::{Seek, SeekFrom},
    sync::Arc,
};

use arrow::{array::ArrayRef, ipc::writer::FileWriter, record_batch::RecordBatch};
use beacon_nd_arrow::array::NdArrowArray;
use object_store::ObjectStore;

use crate::{
    IPC_WRITE_OPTS,
    array::{
        layout::{ArrayLayout, ArrayLayouts},
        statistics::StatisticsWriter,
    },
    consts, util,
};

/// Writes chunked ND Arrow arrays to object storage using buffered IPC batches.
///
/// The writer stores array values in `array.arrow` and emits the corresponding
/// dataset layouts in `layout.arrow` within the provided object store path.
pub struct ArrayWriter<S: ObjectStore + Clone> {
    store: S,
    array_datatype: arrow::datatypes::DataType,
    array_path: object_store::path::Path,
    layout_path: object_store::path::Path,
    statistics_path: object_store::path::Path,
    temp_writer: FileWriter<File>,
    statistics_writer: StatisticsWriter,
    row_count: usize,  // Total number of rows flushed to the IPC file
    chunk_size: usize, // Target number of rows per buffered batch

    current_buffer_size: usize,   // Current number of rows buffered
    buffer_arrays: Vec<ArrayRef>, // Buffered arrays to write in the next batch

    // Layouts of the arrays written so far.
    layouts: Vec<ArrayLayout>,
}

impl<S: ObjectStore + Clone> ArrayWriter<S> {
    pub const FIELD_NAME: &'static str = "array";
    /// Create a new chunked array writer.
    ///
    /// `chunk_size` controls when buffered arrays are flushed to the temp IPC file.
    pub fn new(
        store: S,
        array_path: object_store::path::Path,
        layout_path: object_store::path::Path,
        statistics_path: object_store::path::Path,
        array_datatype: arrow::datatypes::DataType,
        chunk_size: usize,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(chunk_size > 0, "chunk_size must be greater than zero");

        let field = arrow::datatypes::Field::new(Self::FIELD_NAME, array_datatype.clone(), true);
        let schema = arrow::datatypes::Schema::new(vec![field]);
        let temp_writer = FileWriter::try_new_with_options(
            tempfile::tempfile().unwrap(),
            &schema,
            IPC_WRITE_OPTS.clone(),
        )
        .map_err(|e| anyhow::anyhow!("failed to create IPC writer: {e}"))?;

        Ok(Self {
            array_datatype: array_datatype.clone(),
            statistics_writer: StatisticsWriter::new(array_datatype.clone()),
            store,
            array_path,
            layout_path,
            statistics_path,
            temp_writer,
            chunk_size,
            row_count: 0,
            current_buffer_size: 0,
            buffer_arrays: Vec::new(),
            layouts: Vec::new(),
        })
    }

    pub fn data_type(&self) -> &arrow::datatypes::DataType {
        &self.array_datatype
    }

    /// Flush remaining batches and upload the IPC file to object storage.
    ///
    /// Writes `array.arrow` (values) and `layout.arrow` (layout metadata)
    /// under the configured path.
    pub async fn finalize(mut self) -> anyhow::Result<()> {
        // Flush any remaining data, including a final partial batch.
        self.flush_all().await?;

        self.temp_writer.finish()?;

        // Upload the temp file to object store
        let mut temp_file = self.temp_writer.into_inner()?;
        temp_file.seek(SeekFrom::Start(0))?;

        util::stream_file_to_store::<S>(
            &self.store,
            &self.array_path,
            &mut temp_file,
            consts::STREAM_CHUNK_SIZE,
        )
        .await?;

        // Create a layout file
        let layout = ArrayLayouts::new(self.layouts);
        layout
            .save::<S>(self.store.clone(), self.layout_path)
            .await?;

        let store = self.store.clone();
        let statistics_path = self.statistics_path.clone();
        let statistics_batch = self.statistics_writer.finish()?;
        Self::write_statistics_to_store(&store, &statistics_path, statistics_batch).await?;

        Ok(())
    }

    async fn write_statistics_to_store(
        store: &S,
        statistics_path: &object_store::path::Path,
        statistics_batch: RecordBatch,
    ) -> anyhow::Result<()> {
        let schema = statistics_batch.schema();
        let mut writer = FileWriter::try_new_with_options(
            tempfile::tempfile()?,
            &schema,
            IPC_WRITE_OPTS.clone(),
        )?;
        writer.write(&statistics_batch)?;
        writer.finish()?;

        let mut file = writer.into_inner()?;
        file.seek(SeekFrom::Start(0))?;
        util::stream_file_to_store::<S>(
            store,
            statistics_path,
            &mut file,
            consts::STREAM_CHUNK_SIZE,
        )
        .await
    }

    /// Append a stream of chunked arrays for a dataset index.
    ///
    /// The layout entry is recorded immediately using the current write offset,
    /// which includes buffered (not-yet-flushed) rows.
    pub async fn append_array(
        &mut self,
        dataset_index: u32,
        array: Arc<dyn NdArrowArray>,
    ) -> anyhow::Result<()> {
        // Check if datatype aligns with writer
        if array.data_type() != self.array_datatype {
            return Err(anyhow::anyhow!(
                "Array datatype does not match writer datatype"
            ));
        }

        let array_start =
            self.row_count
                .checked_add(self.current_buffer_size)
                .ok_or_else(|| anyhow::anyhow!("array offset overflow"))? as u64;

        let array_shape = array.shape();
        let array_dimensions = array.dimensions();
        let array = array.as_arrow_array_ref().await?;

        self.statistics_writer.append(&array)?;

        self.layouts.push(ArrayLayout {
            dataset_index,
            dimensions: array_dimensions,
            array_len: array.len() as u64,
            array_shape: array_shape.iter().map(|d| *d as u32).collect(),
            chunk_shape: array_shape.iter().map(|d| *d as u32).collect(),
            array_start,
        });

        self.current_buffer_size += array.len();
        self.buffer_arrays.push(array);

        if self.current_buffer_size >= self.chunk_size {
            self.flush().await?;
        }

        Ok(())
    }

    /// Flush any remaining buffered data to the temp file.
    pub async fn flush(&mut self) -> anyhow::Result<()> {
        self.flush_buffer(false).await
    }

    async fn flush_all(&mut self) -> anyhow::Result<()> {
        self.flush_buffer(true).await
    }

    async fn flush_buffer(&mut self, include_partial_batch: bool) -> anyhow::Result<()> {
        while !self.buffer_arrays.is_empty()
            && (self.current_buffer_size >= self.chunk_size
                || (include_partial_batch && self.current_buffer_size > 0))
        {
            let rows_to_write = if self.current_buffer_size >= self.chunk_size {
                self.chunk_size
            } else {
                self.current_buffer_size
            };

            let column_arrays = Self::take_rows(&mut self.buffer_arrays, rows_to_write);

            // concatenate the buffered arrays into a single column array
            let column_array = arrow::compute::concat(
                &column_arrays.iter().map(|a| a.as_ref()).collect::<Vec<_>>(),
            )?;

            // Create the schema for the batch
            let field =
                arrow::datatypes::Field::new(Self::FIELD_NAME, self.array_datatype.clone(), true);
            let schema = arrow::datatypes::Schema::new(vec![field]);
            let batch = RecordBatch::try_new(Arc::new(schema), vec![column_array])?;

            // Write the batch to the temp file
            self.temp_writer.write(&batch)?;
            self.row_count = self
                .row_count
                .checked_add(batch.num_rows())
                .ok_or_else(|| anyhow::anyhow!("row count overflow"))?;
            self.current_buffer_size = self
                .current_buffer_size
                .checked_sub(batch.num_rows())
                .ok_or_else(|| anyhow::anyhow!("buffer size underflow"))?;
        }

        Ok(())
    }

    fn take_rows(buffer_arrays: &mut Vec<ArrayRef>, rows_to_take: usize) -> Vec<ArrayRef> {
        let mut remaining_rows = rows_to_take;
        let mut taken = Vec::new();
        let mut remainder = Vec::new();

        for array in std::mem::take(buffer_arrays) {
            if remaining_rows == 0 {
                remainder.push(array);
                continue;
            }

            let array_len = array.len();
            if array_len <= remaining_rows {
                taken.push(array);
                remaining_rows -= array_len;
            } else {
                taken.push(array.slice(0, remaining_rows));
                remainder.push(array.slice(remaining_rows, array_len - remaining_rows));
                remaining_rows = 0;
            }
        }

        *buffer_arrays = remainder;
        debug_assert_eq!(
            remaining_rows, 0,
            "attempted to take more rows than buffered"
        );

        taken
    }
}

#[cfg(test)]
mod tests {
    use super::ArrayWriter;
    use crate::array::{io_cache::IoCache, reader::ArrayReader};
    use std::{io::Cursor, sync::Arc};

    use arrow::{
        array::{Array as ArrowArray, Int32Array},
        datatypes::DataType,
        ipc::reader::FileReader,
    };
    use beacon_nd_arrow::array::{NdArrowArray, NdArrowArrayDispatch, subset::ArraySubset};
    use object_store::{ObjectStore, memory::InMemory, path::Path};

    fn make_i32_array(values: Vec<i32>) -> Arc<dyn NdArrowArray> {
        Arc::new(
            NdArrowArrayDispatch::new_in_mem(
                values.clone(),
                vec![values.len()],
                vec!["x".to_string()],
                None,
            )
            .expect("valid test array"),
        )
    }

    #[tokio::test]
    async fn flush_writes_full_chunks_and_keeps_remainder_buffered() -> anyhow::Result<()> {
        let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let mut writer = ArrayWriter::new(
            store,
            Path::from("array.arrow"),
            Path::from("layout.arrow"),
            Path::from("statistics.arrow"),
            DataType::Int32,
            5,
        )?;

        writer
            .append_array(0, make_i32_array(vec![1, 2, 3, 4]))
            .await?;
        writer
            .append_array(1, make_i32_array(vec![5, 6, 7]))
            .await?;

        assert_eq!(writer.row_count, 5);
        assert_eq!(writer.current_buffer_size, 2);
        assert_eq!(writer.buffer_arrays.len(), 1);

        let remainder = writer.buffer_arrays[0]
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("int32 remainder array");
        assert_eq!(remainder.len(), 2);
        assert_eq!(remainder.value(0), 6);
        assert_eq!(remainder.value(1), 7);

        Ok(())
    }

    #[tokio::test]
    async fn finalize_flushes_remaining_partial_batch() -> anyhow::Result<()> {
        let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let array_path = Path::from("finalize-array.arrow");
        let layout_path = Path::from("finalize-layout.arrow");

        let mut writer = ArrayWriter::new(
            store.clone(),
            array_path.clone(),
            layout_path,
            Path::from("finalize-statistics.arrow"),
            DataType::Int32,
            5,
        )?;

        writer
            .append_array(0, make_i32_array(vec![10, 11, 12, 13]))
            .await?;
        writer.finalize().await?;

        let bytes = store.get(&array_path).await?.bytes().await?;
        let reader = FileReader::try_new(Cursor::new(bytes.to_vec()), None)?;
        let rows: usize = reader.map(|batch| batch.expect("batch").num_rows()).sum();

        assert_eq!(rows, 4);

        Ok(())
    }

    #[tokio::test]
    async fn finalize_writes_layouts_and_subset_reads_across_chunked_batches() -> anyhow::Result<()>
    {
        let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let array_path = Path::from("pipeline-array.arrow");
        let layout_path = Path::from("pipeline-layout.arrow");

        let mut writer = ArrayWriter::new(
            store.clone(),
            array_path.clone(),
            layout_path.clone(),
            Path::from("pipeline-statistics.arrow"),
            DataType::Int32,
            4,
        )?;

        writer
            .append_array(10, make_i32_array(vec![1, 2, 3]))
            .await?;
        writer
            .append_array(
                11,
                Arc::new(NdArrowArrayDispatch::new_in_mem(
                    vec![100, 101, 102, 103, 104, 105],
                    vec![2, 3],
                    vec!["lat".to_string(), "lon".to_string()],
                    None,
                )?),
            )
            .await?;
        writer.finalize().await?;

        let reader = ArrayReader::new_with_cache(
            store,
            layout_path,
            array_path,
            Arc::new(IoCache::new(1024 * 1024)),
        )
        .await?;

        let first_layout = reader
            .layouts()
            .find_dataset_array_layout(10)
            .expect("first layout exists");
        assert_eq!(first_layout.array_start, 0);
        assert_eq!(first_layout.array_len, 3);

        let second_layout = reader
            .layouts()
            .find_dataset_array_layout(11)
            .expect("second layout exists");
        assert_eq!(second_layout.array_start, 3);
        assert_eq!(second_layout.array_shape, vec![2, 3]);
        assert_eq!(second_layout.chunk_shape, vec![2, 3]);
        assert_eq!(
            second_layout.dimensions,
            vec!["lat".to_string(), "lon".to_string()]
        );

        let array = reader
            .read_dataset_array(11)
            .transpose()?
            .expect("dataset array exists");
        let subset = array
            .subset(ArraySubset {
                start: vec![0, 1],
                shape: vec![2, 2],
            })
            .await?;
        let materialized = subset.as_arrow_array_ref().await?;
        let values = materialized
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("int32 subset");

        assert_eq!(subset.shape(), vec![2, 2]);
        assert_eq!(
            subset.dimensions(),
            vec!["lat".to_string(), "lon".to_string()]
        );
        assert_eq!(values.values(), &[101, 102, 104, 105]);

        Ok(())
    }

    #[tokio::test]
    async fn finalize_writes_statistics_with_one_row_per_appended_nd_array() -> anyhow::Result<()> {
        let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let array_path = Path::from("stats-array.arrow");
        let layout_path = Path::from("stats-layout.arrow");
        let statistics_path = Path::from("stats-statistics.arrow");

        let mut writer = ArrayWriter::new(
            store.clone(),
            array_path,
            layout_path,
            statistics_path.clone(),
            DataType::Int32,
            8,
        )?;

        writer
            .append_array(0, make_i32_array(vec![1, 2, 3]))
            .await?;
        writer
            .append_array(1, make_i32_array(vec![7, 8, 9]))
            .await?;

        writer.finalize().await?;

        let bytes = store.get(&statistics_path).await?.bytes().await?;
        let reader = FileReader::try_new(Cursor::new(bytes.to_vec()), None)?;
        let schema = reader.schema();
        let batches = reader.collect::<Result<Vec<_>, arrow::error::ArrowError>>()?;
        let batch = arrow::compute::concat_batches(&schema, &batches)?;

        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.schema().field(0).name(), "min");
        assert_eq!(batch.schema().field(1).name(), "max");
        assert_eq!(batch.schema().field(2).name(), "null_count");
        assert_eq!(batch.schema().field(3).name(), "row_count");

        let min = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let max = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let null_count = batch
            .column(2)
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .unwrap();
        let row_count = batch
            .column(3)
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .unwrap();

        assert_eq!(min.value(0), 1);
        assert_eq!(max.value(0), 3);
        assert_eq!(null_count.value(0), 0);
        assert_eq!(row_count.value(0), 3);

        assert_eq!(min.value(1), 7);
        assert_eq!(max.value(1), 9);
        assert_eq!(null_count.value(1), 0);
        assert_eq!(row_count.value(1), 3);

        Ok(())
    }
}
