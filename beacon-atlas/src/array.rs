use std::collections::HashSet;
use std::io::{Read, Seek, SeekFrom};
use std::sync::Arc;

use anyhow::{Result, anyhow};
use arrow::buffer::Buffer;
use arrow::ipc::reader::{FileDecoder, read_footer_length};
use arrow::ipc::{Block, convert::fb_to_schema, root_as_footer};
use arrow::{
    array::{
        Array, AsArray, BinaryArray, BinaryBuilder, BooleanBuilder, Int64Array, Int64Builder,
        PrimitiveBuilder, StringBuilder,
    },
    ipc::{reader::FileReader, writer::FileWriter},
    record_batch::RecordBatch,
};
use beacon_nd_arrow::{NdArrowArray, column::NdArrowArrayColumn, extension::nd_column_field};
use bloomfilter::Bloom;
use bytes::Bytes;
use datafusion::scalar::ScalarValue;
use moka::future::Cache;
use object_store::ObjectStore;
use parking_lot::Mutex;
use tempfile::tempfile;

use crate::IPC_WRITE_OPTS;

const BLOOM_MAX_UNIQUE: usize = 1_000_000;
const BLOOM_FP_RATE: f64 = 0.01;

/// Shared cache for decoded record batches keyed by (path, batch index).
pub type BatchCache = Cache<(String, usize), Arc<RecordBatch>>;

/// Flush to IPC when estimated batch bytes exceed 4 MiB.
const BATCH_SIZE: usize = 4 * 1024 * 1024;
/// Stream uploads in 1 MiB chunks.
const STREAM_CHUNK_SIZE: usize = 1024 * 1024;

/// Writes ND Arrow arrays to a temporary IPC file and uploads to object storage.
///
/// This writer buffers ND arrays and writes RecordBatches as the estimated
/// batch size grows. Each row is stored as a single ND array value.
pub struct ArrayWriter<S: ObjectStore + Clone> {
    /// Object store used to persist the IPC file.
    pub store: S,
    /// Target object store directory.
    pub path: object_store::path::Path,
    /// Storage data type for ND arrays.
    pub data_type: arrow::datatypes::DataType,
    /// Buffered rows (None represents a null row).
    pub arrays: Vec<Option<NdArrowArray>>,
    /// Estimated current batch size in bytes.
    pub current_batch_size: usize,
    /// IPC writer backed by a temporary file.
    pub temp_arrow_file: FileWriter<std::fs::File>,
    /// Optional pruning index writer.
    pub pruning_writer: Option<FileWriter<std::fs::File>>,
    /// Schema for the pruning index.
    pub pruning_schema: Option<Arc<arrow::datatypes::Schema>>,
    /// Optional bloom filter index writer.
    pub bloom_writer: Option<FileWriter<std::fs::File>>,
    /// Schema for the bloom filter index.
    pub bloom_schema: Option<Arc<arrow::datatypes::Schema>>,
    /// Schema for the ND column.
    pub schema: Arc<arrow::datatypes::Schema>,
    /// Batch index ranges (start, end).
    pub batch_ranges: Vec<(usize, usize)>,
    /// Total rows written across batches.
    pub total_rows: usize,
}

/// Reads ND Arrow arrays from object storage using layout ranges.
pub struct ArrayReader<S: ObjectStore + Clone> {
    /// Object store used to read IPC files.
    pub store: S,
    /// Directory containing array.arrow and layout.arrow.
    pub path: object_store::path::Path,
    /// Path to the array IPC file.
    pub array_path: object_store::path::Path,
    /// Batch index ranges (start, end).
    pub batch_ranges: Vec<(usize, usize)>,
    /// Decoder state for the array IPC file.
    decoder_state: Arc<Mutex<DecoderState>>,
    /// Cached RecordBatches read from the array IPC footer.
    pub batches: Arc<Vec<Block>>,
    /// Optional cache for decoded RecordBatches keyed by (path, batch index).
    cache: Option<Arc<BatchCache>>,
}

/// Pruning index data loaded from pruning.arrow.
pub struct PruningIndex {
    pub batches: Vec<RecordBatch>,
}

/// Bloom filter index data loaded from bloom.arrow.
pub struct BloomIndex {
    pub batches: Vec<RecordBatch>,
}

impl PruningIndex {
    pub fn min_value(&self, batch_index: usize, row_index: usize) -> Result<ScalarValue> {
        let batch = self
            .batches
            .get(batch_index)
            .ok_or_else(|| anyhow!("pruning batch index out of range"))?;
        ScalarValue::try_from_array(batch.column(0), row_index).map_err(|err| anyhow!(err))
    }

    pub fn max_value(&self, batch_index: usize, row_index: usize) -> Result<ScalarValue> {
        let batch = self
            .batches
            .get(batch_index)
            .ok_or_else(|| anyhow!("pruning batch index out of range"))?;
        ScalarValue::try_from_array(batch.column(1), row_index).map_err(|err| anyhow!(err))
    }

    pub fn row_count(&self, batch_index: usize, row_index: usize) -> Result<Option<i64>> {
        let batch = self
            .batches
            .get(batch_index)
            .ok_or_else(|| anyhow!("pruning batch index out of range"))?;
        let array = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| anyhow!("pruning row_count column must be Int64"))?;
        if array.is_null(row_index) {
            Ok(None)
        } else {
            Ok(Some(array.value(row_index)))
        }
    }

    pub fn null_count(&self, batch_index: usize, row_index: usize) -> Result<Option<i64>> {
        let batch = self
            .batches
            .get(batch_index)
            .ok_or_else(|| anyhow!("pruning batch index out of range"))?;
        let array = batch
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| anyhow!("pruning null_count column must be Int64"))?;
        if array.is_null(row_index) {
            Ok(None)
        } else {
            Ok(Some(array.value(row_index)))
        }
    }
}

impl BloomIndex {
    pub fn bloom_filter(
        &self,
        batch_index: usize,
        row_index: usize,
    ) -> Result<Option<Bloom<String>>> {
        let batch = self
            .batches
            .get(batch_index)
            .ok_or_else(|| anyhow!("bloom batch index out of range"))?;
        let array = batch
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| anyhow!("bloom column must be Binary"))?;
        if array.is_null(row_index) {
            return Ok(None);
        }
        let bytes = array.value(row_index);
        let bloom = bincode::deserialize::<Bloom<String>>(bytes).map_err(|err| anyhow!(err))?;
        Ok(Some(bloom))
    }
}

/// Shared decoder state for zero-copy IPC reads.
struct DecoderState {
    decoder: FileDecoder,
}

impl<S: ObjectStore + Clone> ArrayWriter<S> {
    /// Create a new writer that stores ND arrays with the given storage `data_type`.
    pub fn new(
        store: S,
        path: object_store::path::Path,
        data_type: arrow::datatypes::DataType,
        pre_length: usize,
    ) -> Result<Self> {
        let temp_file = tempfile().map_err(|err| anyhow!(err))?;
        let field =
            nd_column_field("nd_arrays", data_type.clone(), true).map_err(|err| anyhow!(err))?;
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![field]));
        let writer = FileWriter::try_new_with_options(temp_file, &schema, IPC_WRITE_OPTS.clone())
            .map_err(|err| anyhow!(err))?;
        let (pruning_schema, pruning_writer) = if Self::pruning_supported(&data_type) {
            let pruning_schema = Arc::new(arrow::datatypes::Schema::new(vec![
                arrow::datatypes::Field::new("min", data_type.clone(), true),
                arrow::datatypes::Field::new("max", data_type.clone(), true),
                arrow::datatypes::Field::new("row_count", arrow::datatypes::DataType::Int64, false),
                arrow::datatypes::Field::new(
                    "null_count",
                    arrow::datatypes::DataType::Int64,
                    false,
                ),
            ]));
            let pruning_file = tempfile().map_err(|err| anyhow!(err))?;
            let pruning_writer = FileWriter::try_new_with_options(
                pruning_file,
                &pruning_schema,
                IPC_WRITE_OPTS.clone(),
            )
            .map_err(|err| anyhow!(err))?;
            (Some(pruning_schema), Some(pruning_writer))
        } else {
            (None, None)
        };
        let (bloom_schema, bloom_writer) = if matches!(data_type, arrow::datatypes::DataType::Utf8)
        {
            let bloom_schema = Arc::new(arrow::datatypes::Schema::new(vec![
                arrow::datatypes::Field::new("bloom", arrow::datatypes::DataType::Binary, true),
            ]));
            let bloom_file = tempfile().map_err(|err| anyhow!(err))?;
            let bloom_writer =
                FileWriter::try_new_with_options(bloom_file, &bloom_schema, IPC_WRITE_OPTS.clone())
                    .map_err(|err| anyhow!(err))?;
            (Some(bloom_schema), Some(bloom_writer))
        } else {
            (None, None)
        };

        Ok(Self {
            store,
            path,
            data_type,
            arrays: vec![None; pre_length],
            current_batch_size: 0,
            temp_arrow_file: writer,
            pruning_writer,
            pruning_schema,
            bloom_writer,
            bloom_schema,
            schema,
            batch_ranges: Vec::new(),
            total_rows: 0,
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

        let rows = if self.pruning_writer.is_some() {
            let mut pruning_writer = self
                .pruning_writer
                .take()
                .ok_or_else(|| anyhow!("missing pruning writer"))?;
            let rows = self.flush_with_pruning(&mut pruning_writer)?;
            self.pruning_writer = Some(pruning_writer);
            rows
        } else {
            self.flush_without_pruning()?
        };
        if self.bloom_writer.is_some() {
            let mut bloom_writer = self
                .bloom_writer
                .take()
                .ok_or_else(|| anyhow!("missing bloom writer"))?;
            self.write_bloom_batch(&mut bloom_writer, &rows)?;
            self.bloom_writer = Some(bloom_writer);
        }

        let batch_rows = rows.len();
        let start = self.total_rows;
        let end = self.total_rows + batch_rows;
        self.batch_ranges.push((start, end));
        self.total_rows = end;

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

    fn layout_path(&self) -> object_store::path::Path {
        object_store::path::Path::from(format!("{}/layout.arrow", self.path))
    }

    fn array_path(&self) -> object_store::path::Path {
        object_store::path::Path::from(format!("{}/array.arrow", self.path))
    }

    fn pruning_path(&self) -> object_store::path::Path {
        object_store::path::Path::from(format!("{}/pruning.arrow", self.path))
    }

    fn bloom_path(&self) -> object_store::path::Path {
        object_store::path::Path::from(format!("{}/bloom.arrow", self.path))
    }

    async fn stream_file_to_store(
        store: &S,
        mut file: std::fs::File,
        path: &object_store::path::Path,
    ) -> Result<()> {
        file.seek(SeekFrom::Start(0)).map_err(|err| anyhow!(err))?;
        let mut buf = vec![0u8; STREAM_CHUNK_SIZE];
        let mut total_read = 0usize;
        let mut uploader = store
            .put_multipart(path)
            .await
            .map_err(|err| anyhow!(err))?;

        loop {
            let read = file.read(&mut buf).map_err(|err| anyhow!(err))?;
            if read == 0 {
                break;
            }
            total_read += read;
            uploader
                .put_part(Bytes::copy_from_slice(&buf[..read]).into())
                .await
                .map_err(|err| anyhow!(err))?;
        }

        if total_read == 0 {
            store
                .put(path, Bytes::new().into())
                .await
                .map_err(|err| anyhow!(err))?;
        } else {
            uploader.complete().await.map_err(|err| anyhow!(err))?;
        }
        Ok(())
    }

    /// Finalize and upload the IPC file to object storage.
    pub async fn finish(mut self) -> Result<()> {
        self.flush()?;
        let array_path = self.array_path();
        let layout_path = self.layout_path();
        let pruning_path = self.pruning_path();
        let bloom_path = self.bloom_path();
        let batch_ranges = self.batch_ranges.clone();
        let store = self.store.clone();
        self.temp_arrow_file.finish().map_err(|err| anyhow!(err))?;
        let inner = self
            .temp_arrow_file
            .into_inner()
            .map_err(|err| anyhow!(err))?;
        Self::stream_file_to_store(&store, inner, &array_path).await?;
        Self::write_layout(&store, &layout_path, &batch_ranges).await?;
        if let Some(mut pruning_writer) = self.pruning_writer {
            pruning_writer.finish().map_err(|err| anyhow!(err))?;
            let inner = pruning_writer.into_inner().map_err(|err| anyhow!(err))?;
            Self::stream_file_to_store(&store, inner, &pruning_path).await?;
        }
        if let Some(mut bloom_writer) = self.bloom_writer {
            bloom_writer.finish().map_err(|err| anyhow!(err))?;
            let inner = bloom_writer.into_inner().map_err(|err| anyhow!(err))?;
            Self::stream_file_to_store(&store, inner, &bloom_path).await?;
        }
        Ok(())
    }

    async fn write_layout(
        store: &S,
        layout_path: &object_store::path::Path,
        batch_ranges: &[(usize, usize)],
    ) -> Result<()> {
        let layout_schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("start", arrow::datatypes::DataType::Int64, false),
            arrow::datatypes::Field::new("end", arrow::datatypes::DataType::Int64, false),
        ]));
        let tmp = tempfile().map_err(|err| anyhow!(err))?;
        let mut writer = FileWriter::try_new_with_options(
            tmp.try_clone().map_err(|err| anyhow!(err))?,
            &layout_schema,
            IPC_WRITE_OPTS.clone(),
        )
        .map_err(|err| anyhow!(err))?;

        if !batch_ranges.is_empty() {
            let starts = batch_ranges
                .iter()
                .map(|(s, _)| *s as i64)
                .collect::<Vec<_>>();
            let ends = batch_ranges
                .iter()
                .map(|(_, e)| *e as i64)
                .collect::<Vec<_>>();

            let batch = RecordBatch::try_new(
                layout_schema,
                vec![
                    Arc::new(Int64Array::from(starts)),
                    Arc::new(Int64Array::from(ends)),
                ],
            )
            .map_err(|err| anyhow!(err))?;
            writer.write(&batch).map_err(|err| anyhow!(err))?;
        }
        writer.finish().map_err(|err| anyhow!(err))?;

        Self::stream_file_to_store(store, tmp, layout_path).await
    }

    fn pruning_supported(data_type: &arrow::datatypes::DataType) -> bool {
        matches!(
            data_type,
            arrow::datatypes::DataType::Boolean
                | arrow::datatypes::DataType::Int8
                | arrow::datatypes::DataType::Int16
                | arrow::datatypes::DataType::Int32
                | arrow::datatypes::DataType::Int64
                | arrow::datatypes::DataType::UInt8
                | arrow::datatypes::DataType::UInt16
                | arrow::datatypes::DataType::UInt32
                | arrow::datatypes::DataType::UInt64
                | arrow::datatypes::DataType::Float32
                | arrow::datatypes::DataType::Float64
                | arrow::datatypes::DataType::Utf8
                | arrow::datatypes::DataType::Binary
                | arrow::datatypes::DataType::Timestamp(_, _)
        )
    }

    fn write_pruning_batch(
        &self,
        pruning_writer: &mut FileWriter<std::fs::File>,
        columns: Vec<Arc<dyn arrow::array::Array>>,
    ) -> Result<()> {
        let schema = self
            .pruning_schema
            .as_ref()
            .ok_or_else(|| anyhow!("missing pruning schema"))?;
        let batch = RecordBatch::try_new(schema.clone(), columns).map_err(|err| anyhow!(err))?;
        pruning_writer.write(&batch).map_err(|err| anyhow!(err))?;
        Ok(())
    }

    fn write_bloom_batch(
        &self,
        bloom_writer: &mut FileWriter<std::fs::File>,
        rows: &[NdArrowArray],
    ) -> Result<()> {
        let schema = self
            .bloom_schema
            .as_ref()
            .ok_or_else(|| anyhow!("missing bloom schema"))?;
        let mut builder = BinaryBuilder::new();

        let bloom = Self::build_bloom_filter(rows);
        match bloom {
            Some(bloom) => builder.append_value(bloom),
            None => builder.append_null(),
        }

        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(builder.finish())])
            .map_err(|err| anyhow!(err))?;
        bloom_writer.write(&batch).map_err(|err| anyhow!(err))?;
        Ok(())
    }

    fn build_bloom_filter(rows: &[NdArrowArray]) -> Option<Vec<u8>> {
        let mut unique: HashSet<String> = HashSet::new();

        for row in rows {
            let values = row.values();
            if matches!(values.data_type(), arrow::datatypes::DataType::Utf8) {
                let string_values = values.as_string::<i32>();
                for i in 0..string_values.len() {
                    if string_values.is_null(i) {
                        continue;
                    }
                    let value = string_values.value(i).to_string();
                    if unique.insert(value) && unique.len() >= BLOOM_MAX_UNIQUE {
                        return None;
                    }
                }
            }
        }

        let expected = unique.len().max(1);
        let mut bloom = Bloom::new_for_fp_rate(expected, BLOOM_FP_RATE);
        for value in &unique {
            bloom.set(value);
        }

        bincode::serialize(&bloom).ok()
    }

    fn flush_with_pruning(
        &mut self,
        pruning_writer: &mut FileWriter<std::fs::File>,
    ) -> Result<Vec<NdArrowArray>> {
        let mut row_count_builder = Int64Builder::new();
        let mut null_count_builder = Int64Builder::new();
        match &self.data_type {
            arrow::datatypes::DataType::Boolean => self.flush_boolean_pruning(
                pruning_writer,
                &mut row_count_builder,
                &mut null_count_builder,
            ),
            arrow::datatypes::DataType::Int8 => self
                .flush_primitive_pruning::<arrow::datatypes::Int8Type>(
                    pruning_writer,
                    &mut row_count_builder,
                    &mut null_count_builder,
                ),
            arrow::datatypes::DataType::Int16 => self
                .flush_primitive_pruning::<arrow::datatypes::Int16Type>(
                    pruning_writer,
                    &mut row_count_builder,
                    &mut null_count_builder,
                ),
            arrow::datatypes::DataType::Int32 => self
                .flush_primitive_pruning::<arrow::datatypes::Int32Type>(
                    pruning_writer,
                    &mut row_count_builder,
                    &mut null_count_builder,
                ),
            arrow::datatypes::DataType::Int64 => self
                .flush_primitive_pruning::<arrow::datatypes::Int64Type>(
                    pruning_writer,
                    &mut row_count_builder,
                    &mut null_count_builder,
                ),
            arrow::datatypes::DataType::UInt8 => self
                .flush_primitive_pruning::<arrow::datatypes::UInt8Type>(
                    pruning_writer,
                    &mut row_count_builder,
                    &mut null_count_builder,
                ),
            arrow::datatypes::DataType::UInt16 => self
                .flush_primitive_pruning::<arrow::datatypes::UInt16Type>(
                    pruning_writer,
                    &mut row_count_builder,
                    &mut null_count_builder,
                ),
            arrow::datatypes::DataType::UInt32 => self
                .flush_primitive_pruning::<arrow::datatypes::UInt32Type>(
                    pruning_writer,
                    &mut row_count_builder,
                    &mut null_count_builder,
                ),
            arrow::datatypes::DataType::UInt64 => self
                .flush_primitive_pruning::<arrow::datatypes::UInt64Type>(
                    pruning_writer,
                    &mut row_count_builder,
                    &mut null_count_builder,
                ),
            arrow::datatypes::DataType::Float32 => self
                .flush_primitive_pruning::<arrow::datatypes::Float32Type>(
                    pruning_writer,
                    &mut row_count_builder,
                    &mut null_count_builder,
                ),
            arrow::datatypes::DataType::Float64 => self
                .flush_primitive_pruning::<arrow::datatypes::Float64Type>(
                    pruning_writer,
                    &mut row_count_builder,
                    &mut null_count_builder,
                ),
            arrow::datatypes::DataType::Timestamp(unit, _) => match unit {
                arrow::datatypes::TimeUnit::Second => self
                    .flush_primitive_pruning::<arrow::datatypes::TimestampSecondType>(
                        pruning_writer,
                        &mut row_count_builder,
                        &mut null_count_builder,
                    ),
                arrow::datatypes::TimeUnit::Millisecond => {
                    self.flush_primitive_pruning::<arrow::datatypes::TimestampMillisecondType>(
                        pruning_writer,
                        &mut row_count_builder,
                        &mut null_count_builder,
                    )
                }
                arrow::datatypes::TimeUnit::Microsecond => {
                    self.flush_primitive_pruning::<arrow::datatypes::TimestampMicrosecondType>(
                        pruning_writer,
                        &mut row_count_builder,
                        &mut null_count_builder,
                    )
                }
                arrow::datatypes::TimeUnit::Nanosecond => self
                    .flush_primitive_pruning::<arrow::datatypes::TimestampNanosecondType>(
                    pruning_writer,
                    &mut row_count_builder,
                    &mut null_count_builder,
                ),
            },
            arrow::datatypes::DataType::Utf8 => self.flush_string_pruning(
                pruning_writer,
                &mut row_count_builder,
                &mut null_count_builder,
            ),
            arrow::datatypes::DataType::Binary => self.flush_binary_pruning(
                pruning_writer,
                &mut row_count_builder,
                &mut null_count_builder,
            ),
            _ => self.flush_without_pruning(),
        }
    }

    fn flush_without_pruning(&mut self) -> Result<Vec<NdArrowArray>> {
        self.arrays
            .drain(..)
            .map(|opt| match opt {
                Some(arr) => Ok(arr),
                None => NdArrowArray::new_null_scalar(Some(self.data_type.clone()))
                    .map_err(|err| anyhow!(err)),
            })
            .collect()
    }

    fn flush_boolean_pruning(
        &mut self,
        pruning_writer: &mut FileWriter<std::fs::File>,
        row_count_builder: &mut Int64Builder,
        null_count_builder: &mut Int64Builder,
    ) -> Result<Vec<NdArrowArray>> {
        let mut min_builder = BooleanBuilder::new();
        let mut max_builder = BooleanBuilder::new();
        let mut rows = Vec::with_capacity(self.arrays.len());
        for opt in self.arrays.drain(..) {
            match opt {
                Some(arr) => {
                    let values = arr.values();
                    let min = arrow::compute::min_boolean(values.as_boolean());
                    let max = arrow::compute::max_boolean(values.as_boolean());
                    min_builder.append_option(min);
                    max_builder.append_option(max);
                    row_count_builder.append_value(values.len() as i64);
                    null_count_builder.append_value(values.null_count() as i64);
                    rows.push(arr);
                }
                None => {
                    min_builder.append_null();
                    max_builder.append_null();
                    row_count_builder.append_value(0);
                    null_count_builder.append_value(0);
                    rows.push(
                        NdArrowArray::new_null_scalar(Some(self.data_type.clone()))
                            .map_err(|err| anyhow!(err))?,
                    );
                }
            }
        }
        self.write_pruning_batch(
            pruning_writer,
            vec![
                Arc::new(min_builder.finish()),
                Arc::new(max_builder.finish()),
                Arc::new(row_count_builder.finish()),
                Arc::new(null_count_builder.finish()),
            ],
        )?;
        Ok(rows)
    }

    fn flush_string_pruning(
        &mut self,
        pruning_writer: &mut FileWriter<std::fs::File>,
        row_count_builder: &mut Int64Builder,
        null_count_builder: &mut Int64Builder,
    ) -> Result<Vec<NdArrowArray>> {
        let mut min_builder = StringBuilder::new();
        let mut max_builder = StringBuilder::new();
        let mut rows = Vec::with_capacity(self.arrays.len());
        for opt in self.arrays.drain(..) {
            match opt {
                Some(arr) => {
                    let values = arr.values();
                    let min = arrow::compute::min_string(values.as_string::<i32>());
                    let max = arrow::compute::max_string(values.as_string::<i32>());
                    min_builder.append_option(min);
                    max_builder.append_option(max);
                    row_count_builder.append_value(values.len() as i64);
                    null_count_builder.append_value(values.null_count() as i64);
                    rows.push(arr);
                }
                None => {
                    min_builder.append_null();
                    max_builder.append_null();
                    row_count_builder.append_value(0);
                    null_count_builder.append_value(0);
                    rows.push(
                        NdArrowArray::new_null_scalar(Some(self.data_type.clone()))
                            .map_err(|err| anyhow!(err))?,
                    );
                }
            }
        }
        self.write_pruning_batch(
            pruning_writer,
            vec![
                Arc::new(min_builder.finish()),
                Arc::new(max_builder.finish()),
                Arc::new(row_count_builder.finish()),
                Arc::new(null_count_builder.finish()),
            ],
        )?;
        Ok(rows)
    }

    fn flush_binary_pruning(
        &mut self,
        pruning_writer: &mut FileWriter<std::fs::File>,
        row_count_builder: &mut Int64Builder,
        null_count_builder: &mut Int64Builder,
    ) -> Result<Vec<NdArrowArray>> {
        let mut min_builder = BinaryBuilder::new();
        let mut max_builder = BinaryBuilder::new();
        let mut rows = Vec::with_capacity(self.arrays.len());
        for opt in self.arrays.drain(..) {
            match opt {
                Some(arr) => {
                    let values = arr.values();
                    let min = arrow::compute::min_binary(values.as_binary::<i32>());
                    let max = arrow::compute::max_binary(values.as_binary::<i32>());
                    min_builder.append_option(min);
                    max_builder.append_option(max);
                    row_count_builder.append_value(values.len() as i64);
                    null_count_builder.append_value(values.null_count() as i64);
                    rows.push(arr);
                }
                None => {
                    min_builder.append_null();
                    max_builder.append_null();
                    row_count_builder.append_value(0);
                    null_count_builder.append_value(0);
                    rows.push(
                        NdArrowArray::new_null_scalar(Some(self.data_type.clone()))
                            .map_err(|err| anyhow!(err))?,
                    );
                }
            }
        }
        self.write_pruning_batch(
            pruning_writer,
            vec![
                Arc::new(min_builder.finish()),
                Arc::new(max_builder.finish()),
                Arc::new(row_count_builder.finish()),
                Arc::new(null_count_builder.finish()),
            ],
        )?;
        Ok(rows)
    }

    fn flush_primitive_pruning<T>(
        &mut self,
        pruning_writer: &mut FileWriter<std::fs::File>,
        row_count_builder: &mut Int64Builder,
        null_count_builder: &mut Int64Builder,
    ) -> Result<Vec<NdArrowArray>>
    where
        T: arrow::datatypes::ArrowPrimitiveType,
    {
        let mut min_builder = PrimitiveBuilder::<T>::new();
        let mut max_builder = PrimitiveBuilder::<T>::new();
        let mut rows = Vec::with_capacity(self.arrays.len());
        for opt in self.arrays.drain(..) {
            Self::append_primitive_stats::<T>(
                &self.data_type,
                opt,
                &mut min_builder,
                &mut max_builder,
                row_count_builder,
                null_count_builder,
                &mut rows,
            )?;
        }
        self.write_pruning_batch(
            pruning_writer,
            vec![
                Arc::new(min_builder.finish()),
                Arc::new(max_builder.finish()),
                Arc::new(row_count_builder.finish()),
                Arc::new(null_count_builder.finish()),
            ],
        )?;
        Ok(rows)
    }

    fn append_primitive_stats<T>(
        data_type: &arrow::datatypes::DataType,
        opt: Option<NdArrowArray>,
        min_builder: &mut PrimitiveBuilder<T>,
        max_builder: &mut PrimitiveBuilder<T>,
        row_count_builder: &mut Int64Builder,
        null_count_builder: &mut Int64Builder,
        rows: &mut Vec<NdArrowArray>,
    ) -> Result<()>
    where
        T: arrow::datatypes::ArrowPrimitiveType,
    {
        match opt {
            Some(arr) => {
                let values = arr.values();
                let min = arrow::compute::min(values.as_primitive::<T>());
                let max = arrow::compute::max(values.as_primitive::<T>());
                min_builder.append_option(min);
                max_builder.append_option(max);
                row_count_builder.append_value(values.len() as i64);
                null_count_builder.append_value(values.null_count() as i64);
                rows.push(arr);
            }
            None => {
                min_builder.append_null();
                max_builder.append_null();
                row_count_builder.append_value(0);
                null_count_builder.append_value(0);
                rows.push(
                    NdArrowArray::new_null_scalar(Some(data_type.clone()))
                        .map_err(|err| anyhow!(err))?,
                );
            }
        }
        Ok(())
    }
}

impl<S: ObjectStore + Clone> ArrayReader<S> {
    /// Open an ND array reader from a directory path.
    pub async fn open(store: S, path: object_store::path::Path) -> Result<Self> {
        Self::open_with_cache(store, path, None).await
    }

    /// Open an ND array reader with an optional shared batch cache.
    pub async fn open_with_cache(
        store: S,
        path: object_store::path::Path,
        cache: Option<Arc<BatchCache>>,
    ) -> Result<Self> {
        let layout_path = object_store::path::Path::from(format!("{}/layout.arrow", path));
        let layout_bytes = store
            .get(&layout_path)
            .await
            .map_err(|err| anyhow!(err))?
            .bytes()
            .await
            .map_err(|err| anyhow!(err))?;
        let reader = FileReader::try_new(std::io::Cursor::new(layout_bytes.to_vec()), None)
            .map_err(|err| anyhow!(err))?;
        let mut ranges = Vec::new();

        for batch in reader.into_iter() {
            let batch = batch.map_err(|err| anyhow!(err))?;
            let starts = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| anyhow!("layout start column must be Int64"))?;
            let ends = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| anyhow!("layout end column must be Int64"))?;
            for i in 0..starts.len() {
                let start = usize::try_from(starts.value(i)).map_err(|err| anyhow!(err))?;
                let end = usize::try_from(ends.value(i)).map_err(|err| anyhow!(err))?;
                ranges.push((start, end));
            }
        }

        let array_path = object_store::path::Path::from(format!("{}/array.arrow", path));
        // Read only the IPC footer bytes (last 10 bytes + footer length) to discover blocks.
        let size = store
            .head(&array_path)
            .await
            .map_err(|err| anyhow!(err))?
            .size;
        if size < 10 {
            return Err(anyhow!("IPC file too small for footer"));
        }
        let size_usize = usize::try_from(size).map_err(|err| anyhow!(err))?;
        let trailer_start = size_usize - 10;
        let trailer_start_u64 = u64::try_from(trailer_start).map_err(|err| anyhow!(err))?;
        // Read the 10-byte IPC trailer with the footer length and magic.
        let trailer_bytes = store
            .get_range(&array_path, trailer_start_u64..size)
            .await
            .map_err(|err| anyhow!(err))?;
        let trailer: [u8; 10] = trailer_bytes
            .as_ref()
            .try_into()
            .map_err(|_| anyhow!("invalid IPC trailer length"))?;
        let footer_len = read_footer_length(trailer).map_err(|err| anyhow!(err))?;
        let footer_start = trailer_start
            .checked_sub(footer_len)
            .ok_or_else(|| anyhow!("IPC footer length underflow"))?;
        let footer_start_u64 = u64::try_from(footer_start).map_err(|err| anyhow!(err))?;
        // Read the footer using the length from the trailer.
        let footer_bytes = store
            .get_range(&array_path, footer_start_u64..trailer_start_u64)
            .await
            .map_err(|err| anyhow!(err))?;
        let footer = root_as_footer(footer_bytes.as_ref()).map_err(|err| anyhow!(err))?;
        let schema = fb_to_schema(
            footer
                .schema()
                .ok_or_else(|| anyhow!("missing IPC schema"))?,
        );
        // Build a decoder without reading dictionaries yet.
        let mut decoder = FileDecoder::new(Arc::new(schema), footer.version());
        let dictionary_blocks: Vec<Block> = footer
            .dictionaries()
            .map(|b| b.iter().copied().collect())
            .unwrap_or_default();
        // Read all dictionary blocks up-front so record batch reads are ready.
        for block in &dictionary_blocks {
            let block_len = (block.bodyLength() as usize) + (block.metaDataLength() as usize);
            let offset = block.offset() as u64;
            let block_len_u64 = u64::try_from(block_len).map_err(|err| anyhow!(err))?;
            let data = store
                .get_range(&array_path, offset..(offset + block_len_u64))
                .await
                .map_err(|err: object_store::Error| anyhow!(err))?;
            let buffer = Buffer::from(data);
            decoder
                .read_dictionary(block, &buffer)
                .map_err(|err| anyhow!(err))?;
        }
        let batches = footer
            .recordBatches()
            .map(|b| b.iter().copied().collect())
            .unwrap_or_default();

        Ok(Self {
            store,
            path,
            array_path,
            batch_ranges: ranges,
            decoder_state: Arc::new(Mutex::new(DecoderState { decoder })),
            batches: Arc::new(batches),
            cache,
        })
    }

    /// Read a single ND array at the given logical index.
    pub async fn read_index(&self, index: usize) -> Result<Option<NdArrowArray>> {
        let (batch_index, start) = match self
            .batch_ranges
            .iter()
            .enumerate()
            .find(|(_, (s, e))| index >= *s && index < *e)
        {
            Some((idx, (s, _))) => (idx, *s),
            None => return Ok(None),
        };
        // Locate the record batch block for this index.
        let block = match self.batches.get(batch_index) {
            Some(block) => block,
            None => return Ok(None),
        };
        let batch = if let Some(cache) = &self.cache {
            let cache_key = self.cache_key(batch_index);
            let store = self.store.clone();
            let array_path = self.array_path.clone();
            let decoder_state = Arc::clone(&self.decoder_state);
            let block = *block;
            cache
                .try_get_with(cache_key, async move {
                    Self::read_batch_with(store, array_path, decoder_state, block).await
                })
                .await
                .map_err(|err| anyhow!(err))?
        } else {
            Self::read_batch_with(
                self.store.clone(),
                self.array_path.clone(),
                Arc::clone(&self.decoder_state),
                *block,
            )
            .await?
        };
        self.row_from_batch(&batch, index, start)
    }

    /// Read pruning index batches if present.
    pub async fn pruning_index(&self) -> Result<Option<PruningIndex>> {
        let batches = Self::read_index_batches(&self.store, &self.pruning_path()).await?;
        Ok(batches.map(|batches| PruningIndex { batches }))
    }

    /// Read bloom index batches if present.
    pub async fn bloom_index(&self) -> Result<Option<BloomIndex>> {
        let batches = Self::read_index_batches(&self.store, &self.bloom_path()).await?;
        Ok(batches.map(|batches| BloomIndex { batches }))
    }

    fn cache_key(&self, batch_index: usize) -> (String, usize) {
        (self.array_path.to_string(), batch_index)
    }

    fn pruning_path(&self) -> object_store::path::Path {
        object_store::path::Path::from(format!("{}/pruning.arrow", self.path))
    }

    fn bloom_path(&self) -> object_store::path::Path {
        object_store::path::Path::from(format!("{}/bloom.arrow", self.path))
    }

    async fn read_index_batches(
        store: &S,
        path: &object_store::path::Path,
    ) -> Result<Option<Vec<RecordBatch>>> {
        let bytes = match store.get(path).await {
            Ok(result) => result.bytes().await.map_err(|err| anyhow!(err))?,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(err) => return Err(anyhow!(err)),
        };
        let reader = FileReader::try_new(std::io::Cursor::new(bytes.to_vec()), None)
            .map_err(|err| anyhow!(err))?;
        let mut batches = Vec::new();
        for batch in reader {
            batches.push(batch.map_err(|err| anyhow!(err))?);
        }
        Ok(Some(batches))
    }

    fn row_from_batch(
        &self,
        batch: &RecordBatch,
        index: usize,
        start: usize,
    ) -> Result<Option<NdArrowArray>> {
        let column = batch.column(0).clone();
        let nd_column = NdArrowArrayColumn::try_from_array(column).map_err(|err| anyhow!(err))?;
        let row_index = index - start;
        let nd = nd_column.row(row_index).map_err(|err| anyhow!(err))?;
        Ok(Some(nd))
    }

    async fn read_batch_with(
        store: S,
        array_path: object_store::path::Path,
        decoder_state: Arc<Mutex<DecoderState>>,
        block: Block,
    ) -> Result<Arc<RecordBatch>> {
        let block_len = (block.bodyLength() as usize) + (block.metaDataLength() as usize);
        let offset = block.offset() as u64;
        let block_len_u64 = u64::try_from(block_len).map_err(|err| anyhow!(err))?;
        let data = store
            .get_range(&array_path, offset..(offset + block_len_u64))
            .await
            .map_err(|err: object_store::Error| anyhow!(err))?;
        let data = Buffer::from(data);
        let batch = {
            let state = decoder_state.lock();
            state
                .decoder
                .read_record_batch(&block, &data)
                .map_err(|err| anyhow!(err))?
        };
        match batch {
            Some(batch) => Ok(Arc::new(batch)),
            None => Err(anyhow!("record batch not found")),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{Array, Int32Array, Int64Array},
        datatypes::DataType,
    };
    use beacon_nd_arrow::dimensions::{Dimension, Dimensions};
    use moka::future::Cache;
    use object_store::{ObjectStore, memory::InMemory, path::Path};

    use super::*;

    #[tokio::test]
    async fn array_writer_writes_ipc() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("nd/arrays");
        let mut writer = ArrayWriter::new(store.clone(), path.clone(), DataType::Int32, 0).unwrap();

        let dims = Dimensions::new(vec![Dimension::try_new("x", 3).unwrap()]);
        let arr = NdArrowArray::new(Arc::new(Int32Array::from(vec![1, 2, 3])), dims).unwrap();
        writer.append_array(arr).unwrap();
        writer.append_null().unwrap();

        writer.finish().await.unwrap();
        let bytes = store
            .get(&Path::from("nd/arrays/array.arrow"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let reader =
            arrow::ipc::reader::FileReader::try_new(std::io::Cursor::new(bytes.to_vec()), None)
                .unwrap();
        let batch = reader.into_iter().next().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 2);

        let layout_path = Path::from("nd/arrays/layout.arrow");
        let layout_bytes = store
            .get(&layout_path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let layout_reader = arrow::ipc::reader::FileReader::try_new(
            std::io::Cursor::new(layout_bytes.to_vec()),
            None,
        )
        .unwrap();
        let layout_batch = layout_reader.into_iter().next().unwrap().unwrap();
        assert_eq!(layout_batch.num_rows(), 1);
        let starts = layout_batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let ends = layout_batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(starts.value(0), 0);
        assert_eq!(ends.value(0), 2);
    }

    #[tokio::test]
    async fn array_writer_writes_multiple_batches_layout() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("nd/multi");
        let mut writer = ArrayWriter::new(store.clone(), path.clone(), DataType::Int32, 0).unwrap();

        let values = vec![1; 1_100_000];
        let dims = Dimensions::new(vec![Dimension::try_new("x", values.len()).unwrap()]);
        let arr = NdArrowArray::new(Arc::new(Int32Array::from(values)), dims).unwrap();
        writer.append_array(arr).unwrap();

        let values = vec![2; 1_100_000];
        let dims = Dimensions::new(vec![Dimension::try_new("x", values.len()).unwrap()]);
        let arr = NdArrowArray::new(Arc::new(Int32Array::from(values)), dims).unwrap();
        writer.append_array(arr).unwrap();

        writer.finish().await.unwrap();

        let layout_path = Path::from("nd/multi/layout.arrow");
        let layout_bytes = store
            .get(&layout_path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let layout_reader = arrow::ipc::reader::FileReader::try_new(
            std::io::Cursor::new(layout_bytes.to_vec()),
            None,
        )
        .unwrap();
        let layout_batch = layout_reader.into_iter().next().unwrap().unwrap();
        assert_eq!(layout_batch.num_rows(), 2);
        let starts = layout_batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let ends = layout_batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(starts.value(0), 0);
        assert_eq!(ends.value(0), 1);
        assert_eq!(starts.value(1), 1);
        assert_eq!(ends.value(1), 2);
    }

    #[tokio::test]
    async fn array_writer_writes_bloom_for_utf8() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("nd/bloom");
        let mut writer = ArrayWriter::new(store.clone(), path.clone(), DataType::Utf8, 0).unwrap();

        let dims = Dimensions::new(vec![Dimension::try_new("x", 3).unwrap()]);
        let arr = NdArrowArray::new(
            Arc::new(arrow::array::StringArray::from(vec![
                Some("alpha"),
                Some("beta"),
                Some("alpha"),
            ])),
            dims,
        )
        .unwrap();
        writer.append_array(arr).unwrap();

        writer.finish().await.unwrap();

        let bloom_path = Path::from("nd/bloom/bloom.arrow");
        let bloom_bytes = store.get(&bloom_path).await.unwrap().bytes().await.unwrap();
        let bloom_reader = arrow::ipc::reader::FileReader::try_new(
            std::io::Cursor::new(bloom_bytes.to_vec()),
            None,
        )
        .unwrap();
        let bloom_batch = bloom_reader.into_iter().next().unwrap().unwrap();
        let bloom_column = bloom_batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::BinaryArray>()
            .unwrap();
        assert_eq!(bloom_column.len(), 1);
        assert_eq!(bloom_column.null_count(), 0);
        assert!(!bloom_column.value(0).is_empty());
        let bloom: bloomfilter::Bloom<String> =
            bincode::deserialize(bloom_column.value(0)).unwrap();
        assert!(bloom.check(&"alpha".to_string()));
    }

    #[tokio::test]
    async fn array_writer_skips_bloom_for_non_utf8() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("nd/no-bloom");
        let mut writer = ArrayWriter::new(store.clone(), path.clone(), DataType::Int32, 0).unwrap();

        let dims = Dimensions::new(vec![Dimension::try_new("x", 2).unwrap()]);
        let arr = NdArrowArray::new(Arc::new(Int32Array::from(vec![1, 2])), dims).unwrap();
        writer.append_array(arr).unwrap();

        writer.finish().await.unwrap();

        let bloom_path = Path::from("nd/no-bloom/bloom.arrow");
        assert!(store.get(&bloom_path).await.is_err());
    }

    #[tokio::test]
    async fn array_writer_writes_empty_layout() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("nd/empty");
        let writer = ArrayWriter::new(store.clone(), path.clone(), DataType::Int32, 0).unwrap();

        writer.finish().await.unwrap();

        let layout_path = Path::from("nd/empty/layout.arrow");
        let layout_bytes = store
            .get(&layout_path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let layout_reader = arrow::ipc::reader::FileReader::try_new(
            std::io::Cursor::new(layout_bytes.to_vec()),
            None,
        )
        .unwrap();
        assert!(layout_reader.into_iter().next().is_none());
    }

    #[tokio::test]
    async fn array_reader_reads_index() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("nd/read");
        let mut writer = ArrayWriter::new(store.clone(), path.clone(), DataType::Int32, 0).unwrap();

        let dims = Dimensions::new(vec![Dimension::try_new("x", 3).unwrap()]);
        let arr = NdArrowArray::new(Arc::new(Int32Array::from(vec![1, 2, 3])), dims).unwrap();
        writer.append_array(arr).unwrap();

        let dims = Dimensions::new(vec![Dimension::try_new("x", 2).unwrap()]);
        let arr = NdArrowArray::new(Arc::new(Int32Array::from(vec![4, 5])), dims).unwrap();
        writer.append_array(arr).unwrap();

        writer.finish().await.unwrap();

        let reader = ArrayReader::open(store.clone(), path.clone())
            .await
            .unwrap();
        let nd = reader.read_index(1).await.unwrap().unwrap();
        assert_eq!(nd.values().len(), 2);
        assert_eq!(nd.dimensions().shape(), vec![2]);
        let values = nd.values().as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(values.values(), &[4, 5]);
        assert!(reader.read_index(2).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn array_reader_reads_multi_batch_index() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("nd/read_multi");
        let mut writer = ArrayWriter::new(store.clone(), path.clone(), DataType::Int32, 0).unwrap();

        let values = vec![1; 1_100_000];
        let dims = Dimensions::new(vec![Dimension::try_new("x", values.len()).unwrap()]);
        let arr = NdArrowArray::new(Arc::new(Int32Array::from(values)), dims).unwrap();
        writer.append_array(arr).unwrap();

        let values = vec![2; 1_100_000];
        let dims = Dimensions::new(vec![Dimension::try_new("x", values.len()).unwrap()]);
        let arr = NdArrowArray::new(Arc::new(Int32Array::from(values)), dims).unwrap();
        writer.append_array(arr).unwrap();

        writer.finish().await.unwrap();

        let reader = ArrayReader::open(store.clone(), path.clone())
            .await
            .unwrap();
        let nd = reader.read_index(1).await.unwrap().unwrap();
        let values = nd.values().as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(values.value(0), 2);
    }

    #[tokio::test]
    async fn array_reader_shared_cache_by_path_and_batch() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let cache = Arc::new(Cache::new(16));

        let path = Path::from("nd/cache_a");
        let mut writer = ArrayWriter::new(store.clone(), path.clone(), DataType::Int32, 0).unwrap();
        let dims = Dimensions::new(vec![Dimension::try_new("x", 2).unwrap()]);
        let arr = NdArrowArray::new(Arc::new(Int32Array::from(vec![10, 11])), dims).unwrap();
        writer.append_array(arr).unwrap();
        writer.finish().await.unwrap();

        let reader1 =
            ArrayReader::open_with_cache(store.clone(), path.clone(), Some(cache.clone()))
                .await
                .unwrap();
        let reader2 =
            ArrayReader::open_with_cache(store.clone(), path.clone(), Some(cache.clone()))
                .await
                .unwrap();

        let nd1 = reader1.read_index(0).await.unwrap().unwrap();
        assert_eq!(nd1.values().len(), 2);
        let key = (Path::from("nd/cache_a/array.arrow").to_string(), 0usize);
        assert!(cache.get(&key).await.is_some());

        let nd2 = reader2.read_index(0).await.unwrap().unwrap();
        assert_eq!(nd2.values().len(), 2);
    }

    #[tokio::test]
    async fn array_reader_cache_separates_paths() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let cache = Arc::new(Cache::new(16));

        let path_a = Path::from("nd/cache_path_a");
        let mut writer =
            ArrayWriter::new(store.clone(), path_a.clone(), DataType::Int32, 0).unwrap();
        let dims = Dimensions::new(vec![Dimension::try_new("x", 1).unwrap()]);
        let arr = NdArrowArray::new(Arc::new(Int32Array::from(vec![1])), dims).unwrap();
        writer.append_array(arr).unwrap();
        writer.finish().await.unwrap();

        let path_b = Path::from("nd/cache_path_b");
        let mut writer =
            ArrayWriter::new(store.clone(), path_b.clone(), DataType::Int32, 0).unwrap();
        let dims = Dimensions::new(vec![Dimension::try_new("x", 1).unwrap()]);
        let arr = NdArrowArray::new(Arc::new(Int32Array::from(vec![2])), dims).unwrap();
        writer.append_array(arr).unwrap();
        writer.finish().await.unwrap();

        let reader_a =
            ArrayReader::open_with_cache(store.clone(), path_a.clone(), Some(cache.clone()))
                .await
                .unwrap();
        let reader_b =
            ArrayReader::open_with_cache(store.clone(), path_b.clone(), Some(cache.clone()))
                .await
                .unwrap();

        reader_a.read_index(0).await.unwrap();
        reader_b.read_index(0).await.unwrap();

        let key_a = (
            Path::from("nd/cache_path_a/array.arrow").to_string(),
            0usize,
        );
        let key_b = (
            Path::from("nd/cache_path_b/array.arrow").to_string(),
            0usize,
        );
        assert!(cache.get(&key_a).await.is_some());
        assert!(cache.get(&key_b).await.is_some());
    }

    #[tokio::test]
    async fn array_reader_loads_pruning_index_lazily() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("nd/pruning-read");
        let mut writer = ArrayWriter::new(store.clone(), path.clone(), DataType::Int32, 0).unwrap();

        let dims = Dimensions::new(vec![Dimension::try_new("x", 2).unwrap()]);
        let arr = NdArrowArray::new(Arc::new(Int32Array::from(vec![1, 2])), dims).unwrap();
        writer.append_array(arr).unwrap();
        writer.finish().await.unwrap();

        let reader = ArrayReader::open(store.clone(), path).await.unwrap();
        let pruning = reader.pruning_index().await.unwrap().unwrap();
        assert_eq!(pruning.batches.len(), 1);
        assert_eq!(
            pruning.min_value(0, 0).unwrap(),
            ScalarValue::Int32(Some(1))
        );
        assert_eq!(
            pruning.max_value(0, 0).unwrap(),
            ScalarValue::Int32(Some(2))
        );
        assert_eq!(pruning.row_count(0, 0).unwrap(), Some(2));
        assert_eq!(pruning.null_count(0, 0).unwrap(), Some(0));
    }

    #[tokio::test]
    async fn array_reader_loads_bloom_index_lazily() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("nd/bloom-read");
        let mut writer = ArrayWriter::new(store.clone(), path.clone(), DataType::Utf8, 0).unwrap();

        let dims = Dimensions::new(vec![Dimension::try_new("x", 2).unwrap()]);
        let arr = NdArrowArray::new(
            Arc::new(arrow::array::StringArray::from(vec![
                Some("alpha"),
                Some("beta"),
            ])),
            dims,
        )
        .unwrap();
        writer.append_array(arr).unwrap();
        writer.finish().await.unwrap();

        let reader = ArrayReader::open(store.clone(), path).await.unwrap();
        let bloom = reader.bloom_index().await.unwrap().unwrap();
        assert_eq!(bloom.batches.len(), 1);
        let filter = bloom.bloom_filter(0, 0).unwrap().unwrap();
        assert!(filter.check(&"alpha".to_string()));
    }

    #[tokio::test]
    async fn array_writer_writes_pruning_index() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("nd/pruning");
        let mut writer = ArrayWriter::new(store.clone(), path.clone(), DataType::Int32, 0).unwrap();

        let dims = Dimensions::new(vec![Dimension::try_new("x", 3).unwrap()]);
        let arr = NdArrowArray::new(Arc::new(Int32Array::from(vec![1, 2, 3])), dims).unwrap();
        writer.append_array(arr).unwrap();
        writer.append_null().unwrap();
        let dims = Dimensions::new(vec![Dimension::try_new("x", 2).unwrap()]);
        let arr = NdArrowArray::new(Arc::new(Int32Array::from(vec![4, 5])), dims).unwrap();
        writer.append_array(arr).unwrap();

        writer.finish().await.unwrap();

        let pruning_path = Path::from("nd/pruning/pruning.arrow");
        let pruning_bytes = store
            .get(&pruning_path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let reader = arrow::ipc::reader::FileReader::try_new(
            std::io::Cursor::new(pruning_bytes.to_vec()),
            None,
        )
        .unwrap();
        let batch = reader.into_iter().next().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 3);
        let mins = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let maxs = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let row_counts = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let null_counts = batch
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(mins.value(0), 1);
        assert_eq!(maxs.value(0), 3);
        assert!(mins.is_null(1));
        assert!(maxs.is_null(1));
        assert_eq!(mins.value(2), 4);
        assert_eq!(maxs.value(2), 5);
        assert_eq!(row_counts.value(0), 3);
        assert_eq!(row_counts.value(1), 0);
        assert_eq!(row_counts.value(2), 2);
        assert_eq!(null_counts.value(0), 0);
        assert_eq!(null_counts.value(1), 0);
        assert_eq!(null_counts.value(2), 0);
    }
}
