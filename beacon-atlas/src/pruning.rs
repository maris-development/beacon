use std::fs::File;
use std::sync::Arc;

use anyhow::{Context, anyhow};
use arrow::array::{
    Array, ArrayBuilder, ArrayRef, ArrowPrimitiveType, AsArray, PrimitiveArray, PrimitiveBuilder,
    Scalar, StringArray, StringBuilder, make_builder,
};
use arrow::datatypes::{
    DataType, Int8Type, Int16Type, Int32Type, Int64Type, TimeUnit, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType, UInt8Type, UInt16Type,
    UInt32Type, UInt64Type,
};
use arrow::ipc::writer::FileWriter;
use object_store::ObjectStore;

use crate::consts;

pub struct ChunkStatistics {
    pub min: Option<Scalar<ArrayRef>>,
    pub max: Option<Scalar<ArrayRef>>,
    pub null_count: usize,
    pub row_count: usize,
}

impl ChunkStatistics {
    pub fn is_empty(&self) -> bool {
        self.row_count == 0
    }

    pub fn try_merge(statistics: &[ChunkStatistics]) -> anyhow::Result<ChunkStatistics> {
        if statistics.is_empty() {
            return Ok(ChunkStatistics {
                min: None,
                max: None,
                null_count: 0,
                row_count: 0,
            });
        }

        let mut merged_min: Option<Scalar<ArrayRef>> = None;
        let mut merged_max: Option<Scalar<ArrayRef>> = None;
        let mut null_count: usize = 0;
        let mut row_count: usize = 0;

        let mut min_arrays: Vec<ArrayRef> = Vec::new();
        let mut max_arrays: Vec<ArrayRef> = Vec::new();

        for stat in statistics {
            null_count = null_count
                .checked_add(stat.null_count)
                .ok_or_else(|| anyhow!("null_count overflow"))?;
            row_count = row_count
                .checked_add(stat.row_count)
                .ok_or_else(|| anyhow!("row_count overflow"))?;

            if let Some(candidate_min) = &stat.min {
                let array = candidate_min.clone().into_inner();
                min_arrays.push(array.clone());
            }

            if let Some(candidate_max) = &stat.max {
                let array = candidate_max.clone().into_inner();
                max_arrays.push(array.clone());
            }
        }

        if !min_arrays.is_empty() {
            let concat_inputs: Vec<&dyn arrow::array::Array> =
                min_arrays.iter().map(|array| array.as_ref()).collect();
            let concatenated = arrow::compute::concat(&concat_inputs)
                .map_err(|err| anyhow!(err))
                .context("failed to concatenate min statistics")?;
            merged_min = compute_min_scalar(&concatenated)?;
        }

        if !max_arrays.is_empty() {
            let concat_inputs: Vec<&dyn arrow::array::Array> =
                max_arrays.iter().map(|array| array.as_ref()).collect();
            let concatenated = arrow::compute::concat(&concat_inputs)
                .map_err(|err| anyhow!(err))
                .context("failed to concatenate max statistics")?;
            merged_max = compute_max_scalar(&concatenated)?;
        }

        Ok(ChunkStatistics {
            min: merged_min,
            max: merged_max,
            null_count,
            row_count,
        })
    }
}

pub fn compute_min_scalar(values: &ArrayRef) -> anyhow::Result<Option<Scalar<ArrayRef>>> {
    let data_type = values.data_type();
    match data_type {
        DataType::Int8 => scalar_from_min::<Int8Type>(values),
        DataType::Int16 => scalar_from_min::<Int16Type>(values),
        DataType::Int32 => scalar_from_min::<Int32Type>(values),
        DataType::Int64 => scalar_from_min::<Int64Type>(values),
        DataType::UInt8 => scalar_from_min::<UInt8Type>(values),
        DataType::UInt16 => scalar_from_min::<UInt16Type>(values),
        DataType::UInt32 => scalar_from_min::<UInt32Type>(values),
        DataType::UInt64 => scalar_from_min::<UInt64Type>(values),
        DataType::Float32 => scalar_from_min::<arrow::datatypes::Float32Type>(values),
        DataType::Float64 => scalar_from_min::<arrow::datatypes::Float64Type>(values),
        DataType::Timestamp(unit, _) => match unit {
            TimeUnit::Second => scalar_from_min::<TimestampSecondType>(values),
            TimeUnit::Millisecond => scalar_from_min::<TimestampMillisecondType>(values),
            TimeUnit::Microsecond => scalar_from_min::<TimestampMicrosecondType>(values),
            TimeUnit::Nanosecond => scalar_from_min::<TimestampNanosecondType>(values),
        },
        DataType::Utf8 => {
            let string_values = values.as_string::<i32>();
            let min = arrow::compute::kernels::aggregate::min_string(string_values);
            Ok(min
                .map(|s| Scalar::new(Arc::new(StringArray::from(vec![s.to_string()])) as ArrayRef)))
        }
        _ => Err(anyhow!(
            "unsupported data type for chunk statistics merge: {data_type:?}"
        )),
    }
}

pub fn compute_max_scalar(values: &ArrayRef) -> anyhow::Result<Option<Scalar<ArrayRef>>> {
    let data_type = values.data_type();
    match data_type {
        DataType::Int8 => scalar_from_max::<Int8Type>(values),
        DataType::Int16 => scalar_from_max::<Int16Type>(values),
        DataType::Int32 => scalar_from_max::<Int32Type>(values),
        DataType::Int64 => scalar_from_max::<Int64Type>(values),
        DataType::UInt8 => scalar_from_max::<UInt8Type>(values),
        DataType::UInt16 => scalar_from_max::<UInt16Type>(values),
        DataType::UInt32 => scalar_from_max::<UInt32Type>(values),
        DataType::UInt64 => scalar_from_max::<UInt64Type>(values),
        DataType::Float32 => scalar_from_max::<arrow::datatypes::Float32Type>(values),
        DataType::Float64 => scalar_from_max::<arrow::datatypes::Float64Type>(values),
        DataType::Timestamp(unit, _) => match unit {
            TimeUnit::Second => scalar_from_max::<TimestampSecondType>(values),
            TimeUnit::Millisecond => scalar_from_max::<TimestampMillisecondType>(values),
            TimeUnit::Microsecond => scalar_from_max::<TimestampMicrosecondType>(values),
            TimeUnit::Nanosecond => scalar_from_max::<TimestampNanosecondType>(values),
        },
        DataType::Utf8 => {
            let string_values = values.as_string::<i32>();
            let max = arrow::compute::kernels::aggregate::max_string(string_values);
            Ok(max
                .map(|s| Scalar::new(Arc::new(StringArray::from(vec![s.to_string()])) as ArrayRef)))
        }
        _ => Err(anyhow!(
            "unsupported data type for chunk statistics merge: {data_type:?}"
        )),
    }
}

fn scalar_from_min<T: ArrowPrimitiveType>(
    values: &ArrayRef,
) -> anyhow::Result<Option<Scalar<ArrayRef>>>
where
    PrimitiveArray<T>: From<Vec<<T as ArrowPrimitiveType>::Native>>,
{
    let array = values.as_primitive::<T>();
    let min = arrow::compute::kernels::aggregate::min(&array);
    Ok(min.map(|v| Scalar::new(Arc::new(PrimitiveArray::<T>::from(vec![v])) as ArrayRef)))
}

fn scalar_from_max<T: ArrowPrimitiveType>(
    values: &ArrayRef,
) -> anyhow::Result<Option<Scalar<ArrayRef>>>
where
    PrimitiveArray<T>: From<Vec<<T as ArrowPrimitiveType>::Native>>,
{
    let array = values.as_primitive::<T>();
    let max = arrow::compute::kernels::aggregate::max(array);
    Ok(max.map(|v| Scalar::new(Arc::new(PrimitiveArray::<T>::from(vec![v])) as ArrayRef)))
}

pub struct PruningArrayWriter<S: ObjectStore> {
    store: S,
    path: object_store::path::Path,
    data_type: DataType,
    schema: Arc<arrow::datatypes::Schema>,
    min_builder: Box<dyn ArrayBuilder>,
    max_builder: Box<dyn ArrayBuilder>,
    null_count_builder: PrimitiveBuilder<UInt64Type>,
    row_count_builder: PrimitiveBuilder<UInt64Type>,

    temp_file_writer: FileWriter<File>,
}

/// Reads pruning arrays (min/max/null_count/row_count) from object storage into memory.
pub struct PruningArrayReader {
    batch: arrow::record_batch::RecordBatch,
}

impl PruningArrayReader {
    /// Loads the pruning record batch from an Arrow IPC file.
    ///
    /// # Errors
    /// Returns an error if the object cannot be read or decoded.
    pub async fn new<S: ObjectStore + Send + Sync>(
        store: S,
        path: object_store::path::Path,
    ) -> anyhow::Result<Self> {
        let reader = crate::arrow_object_store::ArrowObjectStoreReader::new(store, path).await?;
        let mut batches = Vec::new();
        for i in 0..reader.num_batches() {
            match reader.read_batch(i).await? {
                Some(batch) => batches.push(batch),
                None => break,
            }
        }
        let batch = arrow::compute::concat_batches(&reader.schema(), &batches)
            .map_err(|err| anyhow!(err))
            .context("failed to concatenate pruning record batches")?;
        Ok(Self { batch })
    }

    /// Returns the full pruning record batch.
    pub fn record_batch(&self) -> &arrow::record_batch::RecordBatch {
        &self.batch
    }

    /// Returns the min values array.
    pub fn min_array(&self) -> anyhow::Result<ArrayRef> {
        Ok(self
            .batch
            .column(self.batch.schema().index_of("min")?)
            .clone())
    }

    /// Returns the max values array.
    pub fn max_array(&self) -> anyhow::Result<ArrayRef> {
        Ok(self
            .batch
            .column(self.batch.schema().index_of("max")?)
            .clone())
    }

    /// Returns the null count array.
    pub fn null_count_array(&self) -> anyhow::Result<ArrayRef> {
        Ok(self
            .batch
            .column(self.batch.schema().index_of("null_count")?)
            .clone())
    }

    /// Returns the row count array.
    pub fn row_count_array(&self) -> anyhow::Result<ArrayRef> {
        Ok(self
            .batch
            .column(self.batch.schema().index_of("row_count")?)
            .clone())
    }
}

impl<S: ObjectStore> PruningArrayWriter<S> {
    pub fn new(store: S, path: object_store::path::Path, data_type: DataType) -> Self {
        let fields = vec![
            arrow::datatypes::Field::new("min", data_type.clone(), true),
            arrow::datatypes::Field::new("max", data_type.clone(), true),
            arrow::datatypes::Field::new("null_count", DataType::UInt64, false),
            arrow::datatypes::Field::new("row_count", DataType::UInt64, false),
        ];
        let schema = Arc::new(arrow::datatypes::Schema::new(fields));
        let temp_file = tempfile::tempfile().expect("failed to create temporary file");
        let temp_file_writer =
            FileWriter::try_new(temp_file, &schema).expect("failed to create file writer");
        Self {
            store,
            path,
            data_type: data_type.clone(),
            schema,
            min_builder: Box::new(make_builder(&data_type, 0)),
            max_builder: Box::new(make_builder(&data_type, 0)),
            null_count_builder: PrimitiveBuilder::<UInt64Type>::new(),
            row_count_builder: PrimitiveBuilder::<UInt64Type>::new(),

            temp_file_writer,
        }
    }

    pub fn append(&mut self, stats: Option<ChunkStatistics>) -> anyhow::Result<()> {
        if let Some(stat) = stats {
            match stat.min {
                Some(min) => append_scalar_to_builder(&mut self.min_builder, &self.data_type, min)?,
                None => append_null_to_builder(&mut self.min_builder, &self.data_type)?,
            }

            match stat.max {
                Some(max) => append_scalar_to_builder(&mut self.max_builder, &self.data_type, max)?,
                None => append_null_to_builder(&mut self.max_builder, &self.data_type)?,
            }

            self.null_count_builder.append_value(stat.null_count as u64);
            self.row_count_builder.append_value(stat.row_count as u64);
        } else {
            append_null_to_builder(&mut self.min_builder, &self.data_type)?;
            append_null_to_builder(&mut self.max_builder, &self.data_type)?;
            self.null_count_builder.append_value(0);
            self.row_count_builder.append_value(0);
        }
        Ok(())
    }

    pub async fn finish(mut self) -> anyhow::Result<()> {
        let min_array = self.min_builder.finish();
        let max_array = self.max_builder.finish();
        let null_count_array = Arc::new(self.null_count_builder.finish()) as ArrayRef;
        let row_count_array = Arc::new(self.row_count_builder.finish()) as ArrayRef;

        let record_batch = arrow::record_batch::RecordBatch::try_new(
            self.schema.clone(),
            vec![min_array, max_array, null_count_array, row_count_array],
        )
        .map_err(|err| anyhow!(err))
        .context("failed to create pruning record batch")?;

        self.temp_file_writer
            .write(&record_batch)
            .map_err(|err| anyhow!(err))
            .context("failed to write pruning record batch")?;
        self.temp_file_writer
            .finish()
            .map_err(|err| anyhow!(err))
            .context("failed to finish writing pruning record batch")?;

        let mut temp_file = self
            .temp_file_writer
            .into_inner()
            .map_err(|err| anyhow!(err))
            .context("failed to get inner temporary file")?;
        crate::util::stream_file_to_store(
            &self.store,
            &self.path,
            &mut temp_file,
            consts::STREAM_CHUNK_SIZE,
        )
        .await?;

        Ok(())
    }
}

fn append_null_to_builder(
    builder: &mut Box<dyn ArrayBuilder>,
    data_type: &DataType,
) -> anyhow::Result<()> {
    match data_type {
        DataType::Int8 => append_null_primitive::<Int8Type>(builder),
        DataType::Int16 => append_null_primitive::<Int16Type>(builder),
        DataType::Int32 => append_null_primitive::<Int32Type>(builder),
        DataType::Int64 => append_null_primitive::<Int64Type>(builder),
        DataType::UInt8 => append_null_primitive::<UInt8Type>(builder),
        DataType::UInt16 => append_null_primitive::<UInt16Type>(builder),
        DataType::UInt32 => append_null_primitive::<UInt32Type>(builder),
        DataType::UInt64 => append_null_primitive::<UInt64Type>(builder),
        DataType::Float32 => append_null_primitive::<arrow::datatypes::Float32Type>(builder),
        DataType::Float64 => append_null_primitive::<arrow::datatypes::Float64Type>(builder),
        DataType::Timestamp(unit, _) => match unit {
            TimeUnit::Second => append_null_primitive::<TimestampSecondType>(builder),
            TimeUnit::Millisecond => append_null_primitive::<TimestampMillisecondType>(builder),
            TimeUnit::Microsecond => append_null_primitive::<TimestampMicrosecondType>(builder),
            TimeUnit::Nanosecond => append_null_primitive::<TimestampNanosecondType>(builder),
        },
        DataType::Utf8 => {
            let string_builder = builder
                .as_any_mut()
                .downcast_mut::<StringBuilder>()
                .ok_or_else(|| anyhow!("failed to downcast string builder"))?;
            string_builder.append_null();
            Ok(())
        }
        _ => Err(anyhow!(
            "unsupported data type for pruning builder: {data_type:?}"
        )),
    }
}

fn append_null_primitive<T: ArrowPrimitiveType>(
    builder: &mut Box<dyn ArrayBuilder>,
) -> anyhow::Result<()>
where
    PrimitiveArray<T>: From<Vec<<T as ArrowPrimitiveType>::Native>>,
{
    let primitive_builder = builder
        .as_any_mut()
        .downcast_mut::<PrimitiveBuilder<T>>()
        .ok_or_else(|| anyhow!("failed to downcast primitive builder"))?;
    primitive_builder.append_null();
    Ok(())
}

fn append_scalar_to_builder(
    builder: &mut Box<dyn ArrayBuilder>,
    data_type: &DataType,
    scalar: Scalar<ArrayRef>,
) -> anyhow::Result<()> {
    match data_type {
        DataType::Int8 => append_primitive::<Int8Type>(builder, scalar),
        DataType::Int16 => append_primitive::<Int16Type>(builder, scalar),
        DataType::Int32 => append_primitive::<Int32Type>(builder, scalar),
        DataType::Int64 => append_primitive::<Int64Type>(builder, scalar),
        DataType::UInt8 => append_primitive::<UInt8Type>(builder, scalar),
        DataType::UInt16 => append_primitive::<UInt16Type>(builder, scalar),
        DataType::UInt32 => append_primitive::<UInt32Type>(builder, scalar),
        DataType::UInt64 => append_primitive::<UInt64Type>(builder, scalar),
        DataType::Float32 => append_primitive::<arrow::datatypes::Float32Type>(builder, scalar),
        DataType::Float64 => append_primitive::<arrow::datatypes::Float64Type>(builder, scalar),
        DataType::Timestamp(unit, _) => match unit {
            TimeUnit::Second => append_primitive::<TimestampSecondType>(builder, scalar),
            TimeUnit::Millisecond => append_primitive::<TimestampMillisecondType>(builder, scalar),
            TimeUnit::Microsecond => append_primitive::<TimestampMicrosecondType>(builder, scalar),
            TimeUnit::Nanosecond => append_primitive::<TimestampNanosecondType>(builder, scalar),
        },
        DataType::Utf8 => {
            let string_builder = builder
                .as_any_mut()
                .downcast_mut::<StringBuilder>()
                .ok_or_else(|| anyhow!("failed to downcast string builder"))?;
            let values = scalar.into_inner();
            let array = values.as_string::<i32>();
            if array.is_null(0) {
                string_builder.append_null();
            } else {
                string_builder.append_value(array.value(0));
            }
            Ok(())
        }
        _ => Err(anyhow!(
            "unsupported data type for pruning builder: {data_type:?}"
        )),
    }
}

fn append_primitive<T: ArrowPrimitiveType>(
    builder: &mut Box<dyn ArrayBuilder>,
    scalar: Scalar<ArrayRef>,
) -> anyhow::Result<()>
where
    PrimitiveArray<T>: From<Vec<<T as ArrowPrimitiveType>::Native>>,
{
    let primitive_builder = builder
        .as_any_mut()
        .downcast_mut::<PrimitiveBuilder<T>>()
        .ok_or_else(|| anyhow!("failed to downcast primitive builder"))?;
    let values = scalar.into_inner();
    let array = values.as_primitive::<T>();
    if array.is_null(0) {
        primitive_builder.append_null();
    } else {
        primitive_builder.append_value(array.value(0));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{ChunkStatistics, PruningArrayReader, PruningArrayWriter};
    use anyhow::anyhow;
    use arrow::array::{Array, ArrayRef, Int32Array, Scalar, StringArray};
    use arrow::datatypes::DataType;
    use arrow::ipc::reader::FileReader;
    use object_store::{ObjectStore, memory::InMemory, path::Path};
    use std::sync::Arc;

    fn scalar_i32(value: i32) -> Scalar<ArrayRef> {
        Scalar::new(Arc::new(Int32Array::from(vec![value])) as ArrayRef)
    }

    fn scalar_str(value: &str) -> Scalar<ArrayRef> {
        Scalar::new(Arc::new(StringArray::from(vec![value])) as ArrayRef)
    }

    #[test]
    fn merge_empty_returns_zeroed() -> anyhow::Result<()> {
        let merged = ChunkStatistics::try_merge(&[])?;
        assert_eq!(merged.row_count, 0);
        assert_eq!(merged.null_count, 0);
        assert!(merged.min.is_none());
        assert!(merged.max.is_none());
        Ok(())
    }

    #[test]
    fn merge_int32_statistics() -> anyhow::Result<()> {
        let stats = vec![
            ChunkStatistics {
                min: Some(scalar_i32(2)),
                max: Some(scalar_i32(10)),
                null_count: 1,
                row_count: 4,
            },
            ChunkStatistics {
                min: Some(scalar_i32(-1)),
                max: Some(scalar_i32(7)),
                null_count: 0,
                row_count: 3,
            },
        ];

        let merged = ChunkStatistics::try_merge(&stats)?;
        assert_eq!(merged.row_count, 7);
        assert_eq!(merged.null_count, 1);
        let min = merged.min.expect("min").into_inner();
        let max = merged.max.expect("max").into_inner();
        let min_values = min.as_any().downcast_ref::<Int32Array>().unwrap();
        let max_values = max.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(min_values.value(0), -1);
        assert_eq!(max_values.value(0), 10);
        Ok(())
    }

    #[test]
    fn merge_string_statistics() -> anyhow::Result<()> {
        let stats = vec![
            ChunkStatistics {
                min: Some(scalar_str("b")),
                max: Some(scalar_str("z")),
                null_count: 2,
                row_count: 5,
            },
            ChunkStatistics {
                min: Some(scalar_str("a")),
                max: Some(scalar_str("m")),
                null_count: 0,
                row_count: 2,
            },
        ];

        let merged = ChunkStatistics::try_merge(&stats)?;
        assert_eq!(merged.row_count, 7);
        assert_eq!(merged.null_count, 2);
        let min = merged.min.expect("min").into_inner();
        let max = merged.max.expect("max").into_inner();
        let min_values = min.as_any().downcast_ref::<StringArray>().unwrap();
        let max_values = max.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(min_values.value(0), "a");
        assert_eq!(max_values.value(0), "z");
        Ok(())
    }

    #[test]
    fn pruning_builder_appends_int32_stats() -> anyhow::Result<()> {
        let store = InMemory::new();
        let path = Path::from("pruning");
        let mut builder = PruningArrayWriter::new(store, path, DataType::Int32);

        builder.append(Some(ChunkStatistics {
            min: Some(scalar_i32(1)),
            max: Some(scalar_i32(5)),
            null_count: 0,
            row_count: 2,
        }))?;
        builder.append(None)?;

        let min_array = builder.min_builder.finish();
        let max_array = builder.max_builder.finish();
        let min_values = min_array.as_any().downcast_ref::<Int32Array>().unwrap();
        let max_values = max_array.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(min_values.value(0), 1);
        assert!(min_values.is_null(1));
        assert_eq!(max_values.value(0), 5);
        assert!(max_values.is_null(1));
        Ok(())
    }

    #[test]
    fn pruning_builder_appends_utf8_stats() -> anyhow::Result<()> {
        let store = InMemory::new();
        let path = Path::from("pruning");
        let mut builder = PruningArrayWriter::new(store, path, DataType::Utf8);

        builder.append(Some(ChunkStatistics {
            min: Some(scalar_str("a")),
            max: Some(scalar_str("z")),
            null_count: 1,
            row_count: 3,
        }))?;
        builder.append(None)?;

        let min_array = builder.min_builder.finish();
        let max_array = builder.max_builder.finish();
        let min_values = min_array.as_any().downcast_ref::<StringArray>().unwrap();
        let max_values = max_array.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(min_values.value(0), "a");
        assert!(min_values.is_null(1));
        assert_eq!(max_values.value(0), "z");
        assert!(max_values.is_null(1));
        Ok(())
    }

    #[tokio::test]
    async fn pruning_writer_finishes_and_uploads() -> anyhow::Result<()> {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("pruning.arrow");
        let mut writer = PruningArrayWriter::new(store.clone(), path.clone(), DataType::Int32);

        writer.append(Some(ChunkStatistics {
            min: Some(scalar_i32(3)),
            max: Some(scalar_i32(8)),
            null_count: 1,
            row_count: 2,
        }))?;
        writer.append(None)?;

        writer.finish().await?;

        let bytes = store
            .get(&path)
            .await
            .map_err(|err| anyhow!(err))?
            .bytes()
            .await
            .map_err(|err| anyhow!(err))?;
        let reader = FileReader::try_new(std::io::Cursor::new(bytes.to_vec()), None)?;
        let rows: usize = reader.map(|batch| batch.unwrap().num_rows()).sum();
        assert_eq!(rows, 2);
        Ok(())
    }

    #[tokio::test]
    async fn pruning_reader_loads_arrays() -> anyhow::Result<()> {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("pruning-read.arrow");
        let mut writer = PruningArrayWriter::new(store.clone(), path.clone(), DataType::Int32);

        writer.append(Some(ChunkStatistics {
            min: Some(scalar_i32(1)),
            max: Some(scalar_i32(9)),
            null_count: 2,
            row_count: 5,
        }))?;
        writer.append(None)?;
        writer.finish().await?;

        let reader = PruningArrayReader::new(store, path).await?;
        let min_array = reader.min_array()?;
        let max_array = reader.max_array()?;
        let null_counts = reader.null_count_array()?;
        let row_counts = reader.row_count_array()?;

        let min_values = min_array.as_any().downcast_ref::<Int32Array>().unwrap();
        let max_values = max_array.as_any().downcast_ref::<Int32Array>().unwrap();
        let null_values = null_counts
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .unwrap();
        let row_values = row_counts
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .unwrap();

        assert_eq!(min_values.value(0), 1);
        assert!(min_values.is_null(1));
        assert_eq!(max_values.value(0), 9);
        assert!(max_values.is_null(1));
        assert_eq!(null_values.value(0), 2);
        assert_eq!(row_values.value(0), 5);
        Ok(())
    }
}
