use arrow::{array::*, datatypes::*, record_batch::RecordBatch};
use object_store::ObjectStore;
use std::sync::Arc;

use crate::arrow_object_store::ArrowObjectStoreReader;

pub struct StatisticsWriter {
    data_type: DataType,
    schema: Arc<Schema>,
    min_builder: Box<dyn ArrayBuilder>,
    max_builder: Box<dyn ArrayBuilder>,
    null_count_builder: PrimitiveBuilder<UInt64Type>,
    row_count_builder: PrimitiveBuilder<UInt64Type>,
}

/// Reads and materializes per-dataset statistics stored in Arrow IPC format.
pub struct StatisticsReader {
    batch: RecordBatch,
}

impl StatisticsReader {
    /// Loads all statistics batches from object storage and concatenates them.
    pub async fn new<S: ObjectStore>(
        store: S,
        statistics_path: object_store::path::Path,
    ) -> anyhow::Result<Self> {
        let reader = ArrowObjectStoreReader::new(store, statistics_path).await?;

        let mut batches = Vec::new();
        for i in 0..reader.num_batches() {
            match reader.read_batch(i).await? {
                Some(batch) => batches.push(batch),
                None => break,
            }
        }

        anyhow::ensure!(!batches.is_empty(), "statistics file has no record batches");

        let batch = arrow::compute::concat_batches(&reader.schema(), &batches)
            .map_err(|err| anyhow::anyhow!(err))?;

        for required_field in ["min", "max", "null_count", "row_count"] {
            anyhow::ensure!(
                batch.schema().field_with_name(required_field).is_ok(),
                "missing expected field '{}' in statistics schema",
                required_field
            );
        }

        Ok(Self { batch })
    }

    /// Returns the full per-dataset statistics batch.
    pub fn batch(&self) -> &RecordBatch {
        &self.batch
    }
}

impl StatisticsWriter {
    pub fn new(data_type: DataType) -> Self {
        let schema = Arc::new(Schema::new(vec![
            Field::new("min", data_type.clone(), true),
            Field::new("max", data_type.clone(), true),
            Field::new("null_count", DataType::UInt64, false),
            Field::new("row_count", DataType::UInt64, false),
        ]));

        Self {
            min_builder: make_builder(&data_type, 0),
            max_builder: make_builder(&data_type, 0),
            null_count_builder: PrimitiveBuilder::<UInt64Type>::new(),
            row_count_builder: PrimitiveBuilder::<UInt64Type>::new(),
            data_type,
            schema,
        }
    }

    pub fn append(&mut self, array: &ArrayRef) -> anyhow::Result<()> {
        anyhow::ensure!(
            array.data_type() == &self.data_type,
            "statistics datatype mismatch: expected {:?}, got {:?}",
            self.data_type,
            array.data_type()
        );

        match compute_min_scalar(array) {
            Some(min) => append_scalar_to_builder(&mut self.min_builder, &self.data_type, min)?,
            None => append_null_to_builder(&mut self.min_builder, &self.data_type)?,
        }

        match compute_max_scalar(array) {
            Some(max) => append_scalar_to_builder(&mut self.max_builder, &self.data_type, max)?,
            None => append_null_to_builder(&mut self.max_builder, &self.data_type)?,
        }

        self.null_count_builder
            .append_value(array.null_count() as u64);
        self.row_count_builder.append_value(array.len() as u64);

        Ok(())
    }

    pub fn finish(mut self) -> anyhow::Result<RecordBatch> {
        let min_array = self.min_builder.finish();
        let max_array = self.max_builder.finish();
        let null_count_array = Arc::new(self.null_count_builder.finish()) as ArrayRef;
        let row_count_array = Arc::new(self.row_count_builder.finish()) as ArrayRef;

        Ok(RecordBatch::try_new(
            self.schema,
            vec![min_array, max_array, null_count_array, row_count_array],
        )?)
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
        DataType::Float32 => append_null_primitive::<Float32Type>(builder),
        DataType::Float64 => append_null_primitive::<Float64Type>(builder),
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
                .ok_or_else(|| anyhow::anyhow!("failed to downcast string builder"))?;
            string_builder.append_null();
            Ok(())
        }
        _ => Err(anyhow::anyhow!(
            "unsupported data type for statistics writer: {data_type:?}"
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
        .ok_or_else(|| anyhow::anyhow!("failed to downcast primitive builder"))?;
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
        DataType::Float32 => append_primitive::<Float32Type>(builder, scalar),
        DataType::Float64 => append_primitive::<Float64Type>(builder, scalar),
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
                .ok_or_else(|| anyhow::anyhow!("failed to downcast string builder"))?;
            let values = scalar.into_inner();
            let array = values.as_string::<i32>();
            if array.is_null(0) {
                string_builder.append_null();
            } else {
                string_builder.append_value(array.value(0));
            }
            Ok(())
        }
        _ => Err(anyhow::anyhow!(
            "unsupported data type for statistics writer: {data_type:?}"
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
        .ok_or_else(|| anyhow::anyhow!("failed to downcast primitive builder"))?;
    let values = scalar.into_inner();
    let array = values.as_primitive::<T>();
    if array.is_null(0) {
        primitive_builder.append_null();
    } else {
        primitive_builder.append_value(array.value(0));
    }
    Ok(())
}

pub(crate) fn compute_min_scalar(values: &ArrayRef) -> Option<Scalar<ArrayRef>> {
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
            min.map(|s| Scalar::new(Arc::new(StringArray::from(vec![s.to_string()])) as ArrayRef))
        }
        _ => None,
    }
}

pub(crate) fn compute_max_scalar(values: &ArrayRef) -> Option<Scalar<ArrayRef>> {
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
            max.map(|s| Scalar::new(Arc::new(StringArray::from(vec![s.to_string()])) as ArrayRef))
        }
        _ => None,
    }
}

fn scalar_from_min<T: ArrowPrimitiveType>(values: &ArrayRef) -> Option<Scalar<ArrayRef>>
where
    PrimitiveArray<T>: From<Vec<<T as ArrowPrimitiveType>::Native>>,
{
    let array = values.as_primitive::<T>();
    let min = arrow::compute::kernels::aggregate::min(array);
    min.map(|v| Scalar::new(Arc::new(PrimitiveArray::<T>::from(vec![v])) as ArrayRef))
}

fn scalar_from_max<T: ArrowPrimitiveType>(values: &ArrayRef) -> Option<Scalar<ArrayRef>>
where
    PrimitiveArray<T>: From<Vec<<T as ArrowPrimitiveType>::Native>>,
{
    let array = values.as_primitive::<T>();
    let max = arrow::compute::kernels::aggregate::max(array);
    max.map(|v| Scalar::new(Arc::new(PrimitiveArray::<T>::from(vec![v])) as ArrayRef))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{Array, ArrayRef, Float64Array, Int32Array, StringArray, UInt64Array},
        datatypes::DataType,
    };

    use super::StatisticsWriter;

    #[test]
    fn finish_preserves_one_row_per_append() -> anyhow::Result<()> {
        let mut writer = StatisticsWriter::new(DataType::Int32);
        writer.append(&(Arc::new(Int32Array::from(vec![Some(3), Some(8)])) as ArrayRef))?;
        writer.append(&(Arc::new(Int32Array::from(vec![Some(1), None, Some(5)])) as ArrayRef))?;

        let batch = writer.finish()?;
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 4);

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
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let row_count = batch
            .column(3)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();

        assert_eq!(min.value(0), 3);
        assert_eq!(max.value(0), 8);
        assert_eq!(null_count.value(0), 0);
        assert_eq!(row_count.value(0), 2);

        assert_eq!(min.value(1), 1);
        assert_eq!(max.value(1), 5);
        assert_eq!(null_count.value(1), 1);
        assert_eq!(row_count.value(1), 3);

        Ok(())
    }

    #[test]
    fn supports_utf8_statistics() -> anyhow::Result<()> {
        let mut writer = StatisticsWriter::new(DataType::Utf8);
        writer.append(&(Arc::new(StringArray::from(vec![Some("m"), Some("a")])) as ArrayRef))?;

        let batch = writer.finish()?;
        let min = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let max = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let null_count = batch
            .column(2)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let row_count = batch
            .column(3)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();

        assert_eq!(min.value(0), "a");
        assert_eq!(max.value(0), "m");
        assert_eq!(null_count.value(0), 0);
        assert_eq!(row_count.value(0), 2);

        Ok(())
    }

    #[test]
    fn handles_all_null_numeric_array() -> anyhow::Result<()> {
        let mut writer = StatisticsWriter::new(DataType::Float64);
        writer.append(&(Arc::new(Float64Array::from(vec![None, None])) as ArrayRef))?;

        let batch = writer.finish()?;
        let min = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let max = batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let null_count = batch
            .column(2)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let row_count = batch
            .column(3)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();

        assert!(min.is_null(0));
        assert!(max.is_null(0));
        assert_eq!(null_count.value(0), 2);
        assert_eq!(row_count.value(0), 2);

        Ok(())
    }
}
