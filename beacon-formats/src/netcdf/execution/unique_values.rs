//! Execution plan utilities that capture the unique values seen for selected
//! columns while still forwarding the original record batches untouched.
//!
//! NetCDF exports need to know the list of distinct station identifiers,
//! depth levels, etc. before files are written. The `UniqueValuesExec`
//! execution plan wraps any upstream plan, sorts the requested columns so
//! uniqueness can be computed incrementally, and registers a handle per
//! partition that downstream consumers can inspect once execution finishes.
//! Each stream is wrapped by [`UniqueValueStream`], which mirrors the input
//! batches and populates the per-column collectors stored inside a
//! [`UniqueValuesHandle`].
//!
//! The end result is a lightweight side channel of unique values that can be
//! consumed after a query completes without buffering the full result set.
use std::{
    any::Any,
    cmp::Ordering,
    fmt::Debug,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use arrow::{
    array::{Array, ArrayRef, RecordBatch},
    datatypes::{DataType, Field, FieldRef, Schema, SchemaRef, TimeUnit},
};
use datafusion::{
    common::{HashMap, Statistics},
    error::{DataFusionError, Result},
    execution::{RecordBatchStream, SendableRecordBatchStream},
    physical_expr::{LexOrdering, PhysicalSortExpr},
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, expressions::col, sorts::sort::SortExec,
    },
};
use futures::{Stream, StreamExt};
use indexmap::IndexMap;
use ordered_float::OrderedFloat;
use parking_lot::Mutex;

/// DataFusion execution plan that sorts the required columns, mirrors its
/// child output, and records unique values per partition via shared handles.
///
/// When `execute` is called for a partition, the plan creates a
/// [`UniqueValuesHandle`] that owns collectors for the requested columns,
/// wraps the child stream in a [`UniqueValueStream`], and registers that handle
/// in the shared [`UniqueValuesHandleCollection`]. Downstream consumers (such
/// as the NetCDF sink) can inspect the handle collection after the query runs
/// to obtain per-partition distinct lists without re-scanning the data.
#[derive(Debug)]
pub struct UniqueValuesExec {
    input: Arc<dyn ExecutionPlan>,
    schema: Arc<Schema>,
    columns_to_track: Vec<usize>,
    handle_collection: UniqueValuesHandleCollection,
}

impl UniqueValuesExec {
    /// Creates a new execution plan that tracks the provided column names.
    ///
    /// The child plan is wrapped in a [`SortExec`] that enforces a lexical
    /// ordering on the tracked columns so incremental unique-value collection
    /// remains efficient. The returned tuple also includes the
    /// [`UniqueValuesHandleCollection`] where per-partition handles will be
    /// registered once `execute` is invoked.
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        columns_to_track: Vec<String>,
    ) -> datafusion::error::Result<(Self, UniqueValuesHandleCollection)> {
        let schema = input.schema();
        let mut columns_to_track_indices = Vec::with_capacity(columns_to_track.len());
        let mut sort_expr = Vec::with_capacity(columns_to_track.len());
        for col_name in columns_to_track {
            let index = schema.index_of(&col_name).map_err(|_| {
                DataFusionError::Plan(format!(
                    "Column '{col_name}' not found in input schema for UniqueValuesExec"
                ))
            })?;

            sort_expr.push(PhysicalSortExpr::new_default(col(&col_name, &schema)?));
            columns_to_track_indices.push(index);
        }

        let lex_ordering = LexOrdering::new(sort_expr);

        if let Some(lex) = lex_ordering {
            let sorted_input = Arc::new(SortExec::new(lex, input.clone()));
            let handle_collection = UniqueValuesHandleCollection::new();
            let exec = Self {
                input: sorted_input,
                schema,
                columns_to_track: columns_to_track_indices,
                handle_collection: handle_collection.clone(),
            };
            Ok((exec, handle_collection))
        } else {
            Err(DataFusionError::Plan(
                "UniqueValuesExec requires at least one column to track".to_string(),
            ))
        }
    }
}

impl DisplayAs for UniqueValuesExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "UniqueValuesExec")
    }
}

impl ExecutionPlan for UniqueValuesExec {
    fn name(&self) -> &str {
        "UniqueValuesExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &datafusion::physical_plan::PlanProperties {
        self.input.properties()
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn statistics(&self) -> Result<Statistics> {
        Ok(Statistics::default())
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(
                "UniqueValuesExec expects exactly one child".to_string(),
            ));
        }

        Ok(Arc::new(Self {
            input: children[0].clone(),
            schema: self.schema.clone(),
            columns_to_track: self.columns_to_track.clone(),
            handle_collection: self.handle_collection.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // Each partition receives its own handle so unique values stay scoped
        // to the input slice and can later be merged or inspected independently.
        let input_stream = self.input.execute(partition, context)?;

        let tracked_fields: Vec<FieldRef> = self
            .columns_to_track
            .iter()
            .map(|&idx| Arc::new(self.schema.field(idx).clone()))
            .collect();

        let handle = Arc::new(UniqueValuesHandle::try_new(&tracked_fields)?);
        self.handle_collection.register_handle(handle.clone());

        Ok(Box::pin(UniqueValueStream::new(
            input_stream,
            handle,
            self.columns_to_track.clone(),
        )))
    }
}

/// Thread-safe registry that keeps track of all handles created for each
/// executed partition. Consumers typically call [`Self::handles`] after the
/// plan finishes to aggregate the collected values.
#[derive(Debug, Clone, Default)]
pub struct UniqueValuesHandleCollection {
    handles: Arc<parking_lot::Mutex<Vec<Arc<UniqueValuesHandle>>>>,
}

impl UniqueValuesHandleCollection {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register_handle(&self, handle: Arc<UniqueValuesHandle>) {
        let mut guard = self.handles.lock();
        guard.push(handle);
    }

    pub fn handles(&self) -> Vec<Arc<UniqueValuesHandle>> {
        let guard = self.handles.lock();
        guard.clone()
    }

    pub fn len(&self) -> usize {
        self.handles.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Merges the unique values collected across all registered handles into a single map grouped by column(field).
    pub fn unique_values(&self) -> Option<ColumnValueMap> {
        let handles = self.handles();
        if handles.is_empty() {
            return None;
        }

        let mut merged: ColumnValueMap = ColumnValueMap::new();

        for handle in handles {
            let guard = handle.unique_column_values.lock();
            for (field, source_values) in guard.iter() {
                let field_ref = field.clone();
                let target = match merged.entry(field_ref.clone()) {
                    indexmap::map::Entry::Occupied(entry) => entry.into_mut(),
                    indexmap::map::Entry::Vacant(entry) => entry.insert(
                        UniqueValuesHandle::init_column_values(field_ref.clone())
                            .expect("unsupported data type for unique value tracking"),
                    ),
                };
                merge_column_value_sets(&field_ref, target, source_values);
            }
        }

        Some(merged)
    }
}

fn merge_column_value_sets(
    field: &FieldRef,
    target: &mut ErasedColumnValues,
    source: &ErasedColumnValues,
) {
    let field_name = field.name().clone();

    macro_rules! merge_values {
        ($ty:ty) => {{
            let target_column = target
                .as_mut()
                .as_mut()
                .downcast_mut::<UniqueColumnValues<$ty>>()
                .unwrap_or_else(|| panic!("Unexpected unique value type for column {field_name}"));
            let source_column = source
                .as_any()
                .downcast_ref::<UniqueColumnValues<$ty>>()
                .unwrap_or_else(|| panic!("Unexpected unique value type for column {field_name}"));
            target_column.compare_and_add(source_column.values.iter().cloned());
        }};
    }

    match field.data_type() {
        DataType::Utf8 | DataType::LargeUtf8 => merge_values!(String),
        DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(_) => {
            merge_values!(Vec<u8>)
        }
        DataType::Boolean => merge_values!(bool),
        DataType::Int8 => merge_values!(i8),
        DataType::Int16 => merge_values!(i16),
        DataType::Int32 | DataType::Date32 | DataType::Time32(_) => merge_values!(i32),
        DataType::Int64
        | DataType::Date64
        | DataType::Time64(_)
        | DataType::Timestamp(_, _)
        | DataType::Duration(_)
        | DataType::Interval(_) => merge_values!(i64),
        DataType::UInt8 => merge_values!(u8),
        DataType::UInt16 => merge_values!(u16),
        DataType::UInt32 => merge_values!(u32),
        DataType::UInt64 => merge_values!(u64),
        DataType::Float32 => merge_values!(OrderedFloat<f32>),
        DataType::Float64 => merge_values!(OrderedFloat<f64>),
        DataType::Decimal128(_, _) => merge_values!(i128),
        other => panic!("Unsupported data type for unique value merge: {:?}", other),
    }
}

pub type ErasedColumnValues = Box<dyn UniqueVec + Send + Sync>;
pub type ColumnValueMap = IndexMap<FieldRef, ErasedColumnValues>;

/// Shared state that keeps track of the unique values discovered for each column.
///
/// The handle is wrapped in an [`Arc`] and guarded by a [`Mutex`] so it can be
/// updated from multiple execution streams while batches are flowing towards the
/// NetCDF sink.
#[derive(Debug)]
pub struct UniqueValuesHandle {
    pub unique_column_values: Mutex<ColumnValueMap>,
}

impl UniqueValuesHandle {
    /// Creates a new handle for the provided set of fields.
    ///
    /// Panics if a field uses a data type that is not currently supported. Use
    /// [`Self::try_new`] if you want to handle that error explicitly.
    pub fn new(fields: &[FieldRef]) -> Self {
        Self::try_new(fields).expect("unsupported data type for unique value tracking")
    }

    /// Fallible constructor that initializes the per-column collectors based on
    /// the Arrow data types present in `fields`.
    pub fn try_new(fields: &[FieldRef]) -> Result<Self> {
        let mut unique_column_values = ColumnValueMap::with_capacity(fields.len());
        for field in fields {
            unique_column_values.insert(field.clone(), Self::init_column_values(field.clone())?);
        }

        Ok(Self {
            unique_column_values: Mutex::new(unique_column_values),
        })
    }

    /// Convenience helper for callers that already have a schema reference.
    pub fn from_schema(schema: SchemaRef) -> Result<Self> {
        let fields: Vec<FieldRef> = schema.fields().iter().cloned().collect();
        Self::try_new(&fields)
    }

    fn init_column_values(field: FieldRef) -> Result<ErasedColumnValues> {
        match field.data_type() {
            DataType::Utf8 | DataType::LargeUtf8 => Ok(Box::new(UniqueColumnValues::<String> {
                values: Vec::new(),
            })),
            DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(_) => {
                Ok(Box::new(UniqueColumnValues::<Vec<u8>> {
                    values: Vec::new(),
                }))
            }
            DataType::Boolean => Ok(Box::new(UniqueColumnValues::<bool> { values: Vec::new() })),
            DataType::Int8 => Ok(Box::new(UniqueColumnValues::<i8> { values: Vec::new() })),
            DataType::Int16 => Ok(Box::new(UniqueColumnValues::<i16> { values: Vec::new() })),
            DataType::Int32 | DataType::Date32 | DataType::Time32(_) => {
                Ok(Box::new(UniqueColumnValues::<i32> { values: Vec::new() }))
            }
            DataType::Int64
            | DataType::Date64
            | DataType::Time64(_)
            | DataType::Timestamp(_, _)
            | DataType::Duration(_)
            | DataType::Interval(_) => {
                Ok(Box::new(UniqueColumnValues::<i64> { values: Vec::new() }))
            }
            DataType::UInt8 => Ok(Box::new(UniqueColumnValues::<u8> { values: Vec::new() })),
            DataType::UInt16 => Ok(Box::new(UniqueColumnValues::<u16> { values: Vec::new() })),
            DataType::UInt32 => Ok(Box::new(UniqueColumnValues::<u32> { values: Vec::new() })),
            DataType::UInt64 => Ok(Box::new(UniqueColumnValues::<u64> { values: Vec::new() })),
            DataType::Float32 => Ok(Box::new(UniqueColumnValues::<OrderedFloat<f32>> {
                values: Vec::new(),
            })),
            DataType::Float64 => Ok(Box::new(UniqueColumnValues::<OrderedFloat<f64>> {
                values: Vec::new(),
            })),
            DataType::Decimal128(_, _) => {
                Ok(Box::new(UniqueColumnValues::<i128> { values: Vec::new() }))
            }
            other => Err(DataFusionError::NotImplemented(format!(
                "Unsupported data type for unique value tracking: {:?}",
                other
            ))),
        }
    }
}

pub trait UniqueVec: Any + Debug {
    fn len(&self) -> usize;
    fn as_any(&self) -> &dyn Any;
    fn as_mut(&mut self) -> &mut dyn Any;
}

/// Sorted container that stores the unique values for a single column.
#[derive(Debug)]
pub(crate) struct UniqueColumnValues<T: Ord + Eq + Send + Sync + 'static> {
    pub values: Vec<T>,
}

impl<T: Ord + Eq + Send + Sync + Debug + 'static> UniqueVec for UniqueColumnValues<T> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_mut(&mut self) -> &mut dyn Any {
        self
    }
    fn len(&self) -> usize {
        self.values.len()
    }
}

impl<T: Ord + Eq + Send + Sync + 'static> UniqueColumnValues<T> {
    pub fn needle_position(&self, needle: &T) -> Result<usize> {
        match self.values.binary_search(needle) {
            Ok(pos) => Ok(pos),
            Err(pos) => Ok(pos),
        }
    }

    /// Inserts values that are not already present while maintaining sorted
    /// order. The implementation sorts the incoming values and performs a
    /// classic merge with the existing sorted set.
    pub fn compare_and_add<I: Iterator<Item = T>>(&mut self, values: I) {
        let mut incoming_values: Vec<T> = values.collect();
        if incoming_values.is_empty() {
            return;
        }

        incoming_values.sort();
        incoming_values.dedup();

        if self.values.is_empty() {
            self.values = incoming_values;
            return;
        }

        let existing_values = std::mem::take(&mut self.values);
        let existing_len = existing_values.len();
        let incoming_len = incoming_values.len();
        let mut existing = existing_values.into_iter().peekable();
        let mut incoming = incoming_values.into_iter().peekable();
        let mut merged = Vec::with_capacity(existing_len + incoming_len);

        loop {
            match (existing.peek(), incoming.peek()) {
                (Some(_), Some(_)) => {
                    match existing.peek().unwrap().cmp(incoming.peek().unwrap()) {
                        Ordering::Less => merged.push(existing.next().unwrap()),
                        Ordering::Greater => merged.push(incoming.next().unwrap()),
                        Ordering::Equal => {
                            merged.push(existing.next().unwrap());
                            incoming.next();
                        }
                    }
                }
                (Some(_), None) => {
                    merged.push(existing.next().unwrap());
                    merged.extend(existing);
                    break;
                }
                (None, Some(_)) => {
                    merged.push(incoming.next().unwrap());
                    merged.extend(incoming);
                    break;
                }
                (None, None) => break,
            }
        }

        self.values = merged;
    }
}

/// Stream adapter that records the unique values for the requested columns while
/// forwarding all batches downstream unchanged.
pub struct UniqueValueStream {
    pub input: SendableRecordBatchStream,
    pub unique_values_handle: Arc<UniqueValuesHandle>,
    pub unique_value_columns_projection: Vec<usize>,
}

impl UniqueValueStream {
    /// Creates a new stream adapter.
    ///
    /// * `input` - upstream record batch stream.
    /// * `unique_values` - shared handle that stores the collected values.
    /// * `unique_value_columns_projection` - zero-based column indices to track.
    pub fn new(
        input: SendableRecordBatchStream,
        unique_values: Arc<UniqueValuesHandle>,
        unique_value_columns_projection: Vec<usize>,
    ) -> Self {
        Self {
            input,
            unique_values_handle: unique_values,
            unique_value_columns_projection,
        }
    }

    fn capture_unique_values(
        batch: &RecordBatch,
        projection: &[usize],
        unique_values: &mut ColumnValueMap,
    ) -> Result<()> {
        let schema = batch.schema();
        for &col_idx in projection {
            let field = schema.field(col_idx);
            let field_name = field.name().clone();
            let (_, column_values_any) = unique_values.get_index_mut(col_idx).ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "Unique values handle missing column {field_name}"
                ))
            })?;
            Self::collect_column_values(field, batch.column(col_idx).clone(), column_values_any)?;
        }
        Ok(())
    }

    fn collect_column_values(
        field: &Field,
        array: ArrayRef,
        column_values_any: &mut ErasedColumnValues,
    ) -> Result<()> {
        let field_name = field.name().clone();

        macro_rules! handle_primitive {
            ($array_ty:ty, $value_ty:ty, $map:expr) => {{
                let column_values = column_values_any
                    .as_mut()
                    .as_mut()
                    .downcast_mut::<UniqueColumnValues<$value_ty>>()
                    .ok_or_else(|| unexpected_unique_value_type(&field_name))?;
                let typed_array = array
                    .as_any()
                    .downcast_ref::<$array_ty>()
                    .ok_or_else(|| unexpected_array_type(&field_name))?;
                let values_iter = typed_array.iter().flatten().map($map);
                column_values.compare_and_add(values_iter);
                Ok(())
            }};
        }

        macro_rules! handle_string {
            ($array_ty:ty) => {{
                let column_values = column_values_any
                    .as_mut()
                    .as_mut()
                    .downcast_mut::<UniqueColumnValues<String>>()
                    .ok_or_else(|| unexpected_unique_value_type(&field_name))?;
                let typed_array = array
                    .as_any()
                    .downcast_ref::<$array_ty>()
                    .ok_or_else(|| unexpected_array_type(&field_name))?;
                let values_iter = typed_array.iter().flatten().map(|value| value.to_string());
                column_values.compare_and_add(values_iter);
                Ok(())
            }};
        }

        macro_rules! handle_binary {
            ($array_ty:ty) => {{
                let column_values = column_values_any
                    .as_mut()
                    .as_mut()
                    .downcast_mut::<UniqueColumnValues<Vec<u8>>>()
                    .ok_or_else(|| unexpected_unique_value_type(&field_name))?;
                let typed_array = array
                    .as_any()
                    .downcast_ref::<$array_ty>()
                    .ok_or_else(|| unexpected_array_type(&field_name))?;
                let values_iter = typed_array.iter().flatten().map(|value| value.to_vec());
                column_values.compare_and_add(values_iter);
                Ok(())
            }};
        }

        match field.data_type() {
            DataType::Utf8 => handle_string!(arrow::array::StringArray),
            DataType::LargeUtf8 => handle_string!(arrow::array::LargeStringArray),
            DataType::Binary => handle_binary!(arrow::array::BinaryArray),
            DataType::LargeBinary => handle_binary!(arrow::array::LargeBinaryArray),
            DataType::FixedSizeBinary(_) => handle_binary!(arrow::array::FixedSizeBinaryArray),
            DataType::Boolean => handle_primitive!(arrow::array::BooleanArray, bool, |v| v),
            DataType::Int8 => handle_primitive!(arrow::array::Int8Array, i8, |v| v),
            DataType::Int16 => handle_primitive!(arrow::array::Int16Array, i16, |v| v),
            DataType::Int32 => handle_primitive!(arrow::array::Int32Array, i32, |v| v),
            DataType::Int64 => handle_primitive!(arrow::array::Int64Array, i64, |v| v),
            DataType::UInt8 => handle_primitive!(arrow::array::UInt8Array, u8, |v| v),
            DataType::UInt16 => handle_primitive!(arrow::array::UInt16Array, u16, |v| v),
            DataType::UInt32 => handle_primitive!(arrow::array::UInt32Array, u32, |v| v),
            DataType::UInt64 => handle_primitive!(arrow::array::UInt64Array, u64, |v| v),
            DataType::Float32 => {
                handle_primitive!(arrow::array::Float32Array, OrderedFloat<f32>, |v| {
                    OrderedFloat::<f32>(v)
                })
            }
            DataType::Float64 => {
                handle_primitive!(arrow::array::Float64Array, OrderedFloat<f64>, |v| {
                    OrderedFloat::<f64>(v)
                })
            }
            DataType::Date32 => handle_primitive!(arrow::array::Date32Array, i32, |v| v),
            DataType::Date64 => handle_primitive!(arrow::array::Date64Array, i64, |v| v),
            DataType::Time32(unit) => match unit {
                TimeUnit::Second => handle_primitive!(arrow::array::Time32SecondArray, i32, |v| v),
                TimeUnit::Millisecond => {
                    handle_primitive!(arrow::array::Time32MillisecondArray, i32, |v| v)
                }
                other => Err(DataFusionError::NotImplemented(format!(
                    "Unique value tracking not implemented for Time32 unit {:?}",
                    other
                ))),
            },
            DataType::Time64(unit) => match unit {
                TimeUnit::Microsecond => {
                    handle_primitive!(arrow::array::Time64MicrosecondArray, i64, |v| v)
                }
                TimeUnit::Nanosecond => {
                    handle_primitive!(arrow::array::Time64NanosecondArray, i64, |v| v)
                }
                other => Err(DataFusionError::NotImplemented(format!(
                    "Unique value tracking not implemented for Time64 unit {:?}",
                    other
                ))),
            },
            DataType::Timestamp(unit, _) => match unit {
                TimeUnit::Second => {
                    handle_primitive!(arrow::array::TimestampSecondArray, i64, |v| v)
                }
                TimeUnit::Millisecond => {
                    handle_primitive!(arrow::array::TimestampMillisecondArray, i64, |v| v)
                }
                TimeUnit::Microsecond => {
                    handle_primitive!(arrow::array::TimestampMicrosecondArray, i64, |v| v)
                }
                TimeUnit::Nanosecond => {
                    handle_primitive!(arrow::array::TimestampNanosecondArray, i64, |v| v)
                }
            },
            DataType::Duration(unit) => match unit {
                TimeUnit::Second => {
                    handle_primitive!(arrow::array::DurationSecondArray, i64, |v| v)
                }
                TimeUnit::Millisecond => {
                    handle_primitive!(arrow::array::DurationMillisecondArray, i64, |v| v)
                }
                TimeUnit::Microsecond => {
                    handle_primitive!(arrow::array::DurationMicrosecondArray, i64, |v| v)
                }
                TimeUnit::Nanosecond => {
                    handle_primitive!(arrow::array::DurationNanosecondArray, i64, |v| v)
                }
            },
            DataType::Decimal128(_, _) => {
                handle_primitive!(arrow::array::Decimal128Array, i128, |v| v)
            }
            other => Err(DataFusionError::NotImplemented(format!(
                "Unique value tracking not implemented for data type {:?}",
                other
            ))),
        }
    }
}

fn unexpected_unique_value_type(field_name: &str) -> DataFusionError {
    DataFusionError::Internal(format!(
        "Unexpected unique value type for column {field_name}"
    ))
}

fn unexpected_array_type(field_name: &str) -> DataFusionError {
    DataFusionError::Internal(format!("Unexpected array type for column {field_name}"))
}

impl Stream for UniqueValueStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match futures::ready!(self.input.poll_next_unpin(cx)) {
            Some(Ok(batch)) => {
                let mut unique_values = self.unique_values_handle.unique_column_values.lock();
                if let Err(err) = Self::capture_unique_values(
                    &batch,
                    &self.unique_value_columns_projection,
                    &mut unique_values,
                ) {
                    return Poll::Ready(Some(Err(err)));
                }

                Poll::Ready(Some(Ok(batch)))
            }
            Some(Err(e)) => Poll::Ready(Some(Err(e))),
            None => Poll::Ready(None),
        }
    }
}

impl RecordBatchStream for UniqueValueStream {
    fn schema(&self) -> Arc<Schema> {
        self.input.schema()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::{
        array::{BinaryArray, Int32Array, StringArray},
        datatypes::{Field, Schema},
        util::pretty::pretty_format_batches,
    };
    use datafusion::{
        physical_plan::{ExecutionPlan, common, test::TestMemoryExec},
        prelude::SessionContext,
    };
    use std::collections::BTreeSet;

    fn build_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("station", DataType::Utf8, true),
            Field::new("depth", DataType::Int32, true),
            Field::new("payload", DataType::Binary, true),
        ]))
    }

    fn batch_from(
        stations: Vec<Option<&str>>,
        depths: Vec<Option<i32>>,
        payloads: Vec<Option<&[u8]>>,
    ) -> RecordBatch {
        assert_eq!(stations.len(), depths.len());
        assert_eq!(stations.len(), payloads.len());

        let schema = build_schema();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(stations)),
                Arc::new(Int32Array::from(depths)),
                Arc::new(BinaryArray::from(payloads)),
            ],
        )
        .expect("valid batch")
    }

    fn assert_batches_eq(expected: &[RecordBatch], actual: &[RecordBatch]) {
        let expected_pretty = pretty_format_batches(expected)
            .expect("expected pretty")
            .to_string();
        let actual_pretty = pretty_format_batches(actual)
            .expect("actual pretty")
            .to_string();
        assert_eq!(expected_pretty, actual_pretty);
    }

    fn add_string_values(handle: &Arc<UniqueValuesHandle>, field_name: &str, values: &[&str]) {
        let mut guard = handle.unique_column_values.lock();
        let column = guard
            .iter_mut()
            .find_map(|(field, erased)| {
                if field.name() == field_name {
                    erased
                        .as_mut()
                        .as_mut()
                        .downcast_mut::<UniqueColumnValues<String>>()
                        .map(|col| (field.clone(), col))
                } else {
                    None
                }
            })
            .unwrap_or_else(|| panic!("field {field_name} not found"))
            .1;

        column.compare_and_add(values.iter().map(|value| value.to_string()));
    }

    fn add_i32_values(handle: &Arc<UniqueValuesHandle>, field_name: &str, values: &[i32]) {
        let mut guard = handle.unique_column_values.lock();
        let column = guard
            .iter_mut()
            .find_map(|(field, erased)| {
                if field.name() == field_name {
                    erased
                        .as_mut()
                        .as_mut()
                        .downcast_mut::<UniqueColumnValues<i32>>()
                        .map(|col| (field.clone(), col))
                } else {
                    None
                }
            })
            .unwrap_or_else(|| panic!("field {field_name} not found"))
            .1;

        column.compare_and_add(values.iter().copied());
    }

    #[tokio::test]
    async fn collects_unique_values_and_replays_input() {
        let batch1 = batch_from(
            vec![Some("A"), Some("B"), Some("A")],
            vec![Some(5), Some(10), Some(5)],
            vec![Some(b"foo"), Some(b"bar"), None],
        );
        let batch2 = batch_from(
            vec![Some("C"), Some("B"), None],
            vec![Some(7), Some(10), Some(12)],
            vec![Some(b"baz"), Some(b"foo"), Some(b"zzz")],
        );

        let partitions = vec![vec![batch1.clone(), batch2.clone()]];
        let schema = build_schema();
        let exec = Arc::new(
            TestMemoryExec::try_new(&partitions, schema.clone(), None).expect("memory exec"),
        );

        let session = SessionContext::new();
        let input_stream = exec
            .execute(0, session.task_ctx())
            .expect("execute memory exec");
        let handle = Arc::new(UniqueValuesHandle::from_schema(schema).expect("handle"));
        let projection = vec![0, 1, 2];
        let stream: SendableRecordBatchStream = Box::pin(UniqueValueStream::new(
            input_stream,
            handle.clone(),
            projection,
        ));
        let output = common::collect(stream).await.expect("collect stream");
        assert_batches_eq(&[batch1.clone(), batch2.clone()], &output);

        let unique_values = handle.unique_column_values.lock();
        let station_values = unique_values
            .iter()
            .find_map(|(f, b)| if f.name() == "station" { Some(b) } else { None })
            .unwrap()
            .as_any()
            .downcast_ref::<UniqueColumnValues<String>>()
            .expect("station unique values");
        assert_eq!(station_values.values, vec!["A", "B", "C"]);

        let depth_values = unique_values
            .iter()
            .find_map(|(f, b)| if f.name() == "depth" { Some(b) } else { None })
            .unwrap()
            .as_any()
            .downcast_ref::<UniqueColumnValues<i32>>()
            .expect("depth unique values");
        assert_eq!(depth_values.values, vec![5, 7, 10, 12]);

        let payload_values = unique_values
            .iter()
            .find_map(|(f, b)| if f.name() == "payload" { Some(b) } else { None })
            .unwrap()
            .as_any()
            .downcast_ref::<UniqueColumnValues<Vec<u8>>>()
            .expect("payload unique values");
        assert_eq!(
            payload_values.values,
            vec![
                b"bar".to_vec(),
                b"baz".to_vec(),
                b"foo".to_vec(),
                b"zzz".to_vec()
            ]
        );
    }

    #[tokio::test]
    async fn projection_controls_tracked_columns() {
        let batch = batch_from(
            vec![Some("A"), Some("B"), Some("C")],
            vec![Some(1), Some(2), Some(3)],
            vec![Some(b"a"), Some(b"b"), Some(b"c")],
        );
        let partitions = vec![vec![batch.clone()]];
        let schema = build_schema();
        let exec = Arc::new(
            TestMemoryExec::try_new(&partitions, schema.clone(), None).expect("memory exec"),
        );

        let session = SessionContext::new();
        let input_stream = exec
            .execute(0, session.task_ctx())
            .expect("execute memory exec");
        let handle = Arc::new(UniqueValuesHandle::from_schema(schema).expect("handle"));
        let projection = vec![0]; // only track the first column
        let stream: SendableRecordBatchStream = Box::pin(UniqueValueStream::new(
            input_stream,
            handle.clone(),
            projection,
        ));
        let output = common::collect(stream).await.expect("collect stream");
        assert_batches_eq(std::slice::from_ref(&batch), &output);

        let unique_values = handle.unique_column_values.lock();
        let station_values = unique_values
            .iter()
            .find_map(|(f, b)| if f.name() == "station" { Some(b) } else { None })
            .unwrap()
            .as_any()
            .downcast_ref::<UniqueColumnValues<String>>()
            .expect("station unique values");
        assert_eq!(station_values.values, vec!["A", "B", "C"]);

        let depth_values = unique_values
            .iter()
            .find_map(|(f, b)| if f.name() == "depth" { Some(b) } else { None })
            .unwrap()
            .as_any()
            .downcast_ref::<UniqueColumnValues<i32>>()
            .expect("depth unique values");
        assert!(depth_values.values.is_empty());

        let payload_values = unique_values
            .iter()
            .find_map(|(f, b)| if f.name() == "payload" { Some(b) } else { None })
            .unwrap()
            .as_any()
            .downcast_ref::<UniqueColumnValues<Vec<u8>>>()
            .expect("payload unique values");
        assert!(payload_values.values.is_empty());
    }

    #[tokio::test]
    async fn unique_values_exec_registers_handles_per_partition() {
        let partitions = vec![
            vec![batch_from(
                vec![Some("B"), Some("A")],
                vec![Some(5), Some(3)],
                vec![Some(b"x"), Some(b"y")],
            )],
            vec![batch_from(
                vec![Some("C"), Some("B")],
                vec![Some(7), Some(5)],
                vec![Some(b"z"), Some(b"x")],
            )],
        ];

        let schema = build_schema();
        let base_exec: Arc<dyn ExecutionPlan> = Arc::new(
            TestMemoryExec::try_new(&partitions, schema.clone(), None).expect("memory exec"),
        );

        let (plan, handle_collection) =
            UniqueValuesExec::new(base_exec, vec!["station".to_string(), "depth".to_string()])
                .expect("unique exec");
        let exec = Arc::new(plan);
        let session = SessionContext::new();

        for partition in 0..partitions.len() {
            let stream = exec
                .execute(partition, session.task_ctx())
                .expect("execute unique exec");
            // We only care that the stream drains successfully.
            common::collect(stream)
                .await
                .expect("collect unique exec partition");
        }

        let handles = handle_collection.handles();
        assert_eq!(handles.len(), partitions.len());

        let mut stations = BTreeSet::new();
        let mut depths = BTreeSet::new();
        for handle in handles {
            let guard = handle.unique_column_values.lock();
            let station_values = guard
                .iter()
                .find_map(|(f, b)| if f.name() == "station" { Some(b) } else { None })
                .unwrap()
                .as_any()
                .downcast_ref::<UniqueColumnValues<String>>()
                .expect("station values");
            stations.extend(station_values.values.iter().cloned());

            let depth_values = guard
                .iter()
                .find_map(|(f, b)| if f.name() == "depth" { Some(b) } else { None })
                .unwrap()
                .as_any()
                .downcast_ref::<UniqueColumnValues<i32>>()
                .expect("depth values");
            depths.extend(depth_values.values.iter().copied());

            assert!(
                guard
                    .iter()
                    .find_map(|(f, b)| if f.name() == "payload" { Some(b) } else { None })
                    .is_none()
            );
        }

        assert_eq!(
            stations.into_iter().collect::<Vec<_>>(),
            vec!["A", "B", "C"]
        );
        assert_eq!(depths.into_iter().collect::<Vec<_>>(), vec![3, 5, 7]);
    }

    #[test]
    fn unique_value_handles_merge_across_partitions() {
        let schema = build_schema();
        let collection = UniqueValuesHandleCollection::new();

        let handle_a = Arc::new(UniqueValuesHandle::from_schema(schema.clone()).unwrap());
        add_string_values(&handle_a, "station", &["A", "B"]);
        add_i32_values(&handle_a, "depth", &[5, 3]);
        collection.register_handle(handle_a);

        let handle_b = Arc::new(UniqueValuesHandle::from_schema(schema.clone()).unwrap());
        add_string_values(&handle_b, "station", &["C", "B"]);
        add_i32_values(&handle_b, "depth", &[7]);
        collection.register_handle(handle_b);

        let merged_map = collection
            .unique_values()
            .expect("expected single merged map");

        let station_values = merged_map
            .iter()
            .find_map(|(f, erased)| {
                if f.name() == "station" {
                    Some(erased)
                } else {
                    None
                }
            })
            .unwrap()
            .as_any()
            .downcast_ref::<UniqueColumnValues<String>>()
            .unwrap();
        assert_eq!(station_values.values, vec!["A", "B", "C"]);

        let depth_values = merged_map
            .iter()
            .find_map(|(f, erased)| {
                if f.name() == "depth" {
                    Some(erased)
                } else {
                    None
                }
            })
            .unwrap()
            .as_any()
            .downcast_ref::<UniqueColumnValues<i32>>()
            .unwrap();
        assert_eq!(depth_values.values, vec![3, 5, 7]);
    }

    #[test]
    fn unique_value_handles_return_empty_when_no_handles_registered() {
        let collection = UniqueValuesHandleCollection::new();
        assert!(collection.unique_values().is_none());
    }
}
