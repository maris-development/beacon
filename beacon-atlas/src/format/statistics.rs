//! Per-column statistics for dataset pruning and query optimization.
//!
//! This module computes and stores min/max/null statistics for each dataset's
//! arrays. These statistics enable the reader to skip datasets that cannot
//! match a query predicate without reading any array data.
//!
//! # How it works
//!
//! 1. When a dataset is written, [`compute_statistics`] scans each array and
//!    produces a [`DatasetStatisticsEntry`] containing row count, null count, and
//!    min/max values.
//!
//! 2. Statistics are stored per-column in [`ColumnStatistics`], which maps
//!    `DatasetId → DatasetStatisticsEntry`. Each column's statistics are persisted
//!    as a `{column}.stats.atlas` file alongside the `.arrf` data file.
//!
//! 3. At read time, the reader loads column statistics and uses them to prune
//!    datasets that fall outside requested value ranges.
//!
//! # Example
//!
//! ```rust
//! use beacon_atlas::statistics::{
//!     compute_statistics, extract_fill_value, ColumnStatistics, StatisticsValue,
//! };
//! use beacon_atlas::schema::DatasetId;
//! use beacon_nd_array::NdArray;
//!
//! # tokio_test::block_on(async {
//! // Create an array with a fill value representing missing data
//! let array = NdArray::<f64>::try_new_from_vec_in_mem(
//!     vec![-999.0, 1.0, 5.0, -999.0, 3.0],
//!     vec![5],
//!     vec!["obs".into()],
//!     Some(-999.0),
//! ).unwrap();
//!
//! // Extract fill value and compute statistics
//! let fill = extract_fill_value(&array).await;
//! let stats = compute_statistics(&array, fill.as_ref()).await;
//!
//! assert_eq!(stats.row_count, 5);
//! assert_eq!(stats.null_count, 2);              // two -999.0 fill values
//! assert_eq!(stats.min_value, Some(StatisticsValue::F64(1.0)));
//! assert_eq!(stats.max_value, Some(StatisticsValue::F64(5.0)));
//!
//! // Store in a ColumnStatistics index
//! let mut col_stats = ColumnStatistics::new();
//! col_stats.set(DatasetId::from_raw(0), stats);
//! assert!(col_stats.get(DatasetId::from_raw(0)).is_some());
//! # });
//! ```

use std::io::{Read, Write};

use beacon_nd_array::datatypes::{NdArrayDataType, NdArrayType, TimestampNanosecond};
use beacon_nd_array::{NdArray, NdArrayD};
use serde::{Deserialize, Serialize};

use crate::schema::DatasetId;

// ─── StatisticsValue ──────────────────────────────────────────────────────────

/// A type-erased scalar value used for min/max statistics.
///
/// Each variant corresponds to one [`NdArrayDataType`] (except `Binary`, which
/// has no meaningful ordering and is excluded).
///
/// Values of the **same** variant are comparable via [`PartialOrd`]; values
/// of **different** variants always return `None`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum StatisticsValue {
    Bool(bool),
    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
    F32(f32),
    F64(f64),
    String(String),
    Timestamp(i64),
}

impl PartialOrd for StatisticsValue {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (Self::Bool(a), Self::Bool(b)) => a.partial_cmp(b),
            (Self::I8(a), Self::I8(b)) => a.partial_cmp(b),
            (Self::I16(a), Self::I16(b)) => a.partial_cmp(b),
            (Self::I32(a), Self::I32(b)) => a.partial_cmp(b),
            (Self::I64(a), Self::I64(b)) => a.partial_cmp(b),
            (Self::U8(a), Self::U8(b)) => a.partial_cmp(b),
            (Self::U16(a), Self::U16(b)) => a.partial_cmp(b),
            (Self::U32(a), Self::U32(b)) => a.partial_cmp(b),
            (Self::U64(a), Self::U64(b)) => a.partial_cmp(b),
            (Self::F32(a), Self::F32(b)) => a.partial_cmp(b),
            (Self::F64(a), Self::F64(b)) => a.partial_cmp(b),
            (Self::String(a), Self::String(b)) => a.partial_cmp(b),
            (Self::Timestamp(a), Self::Timestamp(b)) => a.partial_cmp(b),
            _ => None,
        }
    }
}

// ─── DatasetStatisticsEntry ───────────────────────────────────────────────────

/// Statistics for a single dataset within a single column.
///
/// - `row_count` — total number of elements (includes fill/null values).
/// - `null_count` — number of elements matching the fill value.
/// - `min_value` / `max_value` — extremes over non-null elements, or `None`
///   if every element is null.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DatasetStatisticsEntry {
    pub row_count: u64,
    pub null_count: u64,
    pub min_value: Option<StatisticsValue>,
    pub max_value: Option<StatisticsValue>,
}

// ─── ColumnStatistics ─────────────────────────────────────────────────────────

/// Per-dataset statistics for a single column, indexed by [`DatasetId`].
///
/// Internally backed by a sparse `Vec` where position `i` holds the stats
/// for `DatasetId(i)`. Persisted as `{column}.stats.atlas` alongside the
/// `.arrf` data file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnStatistics {
    entries: Vec<Option<DatasetStatisticsEntry>>,
}

impl ColumnStatistics {
    /// Create an empty statistics index.
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    /// Get statistics for a specific dataset.
    pub fn get(&self, id: DatasetId) -> Option<&DatasetStatisticsEntry> {
        self.entries
            .get(id.as_u32() as usize)
            .and_then(|e| e.as_ref())
    }

    /// Set statistics for a specific dataset.
    pub fn set(&mut self, id: DatasetId, entry: DatasetStatisticsEntry) {
        let idx = id.as_u32() as usize;
        if self.entries.len() <= idx {
            self.entries.resize_with(idx + 1, || None);
        }
        self.entries[idx] = Some(entry);
    }

    /// Iterate over all entries (indexed by position = DatasetId).
    pub fn entries(&self) -> &[Option<DatasetStatisticsEntry>] {
        &self.entries
    }

    /// Number of slots (not the number of datasets with statistics).
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns `true` if no statistics have been stored.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Serialize using bincode + zstd.
    pub fn save(&self, writer: impl Write) -> anyhow::Result<()> {
        let encoder = zstd::Encoder::new(writer, 3)?.auto_finish();
        bincode::serialize_into(encoder, self)?;
        Ok(())
    }

    /// Deserialize from bincode + zstd.
    pub fn load(reader: impl Read) -> anyhow::Result<Self> {
        let decoder = zstd::Decoder::new(reader)?;
        let stats: Self = bincode::deserialize_from(decoder)?;
        Ok(stats)
    }

    /// Save to an object store at the given path.
    pub async fn save_to_object_store(
        &self,
        store: &dyn object_store::ObjectStore,
        path: &object_store::path::Path,
    ) -> anyhow::Result<()> {
        let mut buf = Vec::new();
        self.save(&mut buf)?;
        store.put(path, bytes::Bytes::from(buf).into()).await?;
        Ok(())
    }

    /// Load from an object store at the given path.
    /// Returns an empty `ColumnStatistics` if the file doesn't exist.
    pub async fn load_from_object_store(
        store: &dyn object_store::ObjectStore,
        path: &object_store::path::Path,
    ) -> anyhow::Result<Self> {
        match store.get(path).await {
            Ok(result) => {
                let bytes = result.bytes().await?;
                Self::load(&bytes[..])
            }
            Err(object_store::Error::NotFound { .. }) => Ok(Self::new()),
            Err(e) => Err(e.into()),
        }
    }
}

// ─── compute_statistics ───────────────────────────────────────────────────────

/// Compute statistics for a single array.
///
/// Fill values are treated as logical nulls:
/// - They are excluded from min/max computation.
/// - They are counted in `null_count`.
/// - `row_count` includes **all** elements (fill values included).
///
/// For flat arrays (where `chunk_shape == shape`), the entire array is loaded
/// into memory. For chunked arrays, data is read one chunk at a time to avoid
/// excessive memory usage.
///
/// # Example
///
/// ```rust
/// use beacon_atlas::statistics::{compute_statistics, StatisticsValue};
/// use beacon_nd_array::NdArray;
///
/// # tokio_test::block_on(async {
/// let array = NdArray::<i32>::try_new_from_vec_in_mem(
///     vec![10, 20, 30],
///     vec![3],
///     vec!["x".into()],
///     None,
/// ).unwrap();
///
/// let stats = compute_statistics(&array, None).await;
/// assert_eq!(stats.row_count, 3);
/// assert_eq!(stats.min_value, Some(StatisticsValue::I32(10)));
/// assert_eq!(stats.max_value, Some(StatisticsValue::I32(30)));
/// # });
/// ```
pub async fn compute_statistics(
    array: &dyn NdArrayD,
    fill_value: Option<&StatisticsValue>,
) -> DatasetStatisticsEntry {
    let row_count: u64 = array.shape().iter().product::<usize>() as u64;

    match array.datatype() {
        NdArrayDataType::Bool => compute_typed::<bool>(array, fill_value, row_count).await,
        NdArrayDataType::I8 => compute_typed::<i8>(array, fill_value, row_count).await,
        NdArrayDataType::I16 => compute_typed::<i16>(array, fill_value, row_count).await,
        NdArrayDataType::I32 => compute_typed::<i32>(array, fill_value, row_count).await,
        NdArrayDataType::I64 => compute_typed::<i64>(array, fill_value, row_count).await,
        NdArrayDataType::U8 => compute_typed::<u8>(array, fill_value, row_count).await,
        NdArrayDataType::U16 => compute_typed::<u16>(array, fill_value, row_count).await,
        NdArrayDataType::U32 => compute_typed::<u32>(array, fill_value, row_count).await,
        NdArrayDataType::U64 => compute_typed::<u64>(array, fill_value, row_count).await,
        NdArrayDataType::F32 => compute_typed::<f32>(array, fill_value, row_count).await,
        NdArrayDataType::F64 => compute_typed::<f64>(array, fill_value, row_count).await,
        NdArrayDataType::String => compute_typed::<String>(array, fill_value, row_count).await,
        NdArrayDataType::Timestamp => {
            compute_typed::<TimestampNanosecond>(array, fill_value, row_count).await
        }
        NdArrayDataType::Binary => DatasetStatisticsEntry {
            row_count,
            null_count: 0,
            min_value: None,
            max_value: None,
        },
    }
}

/// Extract the fill value from an array as a [`StatisticsValue`].
///
/// Returns `None` if the array has no fill value or is of type `Binary`.
///
/// # Example
///
/// ```rust
/// use beacon_atlas::statistics::{extract_fill_value, StatisticsValue};
/// use beacon_nd_array::NdArray;
///
/// # tokio_test::block_on(async {
/// let array = NdArray::<f64>::try_new_from_vec_in_mem(
///     vec![1.0],
///     vec![1],
///     vec!["x".into()],
///     Some(-999.0),
/// ).unwrap();
///
/// let fv = extract_fill_value(&array).await;
/// assert_eq!(fv, Some(StatisticsValue::F64(-999.0)));
/// # });
/// ```
pub async fn extract_fill_value(array: &dyn NdArrayD) -> Option<StatisticsValue> {
    macro_rules! extract {
        ($ty:ty, $variant:ident) => {{
            let nd = array.as_any().downcast_ref::<NdArray<$ty>>()?;
            nd.fill_value().await.map(StatisticsValue::$variant)
        }};
    }

    match array.datatype() {
        NdArrayDataType::Bool => extract!(bool, Bool),
        NdArrayDataType::I8 => extract!(i8, I8),
        NdArrayDataType::I16 => extract!(i16, I16),
        NdArrayDataType::I32 => extract!(i32, I32),
        NdArrayDataType::I64 => extract!(i64, I64),
        NdArrayDataType::U8 => extract!(u8, U8),
        NdArrayDataType::U16 => extract!(u16, U16),
        NdArrayDataType::U32 => extract!(u32, U32),
        NdArrayDataType::U64 => extract!(u64, U64),
        NdArrayDataType::F32 => extract!(f32, F32),
        NdArrayDataType::F64 => extract!(f64, F64),
        NdArrayDataType::String => extract!(String, String),
        NdArrayDataType::Timestamp => {
            let nd = array
                .as_any()
                .downcast_ref::<NdArray<TimestampNanosecond>>()?;
            nd.fill_value()
                .await
                .map(|v| StatisticsValue::Timestamp(v.0))
        }
        NdArrayDataType::Binary => None,
    }
}

// ─── Internal: IntoStatValue Trait ────────────────────────────────────────────

/// Bridge between concrete typed values and the type-erased [`StatisticsValue`].
///
/// Implemented for every type that can participate in statistics computation.
trait IntoStatValue: Clone + PartialOrd + Send + Sync + 'static {
    /// Convert a typed value into a [`StatisticsValue`].
    fn into_stat_value(self) -> StatisticsValue;

    /// Check whether `val` matches the given fill value.
    fn matches_fill(val: &Self, fill: &StatisticsValue) -> bool;
}

// Macro-generated implementations for integer types.
macro_rules! impl_into_stat_int {
    ($ty:ty, $variant:ident) => {
        impl IntoStatValue for $ty {
            fn into_stat_value(self) -> StatisticsValue {
                StatisticsValue::$variant(self)
            }
            fn matches_fill(val: &Self, fill: &StatisticsValue) -> bool {
                matches!(fill, StatisticsValue::$variant(f) if f == val)
            }
        }
    };
}

impl_into_stat_int!(bool, Bool);
impl_into_stat_int!(i8, I8);
impl_into_stat_int!(i16, I16);
impl_into_stat_int!(i32, I32);
impl_into_stat_int!(i64, I64);
impl_into_stat_int!(u8, U8);
impl_into_stat_int!(u16, U16);
impl_into_stat_int!(u32, U32);
impl_into_stat_int!(u64, U64);
impl_into_stat_int!(f32, F32);
impl_into_stat_int!(f64, F64);

impl IntoStatValue for String {
    fn into_stat_value(self) -> StatisticsValue {
        StatisticsValue::String(self)
    }
    fn matches_fill(val: &Self, fill: &StatisticsValue) -> bool {
        matches!(fill, StatisticsValue::String(f) if f == val)
    }
}

impl IntoStatValue for TimestampNanosecond {
    fn into_stat_value(self) -> StatisticsValue {
        StatisticsValue::Timestamp(self.0)
    }
    fn matches_fill(val: &Self, fill: &StatisticsValue) -> bool {
        matches!(fill, StatisticsValue::Timestamp(f) if *f == val.0)
    }
}

// ─── Internal: Typed Computation ──────────────────────────────────────────────

/// Downcast `array` to `NdArray<T>` and compute statistics.
///
/// Chooses between flat (whole-array) and chunked (per-chunk) strategies
/// based on whether the array's chunk shape equals its full shape.
async fn compute_typed<T>(
    array: &dyn NdArrayD,
    fill_value: Option<&StatisticsValue>,
    row_count: u64,
) -> DatasetStatisticsEntry
where
    T: IntoStatValue + NdArrayType,
{
    let nd = match array.as_any().downcast_ref::<NdArray<T>>() {
        Some(nd) => nd,
        None => return DatasetStatisticsEntry::empty(row_count),
    };

    let shape = nd.shape();
    let chunk_shape = nd.chunk_shape();

    if chunk_shape == shape {
        compute_flat(nd, fill_value, row_count).await
    } else {
        compute_chunked(nd, &shape, &chunk_shape, fill_value, row_count).await
    }
}

/// Compute statistics for a flat (unchunked) array by loading all values at once.
async fn compute_flat<T: IntoStatValue + NdArrayType>(
    nd: &NdArray<T>,
    fill_value: Option<&StatisticsValue>,
    row_count: u64,
) -> DatasetStatisticsEntry {
    let values = nd.clone_into_raw_vec().await;
    let mut acc = StatAccumulator::<T>::new();
    acc.ingest_slice(&values, fill_value);
    acc.into_entry(row_count)
}

/// Compute statistics for a chunked array by iterating chunk-by-chunk.
///
/// Only one chunk is held in memory at a time.
async fn compute_chunked<T: IntoStatValue + NdArrayType>(
    nd: &NdArray<T>,
    shape: &[usize],
    chunk_shape: &[usize],
    fill_value: Option<&StatisticsValue>,
    row_count: u64,
) -> DatasetStatisticsEntry {
    use crate::storage::generate_chunk_subsets;

    let subsets = generate_chunk_subsets(shape, chunk_shape);
    let mut acc = StatAccumulator::<T>::new();

    for subset in subsets {
        let chunk = match nd.subset(subset).await {
            Ok(c) => c,
            Err(_) => continue,
        };
        let values = chunk.clone_into_raw_vec().await;
        acc.ingest_slice(&values, fill_value);
    }

    acc.into_entry(row_count)
}

// ─── Internal: StatAccumulator ────────────────────────────────────────────────

/// Incrementally tracks null count, min, and max across one or more value slices.
struct StatAccumulator<T> {
    null_count: u64,
    min_val: Option<T>,
    max_val: Option<T>,
}

impl<T: IntoStatValue> StatAccumulator<T> {
    fn new() -> Self {
        Self {
            null_count: 0,
            min_val: None,
            max_val: None,
        }
    }

    /// Process a slice of values, updating running min/max and null count.
    fn ingest_slice(&mut self, values: &[T], fill_value: Option<&StatisticsValue>) {
        for val in values {
            if let Some(fv) = fill_value {
                if T::matches_fill(val, fv) {
                    self.null_count += 1;
                    continue;
                }
            }
            self.update_min(val);
            self.update_max(val);
        }
    }

    fn update_min(&mut self, val: &T) {
        self.min_val = Some(match self.min_val.take() {
            None => val.clone(),
            Some(current) => {
                if val.partial_cmp(&current) == Some(std::cmp::Ordering::Less) {
                    val.clone()
                } else {
                    current
                }
            }
        });
    }

    fn update_max(&mut self, val: &T) {
        self.max_val = Some(match self.max_val.take() {
            None => val.clone(),
            Some(current) => {
                if val.partial_cmp(&current) == Some(std::cmp::Ordering::Greater) {
                    val.clone()
                } else {
                    current
                }
            }
        });
    }

    /// Consume the accumulator into a finalized [`DatasetStatisticsEntry`].
    fn into_entry(self, row_count: u64) -> DatasetStatisticsEntry {
        DatasetStatisticsEntry {
            row_count,
            null_count: self.null_count,
            min_value: self.min_val.map(|v| v.into_stat_value()),
            max_value: self.max_val.map(|v| v.into_stat_value()),
        }
    }
}

// ─── DatasetStatisticsEntry helpers ───────────────────────────────────────────

impl DatasetStatisticsEntry {
    /// A stat entry with no meaningful data (used when downcast fails).
    fn empty(row_count: u64) -> Self {
        Self {
            row_count,
            null_count: 0,
            min_value: None,
            max_value: None,
        }
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::stats_path;
    use beacon_nd_array::NdArray;
    use object_store::memory::InMemory;
    use object_store::path::Path;

    #[test]
    fn test_stat_value_partial_ord() {
        assert!(StatisticsValue::F64(1.0) < StatisticsValue::F64(2.0));
        assert!(StatisticsValue::I32(-5) < StatisticsValue::I32(10));
        assert!(StatisticsValue::String("abc".into()) < StatisticsValue::String("xyz".into()));
        assert!(StatisticsValue::U64(0) < StatisticsValue::U64(100));

        // Different variants are incomparable
        assert_eq!(
            StatisticsValue::F64(1.0).partial_cmp(&StatisticsValue::I32(1)),
            None
        );
    }

    #[tokio::test]
    async fn test_compute_statistics_f64_no_fill() {
        let arr = NdArray::<f64>::try_new_from_vec_in_mem(
            vec![3.0, 1.0, 4.0, 1.5, 2.0],
            vec![5],
            vec!["obs".into()],
            None,
        )
        .unwrap();

        let stats = compute_statistics(&arr, None).await;
        assert_eq!(stats.row_count, 5);
        assert_eq!(stats.null_count, 0);
        assert_eq!(stats.min_value, Some(StatisticsValue::F64(1.0)));
        assert_eq!(stats.max_value, Some(StatisticsValue::F64(4.0)));
    }

    #[tokio::test]
    async fn test_compute_statistics_f64_with_fill_value() {
        let arr = NdArray::<f64>::try_new_from_vec_in_mem(
            vec![-999.0, 1.0, -999.0, 3.0, 2.0],
            vec![5],
            vec!["obs".into()],
            Some(-999.0),
        )
        .unwrap();

        let fill = StatisticsValue::F64(-999.0);
        let stats = compute_statistics(&arr, Some(&fill)).await;
        assert_eq!(stats.row_count, 5);
        assert_eq!(stats.null_count, 2);
        assert_eq!(stats.min_value, Some(StatisticsValue::F64(1.0)));
        assert_eq!(stats.max_value, Some(StatisticsValue::F64(3.0)));
    }

    #[tokio::test]
    async fn test_compute_statistics_all_fill_values() {
        let arr = NdArray::<i32>::try_new_from_vec_in_mem(
            vec![-9999, -9999, -9999],
            vec![3],
            vec!["obs".into()],
            Some(-9999),
        )
        .unwrap();

        let fill = StatisticsValue::I32(-9999);
        let stats = compute_statistics(&arr, Some(&fill)).await;
        assert_eq!(stats.row_count, 3);
        assert_eq!(stats.null_count, 3);
        assert_eq!(stats.min_value, None);
        assert_eq!(stats.max_value, None);
    }

    #[tokio::test]
    async fn test_compute_statistics_string() {
        let arr = NdArray::<String>::try_new_from_vec_in_mem(
            vec!["banana".into(), "apple".into(), "cherry".into()],
            vec![3],
            vec!["name".into()],
            None,
        )
        .unwrap();

        let stats = compute_statistics(&arr, None).await;
        assert_eq!(stats.row_count, 3);
        assert_eq!(stats.null_count, 0);
        assert_eq!(
            stats.min_value,
            Some(StatisticsValue::String("apple".into()))
        );
        assert_eq!(
            stats.max_value,
            Some(StatisticsValue::String("cherry".into()))
        );
    }

    #[tokio::test]
    async fn test_compute_statistics_binary_no_minmax() {
        let arr = NdArray::<Vec<u8>>::try_new_from_vec_in_mem(
            vec![vec![1, 2], vec![3, 4]],
            vec![2],
            vec!["data".into()],
            None,
        )
        .unwrap();

        let stats = compute_statistics(&arr, None).await;
        assert_eq!(stats.row_count, 2);
        assert_eq!(stats.null_count, 0);
        assert_eq!(stats.min_value, None);
        assert_eq!(stats.max_value, None);
    }

    #[tokio::test]
    async fn test_extract_fill_value_f64() {
        let arr = NdArray::<f64>::try_new_from_vec_in_mem(
            vec![1.0],
            vec![1],
            vec!["x".into()],
            Some(-999.0),
        )
        .unwrap();

        let fv = extract_fill_value(&arr).await;
        assert_eq!(fv, Some(StatisticsValue::F64(-999.0)));
    }

    #[tokio::test]
    async fn test_extract_fill_value_none() {
        let arr =
            NdArray::<f64>::try_new_from_vec_in_mem(vec![1.0], vec![1], vec!["x".into()], None)
                .unwrap();

        let fv = extract_fill_value(&arr).await;
        assert_eq!(fv, None);
    }

    #[test]
    fn test_column_statistics_set_get() {
        let mut stats = ColumnStatistics::new();
        let id = DatasetId::from_raw(3);
        let entry = DatasetStatisticsEntry {
            row_count: 100,
            null_count: 5,
            min_value: Some(StatisticsValue::F64(0.0)),
            max_value: Some(StatisticsValue::F64(99.0)),
        };

        stats.set(id, entry.clone());
        assert_eq!(stats.get(id), Some(&entry));
        assert_eq!(stats.get(DatasetId::from_raw(0)), None);
        assert_eq!(stats.get(DatasetId::from_raw(10)), None);
    }

    #[test]
    fn test_column_statistics_roundtrip() {
        let mut stats = ColumnStatistics::new();
        stats.set(
            DatasetId::from_raw(0),
            DatasetStatisticsEntry {
                row_count: 10,
                null_count: 2,
                min_value: Some(StatisticsValue::I64(-5)),
                max_value: Some(StatisticsValue::I64(100)),
            },
        );
        stats.set(
            DatasetId::from_raw(2),
            DatasetStatisticsEntry {
                row_count: 3,
                null_count: 0,
                min_value: Some(StatisticsValue::I64(50)),
                max_value: Some(StatisticsValue::I64(60)),
            },
        );

        let mut buf = Vec::new();
        stats.save(&mut buf).unwrap();
        let loaded = ColumnStatistics::load(&buf[..]).unwrap();

        assert_eq!(
            loaded.get(DatasetId::from_raw(0)),
            stats.get(DatasetId::from_raw(0))
        );
        assert_eq!(loaded.get(DatasetId::from_raw(1)), None);
        assert_eq!(
            loaded.get(DatasetId::from_raw(2)),
            stats.get(DatasetId::from_raw(2))
        );
    }

    #[tokio::test]
    async fn test_column_statistics_object_store_roundtrip() {
        let store = InMemory::new();
        let path = Path::from("test/temp.stats.atlas");

        let mut stats = ColumnStatistics::new();
        stats.set(
            DatasetId::from_raw(0),
            DatasetStatisticsEntry {
                row_count: 5,
                null_count: 1,
                min_value: Some(StatisticsValue::F32(1.0)),
                max_value: Some(StatisticsValue::F32(9.0)),
            },
        );

        stats.save_to_object_store(&store, &path).await.unwrap();
        let loaded = ColumnStatistics::load_from_object_store(&store, &path)
            .await
            .unwrap();
        assert_eq!(
            loaded.get(DatasetId::from_raw(0)),
            stats.get(DatasetId::from_raw(0))
        );
    }

    #[tokio::test]
    async fn test_column_statistics_missing_file_returns_empty() {
        let store = InMemory::new();
        let path = Path::from("nonexistent/temp.stats.atlas");

        let stats = ColumnStatistics::load_from_object_store(&store, &path)
            .await
            .unwrap();
        assert!(stats.is_empty());
    }

    #[test]
    fn test_stats_path() {
        let base = Path::from("my-atlas");
        assert_eq!(
            stats_path(&base, "temperature").to_string(),
            "my-atlas/columns/temperature.stats.atlas"
        );
        assert_eq!(
            stats_path(&base, "temperature.units").to_string(),
            "my-atlas/columns/temperature/units.stats.atlas"
        );
        assert_eq!(
            stats_path(&base, ".convention").to_string(),
            "my-atlas/columns/.convention.stats.atlas"
        );
    }
}
