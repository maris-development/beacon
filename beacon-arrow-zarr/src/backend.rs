//! Array backend implementations bridging zarrs arrays into the
//! `beacon-nd-array` engine.
//!
//! Three flavors of lazy [`ArrayBackend`] are provided:
//! - [`ZarrArrayBackend`] — reads a zarr array directly as its native dtype.
//! - [`ScaleOffsetBackend`] — applies CF `scale_factor`/`add_offset`,
//!   producing `f64`.
//! - [`CfTimeBackend`] — decodes CF time (`units = "<unit> since <epoch>"`)
//!   numeric values into nanosecond timestamps.
//!
//! Plus [`AttributeBackend`] for scalar attributes surfaced as rank-0 arrays.

use std::{str::FromStr, sync::Arc};

use beacon_nd_array::{
    array::{backend::ArrayBackend, subset::ArraySubset},
    datatypes::{NdArrayType, TimestampNanosecond},
};
use hifitime::Epoch;
use ndarray::ArrayD;
use regex::Regex;
use zarrs_storage::AsyncReadableListableStorageTraits;

use crate::data_types::ZarrDtypeKind;

/// A zarr array backed by an async, readable+listable object store.
pub type ZarrArray = zarrs::array::Array<dyn AsyncReadableListableStorageTraits>;
type ZarrSubset = zarrs::array::ArraySubset;

/// Translate an engine [`ArraySubset`] (start/shape in `usize`) into a zarrs
/// [`ArraySubset`] (half-open `u64` ranges per axis).
fn to_zarr_subset(subset: &ArraySubset) -> ZarrSubset {
    let ranges: Vec<std::ops::Range<u64>> = subset
        .start
        .iter()
        .zip(subset.shape.iter())
        .map(|(start, len)| (*start as u64)..((*start + *len) as u64))
        .collect();
    ZarrSubset::new_with_ranges(&ranges)
}

// ─── ZarrReadable ────────────────────────────────────────────────────────────

/// Trait for `T: NdArrayType` values that can be read directly out of a zarr
/// array as a typed `ArrayD<T>`.
#[async_trait::async_trait]
pub trait ZarrReadable: NdArrayType {
    async fn read(array: &ZarrArray, subset: &ZarrSubset) -> anyhow::Result<ArrayD<Self>>;
}

macro_rules! impl_zarr_readable {
    ($ty:ty) => {
        #[async_trait::async_trait]
        impl ZarrReadable for $ty {
            async fn read(array: &ZarrArray, subset: &ZarrSubset) -> anyhow::Result<ArrayD<Self>> {
                let arr = array
                    .async_retrieve_array_subset::<ArrayD<$ty>>(subset)
                    .await
                    .map_err(|e| anyhow::anyhow!("Failed to read zarr array subset: {}", e))?;
                Ok(arr)
            }
        }
    };
}

impl_zarr_readable!(bool);
impl_zarr_readable!(i8);
impl_zarr_readable!(i16);
impl_zarr_readable!(i32);
impl_zarr_readable!(i64);
impl_zarr_readable!(u8);
impl_zarr_readable!(u16);
impl_zarr_readable!(u32);
impl_zarr_readable!(u64);
impl_zarr_readable!(f32);
impl_zarr_readable!(f64);
impl_zarr_readable!(String);
impl_zarr_readable!(Vec<u8>);

/// Read a numeric zarr array subset and widen every element to `f64`.
///
/// Used by the CF decoders, which operate in `f64` space. Non-numeric dtypes
/// (bool / string / bytes) are rejected.
async fn read_raw_as_f64(
    array: &ZarrArray,
    subset: &ZarrSubset,
    kind: ZarrDtypeKind,
) -> anyhow::Result<ArrayD<f64>> {
    macro_rules! read_widen {
        ($ty:ty) => {{
            let arr = array
                .async_retrieve_array_subset::<ArrayD<$ty>>(subset)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to read zarr array subset: {}", e))?;
            arr.mapv(|v| v as f64)
        }};
    }

    let out = match kind {
        ZarrDtypeKind::Int8 => read_widen!(i8),
        ZarrDtypeKind::Int16 => read_widen!(i16),
        ZarrDtypeKind::Int32 => read_widen!(i32),
        ZarrDtypeKind::Int64 => read_widen!(i64),
        ZarrDtypeKind::UInt8 => read_widen!(u8),
        ZarrDtypeKind::UInt16 => read_widen!(u16),
        ZarrDtypeKind::UInt32 => read_widen!(u32),
        ZarrDtypeKind::UInt64 => read_widen!(u64),
        ZarrDtypeKind::Float32 => read_widen!(f32),
        ZarrDtypeKind::Float64 => read_widen!(f64),
        other => {
            anyhow::bail!("CF decoding is not supported for zarr dtype kind {:?}", other)
        }
    };
    Ok(out)
}

/// Convert a CF time offset (in `unit`s since `epoch`) to a nanosecond timestamp.
fn cf_offset_to_timestamp(value: f64, epoch: Epoch, unit: hifitime::Unit) -> TimestampNanosecond {
    let instant = epoch + (value * unit);
    TimestampNanosecond(instant.to_unix(hifitime::Unit::Nanosecond) as i64)
}

// ─── ZarrArrayBackend (direct dtype) ─────────────────────────────────────────

/// Backend that reads a zarr array lazily as its native dtype `T`.
pub struct ZarrArrayBackend<T: NdArrayType> {
    array: Arc<ZarrArray>,
    shape: Vec<usize>,
    dimensions: Vec<String>,
    chunk_shape: Vec<usize>,
    fill_value: Option<T>,
}

impl<T: NdArrayType> std::fmt::Debug for ZarrArrayBackend<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ZarrArrayBackend")
            .field("shape", &self.shape)
            .field("dimensions", &self.dimensions)
            .field("chunk_shape", &self.chunk_shape)
            .finish()
    }
}

impl<T: NdArrayType> ZarrArrayBackend<T> {
    pub fn new(
        array: Arc<ZarrArray>,
        shape: Vec<usize>,
        dimensions: Vec<String>,
        chunk_shape: Vec<usize>,
        fill_value: Option<T>,
    ) -> Self {
        Self {
            array,
            shape,
            dimensions,
            chunk_shape,
            fill_value,
        }
    }
}

#[async_trait::async_trait]
impl<T: NdArrayType + ZarrReadable> ArrayBackend<T> for ZarrArrayBackend<T> {
    fn len(&self) -> usize {
        self.shape.iter().product()
    }

    fn shape(&self) -> Vec<usize> {
        self.shape.clone()
    }

    fn dimensions(&self) -> Vec<String> {
        self.dimensions.clone()
    }

    fn chunk_shape(&self) -> Vec<usize> {
        self.chunk_shape.clone()
    }

    fn fill_value(&self) -> Option<T> {
        self.fill_value.clone()
    }

    async fn read_subset(&self, subset: ArraySubset) -> anyhow::Result<ArrayD<T>> {
        T::read(&self.array, &to_zarr_subset(&subset)).await
    }
}

// ─── ScaleOffsetBackend (CF scale_factor / add_offset → f64) ─────────────────

/// Backend that applies CF `scale_factor`/`add_offset` packing, decoding the
/// raw numeric values to `f64` (`raw * scale + offset`).
pub struct ScaleOffsetBackend {
    array: Arc<ZarrArray>,
    kind: ZarrDtypeKind,
    shape: Vec<usize>,
    dimensions: Vec<String>,
    chunk_shape: Vec<usize>,
    scale: f64,
    offset: f64,
    fill_value: Option<f64>,
}

impl std::fmt::Debug for ScaleOffsetBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScaleOffsetBackend")
            .field("shape", &self.shape)
            .field("dimensions", &self.dimensions)
            .field("scale", &self.scale)
            .field("offset", &self.offset)
            .finish()
    }
}

impl ScaleOffsetBackend {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        array: Arc<ZarrArray>,
        kind: ZarrDtypeKind,
        shape: Vec<usize>,
        dimensions: Vec<String>,
        chunk_shape: Vec<usize>,
        scale: f64,
        offset: f64,
        raw_fill_value: Option<f64>,
    ) -> Self {
        // The engine nulls elements equal to `fill_value()` *after* decoding.
        // Decode the raw fill with the same arithmetic so a packed fill cell
        // matches exactly.
        let fill_value = raw_fill_value.map(|f| f * scale + offset);
        Self {
            array,
            kind,
            shape,
            dimensions,
            chunk_shape,
            scale,
            offset,
            fill_value,
        }
    }
}

#[async_trait::async_trait]
impl ArrayBackend<f64> for ScaleOffsetBackend {
    fn len(&self) -> usize {
        self.shape.iter().product()
    }

    fn shape(&self) -> Vec<usize> {
        self.shape.clone()
    }

    fn dimensions(&self) -> Vec<String> {
        self.dimensions.clone()
    }

    fn chunk_shape(&self) -> Vec<usize> {
        self.chunk_shape.clone()
    }

    fn fill_value(&self) -> Option<f64> {
        self.fill_value
    }

    async fn read_subset(&self, subset: ArraySubset) -> anyhow::Result<ArrayD<f64>> {
        let raw = read_raw_as_f64(&self.array, &to_zarr_subset(&subset), self.kind).await?;
        let scale = self.scale;
        let offset = self.offset;
        Ok(raw.mapv(|v| v * scale + offset))
    }
}

// ─── CfTimeBackend (CF time units → TimestampNanosecond) ─────────────────────

/// Backend that decodes CF time variables (numeric offsets in `unit`s since
/// `epoch`) into nanosecond timestamps.
pub struct CfTimeBackend {
    array: Arc<ZarrArray>,
    kind: ZarrDtypeKind,
    shape: Vec<usize>,
    dimensions: Vec<String>,
    chunk_shape: Vec<usize>,
    epoch: Epoch,
    unit: hifitime::Unit,
    fill_value: Option<TimestampNanosecond>,
}

impl std::fmt::Debug for CfTimeBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CfTimeBackend")
            .field("shape", &self.shape)
            .field("dimensions", &self.dimensions)
            .finish()
    }
}

impl CfTimeBackend {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        array: Arc<ZarrArray>,
        kind: ZarrDtypeKind,
        shape: Vec<usize>,
        dimensions: Vec<String>,
        chunk_shape: Vec<usize>,
        epoch: Epoch,
        unit: hifitime::Unit,
        raw_fill_value: Option<f64>,
    ) -> Self {
        let fill_value = raw_fill_value.map(|f| cf_offset_to_timestamp(f, epoch, unit));
        Self {
            array,
            kind,
            shape,
            dimensions,
            chunk_shape,
            epoch,
            unit,
            fill_value,
        }
    }
}

#[async_trait::async_trait]
impl ArrayBackend<TimestampNanosecond> for CfTimeBackend {
    fn len(&self) -> usize {
        self.shape.iter().product()
    }

    fn shape(&self) -> Vec<usize> {
        self.shape.clone()
    }

    fn dimensions(&self) -> Vec<String> {
        self.dimensions.clone()
    }

    fn chunk_shape(&self) -> Vec<usize> {
        self.chunk_shape.clone()
    }

    fn fill_value(&self) -> Option<TimestampNanosecond> {
        self.fill_value
    }

    async fn read_subset(&self, subset: ArraySubset) -> anyhow::Result<ArrayD<TimestampNanosecond>> {
        let raw = read_raw_as_f64(&self.array, &to_zarr_subset(&subset), self.kind).await?;
        let epoch = self.epoch;
        let unit = self.unit;
        Ok(raw.mapv(|v| cf_offset_to_timestamp(v, epoch, unit)))
    }
}

// ─── AttributeBackend (rank-0 scalars) ───────────────────────────────────────

/// Backend for scalar attribute values surfaced as rank-0 arrays.
#[derive(Debug)]
pub struct AttributeBackend<T: NdArrayType> {
    value: T,
}

impl<T: NdArrayType> AttributeBackend<T> {
    pub fn new(value: T) -> Self {
        Self { value }
    }
}

#[async_trait::async_trait]
impl<T: NdArrayType> ArrayBackend<T> for AttributeBackend<T> {
    fn len(&self) -> usize {
        1
    }

    fn shape(&self) -> Vec<usize> {
        vec![]
    }

    fn dimensions(&self) -> Vec<String> {
        vec![]
    }

    fn fill_value(&self) -> Option<T> {
        None
    }

    async fn read_subset(&self, _subset: ArraySubset) -> anyhow::Result<ArrayD<T>> {
        Ok(ndarray::arr0(self.value.clone()).into_dyn())
    }
}

// ─── CF time units parsing ───────────────────────────────────────────────────

/// Parse a CF time `units` string (e.g. `"days since 1950-01-01"`) into an
/// `(epoch, unit)` pair, or `None` if it is not a recognized CF time unit.
pub fn parse_cf_time_units(units: &str) -> Option<(Epoch, hifitime::Unit)> {
    let unit = extract_units(units)?;
    let epoch = extract_epoch(units)?;
    Some((epoch, unit))
}

fn extract_units(input: &str) -> Option<hifitime::Unit> {
    let re = Regex::new(r"^(?P<units>\w+) since").unwrap();
    re.captures(input)
        .and_then(|caps| match caps["units"].to_string().as_str() {
            "seconds" => Some(hifitime::Unit::Second),
            "milliseconds" => Some(hifitime::Unit::Millisecond),
            "microseconds" => Some(hifitime::Unit::Microsecond),
            "nanoseconds" => Some(hifitime::Unit::Nanosecond),
            "minutes" => Some(hifitime::Unit::Minute),
            "hours" => Some(hifitime::Unit::Hour),
            "days" => Some(hifitime::Unit::Day),
            "weeks" => Some(hifitime::Unit::Week),
            _ => None,
        })
}

fn extract_epoch(input: &str) -> Option<Epoch> {
    let re = Regex::new(r"since (?P<epoch>-?\d{1,4}-\d{1,2}-\d{1,2})").unwrap();
    re.captures(input).and_then(|caps| {
        let epoch_str = caps["epoch"].to_string();
        let mut epoch = Epoch::from_str(&epoch_str).ok();
        if epoch.is_none() && epoch_str == "-4713-01-01" {
            epoch = Some(Epoch::from_jde_utc(0.0));
        }
        epoch
    })
}
