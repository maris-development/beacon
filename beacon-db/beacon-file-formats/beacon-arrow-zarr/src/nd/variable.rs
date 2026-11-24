//! One Zarr array as a variable of the ND contract.

use std::fmt;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, ArrowPrimitiveType, AsArray, BinaryArray, BooleanArray, PrimitiveArray,
    StringArray,
};
use arrow::datatypes::{
    DataType, Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type,
    UInt16Type, UInt32Type, UInt64Type,
};
use async_trait::async_trait;
use beacon_nd_dataset::{
    Attributes, Dimension, Dimensions, Error, NdArrowArray, NdVariable, Region, Result,
    VariableInfo,
};
use nd_arrow_array::compute::cf::FILL_VALUE;
use zarrs::array::{ArraySubset, ElementOwned};

use super::ZarrArray;
use super::attributes::{attributes_of, decode_attributes};
use super::decode::Decode;
use crate::data_types::{ZarrDtypeKind, classify};

/// The `_FillValue` of a `Boolean` or `Utf8` variable. The read nulls each equal value.
#[derive(Debug, Clone, PartialEq)]
enum Fill {
    Boolean(bool),
    Utf8(String),
}

/// One Zarr array as an [`NdVariable`].
pub(crate) struct ZarrVariable {
    array: Arc<ZarrArray>,
    info: VariableInfo,
    /// The Arrow type of the stored values, before the decode.
    stored: DataType,
    decode: Decode,
    /// The attributes that the decode reads: CF keys in the stored integer type.
    decode_attributes: Attributes,
    fill: Option<Fill>,
}

impl ZarrVariable {
    /// A variable over `array` with the given name and dimensions.
    ///
    /// `Err(reason)` when the array has no supported type.
    pub(crate) fn try_new(
        array: Arc<ZarrArray>,
        name: &str,
        dimensions: Dimensions,
    ) -> std::result::Result<ZarrVariable, String> {
        let stored = stored_type(array.data_type()).ok_or_else(|| {
            format!(
                "the Zarr data type '{}' is not supported",
                array.data_type()
            )
        })?;
        debug_assert_eq!(
            dimensions.shape(),
            array
                .shape()
                .iter()
                .map(|&size| size as usize)
                .collect::<Vec<_>>(),
            "the dimensions of '{name}' have the shape of the array"
        );
        let decode_attributes = decode_attributes(array.attributes(), &stored);
        let decode = Decode::choose(&stored, &decode_attributes);
        let data_type = decode.data_type(&stored, &decode_attributes);
        let mut info = VariableInfo::new(name, data_type, dimensions)
            .with_attributes(attributes_of(array.attributes()));
        if let Some(shape) = chunk_shape(&array) {
            info = info.with_chunk_shape(shape);
        }
        let fill = fill_of(&stored, &decode_attributes);
        Ok(ZarrVariable {
            array,
            info,
            stored,
            decode,
            decode_attributes,
            fill,
        })
    }

    /// The stored values of `subset` in C order, with the fill of `Boolean` and `Utf8` as null.
    async fn retrieve(&self, subset: &ArraySubset) -> Result<ArrayRef> {
        Ok(match &self.stored {
            DataType::Boolean => {
                let fill = match self.fill {
                    Some(Fill::Boolean(fill)) => Some(fill),
                    _ => None,
                };
                let values: Vec<bool> = self.elements(subset).await?;
                let values: BooleanArray = values
                    .into_iter()
                    .map(|value| (Some(value) != fill).then_some(value))
                    .collect();
                Arc::new(values)
            }
            DataType::Int8 => self.primitive::<Int8Type>(subset).await?,
            DataType::Int16 => self.primitive::<Int16Type>(subset).await?,
            DataType::Int32 => self.primitive::<Int32Type>(subset).await?,
            DataType::Int64 => self.primitive::<Int64Type>(subset).await?,
            DataType::UInt8 => self.primitive::<UInt8Type>(subset).await?,
            DataType::UInt16 => self.primitive::<UInt16Type>(subset).await?,
            DataType::UInt32 => self.primitive::<UInt32Type>(subset).await?,
            DataType::UInt64 => self.primitive::<UInt64Type>(subset).await?,
            DataType::Float32 => self.primitive::<Float32Type>(subset).await?,
            DataType::Float64 => self.primitive::<Float64Type>(subset).await?,
            DataType::Utf8 => {
                let fill = match &self.fill {
                    Some(Fill::Utf8(fill)) => Some(fill.as_str()),
                    _ => None,
                };
                let values: Vec<String> = self.elements(subset).await?;
                let values: StringArray = values
                    .iter()
                    .map(|value| (Some(value.as_str()) != fill).then_some(value.as_str()))
                    .collect();
                Arc::new(values)
            }
            DataType::Binary => {
                let values: Vec<Vec<u8>> = self.elements(subset).await?;
                Arc::new(BinaryArray::from_iter_values(values))
            }
            other => {
                return Err(Error::format(format!(
                    "no read for the stored type {other}"
                )));
            }
        })
    }

    async fn primitive<T>(&self, subset: &ArraySubset) -> Result<ArrayRef>
    where
        T: ArrowPrimitiveType,
        T::Native: ElementOwned + Send + Sync,
    {
        let values: Vec<T::Native> = self.elements(subset).await?;
        Ok(Arc::new(PrimitiveArray::<T>::new(values.into(), None)))
    }

    async fn elements<T: ElementOwned + Send + Sync>(
        &self,
        subset: &ArraySubset,
    ) -> Result<Vec<T>> {
        self.array
            .async_retrieve_array_subset::<Vec<T>>(subset)
            .await
            .map_err(Error::format)
    }
}

impl fmt::Debug for ZarrVariable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // The storage of the array has no `Debug`.
        f.debug_struct("ZarrVariable")
            .field("path", &self.array.path().as_str())
            .field("info", &self.info)
            .field("stored", &self.stored)
            .field("decode", &self.decode)
            .field("decode_attributes", &self.decode_attributes)
            .field("fill", &self.fill)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl NdVariable for ZarrVariable {
    fn name(&self) -> &str {
        &self.info.name
    }

    fn data_type(&self) -> &DataType {
        &self.info.data_type
    }

    fn dimensions(&self) -> &Dimensions {
        &self.info.dimensions
    }

    fn chunk_shape(&self) -> Option<&[usize]> {
        self.info.chunk_shape.as_deref()
    }

    fn attributes(&self) -> &Attributes {
        &self.info.attributes
    }

    async fn read(&self, region: &Region) -> Result<NdArrowArray> {
        region.check(self.name(), self.dimensions())?;
        let ranges: Vec<std::ops::Range<u64>> = region
            .start()
            .iter()
            .zip(region.shape())
            .map(|(&start, &length)| start as u64..(start + length) as u64)
            .collect();
        let values = self
            .retrieve(&ArraySubset::new_with_ranges(&ranges))
            .await?;
        let dimensions = Dimensions::try_new(
            self.dimensions()
                .iter()
                .zip(region.shape())
                .map(|(dimension, &length)| Dimension::new(dimension.name(), length)),
        )?;
        let raw = NdArrowArray::try_new(values, dimensions)?
            .with_attributes(self.decode_attributes.clone());
        Ok(self
            .decode
            .apply(raw)?
            .with_attributes(self.info.attributes.clone()))
    }
}

/// The stored Arrow type of a Zarr type. `None` when Beacon does not read it.
pub(crate) fn stored_type(data_type: &zarrs::array::DataType) -> Option<DataType> {
    Some(match classify(data_type) {
        ZarrDtypeKind::Bool => DataType::Boolean,
        ZarrDtypeKind::Int8 => DataType::Int8,
        ZarrDtypeKind::Int16 => DataType::Int16,
        ZarrDtypeKind::Int32 => DataType::Int32,
        ZarrDtypeKind::Int64 => DataType::Int64,
        ZarrDtypeKind::UInt8 => DataType::UInt8,
        ZarrDtypeKind::UInt16 => DataType::UInt16,
        ZarrDtypeKind::UInt32 => DataType::UInt32,
        ZarrDtypeKind::UInt64 => DataType::UInt64,
        ZarrDtypeKind::Float32 => DataType::Float32,
        ZarrDtypeKind::Float64 => DataType::Float64,
        ZarrDtypeKind::String => DataType::Utf8,
        ZarrDtypeKind::Bytes => DataType::Binary,
        ZarrDtypeKind::Other => return None,
    })
}

/// The shape of chunk 0. `None` when the chunk grid does not give it.
fn chunk_shape(array: &ZarrArray) -> Option<Vec<usize>> {
    let origin = vec![0u64; array.dimensionality()];
    let chunk = array.chunk_grid().subset(&origin).ok()??;
    chunk
        .shape()
        .iter()
        .map(|&length| usize::try_from(length).ok())
        .collect()
}

/// The fill of a `Boolean` or `Utf8` variable: a `_FillValue` of the same type.
fn fill_of(stored: &DataType, attributes: &Attributes) -> Option<Fill> {
    match stored {
        DataType::Boolean => {
            let value = attributes.get(FILL_VALUE)?;
            let flags = value.as_boolean_opt()?;
            (flags.len() == 1 && flags.is_valid(0)).then(|| Fill::Boolean(flags.value(0)))
        }
        DataType::Utf8 => attributes
            .get_str(FILL_VALUE)
            .map(|text| Fill::Utf8(text.to_string())),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{
        Array, ArrayRef, BinaryArray, BooleanArray, Float32Array, Float64Array, Int8Array,
        Int16Array, Int32Array, Int64Array, StringArray, TimestampMicrosecondArray, UInt8Array,
        UInt16Array, UInt32Array, UInt64Array,
    };
    use arrow::datatypes::{DataType, TimeUnit};
    use beacon_nd_dataset::{Dimension, Dimensions, Error, NdArrowArray, NdVariable, Region};
    use serde_json::{Value, json};
    use zarrs::array::{ArrayBuilder, FillValue, data_type};

    use super::*;
    use crate::nd::testing::{TestElement, TestStore, write_array};

    /// A variable "v" with the axes "d0", "d1", .. over one written array.
    async fn variable<T: TestElement>(
        shape: &[u64],
        chunks: &[u64],
        attributes: Value,
        values: Vec<T>,
    ) -> ZarrVariable {
        let names: Vec<String> = (0..shape.len()).map(|axis| format!("d{axis}")).collect();
        let dims: Vec<Option<&str>> = names.iter().map(|name| Some(name.as_str())).collect();
        let store = TestStore::new();
        let array = write_array(&store, "/v", shape, chunks, &dims, attributes, values).await;
        let dimensions = Dimensions::try_new(
            names
                .iter()
                .zip(shape)
                .map(|(name, &size)| Dimension::new(name.as_str(), size as usize)),
        )
        .unwrap();
        ZarrVariable::try_new(array, "v", dimensions).unwrap()
    }

    /// Read `region` and check the contract of the result.
    async fn read(variable: &ZarrVariable, region: Region) -> NdArrowArray {
        let out = variable.read(&region).await.unwrap();
        assert_eq!(out.data_type(), variable.data_type());
        assert_eq!(out.dims().names(), variable.dimensions().names());
        assert_eq!(out.shape(), region.shape());
        assert_eq!(out.attributes(), variable.attributes());
        out
    }

    /// Read the whole variable and compare the values with `expected`.
    async fn assert_full(variable: &ZarrVariable, expected: ArrayRef) {
        let out = read(variable, Region::full(variable.dimensions())).await;
        assert_eq!(out.values(), &expected);
    }

    async fn assert_reads<T: TestElement>(values: Vec<T>, expected: ArrayRef) {
        let variable = variable(&[3], &[3], json!({}), values).await;
        assert_eq!(variable.data_type(), expected.data_type());
        assert_full(&variable, expected).await;
    }

    fn grid() -> Vec<i32> {
        (0..16).collect()
    }

    #[tokio::test]
    async fn each_stored_type_reads_in_its_arrow_type() {
        assert_reads(
            vec![true, false, true],
            Arc::new(BooleanArray::from(vec![true, false, true])),
        )
        .await;
        assert_reads(vec![1i8, -2, 3], Arc::new(Int8Array::from(vec![1, -2, 3]))).await;
        assert_reads(
            vec![1i16, -2, 3],
            Arc::new(Int16Array::from(vec![1, -2, 3])),
        )
        .await;
        assert_reads(
            vec![1i32, -2, 3],
            Arc::new(Int32Array::from(vec![1, -2, 3])),
        )
        .await;
        assert_reads(
            vec![1i64, -2, 3],
            Arc::new(Int64Array::from(vec![1, -2, 3])),
        )
        .await;
        assert_reads(vec![1u8, 2, 3], Arc::new(UInt8Array::from(vec![1, 2, 3]))).await;
        assert_reads(vec![1u16, 2, 3], Arc::new(UInt16Array::from(vec![1, 2, 3]))).await;
        assert_reads(vec![1u32, 2, 3], Arc::new(UInt32Array::from(vec![1, 2, 3]))).await;
        assert_reads(vec![1u64, 2, 3], Arc::new(UInt64Array::from(vec![1, 2, 3]))).await;
        assert_reads(
            vec![1.5f32, -2.0, 3.0],
            Arc::new(Float32Array::from(vec![1.5, -2.0, 3.0])),
        )
        .await;
        assert_reads(
            vec![1.5f64, -2.0, 3.0],
            Arc::new(Float64Array::from(vec![1.5, -2.0, 3.0])),
        )
        .await;
        assert_reads(
            vec!["a".to_string(), String::new(), "cd".to_string()],
            Arc::new(StringArray::from(vec!["a", "", "cd"])),
        )
        .await;
        assert_reads(
            vec![b"a".to_vec(), Vec::new(), b"cd".to_vec()],
            Arc::new(BinaryArray::from(vec![&b"a"[..], b"", b"cd"])),
        )
        .await;
    }

    #[tokio::test]
    async fn a_region_across_a_chunk_border_reads_the_exact_values() {
        let variable = variable(&[4, 4], &[2, 2], json!({}), grid()).await;
        let out = read(&variable, Region::new(vec![1, 1], vec![2, 2])).await;
        let expected: ArrayRef = Arc::new(Int32Array::from(vec![5, 6, 9, 10]));
        assert_eq!(out.values(), &expected);
    }

    #[tokio::test]
    async fn a_region_with_no_element_reads_an_empty_array() {
        let variable = variable(&[4, 4], &[2, 2], json!({}), grid()).await;
        let out = read(&variable, Region::new(vec![0, 0], vec![0, 4])).await;
        assert_eq!(out.len(), 0);
        assert_eq!(out.data_type(), &DataType::Int32);
    }

    #[tokio::test]
    async fn a_region_outside_the_variable_is_invalid() {
        let variable = variable(&[4, 4], &[2, 2], json!({}), grid()).await;
        let error = variable
            .read(&Region::new(vec![3, 0], vec![2, 4]))
            .await
            .unwrap_err();
        assert!(
            matches!(error, Error::InvalidRegion { ref variable, .. } if variable == "v"),
            "{error:?}"
        );
    }

    #[tokio::test]
    async fn a_rank_0_array_reads_one_value() {
        let variable = variable(&[], &[], json!({}), vec![7.5f64]).await;
        assert_eq!(variable.dimensions().rank(), 0);
        let expected: ArrayRef = Arc::new(Float64Array::from(vec![7.5]));
        assert_full(&variable, expected).await;
    }

    #[tokio::test]
    async fn a_string_fill_and_a_bool_fill_are_null() {
        let text = variable(
            &[2],
            &[2],
            json!({"_FillValue": "-"}),
            vec!["a".to_string(), "-".to_string()],
        )
        .await;
        let expected: ArrayRef = Arc::new(StringArray::from(vec![Some("a"), None]));
        assert_full(&text, expected).await;

        let flag = variable(&[2], &[2], json!({"_FillValue": false}), vec![true, false]).await;
        let expected: ArrayRef = Arc::new(BooleanArray::from(vec![Some(true), None]));
        assert_full(&flag, expected).await;
    }

    async fn packed() -> ZarrVariable {
        variable(
            &[1],
            &[1],
            json!({"scale_factor": 0.5, "add_offset": 1}),
            vec![10i16],
        )
        .await
    }

    #[tokio::test]
    async fn a_packed_variable_decodes_to_float64() {
        let variable = packed().await;
        assert_eq!(variable.data_type(), &DataType::Float64);
        let expected: ArrayRef = Arc::new(Float64Array::from(vec![6.0]));
        assert_full(&variable, expected).await;
    }

    #[tokio::test]
    async fn an_int64_max_fill_is_null() {
        let variable = variable(
            &[2],
            &[2],
            json!({"_FillValue": 9223372036854775807i64}),
            vec![1i64, i64::MAX],
        )
        .await;
        let expected: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), None]));
        assert_full(&variable, expected).await;
    }

    #[tokio::test]
    async fn a_uint64_max_fill_is_null() {
        let variable = variable(
            &[2],
            &[2],
            json!({"_FillValue": 18446744073709551615u64}),
            vec![1u64, u64::MAX],
        )
        .await;
        let expected: ArrayRef = Arc::new(UInt64Array::from(vec![Some(1), None]));
        assert_full(&variable, expected).await;
    }

    #[tokio::test]
    async fn a_fill_of_another_type_is_ignored_on_a_bool_variable() {
        let flag = variable(&[2], &[2], json!({"_FillValue": 0}), vec![true, false]).await;
        let expected: ArrayRef = Arc::new(BooleanArray::from(vec![Some(true), Some(false)]));
        assert_full(&flag, expected).await;
    }

    #[tokio::test]
    async fn a_time_variable_is_timestamp_micros() {
        let variable = variable(
            &[2],
            &[2],
            json!({"units": "days since 1970-01-01"}),
            vec![1.0f64, f64::NAN],
        )
        .await;
        assert_eq!(
            variable.data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        let expected: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![
            Some(86_400_000_000),
            None,
        ]));
        assert_full(&variable, expected).await;
    }

    #[tokio::test]
    async fn the_attributes_keep_the_cf_keys() {
        let variable = packed().await;
        assert!(variable.attributes().contains("scale_factor"));
        assert!(variable.attributes().contains("add_offset"));
    }

    /// The decode reads the exact fill; the attributes keep the JSON number.
    #[tokio::test]
    async fn the_attributes_keep_an_int64_fill_as_float64() {
        let variable = variable(
            &[1],
            &[1],
            json!({"_FillValue": 9223372036854775807i64}),
            vec![1i64],
        )
        .await;
        let fill = variable.attributes().get("_FillValue").unwrap();
        assert_eq!(fill.data_type(), &DataType::Float64);
    }

    #[tokio::test]
    async fn the_chunk_shape_is_the_shape_of_chunk_0() {
        let variable = variable(&[4, 4], &[2, 2], json!({}), grid()).await;
        assert_eq!(variable.chunk_shape(), Some(&[2, 2][..]));
    }

    #[test]
    fn an_unsupported_type_is_refused() {
        let array = ArrayBuilder::new(
            vec![2],
            vec![2],
            data_type::complex64(),
            FillValue::from([0u8; 8]),
        )
        .build(TestStore::new().storage().inner(), "/c")
        .unwrap();
        let dimensions = Dimensions::try_new([Dimension::new("d0", 2)]).unwrap();
        let reason = ZarrVariable::try_new(Arc::new(array), "c", dimensions).unwrap_err();
        assert!(reason.contains("complex64"), "{reason}");
    }
}
