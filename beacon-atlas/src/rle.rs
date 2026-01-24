//! ```text
//! Encoding follows:
//!     rle_col: struct<
//!                value: dictionary<int32, utf8>,
//!                run_len: int32
//!              >
//!
//!     Semantics:
//!     - Each struct row is one run
//!     - Expanded length = sum(run_len)
//! ```

use std::{collections::HashMap, hash::Hash, mem, slice, sync::Arc};

use anyhow::{Result, anyhow};
use arrow::{
    array::{
        Array, ArrayRef, BinaryArray, DictionaryArray, Int32Array, PrimitiveArray, Scalar,
        StringArray, StructArray,
    },
    datatypes::{DataType, Field, Int32Type},
};

pub const RLE_EXTENSION_NAME: &str = "atlas.rle_dict";

pub fn rle_struct_type(value_type: &DataType) -> DataType {
    DataType::Struct(
        vec![
            Field::new(
                "value",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(value_type.clone())),
                true,
            ),
            Field::new("run_len", DataType::Int32, false),
        ]
        .into(),
    )
}

pub fn rle_extension_field(name: &str, value_type: &DataType) -> Field {
    let mut metadata = HashMap::new();
    metadata.insert(
        "ARROW:extension:name".to_string(),
        RLE_EXTENSION_NAME.to_string(),
    );
    Field::new(name, rle_struct_type(value_type), false).with_metadata(metadata)
}

pub fn is_rle_extension_field(field: &Field) -> bool {
    field
        .metadata()
        .get("ARROW:extension:name")
        .is_some_and(|name| name == RLE_EXTENSION_NAME)
}

#[derive(Debug)]
pub struct RleDictArray {
    array: StructArray,
    run_ends: Vec<usize>,
    logical_len: usize,
}

impl RleDictArray {
    pub fn try_new(array: StructArray) -> Result<Self> {
        let run_len_array = array
            .column_by_name("run_len")
            .ok_or_else(|| anyhow!("missing run_len field"))?
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| anyhow!("run_len field must be Int32Array"))?;

        let mut run_ends = Vec::with_capacity(run_len_array.len());
        let mut logical_len: usize = 0;
        for i in 0..run_len_array.len() {
            if run_len_array.is_null(i) {
                return Err(anyhow!("run_len contains null at index {}", i));
            }
            let len = run_len_array.value(i);
            if len < 0 {
                return Err(anyhow!("run_len must be non-negative, got {}", len));
            }
            let len = len as usize;
            logical_len = logical_len
                .checked_add(len)
                .ok_or_else(|| anyhow!("logical length overflow"))?;
            run_ends.push(logical_len);
        }

        Ok(Self {
            array,
            run_ends,
            logical_len,
        })
    }

    pub fn array(&self) -> &StructArray {
        &self.array
    }

    pub fn logical_len(&self) -> usize {
        self.logical_len
    }

    pub fn physical_len(&self) -> usize {
        self.array.len()
    }

    pub fn run_ends(&self) -> &[usize] {
        &self.run_ends
    }

    pub fn dictionary_array(&self) -> Result<&DictionaryArray<Int32Type>> {
        self.array
            .column_by_name("value")
            .ok_or_else(|| anyhow!("missing value field"))?
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .ok_or_else(|| anyhow!("value field must be DictionaryArray<Int32Type>"))
    }

    pub fn dictionary_key_at(&self, logical_index: usize) -> Result<Option<i32>> {
        if logical_index >= self.logical_len {
            return Ok(None);
        }
        let run_index = self
            .run_ends
            .binary_search(&(logical_index + 1))
            .unwrap_or_else(|idx| idx);
        let dict = self.dictionary_array()?;
        let keys = dict.keys();
        if keys.is_null(run_index) {
            Ok(None)
        } else {
            Ok(Some(keys.value(run_index)))
        }
    }

    pub fn value(&self, logical_index: usize) -> Result<Option<Scalar<ArrayRef>>> {
        let Some(key) = self.dictionary_key_at(logical_index)? else {
            return Ok(None);
        };
        let dict = self.dictionary_array()?;
        let values = dict.values();
        let idx = key
            .try_into()
            .map_err(|_| anyhow!("dictionary key out of range: {}", key))?;
        let value_array = values.slice(idx, 1);
        Ok(Some(Scalar::new(value_array)))
    }
}
#[derive(Debug, Clone, Copy)]
struct Value<T>(T);

impl<T: Copy> Value<T> {
    fn as_bytes(&self) -> &[u8] {
        unsafe { slice::from_raw_parts((&self.0 as *const T) as *const u8, mem::size_of::<T>()) }
    }
}

impl<T: Copy> Hash for Value<T> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.as_bytes().hash(state)
    }
}

impl<T: Copy> PartialEq for Value<T> {
    fn eq(&self, other: &Self) -> bool {
        self.as_bytes().eq(other.as_bytes())
    }
}

impl<T: Copy> Eq for Value<T> {}

/// Dictionary builder for primitive values that tracks dictionary byte size.
///
/// Each appended value is dictionary-encoded using a simple in-memory hash map,
/// and the resulting dictionary size in bytes can be retrieved via
/// `dictionary_bytes()`.
#[derive(Debug)]
pub struct PrimitiveDictionaryTrackerBuilder<T: arrow::array::ArrowPrimitiveType>
where
    T::Native: Copy,
{
    values: Vec<T::Native>,
    keys: Vec<Option<i32>>,
    map: HashMap<Value<T::Native>, i32>,
    dict_bytes: usize,
}

impl<T: arrow::array::ArrowPrimitiveType> PrimitiveDictionaryTrackerBuilder<T>
where
    T::Native: Copy,
{
    /// Create a new empty dictionary builder.
    pub fn new() -> Self {
        Self {
            values: Vec::new(),
            keys: Vec::new(),
            map: HashMap::new(),
            dict_bytes: 0,
        }
    }

    /// Append a non-null value (element-by-element).
    pub fn append_value(&mut self, value: T::Native) {
        let key = match self.map.get(&Value(value)) {
            Some(existing) => *existing,
            None => {
                let idx = self.values.len() as i32;
                self.values.push(value);
                self.map.insert(Value(value), idx);
                self.dict_bytes += mem::size_of::<T::Native>();
                idx
            }
        };
        self.keys.push(Some(key));
    }

    /// Append a null value (element-by-element).
    pub fn append_null(&mut self) {
        self.keys.push(None);
    }

    /// Current dictionary size in bytes (unique values only).
    pub fn dictionary_bytes(&self) -> usize {
        self.dict_bytes
    }

    /// Number of appended elements (including nulls).
    pub fn len(&self) -> usize {
        self.keys.len()
    }

    /// Finish and return a dictionary array of the appended elements.
    pub fn finish(self) -> Result<DictionaryArray<Int32Type>> {
        let keys = Int32Array::from(self.keys);
        let values = PrimitiveArray::<T>::from_iter_values(self.values);
        DictionaryArray::<Int32Type>::try_new(keys, Arc::new(values)).map_err(|err| anyhow!(err))
    }
}

/// Element-by-element primitive RLE builder.
///
/// Usage pattern:
/// 1. Append values and nulls individually.
/// 2. Call `finish()` to build a struct array with dictionary-encoded values
///    and run lengths.
#[derive(Debug)]
pub struct PrimitiveRleStructBuilder<T: arrow::array::ArrowPrimitiveType>
where
    T::Native: Copy,
{
    dict_builder: PrimitiveDictionaryTrackerBuilder<T>,
}

/// Dictionary builder for binary values that tracks dictionary byte size.
///
/// Values are stored as owned `Vec<u8>` entries and dictionary size is the sum
/// of unique byte lengths.
#[derive(Debug)]
pub struct BytesDictionaryTrackerBuilder {
    values: Vec<Vec<u8>>,
    keys: Vec<Option<i32>>,
    map: HashMap<Vec<u8>, i32>,
    dict_bytes: usize,
}

impl BytesDictionaryTrackerBuilder {
    /// Create a new empty dictionary builder.
    pub fn new() -> Self {
        Self {
            values: Vec::new(),
            keys: Vec::new(),
            map: HashMap::new(),
            dict_bytes: 0,
        }
    }

    /// Append a non-null value (element-by-element).
    pub fn append_value(&mut self, value: &[u8]) {
        let key = match self.map.get(value) {
            Some(existing) => *existing,
            None => {
                let idx = self.values.len() as i32;
                let owned = value.to_vec();
                self.values.push(owned.clone());
                self.map.insert(owned, idx);
                self.dict_bytes += value.len();
                idx
            }
        };
        self.keys.push(Some(key));
    }

    /// Append a null value (element-by-element).
    pub fn append_null(&mut self) {
        self.keys.push(None);
    }

    /// Current dictionary size in bytes (unique values only).
    pub fn dictionary_bytes(&self) -> usize {
        self.dict_bytes
    }

    /// Number of appended elements (including nulls).
    pub fn len(&self) -> usize {
        self.keys.len()
    }

    /// Finish and return a dictionary array of the appended elements.
    pub fn finish(self) -> Result<DictionaryArray<Int32Type>> {
        let keys = Int32Array::from(self.keys);
        let values = BinaryArray::from_iter_values(self.values.iter().map(|v| v.as_slice()));
        DictionaryArray::<Int32Type>::try_new(keys, Arc::new(values)).map_err(|err| anyhow!(err))
    }
}

/// Element-by-element RLE builder for generic byte values.
///
/// Usage pattern:
/// 1. Append values and nulls individually.
/// 2. Call `finish()` to build a struct array with dictionary-encoded values
///    and run lengths.
#[derive(Debug)]
pub struct BytesRleStructBuilder {
    dict_builder: BytesDictionaryTrackerBuilder,
}

impl BytesRleStructBuilder {
    /// Create a new empty builder.
    pub fn new() -> Self {
        Self {
            dict_builder: BytesDictionaryTrackerBuilder::new(),
        }
    }

    /// Append a non-null value (element-by-element).
    pub fn append_value(&mut self, value: &[u8]) {
        self.dict_builder.append_value(value);
    }

    /// Append a null value (element-by-element).
    pub fn append_null(&mut self) {
        self.dict_builder.append_null();
    }

    /// Current dictionary size in bytes (unique values only).
    pub fn dictionary_bytes(&self) -> usize {
        self.dict_builder.dictionary_bytes()
    }

    /// Number of appended elements (including nulls).
    pub fn len(&self) -> usize {
        self.dict_builder.len()
    }

    /// Finish and return the dictionary array without RLE.
    pub fn finish_dictionary(self) -> Result<DictionaryArray<Int32Type>> {
        self.dict_builder.finish()
    }

    /// Finish and return the RLE struct array.
    pub fn finish(self) -> Result<StructArray> {
        let dictionary = self.dict_builder.finish()?;
        Self::rle_from_dictionary(dictionary)
    }

    /// Build an RLE struct array from an existing dictionary array.
    pub fn rle_from_dictionary(dictionary: DictionaryArray<Int32Type>) -> Result<StructArray> {
        let keys = dictionary.keys();
        let mut run_keys: Vec<Option<i32>> = Vec::new();
        let mut run_lengths: Vec<i32> = Vec::new();

        let mut current: Option<i32> = None;
        let mut current_len: i64 = 0;

        for key in keys.iter() {
            if current_len == 0 {
                current = key;
                current_len = 1;
                continue;
            }

            if key == current {
                current_len += 1;
            } else {
                let run_len = i32::try_from(current_len)
                    .map_err(|_| anyhow!("run length overflow: {}", current_len))?;
                run_keys.push(current);
                run_lengths.push(run_len);
                current = key;
                current_len = 1;
            }
        }

        if current_len > 0 {
            let run_len = i32::try_from(current_len)
                .map_err(|_| anyhow!("run length overflow: {}", current_len))?;
            run_keys.push(current);
            run_lengths.push(run_len);
        }

        let run_keys_array = Int32Array::from(run_keys);
        let values = dictionary.values().clone();
        let run_dict = DictionaryArray::<Int32Type>::try_new(run_keys_array, values)
            .map_err(|err| anyhow!(err))?;

        let run_len_array = Int32Array::from(run_lengths);

        let fields = vec![
            Field::new(
                "value",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Binary)),
                true,
            ),
            Field::new("run_len", DataType::Int32, false),
        ];

        Ok(StructArray::new(
            fields.into(),
            vec![Arc::new(run_dict) as ArrayRef, Arc::new(run_len_array)],
            None,
        ))
    }
}

/// Dictionary builder for UTF-8 string values that tracks dictionary byte size.
///
/// Values are stored as owned `String` entries and dictionary size is the sum
/// of unique UTF-8 byte lengths.
#[derive(Debug)]
pub struct StringDictionaryTrackerBuilder {
    values: Vec<String>,
    keys: Vec<Option<i32>>,
    map: HashMap<String, i32>,
    dict_bytes: usize,
}

impl StringDictionaryTrackerBuilder {
    /// Create a new empty dictionary builder.
    pub fn new() -> Self {
        Self {
            values: Vec::new(),
            keys: Vec::new(),
            map: HashMap::new(),
            dict_bytes: 0,
        }
    }

    /// Append a non-null value (element-by-element).
    pub fn append_value(&mut self, value: &str) {
        let key = match self.map.get(value) {
            Some(existing) => *existing,
            None => {
                let idx = self.values.len() as i32;
                let owned = value.to_owned();
                self.values.push(owned.clone());
                self.map.insert(owned, idx);
                self.dict_bytes += value.as_bytes().len();
                idx
            }
        };
        self.keys.push(Some(key));
    }

    /// Append a null value (element-by-element).
    pub fn append_null(&mut self) {
        self.keys.push(None);
    }

    /// Current dictionary size in bytes (unique values only).
    pub fn dictionary_bytes(&self) -> usize {
        self.dict_bytes
    }

    /// Number of appended elements (including nulls).
    pub fn len(&self) -> usize {
        self.keys.len()
    }

    /// Finish and return a dictionary array of the appended elements.
    pub fn finish(self) -> Result<DictionaryArray<Int32Type>> {
        let keys = Int32Array::from(self.keys);
        let values = StringArray::from(self.values);
        DictionaryArray::<Int32Type>::try_new(keys, Arc::new(values)).map_err(|err| anyhow!(err))
    }
}

/// Element-by-element RLE builder for UTF-8 string values.
///
/// Usage pattern:
/// 1. Append values and nulls individually.
/// 2. Call `finish()` to build a struct array with dictionary-encoded values
///    and run lengths.
#[derive(Debug)]
pub struct StringRleStructBuilder {
    dict_builder: StringDictionaryTrackerBuilder,
}

impl StringRleStructBuilder {
    /// Create a new empty builder.
    pub fn new() -> Self {
        Self {
            dict_builder: StringDictionaryTrackerBuilder::new(),
        }
    }

    /// Append a non-null value (element-by-element).
    pub fn append_value(&mut self, value: &str) {
        self.dict_builder.append_value(value);
    }

    /// Append a null value (element-by-element).
    pub fn append_null(&mut self) {
        self.dict_builder.append_null();
    }

    /// Current dictionary size in bytes (unique values only).
    pub fn dictionary_bytes(&self) -> usize {
        self.dict_builder.dictionary_bytes()
    }

    /// Number of appended elements (including nulls).
    pub fn len(&self) -> usize {
        self.dict_builder.len()
    }

    /// Finish and return the dictionary array without RLE.
    pub fn finish_dictionary(self) -> Result<DictionaryArray<Int32Type>> {
        self.dict_builder.finish()
    }

    /// Finish and return the RLE struct array.
    pub fn finish(self) -> Result<StructArray> {
        let dictionary = self.dict_builder.finish()?;
        Self::rle_from_dictionary(dictionary)
    }

    /// Build an RLE struct array from an existing dictionary array.
    pub fn rle_from_dictionary(dictionary: DictionaryArray<Int32Type>) -> Result<StructArray> {
        let keys = dictionary.keys();
        let mut run_keys: Vec<Option<i32>> = Vec::new();
        let mut run_lengths: Vec<i32> = Vec::new();

        let mut current: Option<i32> = None;
        let mut current_len: i64 = 0;

        for key in keys.iter() {
            if current_len == 0 {
                current = key;
                current_len = 1;
                continue;
            }

            if key == current {
                current_len += 1;
            } else {
                let run_len = i32::try_from(current_len)
                    .map_err(|_| anyhow!("run length overflow: {}", current_len))?;
                run_keys.push(current);
                run_lengths.push(run_len);
                current = key;
                current_len = 1;
            }
        }

        if current_len > 0 {
            let run_len = i32::try_from(current_len)
                .map_err(|_| anyhow!("run length overflow: {}", current_len))?;
            run_keys.push(current);
            run_lengths.push(run_len);
        }

        let run_keys_array = Int32Array::from(run_keys);
        let values = dictionary.values().clone();
        let run_dict = DictionaryArray::<Int32Type>::try_new(run_keys_array, values)
            .map_err(|err| anyhow!(err))?;

        let run_len_array = Int32Array::from(run_lengths);

        let fields = vec![
            Field::new(
                "value",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                true,
            ),
            Field::new("run_len", DataType::Int32, false),
        ];

        Ok(StructArray::new(
            fields.into(),
            vec![Arc::new(run_dict) as ArrayRef, Arc::new(run_len_array)],
            None,
        ))
    }
}

impl<T: arrow::array::ArrowPrimitiveType> PrimitiveRleStructBuilder<T>
where
    T::Native: Copy,
{
    /// Create a new empty builder.
    pub fn new() -> Self {
        Self {
            dict_builder: PrimitiveDictionaryTrackerBuilder::new(),
        }
    }

    /// Append a non-null value (element-by-element).
    pub fn append_value(&mut self, value: T::Native) {
        self.dict_builder.append_value(value);
    }

    /// Append a null value (element-by-element).
    pub fn append_null(&mut self) {
        self.dict_builder.append_null();
    }

    /// Current dictionary size in bytes (unique values only).
    pub fn dictionary_bytes(&self) -> usize {
        self.dict_builder.dictionary_bytes()
    }

    /// Number of appended elements (including nulls).
    pub fn len(&self) -> usize {
        self.dict_builder.len()
    }

    /// Finish and return the dictionary array without RLE.
    pub fn finish_dictionary(self) -> Result<DictionaryArray<Int32Type>> {
        self.dict_builder.finish()
    }

    /// Finish and return the RLE struct array.
    pub fn finish(self) -> Result<StructArray> {
        let dictionary = self.dict_builder.finish()?;
        Self::rle_from_dictionary(dictionary)
    }

    /// Build an RLE struct array from an existing dictionary array.
    pub fn rle_from_dictionary(dictionary: DictionaryArray<Int32Type>) -> Result<StructArray> {
        let keys = dictionary.keys();
        let mut run_keys: Vec<Option<i32>> = Vec::new();
        let mut run_lengths: Vec<i32> = Vec::new();

        let mut current: Option<i32> = None;
        let mut current_len: i64 = 0;

        for key in keys.iter() {
            if current_len == 0 {
                current = key;
                current_len = 1;
                continue;
            }

            if key == current {
                current_len += 1;
            } else {
                let run_len = i32::try_from(current_len)
                    .map_err(|_| anyhow!("run length overflow: {}", current_len))?;
                run_keys.push(current);
                run_lengths.push(run_len);
                current = key;
                current_len = 1;
            }
        }

        if current_len > 0 {
            let run_len = i32::try_from(current_len)
                .map_err(|_| anyhow!("run length overflow: {}", current_len))?;
            run_keys.push(current);
            run_lengths.push(run_len);
        }

        let run_keys_array = Int32Array::from(run_keys);
        let values = dictionary.values().clone();
        let run_dict = DictionaryArray::<Int32Type>::try_new(run_keys_array, values)
            .map_err(|err| anyhow!(err))?;

        let run_len_array = Int32Array::from(run_lengths);

        let fields = vec![
            Field::new(
                "value",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(T::DATA_TYPE)),
                true,
            ),
            Field::new("run_len", DataType::Int32, false),
        ];

        Ok(StructArray::new(
            fields.into(),
            vec![Arc::new(run_dict) as ArrayRef, Arc::new(run_len_array)],
            None,
        ))
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{Array, DictionaryArray};

    use super::*;

    #[test]
    fn build_primitive_rle_struct() {
        let mut builder = PrimitiveRleStructBuilder::<arrow::datatypes::Int32Type>::new();
        builder.append_value(10);
        builder.append_value(10);
        builder.append_null();
        builder.append_value(20);
        builder.append_value(20);

        assert_eq!(builder.dictionary_bytes(), 8);

        let struct_array = builder.finish().unwrap();
        assert_eq!(struct_array.len(), 3);

        let value_array = struct_array
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .unwrap();
        let run_len_array = struct_array
            .column_by_name("run_len")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();

        assert_eq!(run_len_array.value(0), 2);
        assert_eq!(run_len_array.value(1), 1);
        assert_eq!(run_len_array.value(2), 2);

        assert_eq!(value_array.values().len(), 2);
    }

    #[test]
    fn build_primitive_rle_struct_int64() {
        let mut builder = PrimitiveRleStructBuilder::<arrow::datatypes::Int64Type>::new();
        builder.append_value(5);
        builder.append_value(5);
        builder.append_value(7);
        builder.append_null();
        builder.append_null();

        assert_eq!(builder.dictionary_bytes(), 16);

        let struct_array = builder.finish().unwrap();
        assert_eq!(struct_array.len(), 3);

        let run_len_array = struct_array
            .column_by_name("run_len")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(run_len_array.value(0), 2);
        assert_eq!(run_len_array.value(1), 1);
        assert_eq!(run_len_array.value(2), 2);
    }

    #[test]
    fn build_primitive_rle_struct_float64() {
        let mut builder = PrimitiveRleStructBuilder::<arrow::datatypes::Float64Type>::new();
        builder.append_value(1.5);
        builder.append_value(1.5);
        builder.append_value(-2.0);
        builder.append_value(-2.0);
        builder.append_value(-2.0);

        assert_eq!(builder.dictionary_bytes(), 16);

        let struct_array = builder.finish().unwrap();
        assert_eq!(struct_array.len(), 2);

        let run_len_array = struct_array
            .column_by_name("run_len")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(run_len_array.value(0), 2);
        assert_eq!(run_len_array.value(1), 3);
    }

    #[test]
    fn build_primitive_rle_struct_uint8() {
        let mut builder = PrimitiveRleStructBuilder::<arrow::datatypes::UInt8Type>::new();
        builder.append_value(1);
        builder.append_null();
        builder.append_value(2);
        builder.append_value(2);
        builder.append_value(2);

        assert_eq!(builder.dictionary_bytes(), 2);

        let struct_array = builder.finish().unwrap();
        assert_eq!(struct_array.len(), 3);

        let run_len_array = struct_array
            .column_by_name("run_len")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(run_len_array.value(0), 1);
        assert_eq!(run_len_array.value(1), 1);
        assert_eq!(run_len_array.value(2), 3);
    }

    #[test]
    fn build_bytes_rle_struct() {
        let mut builder = BytesRleStructBuilder::new();
        builder.append_value(b"aa");
        builder.append_value(b"aa");
        builder.append_null();
        builder.append_value(b"bb");
        builder.append_value(b"bb");

        assert_eq!(builder.dictionary_bytes(), 4);

        let struct_array = builder.finish().unwrap();
        assert_eq!(struct_array.len(), 3);

        let run_len_array = struct_array
            .column_by_name("run_len")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(run_len_array.value(0), 2);
        assert_eq!(run_len_array.value(1), 1);
        assert_eq!(run_len_array.value(2), 2);
    }

    #[test]
    fn build_string_rle_struct() {
        let mut builder = StringRleStructBuilder::new();
        builder.append_value("aa");
        builder.append_value("aa");
        builder.append_null();
        builder.append_value("bb");
        builder.append_value("bb");

        assert_eq!(builder.dictionary_bytes(), 4);

        let struct_array = builder.finish().unwrap();
        assert_eq!(struct_array.len(), 3);

        let run_len_array = struct_array
            .column_by_name("run_len")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(run_len_array.value(0), 2);
        assert_eq!(run_len_array.value(1), 1);
        assert_eq!(run_len_array.value(2), 2);
    }

    #[test]
    fn rle_dict_array_logical_len_and_key_access() {
        let mut builder = PrimitiveRleStructBuilder::<arrow::datatypes::Int32Type>::new();
        builder.append_value(10);
        builder.append_value(10);
        builder.append_null();
        builder.append_value(20);
        builder.append_value(20);

        let struct_array = builder.finish().unwrap();
        let rle = RleDictArray::try_new(struct_array).unwrap();
        assert_eq!(rle.physical_len(), 3);
        assert_eq!(rle.logical_len(), 5);
        assert!(rle.dictionary_key_at(0).unwrap().is_some());
        assert!(rle.dictionary_key_at(1).unwrap().is_some());
        assert!(rle.dictionary_key_at(2).unwrap().is_none());
        assert!(rle.dictionary_key_at(4).unwrap().is_some());

        let scalar = rle.value(0).unwrap().unwrap().into_inner();
        let scalar_array = scalar.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(scalar_array.value(0), 10);
        assert!(rle.value(2).unwrap().is_none());
        assert_eq!(
            rle.value(4)
                .unwrap()
                .unwrap()
                .into_inner()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            20
        );
    }

    #[test]
    fn rle_dict_array_out_of_range_value() {
        let mut builder = PrimitiveRleStructBuilder::<arrow::datatypes::Int32Type>::new();
        builder.append_value(1);
        builder.append_value(1);
        let struct_array = builder.finish().unwrap();
        let rle = RleDictArray::try_new(struct_array).unwrap();
        assert!(rle.value(2).unwrap().is_none());
    }

    #[test]
    fn rle_dict_array_empty() {
        let struct_array = BytesRleStructBuilder::new().finish().unwrap();
        let rle = RleDictArray::try_new(struct_array).unwrap();
        assert_eq!(rle.logical_len(), 0);
        assert_eq!(rle.physical_len(), 0);
        assert!(rle.value(0).unwrap().is_none());
    }

    #[test]
    fn rle_dict_array_string_value() {
        let mut builder = StringRleStructBuilder::new();
        builder.append_value("x");
        builder.append_value("x");
        let struct_array = builder.finish().unwrap();
        let rle = RleDictArray::try_new(struct_array).unwrap();
        let scalar = rle.value(0).unwrap().unwrap().into_inner();
        let scalar_array = scalar.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(scalar_array.value(0), "x");
    }
}
