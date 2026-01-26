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

use std::io::{Read, Seek, SeekFrom};
use std::sync::Arc;

use anyhow::{Result, anyhow, bail};
use arrow::{
    array::{
        Array, ArrayRef, BinaryArray, Float32Array, Float64Array, Int8Array, Int16Array,
        Int32Array, Int64Array, StringArray, StructArray, UInt8Array, UInt16Array, UInt32Array,
        UInt64Array,
    },
    datatypes::{DataType, Schema},
    ipc::{reader::FileReader, writer::FileWriter},
    record_batch::RecordBatch,
};
use object_store::ObjectStore;
use tempfile::SpooledTempFile;

use crate::IPC_WRITE_OPTS;
use crate::rle::{
    BytesRleStructBuilder, PrimitiveRleStructBuilder, RleDictArray, StringRleStructBuilder,
    is_rle_extension_field, rle_extension_field,
};

const BATCH_SIZE: usize = 4 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq)]
pub enum AttributeValue {
    Null,
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    Float32(f32),
    Float64(f64),
    Binary(Vec<u8>),
    Utf8(String),
}

impl AttributeValue {
    pub fn arrow_datatype(&self) -> DataType {
        match self {
            AttributeValue::Null => DataType::Null,
            AttributeValue::Int8(_) => DataType::Int8,
            AttributeValue::Int16(_) => DataType::Int16,
            AttributeValue::Int32(_) => DataType::Int32,
            AttributeValue::Int64(_) => DataType::Int64,
            AttributeValue::UInt8(_) => DataType::UInt8,
            AttributeValue::UInt16(_) => DataType::UInt16,
            AttributeValue::UInt32(_) => DataType::UInt32,
            AttributeValue::UInt64(_) => DataType::UInt64,
            AttributeValue::Float32(_) => DataType::Float32,
            AttributeValue::Float64(_) => DataType::Float64,
            AttributeValue::Binary(_) => DataType::Binary,
            AttributeValue::Utf8(_) => DataType::Utf8,
        }
    }
}

impl From<()> for AttributeValue {
    fn from(_: ()) -> Self {
        AttributeValue::Null
    }
}

impl From<i8> for AttributeValue {
    fn from(value: i8) -> Self {
        AttributeValue::Int8(value)
    }
}

impl From<i16> for AttributeValue {
    fn from(value: i16) -> Self {
        AttributeValue::Int16(value)
    }
}

impl From<i32> for AttributeValue {
    fn from(value: i32) -> Self {
        AttributeValue::Int32(value)
    }
}

impl From<i64> for AttributeValue {
    fn from(value: i64) -> Self {
        AttributeValue::Int64(value)
    }
}

impl From<u8> for AttributeValue {
    fn from(value: u8) -> Self {
        AttributeValue::UInt8(value)
    }
}

impl From<u16> for AttributeValue {
    fn from(value: u16) -> Self {
        AttributeValue::UInt16(value)
    }
}

impl From<u32> for AttributeValue {
    fn from(value: u32) -> Self {
        AttributeValue::UInt32(value)
    }
}

impl From<u64> for AttributeValue {
    fn from(value: u64) -> Self {
        AttributeValue::UInt64(value)
    }
}

impl From<f32> for AttributeValue {
    fn from(value: f32) -> Self {
        AttributeValue::Float32(value)
    }
}

impl From<f64> for AttributeValue {
    fn from(value: f64) -> Self {
        AttributeValue::Float64(value)
    }
}

impl From<Vec<u8>> for AttributeValue {
    fn from(value: Vec<u8>) -> Self {
        AttributeValue::Binary(value)
    }
}

impl From<&[u8]> for AttributeValue {
    fn from(value: &[u8]) -> Self {
        AttributeValue::Binary(value.to_vec())
    }
}

impl From<String> for AttributeValue {
    fn from(value: String) -> Self {
        AttributeValue::Utf8(value)
    }
}

impl From<&str> for AttributeValue {
    fn from(value: &str) -> Self {
        AttributeValue::Utf8(value.to_string())
    }
}

#[derive(Debug)]
enum RleBuilder {
    Int8(PrimitiveRleStructBuilder<arrow::datatypes::Int8Type>),
    Int16(PrimitiveRleStructBuilder<arrow::datatypes::Int16Type>),
    Int32(PrimitiveRleStructBuilder<arrow::datatypes::Int32Type>),
    Int64(PrimitiveRleStructBuilder<arrow::datatypes::Int64Type>),
    UInt8(PrimitiveRleStructBuilder<arrow::datatypes::UInt8Type>),
    UInt16(PrimitiveRleStructBuilder<arrow::datatypes::UInt16Type>),
    UInt32(PrimitiveRleStructBuilder<arrow::datatypes::UInt32Type>),
    UInt64(PrimitiveRleStructBuilder<arrow::datatypes::UInt64Type>),
    Float32(PrimitiveRleStructBuilder<arrow::datatypes::Float32Type>),
    Float64(PrimitiveRleStructBuilder<arrow::datatypes::Float64Type>),
    Binary(BytesRleStructBuilder),
    Utf8(StringRleStructBuilder),
}

impl RleBuilder {
    fn new(data_type: &DataType) -> Result<Self> {
        match data_type {
            DataType::Int8 => Ok(Self::Int8(PrimitiveRleStructBuilder::new())),
            DataType::Int16 => Ok(Self::Int16(PrimitiveRleStructBuilder::new())),
            DataType::Int32 => Ok(Self::Int32(PrimitiveRleStructBuilder::new())),
            DataType::Int64 => Ok(Self::Int64(PrimitiveRleStructBuilder::new())),
            DataType::UInt8 => Ok(Self::UInt8(PrimitiveRleStructBuilder::new())),
            DataType::UInt16 => Ok(Self::UInt16(PrimitiveRleStructBuilder::new())),
            DataType::UInt32 => Ok(Self::UInt32(PrimitiveRleStructBuilder::new())),
            DataType::UInt64 => Ok(Self::UInt64(PrimitiveRleStructBuilder::new())),
            DataType::Float32 => Ok(Self::Float32(PrimitiveRleStructBuilder::new())),
            DataType::Float64 => Ok(Self::Float64(PrimitiveRleStructBuilder::new())),
            DataType::Binary => Ok(Self::Binary(BytesRleStructBuilder::new())),
            DataType::Utf8 => Ok(Self::Utf8(StringRleStructBuilder::new())),
            other => bail!("Unsupported datatype for AttributeWriter: {:?}", other),
        }
    }

    fn append_value(&mut self, value: AttributeValue) -> Result<()> {
        match value {
            AttributeValue::Null => self.append_null(),
            AttributeValue::Int8(v) => match self {
                Self::Int8(b) => {
                    b.append_value(v);
                    Ok(())
                }
                _ => bail!("AttributeWriter value type does not match configured datatype"),
            },
            AttributeValue::Int16(v) => match self {
                Self::Int16(b) => {
                    b.append_value(v);
                    Ok(())
                }
                _ => bail!("AttributeWriter value type does not match configured datatype"),
            },
            AttributeValue::Int32(v) => match self {
                Self::Int32(b) => {
                    b.append_value(v);
                    Ok(())
                }
                _ => bail!("AttributeWriter value type does not match configured datatype"),
            },
            AttributeValue::Int64(v) => match self {
                Self::Int64(b) => {
                    b.append_value(v);
                    Ok(())
                }
                _ => bail!("AttributeWriter value type does not match configured datatype"),
            },
            AttributeValue::UInt8(v) => match self {
                Self::UInt8(b) => {
                    b.append_value(v);
                    Ok(())
                }
                _ => bail!("AttributeWriter value type does not match configured datatype"),
            },
            AttributeValue::UInt16(v) => match self {
                Self::UInt16(b) => {
                    b.append_value(v);
                    Ok(())
                }
                _ => bail!("AttributeWriter value type does not match configured datatype"),
            },
            AttributeValue::UInt32(v) => match self {
                Self::UInt32(b) => {
                    b.append_value(v);
                    Ok(())
                }
                _ => bail!("AttributeWriter value type does not match configured datatype"),
            },
            AttributeValue::UInt64(v) => match self {
                Self::UInt64(b) => {
                    b.append_value(v);
                    Ok(())
                }
                _ => bail!("AttributeWriter value type does not match configured datatype"),
            },
            AttributeValue::Float32(v) => match self {
                Self::Float32(b) => {
                    b.append_value(v);
                    Ok(())
                }
                _ => bail!("AttributeWriter value type does not match configured datatype"),
            },
            AttributeValue::Float64(v) => match self {
                Self::Float64(b) => {
                    b.append_value(v);
                    Ok(())
                }
                _ => bail!("AttributeWriter value type does not match configured datatype"),
            },
            AttributeValue::Binary(v) => match self {
                Self::Binary(b) => {
                    b.append_value(&v);
                    Ok(())
                }
                _ => bail!("AttributeWriter value type does not match configured datatype"),
            },
            AttributeValue::Utf8(v) => match self {
                Self::Utf8(b) => {
                    b.append_value(&v);
                    Ok(())
                }
                _ => bail!("AttributeWriter value type does not match configured datatype"),
            },
        }
    }

    fn append_null(&mut self) -> Result<()> {
        match self {
            Self::Int8(b) => b.append_null(),
            Self::Int16(b) => b.append_null(),
            Self::Int32(b) => b.append_null(),
            Self::Int64(b) => b.append_null(),
            Self::UInt8(b) => b.append_null(),
            Self::UInt16(b) => b.append_null(),
            Self::UInt32(b) => b.append_null(),
            Self::UInt64(b) => b.append_null(),
            Self::Float32(b) => b.append_null(),
            Self::Float64(b) => b.append_null(),
            Self::Binary(b) => b.append_null(),
            Self::Utf8(b) => b.append_null(),
        }
        Ok(())
    }

    fn dictionary_bytes(&self) -> usize {
        match self {
            Self::Int8(b) => b.dictionary_bytes(),
            Self::Int16(b) => b.dictionary_bytes(),
            Self::Int32(b) => b.dictionary_bytes(),
            Self::Int64(b) => b.dictionary_bytes(),
            Self::UInt8(b) => b.dictionary_bytes(),
            Self::UInt16(b) => b.dictionary_bytes(),
            Self::UInt32(b) => b.dictionary_bytes(),
            Self::UInt64(b) => b.dictionary_bytes(),
            Self::Float32(b) => b.dictionary_bytes(),
            Self::Float64(b) => b.dictionary_bytes(),
            Self::Binary(b) => b.dictionary_bytes(),
            Self::Utf8(b) => b.dictionary_bytes(),
        }
    }

    fn len(&self) -> usize {
        match self {
            Self::Int8(b) => b.len(),
            Self::Int16(b) => b.len(),
            Self::Int32(b) => b.len(),
            Self::Int64(b) => b.len(),
            Self::UInt8(b) => b.len(),
            Self::UInt16(b) => b.len(),
            Self::UInt32(b) => b.len(),
            Self::UInt64(b) => b.len(),
            Self::Float32(b) => b.len(),
            Self::Float64(b) => b.len(),
            Self::Binary(b) => b.len(),
            Self::Utf8(b) => b.len(),
        }
    }

    fn estimated_bytes(&self) -> usize {
        let dict_bytes = self.dictionary_bytes();
        let key_bytes = self.len() * std::mem::size_of::<i32>();
        dict_bytes + key_bytes
    }

    fn finish(self) -> Result<arrow::array::StructArray> {
        match self {
            Self::Int8(b) => b.finish(),
            Self::Int16(b) => b.finish(),
            Self::Int32(b) => b.finish(),
            Self::Int64(b) => b.finish(),
            Self::UInt8(b) => b.finish(),
            Self::UInt16(b) => b.finish(),
            Self::UInt32(b) => b.finish(),
            Self::UInt64(b) => b.finish(),
            Self::Float32(b) => b.finish(),
            Self::Float64(b) => b.finish(),
            Self::Binary(b) => b.finish(),
            Self::Utf8(b) => b.finish(),
        }
    }
}

pub struct AttributeWriter<S: ObjectStore + Clone> {
    store: S,
    path: object_store::path::Path,
    data_type: DataType,
    builder: RleBuilder,
    schema: Arc<Schema>,
    writer: FileWriter<SpooledTempFile>,
}

/// Reads RLE-encoded attribute values from object storage.
#[derive(Clone)]
pub struct AttributeReader {
    batches: Arc<Vec<RleDictArray>>,
    batch_ranges: Arc<Vec<(usize, usize)>>,
}

impl<S: ObjectStore + Clone> AttributeWriter<S> {
    pub fn new(
        store: S,
        prefix: object_store::path::Path,
        attr_name: &str,
        datatype: DataType,
    ) -> Result<Self> {
        let builder = RleBuilder::new(&datatype)?;
        let field = rle_extension_field(attr_name, &datatype);
        let schema = Arc::new(Schema::new(vec![field]));
        let file = SpooledTempFile::new(256 * 1024 * 1024);
        let writer = FileWriter::try_new_with_options(file, &schema, IPC_WRITE_OPTS.clone())
            .map_err(|err| anyhow!(err))?;

        Ok(Self {
            store,
            path: prefix.child(attr_name),
            data_type: datatype,
            builder,
            schema,
            writer,
        })
    }

    pub fn append<V: Into<AttributeValue>>(&mut self, value: V) -> Result<()> {
        self.builder.append_value(value.into())?;
        if self.builder.estimated_bytes() >= BATCH_SIZE {
            self.flush_current_batch()?;
        }
        Ok(())
    }

    pub fn append_null(&mut self) -> Result<()> {
        self.builder.append_null()?;
        if self.builder.estimated_bytes() >= BATCH_SIZE {
            self.flush_current_batch()?;
        }
        Ok(())
    }

    fn flush_current_batch(&mut self) -> Result<()> {
        if self.builder.len() == 0 {
            return Ok(());
        }

        let builder = std::mem::replace(&mut self.builder, RleBuilder::new(&self.data_type)?);
        let struct_array = builder.finish()?;
        let batch = RecordBatch::try_new(
            self.schema.clone(),
            vec![Arc::new(struct_array) as arrow::array::ArrayRef],
        )
        .map_err(|err| anyhow!(err))?;

        self.writer.write(&batch).map_err(|err| anyhow!(err))?;
        Ok(())
    }

    pub async fn finish(mut self) -> Result<()> {
        self.flush_current_batch()?;
        self.writer.finish().map_err(|err| anyhow!(err))?;
        let mut inner = self.writer.into_inner().map_err(|err| anyhow!(err))?;
        inner.seek(SeekFrom::Start(0)).map_err(|err| anyhow!(err))?;
        let mut data = Vec::new();
        inner.read_to_end(&mut data).map_err(|err| anyhow!(err))?;
        self.store
            .put(&self.path, data.into())
            .await
            .map_err(|err| anyhow!(err))?;
        Ok(())
    }
}

impl AttributeReader {
    /// Open an attribute reader for a named attribute under the given prefix.
    pub async fn open(
        store: impl ObjectStore + Clone,
        prefix: object_store::path::Path,
        attr_name: &str,
    ) -> Result<Self> {
        let path = prefix.child(attr_name);
        Self::open_from_path(store, path).await
    }

    /// Open an attribute reader if the attribute exists; returns `None` for missing paths.
    pub async fn open_optional(
        store: impl ObjectStore + Clone,
        prefix: object_store::path::Path,
        attr_name: &str,
    ) -> Result<Option<Self>> {
        let path = prefix.child(attr_name);
        let bytes = match store.get(&path).await {
            Ok(result) => result.bytes().await.map_err(|err| anyhow!(err))?,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(err) => return Err(anyhow!(err)),
        };
        Ok(Some(Self::open_from_bytes(bytes.to_vec())?))
    }

    /// Read a single attribute value at the logical index as a scalar Arrow array.
    pub fn read_index(&self, index: usize) -> Result<Option<ArrayRef>> {
        let (batch_index, start) = match self
            .batch_ranges
            .iter()
            .enumerate()
            .find(|(_, (s, e))| index >= *s && index < *e)
        {
            Some((idx, (s, _))) => (idx, *s),
            None => return Ok(None),
        };

        let rle = match self.batches.get(batch_index) {
            Some(rle) => rle,
            None => return Ok(None),
        };

        let local_index = index - start;
        let Some(key) = rle.dictionary_key_at(local_index)? else {
            let dict = rle.dictionary_array()?;
            let values = dict.values();
            return Ok(Some(arrow::array::new_null_array(values.data_type(), 1)));
        };
        let dict = rle.dictionary_array()?;
        let values = dict.values();
        let value = Self::array_from_dictionary(values, key)?;
        Ok(Some(value))
    }

    async fn open_from_path(
        store: impl ObjectStore + Clone,
        path: object_store::path::Path,
    ) -> Result<Self> {
        let bytes = store
            .get(&path)
            .await
            .map_err(|err| anyhow!(err))?
            .bytes()
            .await
            .map_err(|err| anyhow!(err))?;
        Self::open_from_bytes(bytes.to_vec())
    }

    fn open_from_bytes(bytes: Vec<u8>) -> Result<Self> {
        let reader =
            FileReader::try_new(std::io::Cursor::new(bytes), None).map_err(|err| anyhow!(err))?;
        let schema = reader.schema();
        let field = schema
            .fields()
            .first()
            .ok_or_else(|| anyhow!("attribute schema missing field"))?;
        if !is_rle_extension_field(field) {
            return Err(anyhow!("attribute field is not RLE-encoded"));
        }

        let mut batches = Vec::new();
        let mut ranges = Vec::new();
        let mut start = 0usize;

        for batch in reader {
            let batch = batch.map_err(|err| anyhow!(err))?;
            let column = batch.column(0).clone();
            let struct_array = column
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| anyhow!("attribute column must be StructArray"))?;
            let rle = RleDictArray::try_new(struct_array.clone())?;
            let end = start
                .checked_add(rle.logical_len())
                .ok_or_else(|| anyhow!("attribute logical length overflow"))?;
            ranges.push((start, end));
            batches.push(rle);
            start = end;
        }

        Ok(Self {
            batches: Arc::new(batches),
            batch_ranges: Arc::new(ranges),
        })
    }

    fn array_from_dictionary(values: &arrow::array::ArrayRef, key: i32) -> Result<ArrayRef> {
        let index = usize::try_from(key).map_err(|_| anyhow!("dictionary key out of range"))?;
        let is_valid = match values.data_type() {
            DataType::Null => return Ok(arrow::array::new_null_array(values.data_type(), 1)),
            DataType::Int8 => values
                .as_any()
                .downcast_ref::<Int8Array>()
                .ok_or_else(|| anyhow!("attribute values must be Int8"))?
                .is_valid(index),
            DataType::Int16 => values
                .as_any()
                .downcast_ref::<Int16Array>()
                .ok_or_else(|| anyhow!("attribute values must be Int16"))?
                .is_valid(index),
            DataType::Int32 => values
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| anyhow!("attribute values must be Int32"))?
                .is_valid(index),
            DataType::Int64 => values
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| anyhow!("attribute values must be Int64"))?
                .is_valid(index),
            DataType::UInt8 => values
                .as_any()
                .downcast_ref::<UInt8Array>()
                .ok_or_else(|| anyhow!("attribute values must be UInt8"))?
                .is_valid(index),
            DataType::UInt16 => values
                .as_any()
                .downcast_ref::<UInt16Array>()
                .ok_or_else(|| anyhow!("attribute values must be UInt16"))?
                .is_valid(index),
            DataType::UInt32 => values
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| anyhow!("attribute values must be UInt32"))?
                .is_valid(index),
            DataType::UInt64 => values
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| anyhow!("attribute values must be UInt64"))?
                .is_valid(index),
            DataType::Float32 => values
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| anyhow!("attribute values must be Float32"))?
                .is_valid(index),
            DataType::Float64 => values
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| anyhow!("attribute values must be Float64"))?
                .is_valid(index),
            DataType::Binary => values
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| anyhow!("attribute values must be Binary"))?
                .is_valid(index),
            DataType::Utf8 => values
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow!("attribute values must be Utf8"))?
                .is_valid(index),
            other => return Err(anyhow!("unsupported attribute datatype: {:?}", other)),
        };

        if is_valid {
            Ok(values.slice(index, 1))
        } else {
            Ok(arrow::array::new_null_array(values.data_type(), 1))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use arrow::{
        array::{Int32Array, StringDictionaryBuilder},
        datatypes::{DataType, Int32Type},
        ipc::reader::FileReader,
    };
    use std::sync::Arc;

    use object_store::{ObjectStore, memory::InMemory, path::Path};

    #[test]
    fn test_name() {
        let mut builder = StringDictionaryBuilder::<Int32Type>::new();
        builder.append_value("a");
        builder.append_null();
        builder.append_value("a");
        builder.append_value("b");
        let _array = builder.finish();
    }

    #[tokio::test]
    async fn attribute_writer_writes_int32_ipc() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let prefix = Path::from("attr");
        let mut writer =
            super::AttributeWriter::new(store.clone(), prefix.clone(), "int32", DataType::Int32)
                .unwrap();

        writer.append(super::AttributeValue::Int32(1)).unwrap();
        writer.append(super::AttributeValue::Int32(1)).unwrap();
        writer.append(super::AttributeValue::Null).unwrap();
        writer.append(super::AttributeValue::Int32(2)).unwrap();
        writer.append(super::AttributeValue::Int32(2)).unwrap();

        writer.finish().await.unwrap();

        let get = store.get(&prefix.child("int32")).await.unwrap();
        let bytes = get.bytes().await.unwrap();
        let cursor = Cursor::new(bytes.to_vec());
        let reader = FileReader::try_new(cursor, None).unwrap();
        let schema = reader.schema();
        assert_eq!(schema.fields().len(), 1);
        let batch = reader.into_iter().next().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 3);
    }

    #[tokio::test]
    async fn attribute_writer_writes_utf8_ipc() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let prefix = Path::from("attr");
        let mut writer =
            super::AttributeWriter::new(store.clone(), prefix.clone(), "utf8", DataType::Utf8)
                .unwrap();

        writer
            .append(super::AttributeValue::Utf8("aa".to_string()))
            .unwrap();
        writer
            .append(super::AttributeValue::Utf8("aa".to_string()))
            .unwrap();
        writer.append(super::AttributeValue::Null).unwrap();
        writer
            .append(super::AttributeValue::Utf8("bb".to_string()))
            .unwrap();
        writer
            .append(super::AttributeValue::Utf8("bb".to_string()))
            .unwrap();

        writer.finish().await.unwrap();

        let get = store.get(&prefix.child("utf8")).await.unwrap();
        let bytes = get.bytes().await.unwrap();
        let cursor = Cursor::new(bytes.to_vec());
        let reader = FileReader::try_new(cursor, None).unwrap();
        let batch = reader.into_iter().next().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 3);
    }

    #[tokio::test]
    async fn attribute_reader_reads_logical_index() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let prefix = Path::from("var/attributes");
        let mut writer =
            super::AttributeWriter::new(store.clone(), prefix.clone(), "quality", DataType::Int32)
                .unwrap();

        writer.append(super::AttributeValue::Int32(1)).unwrap();
        writer.append(super::AttributeValue::Int32(1)).unwrap();
        writer.append(super::AttributeValue::Null).unwrap();
        writer.append(super::AttributeValue::Int32(2)).unwrap();
        writer.append(super::AttributeValue::Int32(2)).unwrap();
        writer.finish().await.unwrap();

        let reader = super::AttributeReader::open(store.clone(), prefix, "quality")
            .await
            .unwrap();
        let v0 = reader.read_index(0).unwrap().unwrap();
        let v0_arr = v0.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(v0_arr.value(0), 1);

        let v2 = reader.read_index(2).unwrap().unwrap();
        assert_eq!(v2.null_count(), 1);

        let v4 = reader.read_index(4).unwrap().unwrap();
        let v4_arr = v4.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(v4_arr.value(0), 2);
        assert!(reader.read_index(5).unwrap().is_none());
    }

    #[tokio::test]
    async fn attribute_reader_reads_utf8_scalar() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let prefix = Path::from("var/attributes_utf8");
        let mut writer =
            super::AttributeWriter::new(store.clone(), prefix.clone(), "unit", DataType::Utf8)
                .unwrap();

        writer
            .append(super::AttributeValue::Utf8("m".to_string()))
            .unwrap();
        writer.append(super::AttributeValue::Null).unwrap();
        writer
            .append(super::AttributeValue::Utf8("cm".to_string()))
            .unwrap();
        writer.finish().await.unwrap();

        let reader = super::AttributeReader::open(store.clone(), prefix, "unit")
            .await
            .unwrap();
        let v0 = reader.read_index(0).unwrap().unwrap();
        let v0_arr = v0
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(v0_arr.value(0), "m");

        let v1 = reader.read_index(1).unwrap().unwrap();
        assert_eq!(v1.null_count(), 1);

        let v2 = reader.read_index(2).unwrap().unwrap();
        let v2_arr = v2
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(v2_arr.value(0), "cm");
    }

    #[tokio::test]
    async fn attribute_reader_optional_missing_returns_none() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let prefix = Path::from("var/attributes_missing");
        let reader = super::AttributeReader::open_optional(store, prefix, "missing")
            .await
            .unwrap();
        assert!(reader.is_none());
    }
}
