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
    datatypes::{DataType, Schema},
    ipc::writer::FileWriter,
    record_batch::RecordBatch,
};
use object_store::ObjectStore;
use tempfile::SpooledTempFile;

use crate::rle::{
    BytesRleStructBuilder, PrimitiveRleStructBuilder, StringRleStructBuilder, rle_extension_field,
};

const BATCH_SIZE: usize = 4 * 1024 * 1024;

#[derive(Debug, Clone)]
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

impl<S: ObjectStore + Clone> AttributeWriter<S> {
    pub fn new(
        store: S,
        path: object_store::path::Path,
        attr_name: &str,
        datatype: DataType,
    ) -> Result<Self> {
        let builder = RleBuilder::new(&datatype)?;
        let field = rle_extension_field(attr_name, &datatype);
        let schema = Arc::new(Schema::new(vec![field]));
        let file = SpooledTempFile::new(256 * 1024 * 1024);
        let writer = FileWriter::try_new(file, &schema).map_err(|err| anyhow!(err))?;

        Ok(Self {
            store,
            path,
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

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use arrow::{
        array::StringDictionaryBuilder,
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
        let path = Path::from("attr/int32.arrow");
        let mut writer =
            super::AttributeWriter::new(store.clone(), path.clone(), "attr", DataType::Int32)
                .unwrap();

        writer.append(super::AttributeValue::Int32(1)).unwrap();
        writer.append(super::AttributeValue::Int32(1)).unwrap();
        writer.append(super::AttributeValue::Null).unwrap();
        writer.append(super::AttributeValue::Int32(2)).unwrap();
        writer.append(super::AttributeValue::Int32(2)).unwrap();

        writer.finish().await.unwrap();

        let get = store.get(&path).await.unwrap();
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
        let path = Path::from("attr/utf8.arrow");
        let mut writer =
            super::AttributeWriter::new(store.clone(), path.clone(), "attr", DataType::Utf8)
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

        let get = store.get(&path).await.unwrap();
        let bytes = get.bytes().await.unwrap();
        let cursor = Cursor::new(bytes.to_vec());
        let reader = FileReader::try_new(cursor, None).unwrap();
        let batch = reader.into_iter().next().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 3);
    }
}
