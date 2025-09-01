use std::{
    collections::HashMap,
    marker::PhantomData,
    path::{Path, PathBuf},
    sync::Arc,
};

use arrow::{
    array::{
        Array, ArrayRef, FixedSizeBinaryArray, FixedSizeBinaryBuilder, RecordBatch, StringArray,
    },
    datatypes::{DataType, Field, SchemaRef},
    ipc::writer::FileWriter,
};
use tempfile::SpooledTempFile;

use crate::{encoders::Encoder, error::ArrowNetCDFError, NcResult};

pub struct Writer<E: Encoder> {
    encoder: E,
}

impl<E: Encoder> Writer<E> {
    pub fn new<P: AsRef<Path>>(path: P, schema: SchemaRef) -> NcResult<Self> {
        let nc_file = netcdf::create(path).map_err(|e| {
            ArrowNetCDFError::NetCDFWriterError(Box::new(ArrowNetCDFError::NetCDFError(e)))
        })?;

        let encoder = E::create(nc_file, schema).map_err(|e| {
            ArrowNetCDFError::EncoderCreationError(Box::new(ArrowNetCDFError::EncoderError(e)))
        })?;

        Ok(Self { encoder })
    }

    pub fn write_record_batch(
        &mut self,
        record_batch: arrow::record_batch::RecordBatch,
    ) -> NcResult<()> {
        self.encoder
            .write_record_batch(record_batch)
            .map_err(ArrowNetCDFError::EncoderError)?;

        Ok(())
    }
}

pub struct ArrowRecordBatchWriter<E: Encoder> {
    path: PathBuf,
    writer: FileWriter<SpooledTempFile>,
    fixed_string_sizes: HashMap<String, usize>,
    encoder: PhantomData<E>,
}

impl<E: Encoder> ArrowRecordBatchWriter<E> {
    pub fn new<P: AsRef<Path>>(path: P, schema: SchemaRef) -> NcResult<Self> {
        let path = path.as_ref().to_path_buf();
        // 256 MB spooled temp file
        let file = SpooledTempFile::new(256 * 1024 * 1024);
        let writer = FileWriter::try_new(file, &schema).map_err(|e| {
            ArrowNetCDFError::NetCDFWriterError(Box::new(ArrowNetCDFError::IpcBufferFileError(e)))
        })?;
        let fixed_string_sizes = HashMap::new();

        Ok(Self {
            path,
            writer,
            fixed_string_sizes,
            encoder: PhantomData,
        })
    }

    pub fn write_record_batch(
        &mut self,
        record_batch: arrow::record_batch::RecordBatch,
    ) -> NcResult<()> {
        self.writer.write(&record_batch).map_err(|e| {
            ArrowNetCDFError::RecordBatchWriteError(Box::new(
                ArrowNetCDFError::IpcBufferWriteError(e),
            ))
        })?;

        Ok(())
    }

    pub fn finish(&mut self) -> NcResult<()> {
        self.writer.finish().map_err(|e| {
            ArrowNetCDFError::RecordBatchWriteError(Box::new(
                ArrowNetCDFError::IpcBufferCloseError(e),
            ))
        })?;
        let inner_f = self.writer.get_mut();
        let reader = arrow::ipc::reader::FileReader::try_new(inner_f, None).map_err(|e| {
            ArrowNetCDFError::RecordBatchReadError(Box::new(ArrowNetCDFError::IpcBufferReadError(
                e,
            )))
        })?;

        let mut updated_schema_fields = vec![];
        for field in reader.schema().fields() {
            if let Some(size) = self.fixed_string_sizes.get(field.name()) {
                updated_schema_fields.push(arrow::datatypes::Field::new(
                    field.name(),
                    arrow::datatypes::DataType::FixedSizeBinary(*size as i32),
                    field.is_nullable(),
                ));
            } else {
                updated_schema_fields.push(Field::new(
                    field.name(),
                    field.data_type().clone(),
                    field.is_nullable(),
                ));
            }
        }

        let updated_schema = Arc::new(arrow::datatypes::Schema::new(updated_schema_fields));

        let mut nc_writer = Writer::<E>::new(&self.path, updated_schema.clone())?;

        for batch in reader {
            let batch = batch.map_err(|e| {
                ArrowNetCDFError::RecordBatchReadError(Box::new(
                    ArrowNetCDFError::IpcBufferReadError(e),
                ))
            })?;
            let updated_batch = Self::map_record_batch(batch, updated_schema.clone());
            nc_writer.write_record_batch(updated_batch)?;
        }

        Ok(())
    }

    fn map_record_batch(record_batch: RecordBatch, schema: SchemaRef) -> RecordBatch {
        let mut arrays = vec![];
        for (idx, column) in record_batch.columns().iter().enumerate() {
            if let DataType::FixedSizeBinary(size) = schema.field(idx).data_type() {
                let string_array = column.as_any().downcast_ref::<StringArray>().unwrap();
                let casted_array = string_array_to_fixed_binary(string_array, *size as usize);
                arrays.push(Arc::new(casted_array) as ArrayRef);
            } else {
                arrays.push(column.clone());
            }
        }

        RecordBatch::try_new(schema, arrays).unwrap()
    }
}

fn string_array_to_fixed_binary(
    string_array: &StringArray,
    fixed_size: usize,
) -> FixedSizeBinaryArray {
    let mut builder = FixedSizeBinaryBuilder::with_capacity(string_array.len(), fixed_size as i32);

    string_array.iter().for_each(|str| {
        if let Some(str) = str {
            let mut fixed_buffer = vec![b'\0'; fixed_size];
            fixed_buffer[..str.len()].copy_from_slice(str.as_bytes());
            builder
                .append_value(fixed_buffer)
                .expect("append_value binary failed");
        } else {
            builder
                .append_value(vec![b'\0'; fixed_size])
                .expect("append_value binary failed");
        }
    });

    builder.finish()
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::Schema;

    use super::*;

    #[test]
    fn test_name() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "text_array",
            DataType::Utf8,
            true,
        )]));
        let arrays = vec![Arc::new(StringArray::from(vec![Some("hello"); 1_000_000])) as ArrayRef];

        // let schema = Arc::new(Schema::new(vec![Field::new(
        //     "text_array",
        //     DataType::Int32,
        //     true,
        // )]));
        // let arrays = vec![Arc::new(Int32Array::from(vec![12; 1_000_000])) as ArrayRef];

        let mut writer = ArrowRecordBatchWriter::<crate::encoders::default::DefaultEncoder>::new(
            "test.nc",
            schema.clone(),
        )
        .unwrap();

        let record_batch = RecordBatch::try_new(schema, arrays).unwrap();
        println!("Record batch created");
        println!("Size: {}", record_batch.get_array_memory_size());

        writer.write_record_batch(record_batch).unwrap();

        println!("To NetCDF");
        writer.finish().unwrap();

        println!("Finished writing");

        drop(writer);

        std::thread::sleep(std::time::Duration::from_secs(1000));
    }
}
