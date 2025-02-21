use std::{
    cell::RefCell,
    collections::HashMap,
    marker::PhantomData,
    path::{Path, PathBuf},
    rc::Rc,
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

use crate::encoders::Encoder;

pub struct Writer<E: Encoder> {
    nc_file: Rc<RefCell<netcdf::FileMut>>,
    encoder: E,
}

impl<E: Encoder> Writer<E> {
    pub fn new<P: AsRef<Path>>(path: P, schema: SchemaRef) -> anyhow::Result<Self> {
        let nc_file = Rc::new(RefCell::new(netcdf::create(path)?));

        let encoder = E::create(nc_file.clone(), schema)?;

        Ok(Self { nc_file, encoder })
    }

    pub fn write_record_batch(
        &mut self,
        record_batch: arrow::record_batch::RecordBatch,
    ) -> anyhow::Result<()> {
        self.encoder.write_record_batch(record_batch)?;

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
    pub fn new<P: AsRef<Path>>(path: P, schema: SchemaRef) -> anyhow::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = SpooledTempFile::new(256_000_000);
        let writer = FileWriter::try_new(file, &schema)?;
        let mut fixed_string_sizes = HashMap::new();

        for field in schema.fields() {
            if field.data_type() == &DataType::Utf8 {
                fixed_string_sizes.insert(field.name().to_string(), 0);
            }
        }

        Ok(Self {
            path,
            writer,
            fixed_string_sizes: HashMap::new(),
            encoder: PhantomData,
        })
    }

    pub fn write_record_batch(
        &mut self,
        record_batch: arrow::record_batch::RecordBatch,
    ) -> anyhow::Result<()> {
        self.writer.write(&record_batch)?;

        for (name, size) in self.fixed_string_sizes.iter_mut() {
            let max_size = record_batch
                .column_by_name(name)
                .unwrap()
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .unwrap()
                .iter()
                .map(|x| x.map(|x| x.len()).unwrap_or(0))
                .max();

            if let Some(max_size) = max_size {
                *size = (*size).max(max_size);
            }
        }

        Ok(())
    }

    pub fn finish(&mut self) -> anyhow::Result<()> {
        self.writer.finish()?;
        let inner_f = self.writer.get_mut();
        let reader = arrow::ipc::reader::FileReader::try_new(inner_f, None)?;

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
            let batch = batch?;
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
