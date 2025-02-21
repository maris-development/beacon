use std::{
    cell::RefCell,
    collections::HashMap,
    marker::PhantomData,
    path::{Path, PathBuf},
    rc::Rc,
};

use arrow::{
    array::{Array, FixedSizeBinaryArray, FixedSizeBinaryBuilder, StringArray},
    datatypes::{DataType, SchemaRef},
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

        let nc_writer = Writer::<E>::new(&self.path, self.writer.schema().clone())?;

        Ok(())
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
