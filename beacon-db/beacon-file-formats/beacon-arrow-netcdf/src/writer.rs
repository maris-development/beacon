//! High-level writers for serializing Arrow record batches into NetCDF.

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

use crate::encoders::Encoder;

/// Generic writer backed by an [`Encoder`] implementation.
pub struct Writer<E: Encoder> {
    encoder: E,
}

impl<E: Encoder> Writer<E> {
    /// Create a writer for `path` using the provided Arrow `schema`.
    pub fn new<P: AsRef<Path>>(path: P, schema: SchemaRef) -> anyhow::Result<Self> {
        let nc_file = netcdf::create(path).map_err(|e| anyhow::anyhow!(e))?;

        let encoder = E::create(nc_file, schema).map_err(|e| anyhow::anyhow!(e))?;

        Ok(Self { encoder })
    }

    /// Write one Arrow record batch into the target NetCDF file.
    pub fn write_record_batch(
        &mut self,
        record_batch: arrow::record_batch::RecordBatch,
    ) -> anyhow::Result<()> {
        self.encoder
            .write_record_batch(record_batch)
            .map_err(|e| anyhow::anyhow!(e))?;

        Ok(())
    }
}

/// Buffered Arrow IPC writer that converts to NetCDF on [`finish`](Self::finish).
///
/// This writer collects Arrow batches in a temporary IPC file first, allowing
/// post-processing of schema details (such as fixed-size string columns)
/// before materializing the NetCDF output.
pub struct ArrowRecordBatchWriter<E: Encoder> {
    path: PathBuf,
    writer: FileWriter<SpooledTempFile>,
    /// The input schema with every column mapped to a type the encoder writes.
    schema: SchemaRef,
    fixed_string_sizes: HashMap<String, usize>,
    encoder: PhantomData<E>,
}

/// The type the encoder writes a column of `data_type` as, or `None` when it writes the type
/// as it is.
fn encodable_type(data_type: &DataType) -> Option<DataType> {
    match data_type {
        DataType::LargeUtf8 | DataType::Utf8View => Some(DataType::Utf8),
        DataType::Dictionary(_, values)
            if matches!(
                values.as_ref(),
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
            ) =>
        {
            Some(DataType::Utf8)
        }
        // NetCDF has no boolean type. The ND sink writes one as `u8` too.
        DataType::Boolean => Some(DataType::UInt8),
        _ => None,
    }
}

impl<E: Encoder> ArrowRecordBatchWriter<E> {
    /// Create a buffered writer targeting `path`.
    pub fn new<P: AsRef<Path>>(path: P, schema: SchemaRef) -> anyhow::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let schema = Arc::new(arrow::datatypes::Schema::new_with_metadata(
            schema
                .fields()
                .iter()
                .map(|field| match encodable_type(field.data_type()) {
                    Some(data_type) => field.as_ref().clone().with_data_type(data_type),
                    None => field.as_ref().clone(),
                })
                .collect::<Vec<_>>(),
            schema.metadata().clone(),
        ));
        // 256 MB spooled temp file
        let file = SpooledTempFile::new(256 * 1024 * 1024);
        let writer = FileWriter::try_new(file, &schema).map_err(|e| anyhow::anyhow!(e))?;
        let fixed_string_sizes = HashMap::new();

        Ok(Self {
            path,
            writer,
            schema,
            fixed_string_sizes,
            encoder: PhantomData,
        })
    }

    /// Append an Arrow record batch to the buffered stream.
    pub fn write_record_batch(
        &mut self,
        record_batch: arrow::record_batch::RecordBatch,
    ) -> anyhow::Result<()> {
        let columns = record_batch
            .columns()
            .iter()
            .zip(self.schema.fields())
            .map(|(column, field)| {
                if column.data_type() == field.data_type() {
                    Ok(column.clone())
                } else {
                    arrow::compute::cast(column, field.data_type())
                }
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| anyhow::anyhow!(e))?;
        let record_batch =
            RecordBatch::try_new(self.schema.clone(), columns).map_err(|e| anyhow::anyhow!(e))?;

        self.writer
            .write(&record_batch)
            .map_err(|e| anyhow::anyhow!(e))?;

        Ok(())
    }

    /// Finalize buffered data and write the NetCDF file.
    pub fn finish(&mut self) -> anyhow::Result<()> {
        self.writer.finish().map_err(|e| anyhow::anyhow!(e))?;
        let inner_f = self.writer.get_mut();
        let reader = arrow::ipc::reader::FileReader::try_new(inner_f, None)
            .map_err(|e| anyhow::anyhow!(e))?;

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
            let batch = batch.map_err(|e| anyhow::anyhow!(e))?;
            let updated_batch = Self::map_record_batch(batch, updated_schema.clone())?;
            nc_writer.write_record_batch(updated_batch)?;
        }

        Ok(())
    }

    fn map_record_batch(
        record_batch: RecordBatch,
        schema: SchemaRef,
    ) -> anyhow::Result<RecordBatch> {
        let mut arrays = vec![];
        for (idx, column) in record_batch.columns().iter().enumerate() {
            if let DataType::FixedSizeBinary(size) = schema.field(idx).data_type() {
                let string_array = column.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                    anyhow::anyhow!(
                        "column {} expected Utf8/StringArray for FixedSizeBinary conversion, got {:?}",
                        schema.field(idx).name(),
                        column.data_type()
                    )
                })?;
                let casted_array = string_array_to_fixed_binary(string_array, *size as usize);
                arrays.push(Arc::new(casted_array) as ArrayRef);
            } else {
                arrays.push(column.clone());
            }
        }

        Ok(RecordBatch::try_new(schema, arrays)?)
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
            // Copy at most `fixed_size` bytes; a longer string is truncated to fit
            // the fixed width rather than panicking on the slice copy.
            let n = str.len().min(fixed_size);
            if str.len() > fixed_size {
                tracing::warn!(
                    string_len = str.len(),
                    fixed_size,
                    "string longer than fixed NetCDF width; truncating"
                );
            }
            fixed_buffer[..n].copy_from_slice(&str.as_bytes()[..n]);
            // `fixed_buffer` is always exactly `fixed_size` bytes, so this never fails.
            builder
                .append_value(fixed_buffer)
                .expect("append_value with fixed-size buffer is infallible");
        } else {
            builder
                .append_value(vec![b'\0'; fixed_size])
                .expect("append_value binary failed");
        }
    });

    builder.finish()
}
