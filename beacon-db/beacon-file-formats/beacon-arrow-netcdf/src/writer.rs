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

use crate::attributes::{AttributeCombiner, AttributePlan, AttributeTarget, CombineAttrs};
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

    /// Write attributes. Call this before the first batch.
    pub fn put_attributes(
        &mut self,
        attributes: &[(AttributeTarget, netcdf::AttributeValue)],
    ) -> anyhow::Result<()> {
        for (target, value) in attributes {
            self.encoder
                .put_attribute(target.variable(), target.name(), value.clone())
                .map_err(|e| anyhow::anyhow!(e))?;
        }
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
    /// Records attribute column values when `combine_attrs` is set.
    combiner: Option<AttributeCombiner>,
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
        Self::with_combine_attrs(path, schema, None)
    }

    /// Create a buffered writer that folds attribute columns by `policy`.
    pub fn with_combine_attrs<P: AsRef<Path>>(
        path: P,
        schema: SchemaRef,
        policy: Option<CombineAttrs>,
    ) -> anyhow::Result<Self> {
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
        // The combiner sees the cast schema, so a dictionary column compares as Utf8.
        let combiner = policy.map(|policy| AttributeCombiner::new(&schema, policy));
        // 256 MB spooled temp file
        let file = SpooledTempFile::new(256 * 1024 * 1024);
        let writer = FileWriter::try_new(file, &schema).map_err(|e| anyhow::anyhow!(e))?;
        let fixed_string_sizes = HashMap::new();

        Ok(Self {
            path,
            writer,
            schema,
            fixed_string_sizes,
            combiner,
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

        if let Some(combiner) = &mut self.combiner {
            combiner.observe(&record_batch)?;
        }

        self.writer
            .write(&record_batch)
            .map_err(|e| anyhow::anyhow!(e))?;

        Ok(())
    }

    /// Finalize buffered data and write the NetCDF file.
    pub fn finish(&mut self) -> anyhow::Result<()> {
        self.writer.finish().map_err(|e| anyhow::anyhow!(e))?;
        let plan = match self.combiner.take() {
            Some(combiner) => combiner.resolve()?,
            None => AttributePlan::default(),
        };
        let inner_f = self.writer.get_mut();
        let reader = arrow::ipc::reader::FileReader::try_new(inner_f, None)
            .map_err(|e| anyhow::anyhow!(e))?;

        let mut kept_columns = vec![];
        let mut updated_schema_fields = vec![];
        for (index, field) in reader.schema().fields().iter().enumerate() {
            if plan.skipped.contains(field.name()) {
                continue;
            }
            kept_columns.push(index);
            let name = plan.renamed.get(field.name()).unwrap_or(field.name());
            if let Some(size) = self.fixed_string_sizes.get(field.name()) {
                updated_schema_fields.push(arrow::datatypes::Field::new(
                    name,
                    arrow::datatypes::DataType::FixedSizeBinary(*size as i32),
                    field.is_nullable(),
                ));
            } else {
                updated_schema_fields.push(Field::new(
                    name,
                    field.data_type().clone(),
                    field.is_nullable(),
                ));
            }
        }

        let updated_schema = Arc::new(arrow::datatypes::Schema::new(updated_schema_fields));

        let mut nc_writer = Writer::<E>::new(&self.path, updated_schema.clone())?;
        nc_writer.put_attributes(&plan.attributes)?;

        // A batch with no column has no row count, so the file keeps its attributes only.
        if kept_columns.is_empty() {
            return Ok(());
        }

        for batch in reader {
            let batch = batch.map_err(|e| anyhow::anyhow!(e))?;
            let batch = batch
                .project(&kept_columns)
                .map_err(|e| anyhow::anyhow!(e))?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{attributes::CombineAttrs, encoders::default::DefaultEncoder};
    use arrow::array::{DictionaryArray, Float64Array, TimestampNanosecondArray};
    use arrow::datatypes::{Int32Type, Schema, TimeUnit};
    use netcdf::AttributeValue;

    fn write(
        path: &Path,
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
        policy: Option<CombineAttrs>,
    ) {
        let mut writer =
            ArrowRecordBatchWriter::<DefaultEncoder>::with_combine_attrs(path, schema, policy)
                .expect("a netCDF writer");
        for batch in batches {
            writer.write_record_batch(batch).expect("write the batch");
        }
        writer.finish().expect("finish the file");
    }

    fn str_attribute(file: &netcdf::File, variable: Option<&str>, name: &str) -> Option<String> {
        let value = match variable {
            Some(variable) => file.variable(variable)?.attribute(name)?.value().ok()?,
            None => file.attribute(name)?.value().ok()?,
        };
        match value {
            AttributeValue::Str(value) => Some(value),
            _ => None,
        }
    }

    fn units_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("temp", DataType::Float64, true),
            Field::new("temp.units", DataType::Utf8, true),
            Field::new(".title", DataType::Utf8, true),
        ]))
    }

    fn units_batch(schema: &SchemaRef, units: &str, title: &str, rows: usize) -> RecordBatch {
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Float64Array::from(vec![1.5; rows])),
                Arc::new(StringArray::from(vec![units; rows])),
                Arc::new(StringArray::from(vec![title; rows])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn without_a_policy_every_column_is_a_variable() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("plain.nc");
        let schema = Arc::new(Schema::new(vec![
            Field::new("temp", DataType::Float64, true),
            Field::new("temp.units", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Float64Array::from(vec![1.0])),
                Arc::new(StringArray::from(vec!["degC"])),
            ],
        )
        .unwrap();
        write(&path, schema, vec![batch], None);

        let file = netcdf::open(&path).unwrap();
        assert!(file.variable("temp.units").is_some());
        assert_eq!(str_attribute(&file, Some("temp"), "units"), None);
    }

    #[test]
    fn identical_writes_a_unique_value_as_an_attribute() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("folded.nc");
        let schema = units_schema();
        let batches = vec![
            units_batch(&schema, "degC", "Argo", 2),
            units_batch(&schema, "degC", "Argo", 2),
        ];
        write(&path, schema, batches, Some(CombineAttrs::Identical));

        let file = netcdf::open(&path).unwrap();
        assert_eq!(
            str_attribute(&file, Some("temp"), "units").as_deref(),
            Some("degC")
        );
        assert_eq!(str_attribute(&file, None, "title").as_deref(), Some("Argo"));
        assert!(file.variable("temp.units").is_none());
        assert!(file.variable("global.title").is_none());
        assert_eq!(file.variable("temp").unwrap().len(), 4);
    }

    #[test]
    fn identical_keeps_two_values_as_a_variable() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("conflict.nc");
        let schema = units_schema();
        let batches = vec![
            units_batch(&schema, "degC", "Argo", 1),
            units_batch(&schema, "K", "Glider", 1),
        ];
        write(&path, schema, batches, Some(CombineAttrs::Identical));

        let file = netcdf::open(&path).unwrap();
        assert_eq!(str_attribute(&file, Some("temp"), "units"), None);
        assert_eq!(file.variable("temp.units").unwrap().len(), 2);
        assert_eq!(file.variable("global.title").unwrap().len(), 2);
    }

    #[test]
    fn a_dictionary_column_folds_after_the_cast() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dictionary.nc");
        let schema = Arc::new(Schema::new(vec![
            Field::new("temp", DataType::Float64, true),
            Field::new(
                "temp.units",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                true,
            ),
        ]));
        let units: DictionaryArray<Int32Type> = vec!["degC", "degC"].into_iter().collect();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Float64Array::from(vec![1.0, 2.0])),
                Arc::new(units),
            ],
        )
        .unwrap();
        write(&path, schema, vec![batch], Some(CombineAttrs::Identical));

        let file = netcdf::open(&path).unwrap();
        assert_eq!(
            str_attribute(&file, Some("temp"), "units").as_deref(),
            Some("degC")
        );
    }

    #[test]
    fn a_timestamp_keeps_the_encoder_units() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("time.nc");
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "time",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ),
            Field::new("time.units", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(TimestampNanosecondArray::from(vec![0, 1])),
                Arc::new(StringArray::from(vec![
                    "days since 1950-01-01",
                    "days since 1950-01-01",
                ])),
            ],
        )
        .unwrap();
        write(&path, schema, vec![batch], Some(CombineAttrs::Identical));

        let file = netcdf::open(&path).unwrap();
        assert_eq!(
            str_attribute(&file, Some("time"), "units").as_deref(),
            Some("nanoseconds since 1970-01-01T00:00:00Z")
        );
        assert!(file.variable("time.units").is_none());
    }

    #[test]
    fn only_global_attribute_columns_write_a_file_with_attributes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("globals.nc");
        let schema = Arc::new(Schema::new(vec![Field::new(
            ".title",
            DataType::Utf8,
            true,
        )]));
        let batch = |rows: usize| {
            RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(StringArray::from(vec!["Argo"; rows]))],
            )
            .unwrap()
        };
        write(
            &path,
            schema.clone(),
            vec![batch(2), batch(3)],
            Some(CombineAttrs::Identical),
        );

        let file = netcdf::open(&path).unwrap();
        assert_eq!(str_attribute(&file, None, "title").as_deref(), Some("Argo"));
        assert_eq!(file.variables().count(), 0);
    }

    #[test]
    fn zero_rows_write_a_valid_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty.nc");
        write(&path, units_schema(), vec![], Some(CombineAttrs::Identical));

        let file = netcdf::open(&path).unwrap();
        assert!(file.variable("temp").is_some());
        assert!(file.variable("temp.units").is_none());
        assert_eq!(str_attribute(&file, Some("temp"), "units"), None);
    }
}
