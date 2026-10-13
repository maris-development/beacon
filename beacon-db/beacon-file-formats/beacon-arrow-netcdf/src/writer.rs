//! High-level writers for serializing Arrow record batches into NetCDF.

use std::{
    collections::{HashMap, HashSet},
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

use crate::attributes::{AttributeCombiner, AttributePlan, AttributeTarget};
use crate::datafusion::options::NetcdfWriterOptions;
use crate::encoders::{Encoder, EncoderOptions};

/// Generic writer backed by an [`Encoder`] implementation.
pub struct Writer<E: Encoder> {
    encoder: E,
}

impl<E: Encoder> Writer<E> {
    /// Create a writer for `path` using the provided Arrow `schema`.
    pub fn new<P: AsRef<Path>>(path: P, schema: SchemaRef) -> anyhow::Result<Self> {
        Self::with_options(path, schema, &EncoderOptions::default())
    }

    /// Create a writer for `path` that defines the variables with `options`.
    pub fn with_options<P: AsRef<Path>>(
        path: P,
        schema: SchemaRef,
        options: &EncoderOptions,
    ) -> anyhow::Result<Self> {
        let nc_file = netcdf::create(path).map_err(|e| anyhow::anyhow!(e))?;

        let encoder = E::create(nc_file, schema, options).map_err(|e| anyhow::anyhow!(e))?;

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
    options: NetcdfWriterOptions,
    /// Rows written so far.
    rows: usize,
    /// Timestamp columns with a value that is not a whole number in `time_units`.
    float_time_columns: HashSet<String>,
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
        Self::with_options(path, schema, NetcdfWriterOptions::default())
    }

    /// Create a buffered writer with the flat output `options`.
    pub fn with_options<P: AsRef<Path>>(
        path: P,
        schema: SchemaRef,
        options: NetcdfWriterOptions,
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
        let combiner = options
            .combine_attrs
            .map(|policy| AttributeCombiner::new(&schema, policy));
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
            options,
            rows: 0,
            float_time_columns: HashSet::new(),
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
        if let Some(units) = &self.options.time_units {
            for (field, column) in self.schema.fields().iter().zip(record_batch.columns()) {
                if matches!(field.data_type(), DataType::Timestamp(_, _))
                    && !self.float_time_columns.contains(field.name())
                    && !units.fits_integer(column)
                {
                    self.float_time_columns.insert(field.name().clone());
                }
            }
        }

        self.writer
            .write(&record_batch)
            .map_err(|e| anyhow::anyhow!(e))?;
        self.rows += record_batch.num_rows();

        Ok(())
    }

    /// The settings the encoder applies when it defines the variables.
    fn encoder_options(&self) -> EncoderOptions {
        EncoderOptions {
            compression: self.options.compression,
            // A chunk larger than the data wastes space, because HDF5 stores full chunks.
            chunk_rows: Some(self.options.chunk_rows().min(self.rows).max(1)),
            time_units: self.options.time_units.clone(),
            float_time_columns: self.float_time_columns.clone(),
            ..Default::default()
        }
    }

    /// Finalize buffered data and write the NetCDF file.
    pub fn finish(&mut self) -> anyhow::Result<()> {
        self.writer.finish().map_err(|e| anyhow::anyhow!(e))?;
        let mut plan = match self.combiner.take() {
            Some(combiner) => combiner.resolve()?,
            None => AttributePlan::default(),
        };
        let mut encoder_options = self.encoder_options();
        encoder_options.fill_values = std::mem::take(&mut plan.fill_values);
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

        let mut nc_writer =
            Writer::<E>::with_options(&self.path, updated_schema.clone(), &encoder_options)?;
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
        let options = NetcdfWriterOptions {
            combine_attrs: policy,
            ..Default::default()
        };
        write_with(path, schema, batches, options).expect("write the file");
    }

    fn write_with(
        path: &Path,
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
        options: NetcdfWriterOptions,
    ) -> anyhow::Result<()> {
        let mut writer =
            ArrowRecordBatchWriter::<DefaultEncoder>::with_options(path, schema, options)?;
        for batch in batches {
            writer.write_record_batch(batch)?;
        }
        writer.finish()
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

    fn series_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("value", DataType::Float64, true),
            Field::new("name", DataType::Utf8, true),
        ]))
    }

    /// Rows with repeated values, so that deflate has something to compress.
    fn series_batch(schema: &SchemaRef, rows: usize) -> RecordBatch {
        let values: Vec<Option<f64>> = (0..rows)
            .map(|row| (row % 7 != 0).then_some((row % 100) as f64 / 4.0))
            .collect();
        let names: Vec<&str> = (0..rows)
            .map(|row| if row % 2 == 0 { "argo" } else { "glider" })
            .collect();
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Float64Array::from(values)),
                Arc::new(StringArray::from(names)),
            ],
        )
        .unwrap()
    }

    fn compressed(level: u8) -> NetcdfWriterOptions {
        NetcdfWriterOptions {
            compression: Some(crate::datafusion::options::Compression {
                level: level.try_into().unwrap(),
                shuffle: true,
            }),
            ..Default::default()
        }
    }

    #[test]
    fn a_compressed_file_is_smaller() {
        let dir = tempfile::tempdir().unwrap();
        // HDF5 does not compress the heap of a variable-length string, so use numbers only.
        let schema = series_schema();
        let batch = series_batch(&schema, 100_000).project(&[0]).unwrap();
        let schema = batch.schema();
        let plain = dir.path().join("plain.nc");
        let packed = dir.path().join("packed.nc");
        let options = NetcdfWriterOptions::default();
        write_with(&plain, schema.clone(), vec![batch.clone()], options).unwrap();
        write_with(&packed, schema, vec![batch], compressed(4)).unwrap();

        let size = |path: &Path| std::fs::metadata(path).unwrap().len();
        assert!(
            size(&packed) * 4 < size(&plain),
            "compressed {} bytes, plain {} bytes",
            size(&packed),
            size(&plain)
        );
    }

    #[test]
    fn a_compressed_file_reads_back_the_same_values() {
        let dir = tempfile::tempdir().unwrap();
        let schema = series_schema();
        let batches = || vec![series_batch(&schema, 50_000), series_batch(&schema, 50_000)];
        let plain = dir.path().join("plain.nc");
        let packed = dir.path().join("packed.nc");
        write_with(
            &plain,
            schema.clone(),
            batches(),
            NetcdfWriterOptions::default(),
        )
        .unwrap();
        write_with(&packed, schema.clone(), batches(), compressed(4)).unwrap();

        let plain = netcdf::open(&plain).unwrap();
        let packed = netcdf::open(&packed).unwrap();
        // A null is NaN, and NaN is not equal to NaN, so compare the bits.
        let values = |file: &netcdf::File| -> Vec<u64> {
            let values: Vec<f64> = file.variable("value").unwrap().get_values(..).unwrap();
            values.into_iter().map(f64::to_bits).collect()
        };
        let names = |file: &netcdf::File| -> Vec<String> {
            file.variable("name").unwrap().get_strings(..).unwrap()
        };
        assert_eq!(values(&packed).len(), 100_000);
        assert_eq!(values(&packed), values(&plain));
        assert_eq!(names(&packed), names(&plain));
    }

    #[test]
    fn chunk_rows_sets_the_chunk_of_obs() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("chunks.nc");
        let schema = series_schema();
        let options = NetcdfWriterOptions {
            chunk_rows: std::num::NonZeroUsize::new(1000),
            ..compressed(1)
        };
        write_with(
            &path,
            schema.clone(),
            vec![series_batch(&schema, 5000)],
            options,
        )
        .unwrap();

        let file = netcdf::open(&path).unwrap();
        let chunking = file.variable("value").unwrap().chunking().unwrap();
        assert_eq!(chunking, Some(vec![1000]));
    }

    #[test]
    fn the_default_chunk_is_not_larger_than_the_rows() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("small.nc");
        let schema = series_schema();
        write_with(
            &path,
            schema.clone(),
            vec![series_batch(&schema, 10)],
            NetcdfWriterOptions::default(),
        )
        .unwrap();

        let file = netcdf::open(&path).unwrap();
        let chunking = file.variable("value").unwrap().chunking().unwrap();
        assert_eq!(chunking, Some(vec![10]));
    }

    #[test]
    fn a_null_float_is_nan_and_a_null_integer_is_the_type_maximum() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("fill.nc");
        let schema = Arc::new(Schema::new(vec![
            Field::new("temp", DataType::Float32, true),
            Field::new("count", DataType::Int32, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(arrow::array::Float32Array::from(vec![Some(1.5), None])),
                Arc::new(arrow::array::Int32Array::from(vec![Some(3), None])),
            ],
        )
        .unwrap();
        write(&path, schema, vec![batch], None);

        let file = netcdf::open(&path).unwrap();
        let temp = file.variable("temp").unwrap();
        assert!(temp.fill_value::<f32>().unwrap().unwrap().is_nan());
        let values: Vec<f32> = temp.get_values(..).unwrap();
        assert_eq!(values[0], 1.5);
        assert!(values[1].is_nan());
        let count = file.variable("count").unwrap();
        assert_eq!(count.fill_value::<i32>().unwrap(), Some(i32::MAX));
        assert_eq!(count.get_values::<i32, _>(..).unwrap(), vec![3, i32::MAX]);
    }

    #[test]
    fn a_source_fill_value_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("source_fill.nc");
        let schema = Arc::new(Schema::new(vec![
            Field::new("temp", DataType::Float64, true),
            Field::new("temp._FillValue", DataType::Float64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Float64Array::from(vec![Some(1.5), None])),
                Arc::new(Float64Array::from(vec![-999.0, -999.0])),
            ],
        )
        .unwrap();
        write(&path, schema, vec![batch], Some(CombineAttrs::Identical));

        let file = netcdf::open(&path).unwrap();
        let temp = file.variable("temp").unwrap();
        assert_eq!(temp.fill_value::<f64>().unwrap(), Some(-999.0));
        assert_eq!(temp.get_values::<f64, _>(..).unwrap(), vec![1.5, -999.0]);
        assert!(file.variable("temp._FillValue").is_none());
    }

    fn write_times(path: &Path, nanoseconds: Vec<Option<i64>>, time_units: Option<&str>) {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "time",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(TimestampNanosecondArray::from(nanoseconds))],
        )
        .unwrap();
        let options = NetcdfWriterOptions {
            time_units: time_units.map(|units| units.parse().unwrap()),
            ..Default::default()
        };
        write_with(path, schema, vec![batch], options).unwrap();
    }

    const DAY_NS: i64 = 86_400_000_000_000;

    #[test]
    fn time_units_write_whole_values_as_integers() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("days.nc");
        write_times(
            &path,
            vec![Some(0), None, Some(2 * DAY_NS)],
            Some("days since 1950-01-01"),
        );

        let file = netcdf::open(&path).unwrap();
        let time = file.variable("time").unwrap();
        assert_eq!(
            str_attribute(&file, Some("time"), "units").as_deref(),
            Some("days since 1950-01-01")
        );
        assert_eq!(
            str_attribute(&file, Some("time"), "calendar").as_deref(),
            Some("standard")
        );
        assert_eq!(
            time.get_values::<i64, _>(..).unwrap(),
            vec![7305, i64::MAX, 7307]
        );
    }

    #[test]
    fn time_units_write_a_fraction_as_a_float() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("fraction.nc");
        write_times(
            &path,
            vec![Some(DAY_NS / 2), None],
            Some("days since 1970-01-01"),
        );

        let file = netcdf::open(&path).unwrap();
        let time = file.variable("time").unwrap();
        let values: Vec<f64> = time.get_values(..).unwrap();
        assert_eq!(values[0], 0.5);
        assert!(values[1].is_nan());
        assert!(time.fill_value::<f64>().unwrap().unwrap().is_nan());
    }

    #[test]
    fn without_time_units_a_timestamp_keeps_its_arrow_unit() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("default_time.nc");
        write_times(&path, vec![Some(5)], None);

        let file = netcdf::open(&path).unwrap();
        assert_eq!(
            str_attribute(&file, Some("time"), "units").as_deref(),
            Some("nanoseconds since 1970-01-01T00:00:00Z")
        );
        assert_eq!(
            str_attribute(&file, Some("time"), "calendar").as_deref(),
            Some("standard")
        );
        let values: Vec<i64> = file.variable("time").unwrap().get_values(..).unwrap();
        assert_eq!(values, vec![5]);
    }
}
