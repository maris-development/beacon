//! High-level writers for serializing Arrow record batches into NetCDF.

use std::{
    collections::{HashMap, VecDeque},
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
use crate::encoders::{discard, Encoder, EncoderOptions};

/// Generic writer backed by an [`Encoder`] implementation.
///
/// [`close`](Self::close) puts the file at its path. A drop without a close leaves no file.
pub struct Writer<E: Encoder> {
    /// `None` after the close.
    encoder: Option<E>,
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
        let nc_file = oxcdf::create(path).map_err(|e| anyhow::anyhow!(e))?;

        let encoder = E::create(nc_file, schema, options).map_err(|e| anyhow::anyhow!(e))?;

        Ok(Self {
            encoder: Some(encoder),
        })
    }

    fn encoder(&mut self) -> anyhow::Result<&mut E> {
        self.encoder
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("the NetCDF writer is closed"))
    }

    /// Write one Arrow record batch into the target NetCDF file.
    pub fn write_record_batch(
        &mut self,
        record_batch: arrow::record_batch::RecordBatch,
    ) -> anyhow::Result<()> {
        self.encoder()?
            .write_record_batch(record_batch)
            .map_err(|e| anyhow::anyhow!(e))?;

        Ok(())
    }

    /// Write attributes. Call this before the first batch.
    pub fn put_attributes(
        &mut self,
        attributes: &[(AttributeTarget, oxcdf::AttributeValue)],
    ) -> anyhow::Result<()> {
        let encoder = self.encoder()?;
        for (target, value) in attributes {
            encoder
                .put_attribute(target.variable(), target.name(), value.clone())
                .map_err(|e| anyhow::anyhow!(e))?;
        }
        Ok(())
    }

    /// Write the file and put it at its path. An error leaves no file at the path.
    pub fn close(mut self) -> anyhow::Result<()> {
        let encoder = self
            .encoder
            .take()
            .ok_or_else(|| anyhow::anyhow!("the NetCDF writer is closed"))?;
        encoder.into_file().close().map_err(|e| anyhow::anyhow!(e))
    }
}

impl<E: Encoder> Drop for Writer<E> {
    fn drop(&mut self) {
        if let Some(encoder) = self.encoder.take() {
            discard(encoder.into_file());
        }
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
        self.rows += record_batch.num_rows();

        Ok(())
    }

    /// The settings the encoder applies when it defines the variables.
    fn encoder_options(&self) -> EncoderOptions {
        EncoderOptions {
            compression: self.options.compression,
            // A chunk larger than the data wastes space, because HDF5 stores full chunks.
            chunk_rows: Some(self.options.chunk_rows().min(self.rows).max(1)),
        }
    }

    /// Finalize buffered data and write the NetCDF file.
    pub fn finish(&mut self) -> anyhow::Result<()> {
        self.writer.finish().map_err(|e| anyhow::anyhow!(e))?;
        let plan = match self.combiner.take() {
            Some(combiner) => combiner.resolve()?,
            None => AttributePlan::default(),
        };
        let encoder_options = self.encoder_options();
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
            return nc_writer.close();
        }

        let batches = reader
            .map(|batch| -> anyhow::Result<RecordBatch> { Ok(batch?.project(&kept_columns)?) });
        // A compressed chunk goes to the file only when one write covers the whole chunk.
        let whole_chunks = encoder_options
            .chunk_rows
            .filter(|_| encoder_options.compression.is_some());
        let batches: Box<dyn Iterator<Item = anyhow::Result<RecordBatch>>> = match whole_chunks {
            Some(rows) => Box::new(WholeChunks::new(batches, rows)),
            None => Box::new(batches),
        };
        for batch in batches {
            let updated_batch = Self::map_record_batch(batch?, updated_schema.clone())?;
            nc_writer.write_record_batch(updated_batch)?;
        }

        nc_writer.close()
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

/// Regroups batches into batches of `rows` rows. The last batch can hold fewer rows.
///
/// It keeps only the rows that do not yet fill a batch.
struct WholeChunks<I> {
    batches: I,
    rows: usize,
    /// Rows that do not yet fill a batch, in input order.
    pending: VecDeque<RecordBatch>,
    pending_rows: usize,
}

impl<I> WholeChunks<I> {
    fn new(batches: I, rows: usize) -> Self {
        Self {
            batches,
            rows: rows.max(1),
            pending: VecDeque::new(),
            pending_rows: 0,
        }
    }

    /// Remove the first `rows` pending rows and return them as one batch.
    fn take(&mut self, rows: usize) -> anyhow::Result<RecordBatch> {
        let mut parts = Vec::new();
        let mut missing = rows;
        while missing > 0 {
            let Some(batch) = self.pending.pop_front() else {
                break;
            };
            if batch.num_rows() > missing {
                parts.push(batch.slice(0, missing));
                self.pending
                    .push_front(batch.slice(missing, batch.num_rows() - missing));
                missing = 0;
            } else {
                missing -= batch.num_rows();
                parts.push(batch);
            }
        }
        self.pending_rows -= rows - missing;
        match parts.len() {
            1 => Ok(parts.remove(0)),
            _ => Ok(arrow::compute::concat_batches(&parts[0].schema(), &parts)?),
        }
    }
}

impl<I: Iterator<Item = anyhow::Result<RecordBatch>>> Iterator for WholeChunks<I> {
    type Item = anyhow::Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        while self.pending_rows < self.rows {
            match self.batches.next() {
                Some(Ok(batch)) if batch.num_rows() == 0 => {}
                Some(Ok(batch)) => {
                    self.pending_rows += batch.num_rows();
                    self.pending.push_back(batch);
                }
                Some(Err(e)) => return Some(Err(e)),
                None if self.pending_rows == 0 => return None,
                None => return Some(self.take(self.pending_rows)),
            }
        }
        Some(self.take(self.rows))
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
    use oxcdf::{AttributeValue, NetcdfFile};

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

    fn open(path: &Path) -> NetcdfFile {
        oxcdf::open(path).expect("open the file")
    }

    fn str_attribute(file: &NetcdfFile, variable: Option<&str>, name: &str) -> Option<String> {
        let attribute = match variable {
            Some(variable) => file.variable(variable)?.attribute(name)?,
            None => file.attribute(name)?,
        };
        match &attribute.value {
            AttributeValue::Str(value) => Some(value.clone()),
            _ => None,
        }
    }

    fn values(file: &NetcdfFile, variable: &str) -> Vec<f64> {
        file.variable(variable).unwrap().get_values(..).unwrap()
    }

    fn strings(file: &NetcdfFile, variable: &str) -> Vec<String> {
        file.variable(variable).unwrap().get_strings(..).unwrap()
    }

    fn chunk_shape(file: &NetcdfFile, variable: &str) -> Option<Vec<usize>> {
        file.variable(variable).unwrap().chunking().unwrap()
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

        let file = open(&path);
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

        let file = open(&path);
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

        let file = open(&path);
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

        let file = open(&path);
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

        let file = open(&path);
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

        let file = open(&path);
        assert_eq!(str_attribute(&file, None, "title").as_deref(), Some("Argo"));
        assert_eq!(file.variables().len(), 0);
    }

    #[test]
    fn zero_rows_write_a_valid_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty.nc");
        write(&path, units_schema(), vec![], Some(CombineAttrs::Identical));

        let file = open(&path);
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
        // Beacon does not compress a string variable, so use numbers only.
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

        let plain = open(&plain);
        let packed = open(&packed);
        assert_eq!(values(&packed, "value").len(), 100_000);
        assert_eq!(values(&packed, "value"), values(&plain, "value"));
        assert_eq!(strings(&packed, "name"), strings(&plain, "name"));
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

        assert_eq!(chunk_shape(&open(&path), "value"), Some(vec![1000]));
    }

    #[test]
    fn a_compressed_write_of_part_chunks_reads_back_in_order() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("parts.nc");
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Float64,
            true,
        )]));
        let batch = |start: usize| {
            let values: Vec<f64> = (start..start + 3).map(|v| v as f64).collect();
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Float64Array::from(values))])
                .unwrap()
        };
        let options = NetcdfWriterOptions {
            chunk_rows: std::num::NonZeroUsize::new(4),
            ..compressed(4)
        };
        write_with(
            &path,
            schema.clone(),
            vec![batch(0), batch(3), batch(6)],
            options,
        )
        .unwrap();

        let file = open(&path);
        assert_eq!(chunk_shape(&file, "value"), Some(vec![4]));
        let expected: Vec<f64> = (0..9).map(|v| v as f64).collect();
        assert_eq!(values(&file, "value"), expected);
    }

    fn int_batches(sizes: &[usize]) -> Vec<anyhow::Result<RecordBatch>> {
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int32, false)]));
        let mut next = 0;
        sizes
            .iter()
            .map(|&size| {
                let values: Vec<i32> = (next..next + size as i32).collect();
                next += size as i32;
                Ok(RecordBatch::try_new(
                    schema.clone(),
                    vec![Arc::new(arrow::array::Int32Array::from(values))],
                )?)
            })
            .collect()
    }

    /// The batch sizes and the values that `WholeChunks` gives for `sizes` and `rows`.
    fn regroup(sizes: &[usize], rows: usize) -> (Vec<usize>, Vec<i32>) {
        let batches: Vec<RecordBatch> = WholeChunks::new(int_batches(sizes).into_iter(), rows)
            .collect::<anyhow::Result<_>>()
            .unwrap();
        let values = batches
            .iter()
            .flat_map(|batch| {
                let column = batch.column(0).as_any();
                let column = column.downcast_ref::<arrow::array::Int32Array>().unwrap();
                column.values().to_vec()
            })
            .collect();
        (batches.iter().map(RecordBatch::num_rows).collect(), values)
    }

    #[test]
    fn whole_chunks_joins_small_batches() {
        assert_eq!(regroup(&[3, 3, 3], 4), (vec![4, 4, 1], (0..9).collect()));
    }

    #[test]
    fn whole_chunks_splits_a_large_batch() {
        assert_eq!(regroup(&[8], 4), (vec![4, 4], (0..8).collect()));
    }

    #[test]
    fn whole_chunks_keeps_a_short_last_batch() {
        assert_eq!(regroup(&[2], 4), (vec![2], vec![0, 1]));
    }

    #[test]
    fn whole_chunks_gives_no_batch_for_no_input() {
        assert_eq!(regroup(&[], 4), (vec![], vec![]));
        assert_eq!(regroup(&[0, 0], 4), (vec![], vec![]));
    }

    #[test]
    fn whole_chunks_passes_an_error_on() {
        let mut input = int_batches(&[3]);
        input.push(Err(anyhow::anyhow!("a broken batch")));
        let mut chunks = WholeChunks::new(input.into_iter(), 4);
        assert!(chunks.next().unwrap().is_err());
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

        assert_eq!(chunk_shape(&open(&path), "value"), Some(vec![10]));
    }

    /// Write `batches` with a plain [`Writer`].
    fn write_plain(
        path: &Path,
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
    ) -> anyhow::Result<()> {
        let mut writer = Writer::<DefaultEncoder>::new(path, schema)?;
        for batch in batches {
            writer.write_record_batch(batch)?;
        }
        writer.close()
    }

    fn name_batch(names: Vec<Option<&str>>) -> (SchemaRef, RecordBatch) {
        let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, true)]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(StringArray::from(names))]).unwrap();
        (schema, batch)
    }

    #[test]
    fn a_null_string_is_written_as_an_empty_string() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("null.nc");
        let (schema, batch) = name_batch(vec![Some("a"), None, Some("c")]);
        write_plain(&path, schema, vec![batch]).unwrap();

        assert_eq!(strings(&open(&path), "name"), ["a", "", "c"]);
    }

    #[test]
    fn a_string_is_cut_at_an_interior_nul() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nul.nc");
        let (schema, batch) = name_batch(vec![Some("ab\0cd")]);
        write_plain(&path, schema, vec![batch]).unwrap();

        assert_eq!(strings(&open(&path), "name"), ["ab"]);
    }

    #[test]
    fn a_fixed_size_string_is_a_char_variable() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("char.nc");
        let schema = Arc::new(Schema::new(vec![Field::new(
            "code",
            DataType::FixedSizeBinary(4),
            true,
        )]));
        let codes = FixedSizeBinaryArray::try_from_iter(
            [b"ab\0\0".as_slice(), b"wxyz".as_slice()].into_iter(),
        )
        .unwrap();
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(codes)]).unwrap();
        write_plain(&path, schema, vec![batch]).unwrap();

        let file = open(&path);
        let code = file.variable("code").unwrap();
        assert_eq!(code.dimensions.len(), 2);
        assert_eq!(file.dimension_len(&code.dimensions[1]), Some(4));
        assert_eq!(code.get_raw_values(..).unwrap(), b"ab\0\0wxyz");
    }

    #[test]
    fn a_result_with_zero_rows_gives_a_file_that_opens() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("zero.nc");
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Float64,
            true,
        )]));
        write_plain(&path, schema, vec![]).unwrap();

        assert_eq!(open(&path).variable("value").unwrap().len(), 0);
    }

    #[test]
    fn a_failed_create_leaves_no_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("missing").join("out.nc");
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Float64,
            true,
        )]));
        assert!(Writer::<DefaultEncoder>::new(&path, schema).is_err());
        assert!(!path.exists());
    }

    #[test]
    fn an_unsupported_column_leaves_no_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("date.nc");
        let schema = Arc::new(Schema::new(vec![Field::new("day", DataType::Date32, true)]));
        assert!(Writer::<DefaultEncoder>::new(&path, schema).is_err());
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
    }

    #[test]
    fn a_writer_dropped_without_close_leaves_no_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dropped.nc");
        let (schema, batch) = name_batch(vec![Some("a")]);
        let mut writer = Writer::<DefaultEncoder>::new(&path, schema).unwrap();
        writer.write_record_batch(batch).unwrap();
        drop(writer);
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
    }

    #[test]
    fn an_attribute_above_64_kib_fails_and_leaves_no_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("large.nc");
        let schema = Arc::new(Schema::new(vec![
            Field::new("temp", DataType::Float64, true),
            Field::new("temp.comment", DataType::Utf8, true),
        ]));
        let comment = "x".repeat(70_000);
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Float64Array::from(vec![1.0])),
                Arc::new(StringArray::from(vec![comment.as_str()])),
            ],
        )
        .unwrap();
        let options = NetcdfWriterOptions {
            combine_attrs: Some(CombineAttrs::Identical),
            ..Default::default()
        };

        let error = write_with(&path, schema, vec![batch], options).unwrap_err();
        let text = format!("{error:#}");
        assert!(text.contains("comment"), "{text}");
        assert!(text.contains("temp"), "{text}");
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
    }

    /// Write a file with one Float64 variable and the global attribute `title`.
    fn write_title(path: &Path, title: AttributeValue) {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Float64,
            true,
        )]));
        let mut writer = Writer::<DefaultEncoder>::new(path, schema).unwrap();
        let target = AttributeTarget::Global {
            name: "title".to_string(),
        };
        writer.put_attributes(&[(target, title)]).unwrap();
        writer.close().unwrap();
    }

    #[test]
    fn a_text_attribute_is_cut_at_an_interior_nul() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("title.nc");
        write_title(&path, AttributeValue::Str("ab\0cd".to_string()));

        assert_eq!(
            str_attribute(&open(&path), None, "title").as_deref(),
            Some("ab")
        );
    }

    #[test]
    fn each_text_of_a_list_attribute_is_cut_at_an_interior_nul() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("titles.nc");
        let titles = vec!["x\0y".to_string(), "z".to_string()];
        write_title(&path, AttributeValue::Strs(titles));

        let file = open(&path);
        let value = &file.attribute("title").unwrap().value;
        assert_eq!(
            value,
            &AttributeValue::Strs(vec!["x".to_string(), "z".to_string()])
        );
    }
}
