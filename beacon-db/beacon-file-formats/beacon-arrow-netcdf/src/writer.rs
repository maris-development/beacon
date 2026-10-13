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
    datatypes::{DataType, Field, Schema, SchemaRef},
    ipc::writer::FileWriter,
};
use tempfile::SpooledTempFile;

use indexmap::IndexMap;

use crate::attributes::{AttributeCombiner, AttributePlan, AttributeTarget, TIME_OWNED};
use crate::datafusion::options::{NetcdfWriterOptions, OBS_DIMENSION};
use crate::encoders::{Encoder, EncoderOptions};

/// The CF version that the flat writer names in the `Conventions` attribute.
const CONVENTIONS: &str = "CF-1.10";

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
    /// Boolean columns, and whether each one has a null so far.
    boolean_columns: HashMap<String, bool>,
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
        let boolean_columns = schema
            .fields()
            .iter()
            .filter(|field| field.data_type() == &DataType::Boolean)
            .map(|field| (field.name().clone(), false))
            .collect();
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

        let writer = Self {
            path,
            writer,
            schema,
            fixed_string_sizes,
            combiner,
            options,
            rows: 0,
            boolean_columns,
            encoder: PhantomData,
        };
        // Fail before the spool. `finish` checks again, because combine_attrs can drop a column.
        Self::check_options(&writer.options, &writer.schema)?;
        Ok(writer)
    }

    /// Append an Arrow record batch to the buffered stream.
    pub fn write_record_batch(
        &mut self,
        record_batch: arrow::record_batch::RecordBatch,
    ) -> anyhow::Result<()> {
        for (field, column) in self.schema.fields().iter().zip(record_batch.columns()) {
            if column.null_count() > 0 {
                if let Some(has_null) = self.boolean_columns.get_mut(field.name()) {
                    *has_null = true;
                }
            }
        }
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
            record_rows: (!self.options.unlimited_obs()).then_some(self.rows),
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

        let attributes = Self::attributes(
            &self.options,
            plan.attributes,
            &updated_schema,
            &self.boolean_columns,
        )?;
        let mut nc_writer =
            Writer::<E>::with_options(&self.path, updated_schema.clone(), &encoder_options)?;
        nc_writer.put_attributes(&attributes)?;

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

    /// Check the options against the columns of `schema`.
    fn check_options(options: &NetcdfWriterOptions, schema: &Schema) -> anyhow::Result<()> {
        if let Some(name) = options
            .unlimited_dims
            .iter()
            .find(|name| *name != OBS_DIMENSION)
        {
            anyhow::bail!(
                "unlimited_dims names '{name}': the flat output has the dimension \
                 '{OBS_DIMENSION}' only"
            );
        }
        let reserved = |name: &str| name.is_empty() || name.starts_with('_');
        if let Some(name) = options.global_attributes.keys().find(|n| reserved(n)) {
            anyhow::bail!("invalid global attribute name '{name}': it is empty or starts with '_'");
        }
        for (variable, attributes) in &options.variable_attributes {
            let field = schema.field_with_name(variable).map_err(|_| {
                anyhow::anyhow!(
                    "variable_attributes names '{variable}', which is not a variable of the output"
                )
            })?;
            for name in attributes.keys() {
                if reserved(name) {
                    anyhow::bail!(
                        "invalid attribute name '{name}' on '{variable}': it is empty or starts \
                         with '_'"
                    );
                }
                if matches!(field.data_type(), DataType::Timestamp(_, _))
                    && TIME_OWNED.contains(&name.as_str())
                {
                    anyhow::bail!(
                        "variable_attributes cannot set '{name}' on the time variable \
                         '{variable}': Beacon writes them from the Arrow time unit"
                    );
                }
            }
        }
        for name in &options.coordinates {
            if schema.field_with_name(name).is_err() {
                anyhow::bail!("coordinates names '{name}', which is not a variable of the output");
            }
        }
        Ok(())
    }

    /// The attributes of the file: the defaults, then `combined`, then the options. A
    /// `variable_attributes` value wins over `coordinates`.
    /// A later value of the same attribute wins.
    fn attributes(
        options: &NetcdfWriterOptions,
        combined: Vec<(AttributeTarget, netcdf::AttributeValue)>,
        schema: &Schema,
        boolean_columns: &HashMap<String, bool>,
    ) -> anyhow::Result<Vec<(AttributeTarget, netcdf::AttributeValue)>> {
        Self::check_options(options, schema)?;
        let global = |name: &str| AttributeTarget::Global {
            name: name.to_string(),
        };
        let history = format!(
            "{} Beacon {}: query output",
            chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ"),
            env!("CARGO_PKG_VERSION")
        );
        let mut attributes = vec![
            (global("Conventions"), CONVENTIONS.into()),
            (global("history"), history.into()),
        ];
        for field in schema.fields() {
            let Some(has_null) = boolean_columns.get(field.name()) else {
                continue;
            };
            let target = |name: &str| AttributeTarget::Variable {
                variable: field.name().clone(),
                name: name.to_string(),
            };
            attributes.push((target("flag_values"), vec![0u8, 1u8].into()));
            attributes.push((target("flag_meanings"), "false true".into()));
            // xarray reads a null as True, so mark only a column with no null as a boolean.
            if !has_null {
                attributes.push((target("dtype"), "bool".into()));
            }
        }
        attributes.extend(combined);
        for (name, value) in &options.global_attributes {
            attributes.push((global(name), value.to_netcdf()));
        }
        if !options.coordinates.is_empty() {
            let coordinates = options.coordinates.join(" ");
            for field in schema.fields() {
                if !options.coordinates.contains(field.name()) {
                    let target = AttributeTarget::Variable {
                        variable: field.name().clone(),
                        name: "coordinates".to_string(),
                    };
                    attributes.push((target, coordinates.clone().into()));
                }
            }
        }
        for (variable, values) in &options.variable_attributes {
            for (name, value) in values {
                let target = AttributeTarget::Variable {
                    variable: variable.clone(),
                    name: name.clone(),
                };
                attributes.push((target, value.to_netcdf()));
            }
        }

        let mut merged = IndexMap::new();
        for (target, value) in attributes {
            merged.insert(target, value);
        }
        Ok(merged.into_iter().collect())
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
        assert_eq!(
            str_attribute(&file, Some("time"), "calendar").as_deref(),
            Some("standard")
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
        // netCDF reads a dimension of length 0 as unlimited.
        assert!(file.dimension("obs").unwrap().is_unlimited());
    }

    #[test]
    fn obs_has_the_row_count_of_the_output() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("fixed.nc");
        let schema = series_schema();
        let batches = vec![series_batch(&schema, 3), series_batch(&schema, 4)];
        write(&path, schema, batches, None);

        let file = netcdf::open(&path).unwrap();
        let obs = file.dimension("obs").unwrap();
        assert!(!obs.is_unlimited());
        assert_eq!(obs.len(), 7);
        let names = file.variable("name").unwrap().get_strings(..).unwrap();
        assert_eq!(names.len(), 7);
        assert_eq!(names[3], "argo");
    }

    #[test]
    fn unlimited_dims_keeps_obs_unlimited() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("unlimited.nc");
        let schema = series_schema();
        let options = NetcdfWriterOptions {
            unlimited_dims: vec!["obs".to_string()],
            ..Default::default()
        };
        write_with(
            &path,
            schema.clone(),
            vec![series_batch(&schema, 5)],
            options,
        )
        .unwrap();

        let file = netcdf::open(&path).unwrap();
        let obs = file.dimension("obs").unwrap();
        assert!(obs.is_unlimited());
        assert_eq!(obs.len(), 5);
    }

    #[test]
    fn an_unknown_unlimited_dimension_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let options = NetcdfWriterOptions {
            unlimited_dims: vec!["time".to_string()],
            ..Default::default()
        };
        let error = write_with(&dir.path().join("bad.nc"), series_schema(), vec![], options)
            .unwrap_err()
            .to_string();
        assert!(error.contains("'time'"), "{error}");
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

    #[test]
    fn every_file_has_conventions_and_history() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cf.nc");
        let schema = series_schema();
        write(&path, schema.clone(), vec![series_batch(&schema, 3)], None);

        let file = netcdf::open(&path).unwrap();
        assert_eq!(
            str_attribute(&file, None, "Conventions").as_deref(),
            Some(CONVENTIONS)
        );
        let history = str_attribute(&file, None, "history").unwrap();
        assert!(history.contains("Beacon"), "{history}");
    }

    fn attribute_options(global: &str, variable: &str) -> NetcdfWriterOptions {
        NetcdfWriterOptions {
            combine_attrs: Some(CombineAttrs::Identical),
            global_attributes: serde_json::from_str(global).unwrap(),
            variable_attributes: serde_json::from_str(variable).unwrap(),
            ..Default::default()
        }
    }

    #[test]
    fn an_attribute_option_wins_over_combine_attrs() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("options.nc");
        let schema = units_schema();
        let options = attribute_options(
            r#"{"title": "From the option", "Conventions": "CF-1.8", "version": 3}"#,
            r#"{"temp": {"units": "K", "long_name": "Temperature"}}"#,
        );
        let batch = units_batch(&schema, "degC", "From the column", 2);
        write_with(&path, schema, vec![batch], options).unwrap();

        let file = netcdf::open(&path).unwrap();
        assert_eq!(
            str_attribute(&file, None, "title").as_deref(),
            Some("From the option")
        );
        assert_eq!(
            str_attribute(&file, None, "Conventions").as_deref(),
            Some("CF-1.8")
        );
        assert_eq!(
            file.attribute("version").unwrap().value().unwrap(),
            AttributeValue::Int(3)
        );
        assert_eq!(
            str_attribute(&file, Some("temp"), "units").as_deref(),
            Some("K")
        );
        assert_eq!(
            str_attribute(&file, Some("temp"), "long_name").as_deref(),
            Some("Temperature")
        );
    }

    #[test]
    fn coordinates_go_on_each_data_variable() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("coordinates.nc");
        let schema = series_schema();
        let options = NetcdfWriterOptions {
            coordinates: vec!["name".to_string()],
            ..Default::default()
        };
        write_with(
            &path,
            schema.clone(),
            vec![series_batch(&schema, 3)],
            options,
        )
        .unwrap();

        let file = netcdf::open(&path).unwrap();
        assert_eq!(
            str_attribute(&file, Some("value"), "coordinates").as_deref(),
            Some("name")
        );
        assert_eq!(str_attribute(&file, Some("name"), "coordinates"), None);
    }

    #[test]
    fn a_boolean_gets_cf_flags_and_the_xarray_marker() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("flags.nc");
        let schema = Arc::new(Schema::new(vec![
            Field::new("good", DataType::Boolean, true),
            Field::new("maybe", DataType::Boolean, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(arrow::array::BooleanArray::from(vec![
                    Some(true),
                    Some(false),
                ])),
                Arc::new(arrow::array::BooleanArray::from(vec![Some(true), None])),
            ],
        )
        .unwrap();
        write(&path, schema, vec![batch], None);

        let file = netcdf::open(&path).unwrap();
        for variable in ["good", "maybe"] {
            let flags = file.variable(variable).unwrap();
            assert_eq!(
                flags.attribute_value("flag_values").unwrap().unwrap(),
                AttributeValue::Uchars(vec![0, 1])
            );
            assert_eq!(
                str_attribute(&file, Some(variable), "flag_meanings").as_deref(),
                Some("false true")
            );
        }
        assert_eq!(
            str_attribute(&file, Some("good"), "dtype").as_deref(),
            Some("bool")
        );
        assert_eq!(str_attribute(&file, Some("maybe"), "dtype"), None);
        let values: Vec<u8> = file.variable("good").unwrap().get_values(..).unwrap();
        assert_eq!(values, vec![1, 0]);
    }

    #[test]
    fn an_unknown_coordinate_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let options = NetcdfWriterOptions {
            coordinates: vec!["lat".to_string()],
            ..Default::default()
        };
        let error = write_with(&dir.path().join("bad.nc"), series_schema(), vec![], options)
            .unwrap_err()
            .to_string();
        assert!(error.contains("'lat'"), "{error}");
    }

    #[test]
    fn bad_attribute_options_are_errors() {
        let dir = tempfile::tempdir().unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("temp", DataType::Float64, true),
            Field::new("time", DataType::Timestamp(TimeUnit::Second, None), true),
        ]));
        for (global, variable, expected) in [
            ("{}", r#"{"salt": {"units": "psu"}}"#, "salt"),
            ("{}", r#"{"time": {"units": "days"}}"#, "Arrow time unit"),
            (r#"{"_NCProperties": "x"}"#, "{}", "_NCProperties"),
        ] {
            let error = write_with(
                &dir.path().join("bad.nc"),
                schema.clone(),
                vec![],
                attribute_options(global, variable),
            )
            .unwrap_err()
            .to_string();
            assert!(error.contains(expected), "{error}");
        }
    }
}
