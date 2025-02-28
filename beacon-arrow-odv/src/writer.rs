use std::{
    io::Write,
    ops::{Deref, DerefMut},
    path::{Path, PathBuf},
    sync::Arc,
};

use arrow::{
    array::RecordBatch,
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch,
};
use tokio::io::AsyncWriteExt;

#[derive(Debug, Clone)]
pub struct ColumnInfo {
    pub label: String,
    pub comment: Option<String>,
    pub significant_digits: Option<u32>,
    pub qf_schema: Option<String>,
    pub qf_column: Option<String>,
}

#[derive(Debug, Clone)]
pub struct OdvOptions {
    pub longitude_column: ColumnInfo,
    pub latitude_column: ColumnInfo,
    pub depth_column: ColumnInfo,
    pub time_column: String,
    pub key_column: String,
    pub qf_schema: String,
    pub data_columns: Vec<ColumnInfo>,
    pub meta_columns: Vec<ColumnInfo>,
}

impl OdvOptions {
    pub fn with_longitude_column(mut self, column: ColumnInfo) -> Self {
        self.longitude_column = column;
        self
    }

    pub fn with_latitude_column(mut self, column: ColumnInfo) -> Self {
        self.latitude_column = column;
        self
    }

    pub fn with_depth_column(mut self, column: ColumnInfo) -> Self {
        self.depth_column = column;
        self
    }

    pub fn with_time_column(mut self, column: String) -> Self {
        self.time_column = column;
        self
    }

    pub fn with_key_column(mut self, column: String) -> Self {
        self.key_column = column;
        self
    }

    pub fn with_qf_schema(mut self, schema: String) -> Self {
        self.qf_schema = schema;
        self
    }
}

impl Default for OdvOptions {
    fn default() -> Self {
        Self {
            time_column: "TIME".to_string(),
            key_column: "CRUISE".to_string(),
            qf_schema: "".to_string(),
            depth_column: ColumnInfo {
                label: "DEPTH".to_string(),
                comment: None,
                significant_digits: None,
                qf_schema: None,
                qf_column: None,
            },
            latitude_column: ColumnInfo {
                label: "LATITUDE".to_string(),
                comment: None,
                significant_digits: None,
                qf_schema: None,
                qf_column: None,
            },
            longitude_column: ColumnInfo {
                label: "LONGITUDE".to_string(),
                comment: None,
                significant_digits: None,
                qf_schema: None,
                qf_column: None,
            },
            data_columns: vec![],
            meta_columns: vec![],
        }
    }
}

pub struct AsyncOdvWriter {
    options: OdvOptions,
    directory: PathBuf,
    error_file: OdvFile<Error>,
    timeseries_file: OdvFile<TimeSeries>,
    profile_file: OdvFile<Profiles>,
    trajectory_file: OdvFile<Trajectories>,
}

impl AsyncOdvWriter {
    pub async fn new<P: AsRef<Path>>(options: OdvOptions, dir_path: P) -> anyhow::Result<Self> {
        todo!()
    }

    pub async fn write(&self, record_batch: RecordBatch) -> anyhow::Result<()> {
        todo!()
    }
}

pub struct OdvFile<T: OdvType> {
    _type: std::marker::PhantomData<T>,
    writer: arrow_csv::Writer<std::fs::File>,
}

impl<T: OdvType> OdvFile<T> {
    pub fn new<P: AsRef<Path>>(
        path: P,
        options: &OdvOptions,
        output_schema: arrow::datatypes::SchemaRef,
    ) -> anyhow::Result<Self> {
        let file = std::fs::File::create(path)?;

        Ok(Self {
            _type: std::marker::PhantomData,
            writer: arrow_csv::WriterBuilder::new()
                .with_header(false)
                .with_timestamp_format("%Y-%m-%dT%H:%M:%S%.3f".to_string())
                .with_delimiter(b'\t')
                .build(file),
        })
    }

    fn write_header<W: Write>(
        writer: &mut W,
        options: &OdvOptions,
        output_schema: arrow::datatypes::SchemaRef,
    ) -> anyhow::Result<()> {
        writeln!(writer, "//<Encoding>UTF-8</Encoding>")?;
        writeln!(writer, "//<DataField>Ocean</DataField>")?;
        writeln!(writer, "{}", T::type_header())?;
        writeln!(writer, "//")?;

        //Write required column header
        writeln!(
            writer,
            "{}",
            Self::meta_header(
                "Cruise",
                &options.qf_schema,
                &Self::arrow_to_value_type(
                    output_schema
                        .field_with_name(&options.key_column)
                        .expect("")
                        .data_type()
                )?,
                ""
            )
        )?;
        //Write required station header
        writeln!(
            writer,
            "{}",
            Self::meta_header("Station", &options.qf_schema, "INDEXED_TEXT", "")
        )?;
        //Write required station header
        writeln!(
            writer,
            "{}",
            Self::meta_header("Type", &options.qf_schema, "TEXT:2", "")
        )?;
        //Write longitude column
        writeln!(
            writer,
            "{}",
            Self::meta_header(
                &options.longitude_column.label,
                &options.qf_schema,
                &Self::arrow_to_value_type(
                    output_schema
                        .field_with_name(&options.longitude_column.label)
                        .expect("")
                        .data_type()
                )?,
                &options.longitude_column.comment.as_deref().unwrap_or("")
            )
        )?;
        //Write latitude column
        writeln!(
            writer,
            "{}",
            Self::meta_header(
                &options.latitude_column.label,
                &options.qf_schema,
                &Self::arrow_to_value_type(
                    output_schema
                        .field_with_name(&options.latitude_column.label)
                        .expect("")
                        .data_type()
                )?,
                &options.latitude_column.comment.as_deref().unwrap_or("")
            )
        )?;

        //Write all the metadata variables/columns
        for column in options.meta_columns.iter() {
            let value_type = Self::arrow_to_value_type(
                output_schema
                    .field_with_name(&column.label)
                    .expect("Column not found in output schema.")
                    .data_type(),
            )?;

            writeln!(
                writer,
                "{}",
                Self::meta_header(
                    &column.label,
                    &options.qf_schema,
                    &value_type,
                    &column.comment.as_deref().unwrap_or("")
                )
            )?;
        }

        writeln!(writer, "//")?;

        //Write all the data variables/columns
        for column in options.data_columns.iter() {
            let value_type = Self::arrow_to_value_type(
                output_schema
                    .field_with_name(&column.label)
                    .expect("Column not found in output schema.")
                    .data_type(),
            )?;

            writeln!(
                writer,
                "{}",
                Self::data_header(
                    &column.label,
                    &options.qf_schema,
                    &value_type,
                    &column.comment.as_deref().unwrap_or("")
                )
            )?;
        }

        todo!()
    }

    fn arrow_to_value_type(arrow_type: &DataType) -> anyhow::Result<String> {
        match arrow_type {
            DataType::Null => Ok("INDEXED_TEXT".to_string()),
            DataType::Boolean => Ok("INDEXED_TEXT".to_string()),
            DataType::Int8 => Ok("INTEGER".to_string()),
            DataType::Int16 => Ok("INTEGER".to_string()),
            DataType::Int32 => Ok("INTEGER".to_string()),
            DataType::Int64 => Ok("INTEGER".to_string()),
            DataType::UInt8 => Ok("INTEGER".to_string()),
            DataType::UInt16 => Ok("INTEGER".to_string()),
            DataType::UInt32 => Ok("INTEGER".to_string()),
            DataType::UInt64 => Ok("INTEGER".to_string()),
            DataType::Float32 => Ok("FLOAT".to_string()),
            DataType::Float64 => Ok("FLOAT".to_string()),
            DataType::Timestamp(_, _) => Ok("INDEXED_TEXT".to_string()),
            DataType::Utf8 => Ok("INDEXED_TEXT".to_string()),
            dtype => anyhow::bail!("Unsupported data type for ODV export: {:?}", dtype),
        }
    }

    fn data_header(label: &str, qf_schema: &str, value_type: &str, comment: &str) -> String {
        const GENERIC_DATA_HEADER : &'static str = "//<DataVariable> label=\"$LABEL\" value_type=\"$VALUE_TYPE\" qf_scheme=\"$QF_SCHEMA\" comment=\"$COMMENT\" </DataVariable>";

        let header = GENERIC_DATA_HEADER
            .replace("$LABEL", label)
            .replace("$VALUE_TYPE", value_type)
            .replace("$QF_SCHEMA", qf_schema)
            .replace("$COMMENT", comment);

        header
    }

    fn meta_header(label: &str, qf_schema: &str, value_type: &str, comment: &str) -> String {
        const GENERIC_META_HEADER: &'static str = "//<MetaVariable> label=\"$LABEL\" value_type=\"$VALUE_TYPE\" qf_schema=\"$QF_SCHEMA\" comment=\"$COMMENT\"</MetaVariable>";

        let header = GENERIC_META_HEADER
            .replace("$LABEL", label)
            .replace("$QF_SCHEMA", qf_schema)
            .replace("$VALUE_TYPE", value_type)
            .replace("$COMMENT", comment);

        header
    }

    pub fn write_batch(&mut self, batch: RecordBatch) -> anyhow::Result<()> {
        self.writer.write(&batch)?;
        Ok(())
    }

    pub fn finish(self) -> anyhow::Result<std::fs::File> {
        Ok(self.writer.into_inner())
    }
}

pub trait OdvType {
    fn type_header() -> String;
    fn map_station(record_batch: RecordBatch) -> RecordBatch {
        let mut fields = record_batch.schema().fields().to_vec();
        fields.push(Arc::new(Field::new("Station", DataType::Int64, false)));

        let mut arrays = record_batch.columns().to_vec();
        let station = arrow::array::Int64Array::from(vec![1; record_batch.num_rows()]);
        arrays.push(Arc::new(station) as Arc<dyn arrow::array::Array>);

        RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
            .expect("Mapping of station number failed.")
    }
    fn map_type(record_batch: RecordBatch) -> RecordBatch {
        let mut fields = record_batch.schema().fields().to_vec();
        fields.push(Arc::new(Field::new("Type", DataType::Utf8, false)));

        let mut arrays = record_batch.columns().to_vec();
        let _type = arrow::array::StringArray::from(vec!["*"; record_batch.num_rows()]);
        arrays.push(Arc::new(_type) as Arc<dyn arrow::array::Array>);

        RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
            .expect("Mapping of type number failed.")
    }
}

pub struct Profiles;
impl OdvType for Profiles {
    fn type_header() -> String {
        format!("//<DataType>Profiles</DataType>")
    }
}

pub struct TimeSeries;
impl OdvType for TimeSeries {
    fn type_header() -> String {
        format!("//<DataType>Timeseries</DataType>")
    }
}

pub struct Trajectories;
impl OdvType for Trajectories {
    fn type_header() -> String {
        format!("//<DataType>Timeseries</DataType>")
    }

    fn map_station(record_batch: RecordBatch) -> RecordBatch {
        todo!()
    }
}

pub struct Error;
impl OdvType for Error {
    fn type_header() -> String {
        format!("//<DataType>Error</DataType>")
    }
}

pub struct OdvBatchSchemaMapper {
    input_schema: SchemaRef,
    output_schema: SchemaRef,
    projection: Arc<[usize]>,
}

impl OdvBatchSchemaMapper {
    pub fn new(input_schema: SchemaRef, odv_options: OdvOptions) -> anyhow::Result<Self> {
        let mut output_fields = vec![];

        odv_options.key_column;

        todo!()
    }

    pub fn output_schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }

    pub fn map(&self, batch: RecordBatch) -> RecordBatch {
        assert_eq!(
            &batch.schema(),
            &self.input_schema,
            "Batch schema does not match expected input schema. This is a bug."
        );
        let projected_batch = batch.project(&self.projection).unwrap();
        let arrays = projected_batch.columns().to_vec();
        RecordBatch::try_new(self.output_schema.clone(), arrays).unwrap()
    }
}
