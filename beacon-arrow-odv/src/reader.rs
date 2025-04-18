use std::{
    collections::HashMap,
    io::{BufRead, BufReader, Read, Seek},
    path::Path,
    sync::Arc,
};

use arrow::{
    array::{RecordBatch, StringArray},
    datatypes::{DataType, Field, SchemaRef},
};
use csv::StringRecord;
use indexmap::IndexMap;
use regex::Regex;

use crate::{error::OdvError, OdvResult};

pub struct OdvReader {
    row_reader: arrow_csv::Reader<BufReader<Box<dyn Read + Send>>>,
    decoder: OdvDecoder,
}

impl OdvReader {
    fn odv_file_reader<P: AsRef<Path>>(path: P) -> anyhow::Result<BufReader<Box<dyn Read + Send>>> {
        match path.as_ref().extension() {
            Some(ext) if ext == "zst" => {
                let file = std::fs::File::open(path)?;
                let decoder = zstd::Decoder::new(file)?;
                Ok(BufReader::new(Box::new(decoder)))
            }
            Some(ext) if ext == "lz4" => {
                let file = std::fs::File::open(path)?;
                let decoder = lz4_flex::frame::FrameDecoder::new(file);
                Ok(BufReader::new(Box::new(decoder)))
            }
            _ => {
                let file = std::fs::File::open(path)?;
                let buf_reader = BufReader::new(Box::new(file) as Box<dyn Read + Send>);
                Ok(buf_reader)
            }
        }
    }

    pub fn new<P: AsRef<Path>>(path: P, batch_size: usize) -> anyhow::Result<Self> {
        let discovered_fields =
            Self::discovered_fields(&mut Self::odv_file_reader(path.as_ref())?)?;
        let header_row = Self::header_row(&mut Self::odv_file_reader(path.as_ref())?)?;

        //Parse the ODV header to get the arrow schema
        let schema = Self::parse_odv_header(discovered_fields, &header_row)
            .map_err(|e| OdvError::ArrowSchemaError(Box::new(e)))?;

        //Create the ODV decoder that uses the Arrow schema to convert the columns to the correct types
        let decoder = OdvDecoder::new(schema.clone());

        //Create the CSV reader
        let row_reader = arrow_csv::ReaderBuilder::new(schema.clone())
            .with_batch_size(batch_size)
            .with_comment(b'/')
            .with_delimiter(b'\t')
            .with_header(true)
            .build(Self::odv_file_reader(path.as_ref())?)
            .map_err(OdvError::ColumnReaderCreationError)?;

        Ok(Self {
            row_reader,
            decoder,
        })
    }

    pub fn schema(&self) -> SchemaRef {
        self.decoder.schema()
    }

    pub fn read<P: AsRef<[usize]>>(
        &mut self,
        projection: Option<P>,
    ) -> Option<OdvResult<RecordBatch>> {
        let batch = self.row_reader.next();
        batch.map(|batch| {
            batch.map_err(OdvError::ColumnReadError).and_then(|batch| {
                self.decoder
                    .decode_batch(projection, batch)
                    .map_err(|e| OdvError::RecordBatchDecodeError(Box::new(e)))
            })
        })
    }

    fn discovered_fields(reader: &mut dyn BufRead) -> OdvResult<IndexMap<String, Field>> {
        let header_lines = Self::metadata_lines(reader)?;
        let mut discovered_fields = IndexMap::new();

        //Insert the default/expected fields
        discovered_fields.insert(
            "Cruise".to_string(),
            Field::new("Cruise", DataType::Utf8, true),
        );
        discovered_fields.insert(
            "Station".to_string(),
            Field::new("Station", DataType::Utf8, true),
        );
        discovered_fields.insert("Type".to_string(), Field::new("Type", DataType::Utf8, true));
        discovered_fields.insert(
            "yyyy-mm-ddThh:mm:ss.sss".to_string(),
            Field::new(
                "yyyy-mm-ddThh:mm:ss.sss",
                DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
                true,
            ),
        );

        for line in &header_lines {
            let field = Self::odv_field_from_header(&line)?;
            if let Some(field) = field {
                discovered_fields.insert(field.name().to_string(), field);
            }
        }

        Ok(discovered_fields)
    }

    fn header_row(reader: &mut dyn BufRead) -> OdvResult<StringRecord> {
        let mut csv_builder = csv::ReaderBuilder::new();
        csv_builder
            .has_headers(true)
            .delimiter(b'\t')
            .comment(Some(b'/'));
        let mut csv_reader = csv_builder.from_reader(reader);
        let header_row = csv_reader.headers().unwrap().clone();
        Ok(header_row)
    }

    fn parse_odv_header(
        discovered_fields: IndexMap<String, Field>,
        header: &StringRecord,
    ) -> OdvResult<SchemaRef> {
        Self::parse_header_row_with_metadata_to_schema(&header, discovered_fields)
    }

    fn metadata_lines<R: BufRead + ?Sized>(read: &mut R) -> OdvResult<Vec<String>> {
        //Read the lines until we find the first line without a // prefix
        let mut header_lines = vec![];

        for line in read.lines() {
            let line = line.map_err(OdvError::MetadataReadError)?;
            if !line.starts_with("//") {
                break;
            }
            header_lines.push(line);
        }

        Ok(header_lines)
    }

    fn odv_field_from_header(line: &str) -> OdvResult<Option<Field>> {
        let re = Regex::new(
            r#"(?m)^//<(?:MetaVariable|DataVariable)>.*?label="([^"]+)".*?value_type="([^"]+)".*?qf_schema="([^"]+)".*?comment="([^"]*)".*?</(?:MetaVariable|DataVariable)>"#
        ).unwrap();

        if let Some(cap) = re.captures(line) {
            let label = &cap[1];
            let value_type = &cap[2];
            let qf_schema = &cap[3];
            let comment = &cap[4];

            let mut metadata = HashMap::new();

            let units_re = Regex::new(r"^(.*?)\s*\[(.*?)\]$").unwrap();

            let field_name = if let Some(caps) = units_re.captures(label) {
                let name = caps.get(1).map_or("", |m| m.as_str());
                let units = caps.get(2).map_or("", |m| m.as_str());

                if !units.is_empty() {
                    metadata.insert("units".to_string(), units.to_string());
                }

                name.to_string()
            } else {
                label.to_string()
            };

            let field = Field::new(
                field_name,
                Self::value_type_to_arrow_type(value_type)?,
                true,
            );

            if !qf_schema.is_empty() {
                metadata.insert("qf_schema".to_string(), qf_schema.to_string());
            }

            if !comment.is_empty() {
                metadata.insert("comment".to_string(), comment.to_string());
            }

            return Ok(Some(field.with_metadata(metadata)));
        }

        Ok(None)
    }

    fn value_type_to_arrow_type(value_type: &str) -> OdvResult<DataType> {
        match value_type {
            "INDEXED_TEXT" => Ok(DataType::Utf8),
            "INTEGER" => Ok(DataType::Int64),
            "FLOAT" => Ok(DataType::Float32),
            "DOUBLE" => Ok(DataType::Float64),
            _ if value_type.starts_with("TEXT:") => Ok(DataType::Utf8),
            dtype => Err(OdvError::UnsupportedDataType(dtype.to_string())),
        }
    }

    fn parse_header_row_with_metadata_to_schema(
        header: &StringRecord,
        discovered_fields: IndexMap<String, Field>,
    ) -> OdvResult<SchemaRef> {
        let mut schema_fields = vec![];

        fn remove_units(s: &str) -> &str {
            if let Some(pos) = s.rfind(" [") {
                if s.ends_with(']') {
                    return &s[..pos];
                }
            }
            s
        }

        // println!("Discovered fields: {:?}", discovered_fields);

        for (_, name) in header.iter().enumerate() {
            let name = remove_units(name);
            if let Some(field) = discovered_fields.get(name) {
                if name.to_lowercase() == "time_iso8601" {
                    schema_fields.push(field.clone().with_data_type(DataType::Timestamp(
                        arrow::datatypes::TimeUnit::Millisecond,
                        None,
                    )));
                } else {
                    schema_fields.push(field.clone());
                }
            } else {
                //Field is not in schema, this means it can be a QF field or an unknown field
                if name.starts_with("QV:") {
                    //Its a QF field
                    //It should be in the form of QV:QF_SCHEMA_NAME:FIELD_NAME where the FIELD_NAME might be optional
                    let parts: Vec<&str> = name.split(':').collect();

                    //If it only contains 2 parts, then the QF field is relative to the previous field
                    if parts.len() == 2 {
                        //Get the previous field
                        let previous_field = schema_fields
                            .last()
                            .ok_or(OdvError::QualityControlFieldNotFound(name.to_string()))?;
                        let qf_field = Field::new(
                            format!("{}_qc", previous_field.name()),
                            DataType::Utf8,
                            true,
                        );
                        schema_fields.push(qf_field);
                    } else if parts.len() == 3 {
                        //If it contains 3 parts, then the QF field is relative to last part which is the field name
                        let qf_field = Field::new(format!("{}_qc", parts[2]), DataType::Utf8, true);
                        schema_fields.push(qf_field);
                    } else {
                        //Invalid QF field
                        return Err(OdvError::InvalidQualityControlField(name.to_string()));
                    }
                } else {
                    //Its an unknown field
                    schema_fields.push(Field::new(name, DataType::Utf8, true));
                }
            }
        }

        Ok(Arc::new(arrow::datatypes::Schema::new(schema_fields)))
    }
}

struct OdvDecoder {
    decoded_schema: SchemaRef,
    added_fields: indexmap::IndexMap<String, String>,
    input_schema: SchemaRef,
}

impl OdvDecoder {
    pub fn new(input_schema: SchemaRef) -> Self {
        let mut fields = vec![];
        let mut added_fields = indexmap::IndexMap::new();
        for field in input_schema.fields() {
            fields.push(field.as_ref().clone());
        }

        for field in input_schema.fields() {
            for (k, v) in field.metadata() {
                fields.push(Field::new(
                    format!("{}.{}", field.name(), k),
                    DataType::Utf8,
                    true,
                ));

                added_fields.insert(format!("{}.{}", field.name(), k), v.clone());
            }
        }

        Self {
            decoded_schema: Arc::new(arrow::datatypes::Schema::new(fields)),
            added_fields,
            input_schema,
        }
    }

    pub fn schema(&self) -> SchemaRef {
        self.decoded_schema.clone()
    }

    pub fn decode_batch<P: AsRef<[usize]>>(
        &self,
        projection: Option<P>,
        batch: RecordBatch,
    ) -> OdvResult<RecordBatch> {
        let mut schema = self.decoded_schema.clone();
        let mut arrays = batch
            .columns()
            .iter()
            .map(|array| array.clone())
            .collect::<Vec<_>>();
        for (_, value) in self.added_fields.iter() {
            let array = Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
                value.clone(),
                batch.num_rows(),
            )));

            arrays.push(array);
        }

        //Apply the projection
        if let Some(projection) = projection {
            let projection = projection.as_ref();
            arrays = projection
                .iter()
                .map(|&idx| arrays[idx].clone())
                .collect::<Vec<_>>();
            schema = Arc::new(
                schema
                    .project(&projection)
                    .map_err(OdvError::SchemaProjectionError)?,
            );
        }

        RecordBatch::try_new(schema, arrays).map_err(OdvError::RecordBatchCreationError)
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_full_file() {
        let path = std::path::Path::new("./test-data/test_file.txt");
        let mut reader = super::OdvReader::new(path, 2).unwrap();

        let schema = reader.schema();
        // println!("{:?}", schema);

        // let batch = reader.read(Some([0, 1, 2, 3, 4, 5, 13])).unwrap();
        // println!("{:?}", batch);
    }
}
