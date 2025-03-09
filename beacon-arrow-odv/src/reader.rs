use std::{
    collections::HashMap,
    io::{BufRead, BufReader, Seek},
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

pub struct OdvReader {
    row_reader: arrow_csv::Reader<std::fs::File>,
    decoder: OdvDecoder,
}

impl OdvReader {
    pub fn new<P: AsRef<Path>>(path: P, batch_size: usize) -> anyhow::Result<Self> {
        let file = std::fs::File::open(path)?;
        let mut buf_reader = BufReader::new(file);
        let schema = Self::parse_odv_header(&mut buf_reader)?;
        buf_reader.seek(std::io::SeekFrom::Start(0)).unwrap();
        let decoder = OdvDecoder::new(schema.clone());
        let row_reader = arrow_csv::ReaderBuilder::new(schema.clone())
            .with_batch_size(batch_size)
            .with_comment(b'/')
            .with_delimiter(b'\t')
            .with_header(true)
            .build(buf_reader.into_inner())?;

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
    ) -> Option<anyhow::Result<RecordBatch>> {
        let batch = self.row_reader.next();
        batch.map(|batch| {
            batch
                .map_err(Into::into)
                .and_then(|batch| self.decoder.decode_batch(projection, batch))
        })
    }

    fn parse_odv_header<R: BufRead + Seek>(reader: &mut R) -> anyhow::Result<SchemaRef> {
        let header_lines = Self::metadata_lines(reader)?;
        let mut discovered_fields = IndexMap::new();

        //Insert the default/expected fields
        let mut field_map = IndexMap::new();
        field_map.insert(
            "Cruise".to_string(),
            Field::new("Cruise", DataType::Utf8, true),
        );
        field_map.insert(
            "Station".to_string(),
            Field::new("Station", DataType::Utf8, true),
        );
        field_map.insert("Type".to_string(), Field::new("Type", DataType::Utf8, true));
        field_map.insert(
            "yyyy-mm-ddThh:mm:ss.sss".to_string(),
            Field::new("yyyy-mm-ddThh:mm:ss.sss", DataType::Utf8, true),
        );

        for line in &header_lines {
            let field = Self::odv_field_from_header(&line)?;
            if let Some(field) = field {
                discovered_fields.insert(field.name().to_string(), field);
            }
        }
        reader.seek(std::io::SeekFrom::Start(0)).unwrap();
        let mut csv_builder = csv::ReaderBuilder::new();
        csv_builder
            .has_headers(true)
            .delimiter(b'\t')
            .comment(Some(b'/'));
        let mut csv_reader = csv_builder.from_reader(reader);
        let header_row = csv_reader.headers().unwrap().clone();

        Self::parse_header_row_with_metadata_to_schema(&header_row, discovered_fields)
    }

    fn metadata_lines<R: BufRead>(read: &mut R) -> anyhow::Result<Vec<String>> {
        //Read the lines until we find the first line without a // prefix
        let mut header_lines = vec![];

        for line in read.lines() {
            let line = line?;
            if !line.starts_with("//") {
                break;
            }
            header_lines.push(line);
        }

        Ok(header_lines)
    }

    fn odv_field_from_header(line: &str) -> anyhow::Result<Option<Field>> {
        let re = Regex::new(
            r#"(?m)^//<(?:MetaVariable|DataVariable)>.*?label="([^"]+)".*?value_type="([^"]+)".*?qf_schema="([^"]+)".*?comment="([^"]*)".*?</(?:MetaVariable|DataVariable)>"#
        ).unwrap();

        if let Some(cap) = re.captures(line) {
            let label = &cap[1];
            let value_type = &cap[2];
            let qf_schema = &cap[3];
            let comment = &cap[4];

            let mut metadata = HashMap::new();
            let field = Field::new(label, Self::value_type_to_arrow_type(value_type)?, true);

            metadata.insert("qf_schema".to_string(), qf_schema.to_string());
            metadata.insert("comment".to_string(), comment.to_string());

            return Ok(Some(field.with_metadata(metadata)));
        }

        Ok(None)
    }

    fn value_type_to_arrow_type(value_type: &str) -> anyhow::Result<DataType> {
        match value_type {
            "INDEXED_TEXT" => Ok(DataType::Utf8),
            "INTEGER" => Ok(DataType::Int64),
            "FLOAT" => Ok(DataType::Float32),
            "DOUBLE" => Ok(DataType::Float64),
            _ if value_type.starts_with("TEXT:") => Ok(DataType::Utf8),
            _ => Err(anyhow::anyhow!("Unsupported value type: {}", value_type)),
        }
    }

    fn parse_header_row_with_metadata_to_schema(
        header: &StringRecord,
        discovered_fields: IndexMap<String, Field>,
    ) -> anyhow::Result<SchemaRef> {
        let mut schema_fields = vec![];

        for (idx, name) in header.iter().enumerate() {
            if let Some(field) = discovered_fields.get(name) {
                schema_fields.push(field.clone());
            } else {
                //Field is not in schema, this means it can be a QF field or an unknown field
                if name.starts_with("QV:") {
                    //Its a QF field
                    //It should be in the form of QV:QF_SCHEMA_NAME:FIELD_NAME where the FIELD_NAME might be optional
                    let parts: Vec<&str> = name.split(':').collect();

                    //If it only contains 2 parts, then the QF field is relative to the previous field
                    if parts.len() == 2 {
                        //Get the previous field
                        let previous_field = schema_fields.last().ok_or(anyhow::anyhow!(
                            "QF field {} is relative to a field that does not exist",
                            name
                        ))?;
                        let qf_field = Field::new(
                            format!("{}_QF", previous_field.name()),
                            DataType::Utf8,
                            true,
                        );
                        schema_fields.push(qf_field);
                    } else if parts.len() == 3 {
                        //If it contains 3 parts, then the QF field is relative to last part which is the field name
                        let qf_field = Field::new(format!("{}_QF", parts[2]), DataType::Utf8, true);
                        schema_fields.push(qf_field);
                    } else {
                        //Invalid QF field
                        return Err(anyhow::anyhow!("Invalid QF field: {}", name));
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
    ) -> anyhow::Result<RecordBatch> {
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
            schema = Arc::new(schema.project(&projection).map_err(|e| {
                anyhow::anyhow!(
                    "Failed to apply projection during ODV batch decoding. {}",
                    e
                )
            })?);
        }

        RecordBatch::try_new(schema, arrays).map_err(|e| {
            anyhow::anyhow!(
                "Failed to create record batch during ODV batch decoding. {}",
                e
            )
        })
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

        let batch = reader.read(Some([0, 1, 2, 3, 4, 5]));
        println!("{:?}", batch);
    }
}
