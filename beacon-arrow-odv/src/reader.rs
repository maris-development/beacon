use std::{
    collections::HashMap,
    io::{BufRead, BufReader, Read, Seek},
    path::Path,
    sync::Arc,
    task::Poll,
};

use arrow::{
    array::{RecordBatch, StringArray},
    datatypes::{DataType, Field, SchemaRef},
    error::ArrowError,
};
use bytes::{Buf, Bytes};
use csv::{ByteRecord, StringRecord};
use futures::{Stream, StreamExt, TryStreamExt};
use indexmap::IndexMap;
use object_store::ObjectStore;
use regex::Regex;
use tokio_util::codec::FramedRead;

use crate::{error::OdvError, OdvResult};

pub struct OdvReaderOptions {
    pub io_read_size: usize,
}

impl Default for OdvReaderOptions {
    fn default() -> Self {
        Self {
            io_read_size: 8 * 1024 * 1024,
        }
    }
}

pub struct OdvObjectReader {
    store: Arc<dyn object_store::ObjectStore>,
    path: object_store::path::Path,
}

impl OdvObjectReader {
    pub fn new(store: Arc<dyn object_store::ObjectStore>, path: object_store::path::Path) -> Self {
        Self { store, path }
    }

    async fn read_odv_header(
        store: Arc<dyn ObjectStore>,
        path: object_store::path::Path,
    ) -> OdvHeader {
        let mut header_lines = Vec::new();

        // Read the header lines
        let mut reader = object_store::buffered::BufReader::new(store, path);
        while let Some(line) = reader.next_line().await {
            header_lines.push(line);
        }

        todo!()
    }
}

struct OdvHeader {
    schema: SchemaRef,
}

pub struct AsyncOdvDecoder {
    header: OdvHeader,
}

impl AsyncOdvDecoder {
    pub fn new(header: OdvHeader) -> Self {
        Self { header }
    }

    pub async fn decode<S: Stream<Item = Result<Bytes, object_store::Error>> + Unpin>(
        &self,
        input: S,
        projection: Option<Vec<usize>>,
    ) -> impl Stream<Item = Result<RecordBatch, ArrowError>> {
        let mut header_decoded_bytes = Vec::new();
        //Read the header lines to get the schema

        let input_stream = input.map(|maybe| maybe.map_err(std::io::Error::other));
        let stream_reader = tokio_util::io::StreamReader::new(input_stream.map(|maybe_batch| {
            maybe_batch.inspect(|batch| {
                header_decoded_bytes.push(Ok(batch.clone()));
            })
        }));
        let mut lines = FramedRead::new(stream_reader, tokio_util::codec::LinesCodec::new());

        let mut fields = indexmap::IndexMap::new();

        fields.insert(
            "Cruise".to_string(),
            Field::new("Cruise", DataType::Utf8, true),
        );
        fields.insert(
            "Station".to_string(),
            Field::new("Station", DataType::Utf8, true),
        );
        fields.insert("Type".to_string(), Field::new("Type", DataType::Utf8, true));
        fields.insert(
            "yyyy-mm-ddThh:mm:ss.sss".to_string(),
            Field::new(
                "yyyy-mm-ddThh:mm:ss.sss",
                DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
                true,
            ),
        );

        let mut odv_schema = None;
        while let Some(line) = lines.next().await.transpose().unwrap() {
            if !line.starts_with("//") {
                // Parse as header row
                let header_row =
                    Self::header_row(&mut std::io::Cursor::new(line.as_bytes())).unwrap();
                odv_schema = Some(
                    Self::parse_header_row_with_metadata_to_schema(&header_row, fields).unwrap(),
                );
                break;
            }
            Self::odv_field_from_header(&line).map(|f| fields.insert(f.name().to_string(), f));
        }

        //Create the body decoder
        let body_decoder = AsyncOdvBodyDecoder::new(odv_schema.unwrap());

        let following_stream = lines.into_inner().into_inner().into_inner();
        let stream = futures::stream::iter(header_decoded_bytes.into_iter());

        let input_stream = stream.chain(following_stream);

        //Decode the body
        body_decoder.decode_odv_body(input_stream, projection.map(|p| p.into()))
    }

    fn odv_field_from_header(line: &str) -> Option<Field> {
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
                Self::value_type_to_arrow_type(value_type).unwrap(),
                true,
            );

            if !qf_schema.is_empty() {
                metadata.insert("qf_schema".to_string(), qf_schema.to_string());
            }

            if !comment.is_empty() {
                metadata.insert("comment".to_string(), comment.to_string());
            }

            return Some(field.with_metadata(metadata));
        }

        None
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

    fn header_row(reader: &mut dyn Read) -> OdvResult<StringRecord> {
        let mut csv_builder = csv::ReaderBuilder::new();
        csv_builder
            .has_headers(true)
            .delimiter(b'\t')
            .comment(Some(b'/'));
        let mut csv_reader = csv_builder.from_reader(reader);
        let header_row = csv_reader.headers().unwrap().clone();
        Ok(header_row)
    }
}

struct AsyncOdvBodyDecoder {
    output_schema: SchemaRef,
    input_schema: SchemaRef,
    metadata_fields: indexmap::IndexMap<String, String>,
}

impl AsyncOdvBodyDecoder {
    pub fn new(input_schema: SchemaRef) -> Self {
        let mut fields = vec![];
        let mut metadata_fields = indexmap::IndexMap::new();
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

                metadata_fields.insert(format!("{}.{}", field.name(), k), v.clone());
            }
        }

        Self {
            output_schema: Arc::new(arrow::datatypes::Schema::new(fields)),
            metadata_fields,
            input_schema,
        }
    }

    pub fn decode_odv_body<S: Stream<Item = Result<Bytes, std::io::Error>> + Unpin>(
        &self,
        input: S,
        projection: Option<Arc<[usize]>>,
    ) -> impl Stream<Item = Result<RecordBatch, ArrowError>> {
        let decoder = arrow::csv::reader::ReaderBuilder::new(self.input_schema.clone())
            .with_batch_size(64 * 1024)
            .with_comment(b'/')
            .with_delimiter(b'\t')
            .with_header(true)
            .build_decoder();

        let metadata_fields = Arc::new(self.metadata_fields.clone());
        let output_schema = self.output_schema.clone();

        Self::decode_byte_stream(decoder, input).map(move |maybe_batch| {
            let output_schema = output_schema.clone();
            let added_fields = metadata_fields.clone();
            let projection = projection.clone();
            maybe_batch.and_then(move |batch| {
                Self::decode_batch(output_schema, &added_fields, projection, batch)
            })
        })
    }

    fn decode_batch<P: AsRef<[usize]>>(
        output_schema: SchemaRef,
        metadata_fields: &indexmap::IndexMap<String, String>,
        projection: Option<P>,
        batch: RecordBatch,
    ) -> Result<RecordBatch, ArrowError> {
        let mut schema = output_schema;
        let mut arrays = batch.columns().to_vec();
        for (_, value) in metadata_fields.iter() {
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
            schema = Arc::new(schema.project(projection)?);
        }

        RecordBatch::try_new(schema, arrays)
    }

    fn decode_byte_stream<S: Stream<Item = Result<Bytes, std::io::Error>> + Unpin>(
        mut decoder: arrow::csv::reader::Decoder,
        mut input: S,
    ) -> impl Stream<Item = Result<RecordBatch, ArrowError>> {
        use futures::StreamExt;
        let mut buffered = Bytes::new();
        futures::stream::poll_fn(move |cx| {
            loop {
                if buffered.is_empty() {
                    if let Some(b) = futures::ready!(input.poll_next_unpin(cx)) {
                        buffered = b?;
                    }
                    // Note: don't break on `None` as the decoder needs
                    // to be called with an empty array to delimit the
                    // final record
                }
                let decoded = match decoder.decode(buffered.as_ref()) {
                    Ok(0) => break,
                    Ok(decoded) => decoded,
                    Err(e) => return Poll::Ready(Some(Err(e))),
                };
                buffered.advance(decoded);
            }

            Poll::Ready(decoder.flush().transpose())
        })
    }
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;
    use object_store::ObjectStore;

    use crate::reader::AsyncOdvDecoder;

    #[tokio::test]
    async fn test_full_file() {
        let object_store =
            object_store::local::LocalFileSystem::new_with_prefix("./test-data/").unwrap();
        let path = object_store::path::Path::from("test_file.txt");

        let stream = object_store.get(&path).await.unwrap().into_stream();

        let decoder = AsyncOdvDecoder::new();
        let mut async_reader = decoder.decode(stream, None).await;

        let output_file = std::fs::File::create("./test-data/test_output.csv").unwrap();
        let mut writer = arrow::csv::Writer::new(output_file);

        while let Some(batch) = async_reader.next().await {
            let batch = batch.unwrap();
            writer.write(&batch).unwrap();
        }
    }
}
