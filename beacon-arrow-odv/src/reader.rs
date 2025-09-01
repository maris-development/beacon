use std::{collections::HashMap, io::Read, sync::Arc, task::Poll};

use arrow::{
    array::{RecordBatch, StringArray},
    datatypes::{DataType, Field, SchemaRef},
    error::ArrowError,
};
use bytes::{Buf, Bytes};
use csv::StringRecord;
use futures::{FutureExt, Stream, StreamExt, TryStreamExt};
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
    schema_mapper: Arc<OdvSchemaMapper>,
}

impl OdvObjectReader {
    pub async fn try_new(
        store: Arc<dyn object_store::ObjectStore>,
        path: object_store::path::Path,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let byte_stream = store.get(&path).await?.into_stream().map_err(Into::into);
        let schema_mapper = AsyncOdvDecoder::decode_schema_mapper(byte_stream).await?;
        Ok(Self {
            store,
            path,
            schema_mapper: Arc::new(schema_mapper),
        })
    }

    pub fn schema(&self) -> SchemaRef {
        self.schema_mapper.output_schema.clone()
    }

    pub async fn read_async(
        &self,
        projection: Option<Vec<usize>>,
    ) -> impl Stream<Item = Result<RecordBatch, ArrowError>> {
        let object = self.store.get(&self.path).await.unwrap();
        let stream = object.into_stream().map_err(Into::into);

        AsyncOdvDecoder::decode(stream, projection, self.schema_mapper.clone()).await
    }
}

pub struct OdvSchemaMapper {
    input_schema: SchemaRef,
    output_schema: SchemaRef,
    metadata_fields: indexmap::IndexMap<String, String>,
}

impl OdvSchemaMapper {
    pub fn output_schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }

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

    pub fn map_batch(
        &self,
        batch: RecordBatch,
        projection: Option<Arc<[usize]>>,
    ) -> Result<RecordBatch, ArrowError> {
        let mut schema = self.output_schema.clone();
        let mut arrays = batch.columns().to_vec();
        for (_, value) in self.metadata_fields.iter() {
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
}

pub struct AsyncOdvDecoder;

impl AsyncOdvDecoder {
    pub async fn decode_schema_mapper<S: Stream<Item = Result<Bytes, std::io::Error>> + Unpin>(
        input: S,
    ) -> Result<OdvSchemaMapper, ArrowError> {
        let mut header_lines = Vec::new();
        let mut header_row = None;

        let stream_reader = tokio_util::io::StreamReader::new(input);
        let mut lines = FramedRead::new(stream_reader, tokio_util::codec::LinesCodec::new());

        while let Some(line) = lines.next().await.transpose().unwrap() {
            if !line.starts_with("//") {
                // Parse as header row
                header_row = Some(line);
                break;
            }
            header_lines.push(line);
        }

        let mut fields = indexmap::IndexMap::new();

        // Insert default fields that should always be there
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

        // Parse each header line
        for line in header_lines {
            AsyncOdvDecoder::odv_field_from_header(&line)
                .map(|f| fields.insert(f.name().to_string(), f));
        }

        // Parse header row
        let header_row = header_row.unwrap();
        let header_row =
            AsyncOdvDecoder::header_row(&mut std::io::Cursor::new(header_row.as_bytes())).unwrap();
        let odv_schema =
            AsyncOdvDecoder::parse_header_row_with_metadata_to_schema(&header_row, fields).unwrap();

        Ok(OdvSchemaMapper::new(odv_schema))
    }

    pub async fn decode<S: Stream<Item = Result<Bytes, std::io::Error>> + Unpin>(
        input: S,
        projection: Option<Vec<usize>>,
        schema_mapper: Arc<OdvSchemaMapper>,
    ) -> impl Stream<Item = Result<RecordBatch, ArrowError>> {
        Self::decode_odv_body(input, projection.map(Into::into), schema_mapper)
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

    fn decode_odv_body<S: Stream<Item = Result<Bytes, std::io::Error>> + Unpin>(
        input: S,
        projection: Option<Arc<[usize]>>,
        schema_mapper: Arc<OdvSchemaMapper>,
    ) -> impl Stream<Item = Result<RecordBatch, ArrowError>> {
        let decoder = arrow::csv::reader::ReaderBuilder::new(schema_mapper.input_schema.clone())
            .with_batch_size(64 * 1024)
            .with_comment(b'/')
            .with_delimiter(b'\t')
            .with_header(true)
            .build_decoder();

        let metadata_fields = Arc::new(schema_mapper.metadata_fields.clone());
        let output_schema = schema_mapper.output_schema.clone();

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
    use std::sync::Arc;

    use futures::StreamExt;

    use crate::reader::OdvObjectReader;

    #[tokio::test]
    async fn test_full_file() {
        let object_store =
            object_store::local::LocalFileSystem::new_with_prefix("./test-data/").unwrap();
        let path = object_store::path::Path::from("test_file.txt");

        let reader = OdvObjectReader::try_new(Arc::new(object_store), path)
            .await
            .unwrap();
        let mut async_stream = reader.read_async(None).await;

        let output_file = std::fs::File::create("./test-data/test_output.csv").unwrap();
        let mut writer = arrow::csv::Writer::new(output_file);

        while let Some(batch) = async_stream.next().await {
            let batch = batch.unwrap();
            writer.write(&batch).unwrap();
        }
    }
}
