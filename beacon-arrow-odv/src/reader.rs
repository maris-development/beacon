use std::{
    io::{BufRead, Read},
    path::Path,
    sync::Arc,
};

use arrow::{
    array::{ArrayRef, Scalar, StringArray},
    datatypes::{DataType, Field},
};
use arrow_csv::reader::BufReader;
use regex::Regex;

pub struct OdvReader {
    inner_reader: arrow_csv::Reader<BufReader<std::fs::File>>,
}

#[derive(Default)]
struct OdvLineMetadata {
    fields: Vec<Field>,
    metadata_fields: Vec<(Field, Scalar<ArrayRef>)>,
}

impl OdvReader {
    pub fn new<P: AsRef<Path>>(path: P, batch_size: usize) -> anyhow::Result<Self> {
        let file = std::fs::File::open(path)?;
        // // let reader = arrow_csv::ReaderBuilder::
        // Ok(Self {
        //     inner_reader: reader,
        // })
        todo!()
    }

    fn metadata_lines<R: BufRead>(read: R) -> anyhow::Result<Vec<String>> {
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

    fn odv_metadata_from_header(line: &str) -> anyhow::Result<OdvLineMetadata> {
        let re = Regex::new(
            r#"(?m)^//<(?:MetaVariable|DataVariable)>.*?label="([^"]+)".*?value_type="([^"]+)".*?qf_schema="([^"]+)".*?comment="([^"]*)".*?</(?:MetaVariable|DataVariable)>"#
        ).unwrap();

        let mut metadata = OdvLineMetadata::default();

        for cap in re.captures_iter(line) {
            let label = &cap[1];
            let value_type = &cap[2];
            let qf_schema = &cap[3];
            let comment = &cap[4];

            let field = Field::new(label, Self::value_type_to_arrow_type(value_type)?, true);
            metadata.fields.push(field);

            let qf_schema_field = Field::new(format!("{}.qf_schema", label), DataType::Utf8, true);
            metadata.metadata_fields.push((
                qf_schema_field,
                Scalar::new(Arc::new(StringArray::from(vec![qf_schema]))),
            ));

            let comment_field = Field::new(format!("{}.comment", label), DataType::Utf8, true);
            metadata.metadata_fields.push((
                comment_field,
                Scalar::new(Arc::new(StringArray::from(vec![comment]))),
            ));
        }

        Ok(metadata)
    }

    fn value_type_to_arrow_type(value_type: &str) -> anyhow::Result<DataType> {
        match value_type {
            "INDEXED_TEXT" => Ok(DataType::Utf8),
            "INTEGER" => Ok(DataType::Int64),
            "FLOAT" => Ok(DataType::Float64),
            _ if value_type.starts_with("TEXT:") => Ok(DataType::Utf8),
            _ => Err(anyhow::anyhow!("Unsupported value type: {}", value_type)),
        }
    }

    fn metadata_from_header(line: &str) -> anyhow::Result<()> {
        todo!()
    }
}
