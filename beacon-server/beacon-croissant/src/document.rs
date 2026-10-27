//! The Croissant JSON-LD documents for a table and for the table catalog.

use percent_encoding::{utf8_percent_encode, AsciiSet, NON_ALPHANUMERIC};
use serde_json::{json, Map, Value};

use crate::context::{context, CONFORMS_TO};
use crate::types::{ServedColumn, ServedSchema};

/// The name and `@id` of the Parquet file in a dataset.
pub const DATA_FILE: &str = "data.parquet";

/// The Parquet media type that `mlcroissant` reads.
const PARQUET_FORMAT: &str = "application/x-parquet";

/// Keeps only the unreserved characters of RFC 3986.
const UNRESERVED: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'.')
    .remove(b'_')
    .remove(b'~');

fn encode(segment: &str) -> String {
    utf8_percent_encode(segment, UNRESERVED).to_string()
}

/// The URL of the Croissant document of `table`. `base` has no trailing slash.
pub fn dataset_url(base: &str, table: &str) -> String {
    format!("{base}/api/croissant/{}", encode(table))
}

/// The URL of the Parquet file of `table`.
pub fn data_url(base: &str, table: &str) -> String {
    format!("{}/{DATA_FILE}", dataset_url(base, table))
}

/// The Croissant dataset document of `table`.
pub fn dataset(table: &str, schema: &ServedSchema, base: &str) -> Value {
    let table_id = encode(table);
    let description = schema
        .table_comment
        .clone()
        .unwrap_or_else(|| format!("Beacon table {table}"));
    let fields: Vec<Value> = schema
        .columns
        .iter()
        .map(|column| field(&table_id, column))
        .collect();
    json!({
        "@context": context(),
        "@type": "sc:Dataset",
        "dct:conformsTo": CONFORMS_TO,
        "name": table,
        "description": description,
        "url": dataset_url(base, table),
        "isLiveDataset": true,
        "distribution": [{
            "@type": "cr:FileObject",
            "@id": DATA_FILE,
            "name": DATA_FILE,
            "contentUrl": data_url(base, table),
            "encodingFormat": PARQUET_FORMAT
        }],
        "recordSet": [{
            "@type": "cr:RecordSet",
            "@id": table_id,
            "name": table,
            "field": fields
        }]
    })
}

fn field(table_id: &str, column: &ServedColumn) -> Value {
    let mut field = Map::new();
    field.insert("@type".into(), json!("cr:Field"));
    field.insert(
        "@id".into(),
        json!(format!("{table_id}/{}", encode(&column.name))),
    );
    field.insert("name".into(), json!(column.name));
    if let Some(comment) = &column.comment {
        field.insert("description".into(), json!(comment));
    }
    field.insert("dataType".into(), json!(column.data_type.name()));
    field.insert(
        "source".into(),
        json!({
            "fileObject": { "@id": DATA_FILE },
            "extract": { "column": column.name }
        }),
    );
    Value::Object(field)
}

/// A table in the Croissant catalog.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogEntry {
    /// The Beacon table name.
    pub name: String,
    /// The `COMMENT ON TABLE` text.
    pub description: Option<String>,
}

/// The schema.org `DataCatalog` that lists the Croissant document of each table.
pub fn catalog(title: &str, entries: &[CatalogEntry], base: &str) -> Value {
    let datasets: Vec<Value> = entries
        .iter()
        .map(|entry| {
            let mut dataset = Map::new();
            dataset.insert("@type".into(), json!("Dataset"));
            dataset.insert("name".into(), json!(entry.name));
            if let Some(description) = &entry.description {
                dataset.insert("description".into(), json!(description));
            }
            dataset.insert("url".into(), json!(dataset_url(base, &entry.name)));
            Value::Object(dataset)
        })
        .collect();
    json!({
        "@context": "https://schema.org/",
        "@type": "DataCatalog",
        "name": title,
        "url": format!("{base}/api/croissant"),
        "dataset": datasets
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{context, CroissantType, ServedColumn, ServedSchema};
    use serde_json::json;

    const BASE: &str = "https://beacon.example";

    fn column(name: &str, data_type: CroissantType, comment: Option<&str>) -> ServedColumn {
        ServedColumn {
            name: name.to_string(),
            data_type,
            comment: comment.map(str::to_string),
        }
    }

    #[test]
    fn dataset_has_the_pinned_shape() {
        let schema = ServedSchema {
            columns: vec![
                column("depth", CroissantType::Float, Some("depth in m")),
                column("station", CroissantType::Text, None),
            ],
            omitted: vec![],
            table_comment: None,
        };

        let document = dataset("obs", &schema, BASE);

        let expected = json!({
            "@context": context(),
            "@type": "sc:Dataset",
            "dct:conformsTo": "http://mlcommons.org/croissant/1.1",
            "name": "obs",
            "description": "Beacon table obs",
            "url": "https://beacon.example/api/croissant/obs",
            "isLiveDataset": true,
            "distribution": [{
                "@type": "cr:FileObject",
                "@id": "data.parquet",
                "name": "data.parquet",
                "contentUrl": "https://beacon.example/api/croissant/obs/data.parquet",
                "encodingFormat": "application/x-parquet"
            }],
            "recordSet": [{
                "@type": "cr:RecordSet",
                "@id": "obs",
                "name": "obs",
                "field": [
                    {
                        "@type": "cr:Field",
                        "@id": "obs/depth",
                        "name": "depth",
                        "description": "depth in m",
                        "dataType": "sc:Float",
                        "source": {
                            "fileObject": { "@id": "data.parquet" },
                            "extract": { "column": "depth" }
                        }
                    },
                    {
                        "@type": "cr:Field",
                        "@id": "obs/station",
                        "name": "station",
                        "dataType": "sc:Text",
                        "source": {
                            "fileObject": { "@id": "data.parquet" },
                            "extract": { "column": "station" }
                        }
                    }
                ]
            }]
        });
        assert_eq!(document, expected);
    }

    #[test]
    fn dataset_description_is_the_table_comment() {
        let schema = ServedSchema {
            columns: vec![],
            omitted: vec![],
            table_comment: Some("Ocean observations".to_string()),
        };

        let document = dataset("obs", &schema, BASE);

        assert_eq!(document["description"], "Ocean observations");
    }

    #[test]
    fn ids_are_encoded() {
        let schema = ServedSchema {
            columns: vec![column("a#b", CroissantType::Integer, None)],
            omitted: vec![],
            table_comment: None,
        };

        let document = dataset("my table/x", &schema, BASE);

        let url = document["url"].as_str().unwrap();
        assert!(url.ends_with("/api/croissant/my%20table%2Fx"), "{url}");
        let field = &document["recordSet"][0]["field"][0];
        assert_eq!(field["@id"], "my%20table%2Fx/a%23b");
        assert_eq!(field["name"], "a#b");
        assert_eq!(field["source"]["extract"]["column"], "a#b");
        assert_eq!(document["recordSet"][0]["@id"], "my%20table%2Fx");
        assert_eq!(document["name"], "my table/x");
    }

    #[test]
    fn encoding_keeps_unreserved_characters() {
        assert_eq!(
            dataset_url(BASE, "Az09-._~"),
            "https://beacon.example/api/croissant/Az09-._~"
        );
        assert_eq!(
            data_url(BASE, "a b"),
            "https://beacon.example/api/croissant/a%20b/data.parquet"
        );
    }

    #[test]
    fn catalog_lists_entries_in_order() {
        let entries = [
            CatalogEntry {
                name: "obs".to_string(),
                description: Some("Ocean observations".to_string()),
            },
            CatalogEntry {
                name: "argo".to_string(),
                description: None,
            },
        ];

        let document = catalog("Beacon", &entries, BASE);

        let expected = json!({
            "@context": "https://schema.org/",
            "@type": "DataCatalog",
            "name": "Beacon",
            "url": "https://beacon.example/api/croissant",
            "dataset": [
                {
                    "@type": "Dataset",
                    "name": "obs",
                    "description": "Ocean observations",
                    "url": "https://beacon.example/api/croissant/obs"
                },
                {
                    "@type": "Dataset",
                    "name": "argo",
                    "url": "https://beacon.example/api/croissant/argo"
                }
            ]
        });
        assert_eq!(document, expected);
        assert!(document["dataset"][1].get("description").is_none());
    }
}
