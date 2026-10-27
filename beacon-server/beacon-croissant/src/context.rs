//! The Croissant 1.1 JSON-LD context.

use std::sync::LazyLock;

use serde_json::Value;

/// The `dct:conformsTo` value of a Croissant 1.1 dataset.
pub const CONFORMS_TO: &str = "http://mlcommons.org/croissant/1.1";

/// The `@context` object from Appendix 1 of the Croissant 1.1 specification.
// The appendix prints a trailing comma after "value"; JSON does not allow it, so it is removed.
const CONTEXT_JSON: &str = r#"{
    "@language": "en",
    "@vocab": "http://schema.org/",
    "sc": "http://schema.org/",
    "cr": "http://mlcommons.org/croissant/",
    "rai": "http://mlcommons.org/croissant/RAI/",
    "dct": "http://purl.org/dc/terms/",
    "annotation": "cr:annotation",
    "arrayShape": "cr:arrayShape",
    "citeAs": "cr:citeAs",
    "column": "cr:column",
    "conformsTo": "dct:conformsTo",
    "containedIn": "cr:containedIn",
    "data": {
      "@id": "cr:data",
      "@type": "@json"
    },
    "dataType": {
      "@id": "cr:dataType",
      "@type": "@vocab"
    },
    "equivalentProperty": "cr:equivalentProperty",
    "examples": {
      "@id": "cr:examples",
      "@type": "@json"
    },
    "excludes": "cr:excludes",
    "extract": "cr:extract",
    "field": "cr:field",
    "fileProperty": "cr:fileProperty",
    "fileObject": "cr:fileObject",
    "fileSet": "cr:fileSet",
    "format": "cr:format",
    "includes": "cr:includes",
    "isArray": "cr:isArray",
    "isLiveDataset": "cr:isLiveDataset",
    "jsonPath": "cr:jsonPath",
    "key": "cr:key",
    "md5": "cr:md5",
    "parentField": "cr:parentField",
    "recordSet": "cr:recordSet",
    "references": "cr:references",
    "regex": "cr:regex",
    "readLines": "cr:readLines",
    "sdVersion": "cr:sdVersion",
    "separator": "cr:separator",
    "source": "cr:source",
    "subField": "cr:subField",
    "transform": "cr:transform",
    "unArchive": "cr:unArchive",
    "value": "cr:value"
}"#;

static CONTEXT: LazyLock<Value> = LazyLock::new(|| {
    serde_json::from_str(CONTEXT_JSON).expect("the Croissant context is valid JSON")
});

/// The `@context` value of a Croissant 1.1 document.
pub fn context() -> Value {
    CONTEXT.clone()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn context_parses_and_has_the_prefixes() {
        let context = context();

        let object = context.as_object().expect("the context is a JSON object");
        for key in ["@vocab", "sc", "cr", "dct"] {
            assert!(object.contains_key(key), "missing {key}");
        }
        assert_eq!(CONFORMS_TO, "http://mlcommons.org/croissant/1.1");
    }
}
