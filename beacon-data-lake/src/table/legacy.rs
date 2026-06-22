//! Compatibility shim for the pre-typetag table catalog format.
//!
//! Before logical tables were removed, a `table.json` was a `Table` wrapper
//! `{ "table_name", "table_type": { <variant>: { .. } } }`, where the file
//! format was a typetag keyed by `file_format`:
//!
//! ```json
//! {
//!     "table_name": "default",
//!     "table_type": { "logical": { "paths": ["**/*.bbf"], "file_format": "bbf" } }
//! }
//! ```
//!
//! This module reads that envelope and converts the `logical` variant into the
//! current [`LogicalTableDefinition`], which in turn builds an external-table
//! provider. Other legacy variants (preset/merged/geospatial/empty) are not
//! supported as a fallback.

use std::collections::HashMap;

use beacon_datafusion_ext::table_ext::LogicalTableDefinition;

/// The legacy `Table` envelope. Only the fields needed to rebuild a logical
/// table are captured; everything else is ignored.
#[derive(serde::Deserialize)]
struct LegacyTable {
    table_name: String,
    /// Externally-tagged variant map, e.g. `{ "logical": { .. } }`. Kept as a
    /// raw map so unsupported variants can be named in error messages instead of
    /// failing deserialization.
    table_type: serde_json::Map<String, serde_json::Value>,
}

/// The body of a legacy `logical` table.
#[derive(serde::Deserialize)]
struct LegacyLogicalTable {
    #[serde(alias = "paths")]
    glob_paths: Vec<String>,
    /// The legacy `TableFileFormat` typetag tag (e.g. `"bbf"`, `"csv"`).
    file_format: String,
    /// Any remaining format-specific fields (e.g. a CSV delimiter) that were
    /// flattened alongside the typetag tag.
    #[serde(flatten)]
    extra: HashMap<String, serde_json::Value>,
}

/// Parse `json_bytes` as the legacy table envelope and convert a `logical`
/// table into a [`LogicalTableDefinition`].
///
/// Returns `Ok(None)` when the bytes are not the legacy envelope at all (so the
/// caller can surface its own error), and `Err` when they are a legacy table of
/// an unsupported type.
pub(crate) fn logical_definition_from_legacy_json(
    json_bytes: &[u8],
) -> anyhow::Result<Option<LogicalTableDefinition>> {
    let Ok(table) = serde_json::from_slice::<LegacyTable>(json_bytes) else {
        return Ok(None);
    };

    let Some(logical_value) = table.table_type.get("logical") else {
        let variant = table
            .table_type
            .keys()
            .next()
            .map(String::as_str)
            .unwrap_or("unknown");
        anyhow::bail!(
            "Legacy table '{}' uses unsupported table type '{}'; only logical tables can be read as external tables",
            table.table_name,
            variant
        );
    };

    let logical: LegacyLogicalTable =
        serde_json::from_value(logical_value.clone()).map_err(|e| {
            anyhow::anyhow!(
                "Failed to parse legacy logical table '{}': {:?}",
                table.table_name,
                e
            )
        })?;

    if !logical.extra.is_empty() {
        let mut keys = logical.extra.keys().cloned().collect::<Vec<_>>();
        keys.sort();
        tracing::warn!(
            table = %table.table_name,
            dropped_options = ?keys,
            "Dropping format-specific options from legacy logical table; recreate the table with CREATE EXTERNAL TABLE to restore them"
        );
    }

    Ok(Some(LogicalTableDefinition {
        name: table.table_name,
        glob_paths: logical.glob_paths,
        file_type: map_legacy_file_format(&logical.file_format),
        options: HashMap::new(),
    }))
}

/// Map a legacy `TableFileFormat` tag to the file type the current format
/// factories are registered under. All tags match except NetCDF, whose factory
/// is now keyed by its `nc` extension.
fn map_legacy_file_format(tag: &str) -> String {
    match tag {
        "netcdf" => "nc".to_string(),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    /// The exact envelope from a real catalog (a `default` BBF logical table)
    /// converts into a logical definition pointing at the same glob and format.
    fn converts_bbf_logical_envelope() {
        let json = serde_json::json!({
            "table_name": "default",
            "table_type": {
                "logical": {
                    "paths": ["**/*.bbf"],
                    "file_format": "bbf"
                }
            }
        });

        let def = logical_definition_from_legacy_json(json.to_string().as_bytes())
            .expect("conversion should succeed")
            .expect("bytes should be the legacy envelope");

        assert_eq!(def.name, "default");
        assert_eq!(def.glob_paths, vec!["**/*.bbf".to_string()]);
        assert_eq!(def.file_type, "bbf");
        assert!(def.options.is_empty());
    }

    #[test]
    /// NetCDF's legacy tag maps onto the current `nc` factory key.
    fn maps_netcdf_tag_to_nc() {
        let json = serde_json::json!({
            "table_name": "obs",
            "table_type": { "logical": { "paths": ["obs/*.nc"], "file_format": "netcdf" } }
        });

        let def = logical_definition_from_legacy_json(json.to_string().as_bytes())
            .unwrap()
            .unwrap();
        assert_eq!(def.file_type, "nc");
    }

    #[test]
    /// Non-envelope JSON (e.g. the current typetag format) yields `None` so the
    /// caller can report its own parse error.
    fn returns_none_for_non_legacy_json() {
        let json = serde_json::json!({
            "definition_type": "logical",
            "name": "x",
            "glob_paths": ["a/*.parquet"],
            "file_type": "parquet"
        });

        let result = logical_definition_from_legacy_json(json.to_string().as_bytes()).unwrap();
        assert!(result.is_none());
    }

    #[test]
    /// A legacy table of an unsupported type is an error that names the variant.
    fn errors_on_unsupported_legacy_variant() {
        let json = serde_json::json!({
            "table_name": "merged_one",
            "table_type": { "merged": { "tables": [] } }
        });

        let err = logical_definition_from_legacy_json(json.to_string().as_bytes())
            .expect_err("unsupported variant should error");
        assert!(err.to_string().contains("merged"));
    }
}
