use std::{collections::HashMap, path::PathBuf};

use arrow::datatypes::{DataType, Fields, Schema, TimeUnit};
use datafusion::{
    common::{
        Column, exec_datafusion_err,
        tree_node::{Transformed, TreeNode},
    },
    datasource::listing::ListingTableUrl,
    execution::object_store::ObjectStoreUrl,
    prelude::Expr,
};
use url::Url;

#[derive(Debug, Clone, thiserror::Error)]
pub enum SuperTypeError {
    #[error("Cannot find a common super type for {left} and {right} in column {column_name}")]
    NoCommonSuperType {
        left: DataType,
        right: DataType,
        column_name: String,
    },
    #[error("No schemas provided")]
    NoSchemasProvided,
}

pub type Result<T> = std::result::Result<T, SuperTypeError>;

pub fn super_type_schema(schemas: &[arrow::datatypes::SchemaRef]) -> Result<Schema> {
    if schemas.is_empty() {
        return Err(SuperTypeError::NoSchemasProvided);
    }

    let mut fields = indexmap::IndexMap::new();
    for schema in schemas {
        for field in schema.fields.iter() {
            let name = field.name().to_string();
            let dtype = field.data_type().clone();
            match fields.get_mut(&name) {
                Some(existing_dtype) => {
                    if let Some(supert_type) = super_type_arrow(existing_dtype, &dtype) {
                        *existing_dtype = supert_type;
                    } else {
                        return Err(SuperTypeError::NoCommonSuperType {
                            left: existing_dtype.clone(),
                            right: dtype,
                            column_name: field.name().to_string(),
                        });
                    }
                }
                None => {
                    fields.insert(name, dtype);
                }
            }
        }
    }

    Ok(arrow::datatypes::Schema::new(Fields::from(
        fields
            .into_iter()
            .map(|(name, dtype)| arrow::datatypes::Field::new(&name, dtype, true))
            .collect::<Vec<_>>(),
    )))
}

pub fn super_type_arrow_schema(
    schemas: &[arrow::datatypes::Schema],
) -> Option<arrow::datatypes::Schema> {
    let mut fields = indexmap::IndexMap::new();
    for schema in schemas {
        for field in schema.fields.iter() {
            let name = field.name().to_string();
            let dtype = field.data_type().clone();
            match fields.get_mut(&name) {
                Some(existing_dtype) => {
                    if let Some(supert_type) = super_type_arrow(existing_dtype, &dtype) {
                        *existing_dtype = supert_type;
                    } else {
                        return None;
                    }
                }
                None => {
                    fields.insert(name, dtype);
                }
            }
        }
    }

    Some(arrow::datatypes::Schema::new(Fields::from(
        fields
            .into_iter()
            .map(|(name, dtype)| arrow::datatypes::Field::new(&name, dtype, false))
            .collect::<Vec<_>>(),
    )))
}

/// Determine the smallest common super type for two Arrow data types.
///
/// This function takes two data types (`left` and `right`) and returns an option
/// containing the common super type if one exists.
/// If both data types are equal, it returns a clone of that type.
/// For certain type combinations, the super type is defined to follow conventions
/// from libraries such as Polars and Numpy.
///
/// # Parameters
///
/// - `left`: A reference to the first Arrow data type.
/// - `right`: A reference to the second Arrow data type.
///
/// # Returns
///
/// An `Option<DataType>` with the common super type if a valid one exists, otherwise `None`.
pub fn super_type_arrow(left: &DataType, right: &DataType) -> Option<DataType> {
    if left == right {
        return Some(left.clone());
    }

    let super_type = match (left.clone(), right.clone()) {
        (DataType::Null, _) => right.clone(),
        (_, DataType::Null) => left.clone(),
        (DataType::Int8, DataType::Boolean) => DataType::Int8,
        (DataType::Int8, DataType::Int16) => DataType::Int16,
        (DataType::Int8, DataType::Int32) => DataType::Int32,
        (DataType::Int8, DataType::Int64) => DataType::Int64,
        (DataType::Int8, DataType::UInt8) => DataType::Int16,
        (DataType::Int8, DataType::UInt16) => DataType::Int32,
        (DataType::Int8, DataType::UInt32) => DataType::Int64,
        // Follow Polars + Numpy
        (DataType::Int8, DataType::UInt64) => DataType::Float64,
        (DataType::Int8, DataType::Float32) => DataType::Float32,
        (DataType::Int8, DataType::Float64) => DataType::Float64,
        (DataType::Int8, DataType::Utf8) => DataType::Utf8,
        (DataType::Int8, DataType::Timestamp(_, _)) => DataType::Int64,

        (DataType::Int16, DataType::Boolean) => DataType::Int16,
        (DataType::Int16, DataType::Int8) => DataType::Int16,
        (DataType::Int16, DataType::Int32) => DataType::Int32,
        (DataType::Int16, DataType::Int64) => DataType::Int64,
        (DataType::Int16, DataType::UInt8) => DataType::Int16,
        (DataType::Int16, DataType::UInt16) => DataType::Int32,
        (DataType::Int16, DataType::UInt32) => DataType::Int64,
        // Follow Polars + Numpy
        (DataType::Int16, DataType::UInt64) => DataType::Float64,
        (DataType::Int16, DataType::Float32) => DataType::Float32,
        (DataType::Int16, DataType::Float64) => DataType::Float64,
        (DataType::Int16, DataType::Utf8) => DataType::Utf8,
        (DataType::Int16, DataType::Timestamp(_, _)) => DataType::Int64,

        (DataType::Int32, DataType::Boolean) => DataType::Int32,
        (DataType::Int32, DataType::Int8) => DataType::Int32,
        (DataType::Int32, DataType::Int16) => DataType::Int32,
        (DataType::Int32, DataType::Int64) => DataType::Int64,
        (DataType::Int32, DataType::UInt8) => DataType::Int32,
        (DataType::Int32, DataType::UInt16) => DataType::Int32,
        (DataType::Int32, DataType::UInt32) => DataType::Int64,
        // Follow Polars + Numpy
        (DataType::Int32, DataType::UInt64) => DataType::Float64,
        (DataType::Int32, DataType::Float32) => DataType::Float32,
        (DataType::Int32, DataType::Float64) => DataType::Float64,
        (DataType::Int32, DataType::Utf8) => DataType::Utf8,
        (DataType::Int32, DataType::Timestamp(_, _)) => DataType::Int64,

        (DataType::Int64, DataType::Boolean) => DataType::Int64,
        (DataType::Int64, DataType::Int8) => DataType::Int64,
        (DataType::Int64, DataType::Int16) => DataType::Int64,
        (DataType::Int64, DataType::Int32) => DataType::Int64,
        (DataType::Int64, DataType::UInt8) => DataType::Int64,
        (DataType::Int64, DataType::UInt16) => DataType::Int64,
        (DataType::Int64, DataType::UInt32) => DataType::Int64,
        // Follow Polars + Numpy
        (DataType::Int64, DataType::UInt64) => DataType::Float64,
        (DataType::Int64, DataType::Float32) => DataType::Float64,
        (DataType::Int64, DataType::Float64) => DataType::Float64,
        (DataType::Int64, DataType::Utf8) => DataType::Utf8,
        (DataType::Int64, DataType::Timestamp(_, _)) => DataType::Int64,

        (DataType::UInt8, DataType::Boolean) => DataType::UInt8,
        (DataType::UInt8, DataType::Int8) => DataType::Int16,
        (DataType::UInt8, DataType::Int16) => DataType::Int16,
        (DataType::UInt8, DataType::Int32) => DataType::Int32,
        (DataType::UInt8, DataType::Int64) => DataType::Int64,
        (DataType::UInt8, DataType::UInt16) => DataType::UInt16,
        (DataType::UInt8, DataType::UInt32) => DataType::UInt32,
        (DataType::UInt8, DataType::UInt64) => DataType::UInt64,
        (DataType::UInt8, DataType::Float32) => DataType::Float32,
        (DataType::UInt8, DataType::Float64) => DataType::Float64,
        (DataType::UInt8, DataType::Utf8) => DataType::Utf8,
        (DataType::UInt8, DataType::Timestamp(_, _)) => DataType::Int64,

        (DataType::UInt16, DataType::Boolean) => DataType::UInt16,
        (DataType::UInt16, DataType::Int8) => DataType::Int32,
        (DataType::UInt16, DataType::Int16) => DataType::Int32,
        (DataType::UInt16, DataType::Int32) => DataType::Int32,
        (DataType::UInt16, DataType::Int64) => DataType::Int64,
        (DataType::UInt16, DataType::UInt8) => DataType::UInt16,
        (DataType::UInt16, DataType::UInt32) => DataType::UInt32,
        (DataType::UInt16, DataType::UInt64) => DataType::UInt64,
        (DataType::UInt16, DataType::Float32) => DataType::Float32,
        (DataType::UInt16, DataType::Float64) => DataType::Float64,
        (DataType::UInt16, DataType::Utf8) => DataType::Utf8,
        (DataType::UInt16, DataType::Timestamp(_, _)) => DataType::Int64,

        (DataType::UInt32, DataType::Boolean) => DataType::UInt32,
        (DataType::UInt32, DataType::Int8) => DataType::Int64,
        (DataType::UInt32, DataType::Int16) => DataType::Int64,
        (DataType::UInt32, DataType::Int32) => DataType::Int64,
        (DataType::UInt32, DataType::Int64) => DataType::Int64,
        (DataType::UInt32, DataType::UInt8) => DataType::UInt32,
        (DataType::UInt32, DataType::UInt16) => DataType::UInt32,
        (DataType::UInt32, DataType::UInt64) => DataType::UInt64,
        (DataType::UInt32, DataType::Float32) => DataType::Float32,
        (DataType::UInt32, DataType::Float64) => DataType::Float64,
        (DataType::UInt32, DataType::Utf8) => DataType::Utf8,
        (DataType::UInt32, DataType::Timestamp(_, _)) => DataType::Int64,

        (DataType::UInt64, DataType::Boolean) => DataType::UInt64,
        (DataType::UInt64, DataType::Int8) => DataType::Float64,
        (DataType::UInt64, DataType::Int16) => DataType::Float64,
        (DataType::UInt64, DataType::Int32) => DataType::Float64,
        (DataType::UInt64, DataType::Int64) => DataType::Float64,
        (DataType::UInt64, DataType::UInt8) => DataType::UInt64,
        (DataType::UInt64, DataType::UInt16) => DataType::UInt64,
        (DataType::UInt64, DataType::UInt32) => DataType::UInt64,
        (DataType::UInt64, DataType::Float32) => DataType::Float64,
        (DataType::UInt64, DataType::Float64) => DataType::Float64,
        (DataType::UInt64, DataType::Utf8) => DataType::Utf8,
        (DataType::UInt64, DataType::Timestamp(_, _)) => DataType::Float64,

        (DataType::Float32, DataType::Boolean) => DataType::Float32,
        (DataType::Float32, DataType::Int8) => DataType::Float32,
        (DataType::Float32, DataType::Int16) => DataType::Float32,
        (DataType::Float32, DataType::Int32) => DataType::Float64,
        (DataType::Float32, DataType::Int64) => DataType::Float64,
        (DataType::Float32, DataType::UInt8) => DataType::Float32,
        (DataType::Float32, DataType::UInt16) => DataType::Float32,
        (DataType::Float32, DataType::UInt32) => DataType::Float64,
        (DataType::Float32, DataType::UInt64) => DataType::Float64,
        (DataType::Float32, DataType::Float64) => DataType::Float64,
        (DataType::Float32, DataType::Utf8) => DataType::Utf8,
        (DataType::Float32, DataType::Timestamp(_, _)) => DataType::Float64,

        (DataType::Float64, DataType::Utf8) => DataType::Utf8,
        (DataType::Float64, _) => DataType::Float64,

        (DataType::LargeUtf8, _) => DataType::LargeUtf8,
        (_, DataType::LargeUtf8) => DataType::LargeUtf8,
        (DataType::Utf8, _) => DataType::Utf8,

        (DataType::Timestamp(_, _), DataType::Int8) => DataType::Int64,
        (DataType::Timestamp(_, _), DataType::Int16) => DataType::Int64,
        (DataType::Timestamp(_, _), DataType::Int32) => DataType::Int64,
        (DataType::Timestamp(_, _), DataType::Int64) => DataType::Int64,
        (DataType::Timestamp(_, _), DataType::UInt8) => DataType::Int64,
        (DataType::Timestamp(_, _), DataType::UInt16) => DataType::Int64,
        (DataType::Timestamp(_, _), DataType::UInt32) => DataType::Int64,
        (DataType::Timestamp(_, _), DataType::UInt64) => DataType::Float64,
        (DataType::Timestamp(_, _), DataType::Float32) => DataType::Float64,
        (DataType::Timestamp(_, _), DataType::Float64) => DataType::Float64,
        (DataType::Timestamp(_, _), DataType::Utf8) => DataType::Utf8,
        (DataType::Timestamp(_, _), DataType::Timestamp(_, _)) => {
            DataType::Timestamp(TimeUnit::Second, None)
        }

        _ => return None,
    };

    Some(super_type)
}

pub fn remap_filter(
    expr: Expr,
    rename_map: &HashMap<String, String>,
) -> datafusion::error::Result<Expr> {
    // expr.transform returns Result<Transformed<Expr>, DataFusionError>
    let transformed: Transformed<Expr> = expr.transform(&|e| {
        Ok(match e {
            // For any column reference...
            Expr::Column(ref c) => {
                // look up the real name
                if let Some(real) = rename_map.get(c.name()) {
                    // yes, replace this Expr with col(real)
                    Transformed::yes(Expr::Column(Column::new_unqualified(real.clone())))
                } else {
                    // no change
                    Transformed::no(e.clone())
                }
            }
            // leave everything else alone
            _ => Transformed::no(e.clone()),
        })
    })?;
    // Turn the Transformed<Expr> back into an Expr
    Ok(transformed.data)
}

pub fn parse_listing_table_url(
    data_directory_store_url: &ObjectStoreUrl,
    data_directory_prefix: &object_store::path::Path,
    glob_path: &str,
) -> datafusion::error::Result<ListingTableUrl> {
    // Check if path contains glob expression
    let (path, glob_pattern) = split_path_and_glob(glob_path);
    let mut full_path = format!("{}{}", data_directory_store_url, data_directory_prefix);
    if path.components().next().is_some() {
        full_path.push_str(format!("/{}", path.as_os_str().to_string_lossy()).as_str());
    }
    if !full_path.ends_with('/') {
        full_path.push('/');
    }

    let glob_pattern_parsed = if let Some(pattern) = glob_pattern {
        Some(
            glob::Pattern::new(&pattern)
                .map_err(|e| exec_datafusion_err!("Failed to parse glob pattern: {}", e))?,
        )
    } else {
        None
    };

    let url =
        Url::parse(&full_path).map_err(|e| exec_datafusion_err!("Failed to parse URL: {}", e))?;

    let table_url = ListingTableUrl::try_new(url, glob_pattern_parsed)
        .map_err(|e| exec_datafusion_err!("Failed to create table URL: {}", e))?;

    Ok(table_url)
}

/// Splits a path containing an optional glob pattern into
/// (base_path, optional_glob_pattern).
///
/// Examples:
/// - "src/**/*.rs" -> ("src", Some("**/*.rs"))
/// - "src/*.rs"    -> ("src", Some("*.rs"))
/// - "*.rs"        -> (".", Some("*.rs"))
/// - "src/main.rs" -> ("src/main.rs", None)
pub fn split_path_and_glob(input: &str) -> (PathBuf, Option<String>) {
    let glob_chars = ['*', '?', '[', ']'];

    // Split the path into components manually (cross-platform)
    let parts: Vec<&str> = input.split(['/', '\\']).collect();

    // Find the index of the first segment that contains any glob char
    if let Some(i) = parts
        .iter()
        .position(|p| p.chars().any(|c| glob_chars.contains(&c)))
    {
        let base = if i == 0 {
            PathBuf::from(".")
        } else {
            PathBuf::from(parts[..i].join(std::path::MAIN_SEPARATOR_STR))
        };
        let glob = parts[i..].join(std::path::MAIN_SEPARATOR_STR);
        (base, Some(glob))
    } else {
        // No glob pattern at all
        (PathBuf::from(input), None)
    }
}

#[cfg(test)]
mod tests {
    use std::path::MAIN_SEPARATOR;

    use super::*;

    #[test]
    fn test_no_glob() {
        let (base, glob) = split_path_and_glob("src/main.rs");
        assert_eq!(base, PathBuf::from("src/main.rs"));
        assert_eq!(glob, None);
    }

    #[test]
    fn test_simple_glob() {
        let (base, glob) = split_path_and_glob("src/*.rs");
        assert_eq!(base, PathBuf::from("src"));
        assert_eq!(glob, Some("*.rs".to_string()));
    }

    #[test]
    fn test_recursive_glob() {
        let (base, glob) = split_path_and_glob("src/**/*.rs");
        assert_eq!(base, PathBuf::from("src"));
        assert_eq!(glob, Some(format!("**{}*.rs", std::path::MAIN_SEPARATOR)));
    }

    #[test]
    fn test_glob_at_start() {
        let (base, glob) = split_path_and_glob("*.txt");
        assert_eq!(base, PathBuf::from("."));
        assert_eq!(glob, Some("*.txt".to_string()));
    }

    #[test]
    fn test_glob_in_middle_segment() {
        let (base, glob) = split_path_and_glob("foo[ab]/bar?.txt");
        assert_eq!(base, PathBuf::from("foo[ab]"));
        assert_eq!(glob, Some("bar?.txt".to_string()));
    }

    #[test]
    fn test_no_separator_but_glob_present() {
        let (base, glob) = split_path_and_glob("file[0-9].log");
        assert_eq!(base, PathBuf::from("."));
        assert_eq!(glob, Some("file[0-9].log".to_string()));
    }

    #[test]
    fn test_windows_style_path() {
        // Works on both Unix and Windows separators
        let input = format!("dir{}**{}*.txt", MAIN_SEPARATOR, MAIN_SEPARATOR);
        let (base, glob) = split_path_and_glob(&input);
        assert_eq!(base, PathBuf::from("dir"));
        assert_eq!(glob, Some(format!("**{}*.txt", MAIN_SEPARATOR)));
    }

    #[test]
    fn test_parse_listing_table_url_no_glob() {
        use datafusion::execution::object_store::ObjectStoreUrl;

        let store_url: ObjectStoreUrl = ObjectStoreUrl::parse("file:///").unwrap();
        let prefix = object_store::path::Path::from("datasets");
        let url = parse_listing_table_url(&store_url, &prefix, "foo/zarr.json").unwrap();
        // Debug representation should contain the base path we constructed
        let dbg = format!("{:?}", url);
        assert!(dbg.contains("/datasets/foo/"), "dbg={}", dbg);
    }

    #[test]
    fn test_parse_listing_table_url_with_glob() {
        use datafusion::execution::object_store::ObjectStoreUrl;

        let store_url: ObjectStoreUrl = ObjectStoreUrl::parse("file:///").unwrap();
        let prefix = object_store::path::Path::from("datasets");
        let url = parse_listing_table_url(&store_url, &prefix, "subdir/*.json").unwrap();
        let dbg = format!("{:?}", url);
        assert!(dbg.contains("/datasets/subdir/"), "dbg={}", dbg);
        // pattern should be present in debug output
        assert!(dbg.contains("*.json"), "dbg={}", dbg);
    }

    #[test]
    fn test_parse_listing_table_url_invalid_glob() {
        use datafusion::execution::object_store::ObjectStoreUrl;

        let store_url: ObjectStoreUrl = ObjectStoreUrl::parse("file:///").unwrap();
        let prefix = object_store::path::Path::from("datasets");
        let res = parse_listing_table_url(&store_url, &prefix, "subdir/[invalid");
        assert!(res.is_err(), "expected error for invalid glob pattern");
    }
}
