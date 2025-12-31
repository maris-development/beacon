use std::path::PathBuf;

use datafusion::{
    common::exec_datafusion_err, datasource::listing::ListingTableUrl,
    execution::object_store::ObjectStoreUrl,
};
use url::Url;

pub fn parse_listing_table_url(
    data_directory_store_url: &ObjectStoreUrl,
    glob_path: &str,
) -> datafusion::error::Result<ListingTableUrl> {
    // Check if path contains glob expression
    let (path, glob_pattern) = split_path_and_glob(glob_path);
    let mut full_path = data_directory_store_url.to_string();
    if path.components().next().is_some() {
        if full_path.ends_with('/') {
            full_path.push_str(format!("{}", path.as_os_str().to_string_lossy()).as_str());
        } else {
            full_path.push_str(format!("/{}", path.as_os_str().to_string_lossy()).as_str());
        }
    }

    let glob_pattern_parsed = if let Some(pattern) = glob_pattern {
        if !full_path.ends_with('/') {
            full_path.push('/');
        }
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

        let store_url: ObjectStoreUrl = ObjectStoreUrl::parse("file://").unwrap();
        let url = parse_listing_table_url(&store_url, "foo/zarr.json").unwrap();
        // Debug representation should contain the base path we constructed
        let dbg = format!("{:?}", url);
        assert!(dbg.contains("/foo/"), "dbg={}", dbg);
    }

    #[test]
    fn test_parse_listing_table_url_with_glob() {
        use datafusion::execution::object_store::ObjectStoreUrl;

        let store_url: ObjectStoreUrl = ObjectStoreUrl::parse("file://").unwrap();
        let url = parse_listing_table_url(&store_url, "subdir/*.json").unwrap();
        let dbg = format!("{:?}", url);
        println!("dbg={}", dbg);
        assert!(dbg.contains("/subdir/"), "dbg={}", dbg);
        // pattern should be present in debug output
        assert!(dbg.contains("*.json"), "dbg={}", dbg);
    }

    #[test]
    fn test_parse_listing_table_url_invalid_glob() {
        use datafusion::execution::object_store::ObjectStoreUrl;

        let store_url: ObjectStoreUrl = ObjectStoreUrl::parse("file://").unwrap();
        let res = parse_listing_table_url(&store_url, "subdir/[invalid");
        assert!(res.is_err(), "expected error for invalid glob pattern");
    }

    #[test]
    fn test_object_store_urls() {
        let store_url = ObjectStoreUrl::parse("datasets://").unwrap();
        let url = Url::parse(store_url.as_str()).unwrap();
        println!("Parsed URL: {}", url);
    }
}
