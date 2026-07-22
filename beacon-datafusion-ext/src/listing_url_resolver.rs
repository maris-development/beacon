use std::path::PathBuf;

use datafusion::{
    common::exec_datafusion_err,
    datasource::listing::ListingTableUrl,
    execution::object_store::{ObjectStoreRegistry, ObjectStoreUrl},
};
use url::Url;

use crate::object_store_registry::{build_object_store, store_key_url};

/// Resolve a user-supplied glob/path into a DataFusion [`ListingTableUrl`],
/// registering the backing object store on demand when the path carries its own scheme.
///
/// Resolution rules (the `default_store_url` and an in-path scheme are mutually
/// exclusive):
/// - `Some(default)` + a schemed path (`s3://…`) → **error**: the default is
///   prepended, so a schemed path would be nonsensical.
/// - `Some(default)` + a relative path → prepend the default store URL (the
///   historical behavior). The default store must already be registered.
/// - `None` + a schemed path → use the scheme verbatim; if it is a remote
///   backend (`s3`/`gs`/`az`/`http`…) and not yet registered, build the store
///   from the environment and register it on `registry`.
/// - `None` + a relative/absolute path → treat it as a **local** filesystem
///   path (`file://` + the absolute path), ensuring the local store is
///   registered.
pub fn parse_listing_table_url(
    default_store_url: Option<ObjectStoreUrl>,
    glob_path: &str,
    registry: &dyn ObjectStoreRegistry,
) -> datafusion::error::Result<ListingTableUrl> {
    // Split the path into (base, optional glob) and detect a leading scheme
    // (e.g. `s3://`) on the raw input.
    let (base_path, glob_pattern) = split_path_and_glob(glob_path);
    let scheme = scheme_of(glob_path);

    // Build the complete base URL string (no glob yet), per the branch.
    let mut full_path: String = match (default_store_url.as_ref(), scheme) {
        // Mutually exclusive: a default store URL AND a schemed path.
        (Some(_), Some(scheme)) => {
            return Err(exec_datafusion_err!(
                "a default object store URL was provided but the path already \
                 contains a `{scheme}://` scheme; these are mutually exclusive"
            ));
        }
        // Default store URL + relative path → prepend the default (historical
        // behavior). Verify the default store is registered first.
        (Some(default), None) => {
            let default_url = Url::parse(default.as_str()).map_err(|e| {
                exec_datafusion_err!("invalid default store URL {}: {e}", default.as_str())
            })?;
            if registry.get_store(&default_url).is_err() {
                return Err(exec_datafusion_err!(
                    "no object store registered for default store URL `{default_url}`"
                ));
            }

            let mut prefix = default.to_string();
            if base_path.components().next().is_some() {
                let normalized = base_path.as_os_str().to_string_lossy().replace('\\', "/");
                if prefix.ends_with('/') {
                    prefix.push_str(normalized.as_str());
                } else {
                    prefix.push('/');
                    prefix.push_str(normalized.as_str());
                }
            }
            prefix
        }
        // No default + schemed path → the path carries its own store. Lazily
        // build + register the backing store, then use the path verbatim.
        (None, Some(_)) => {
            let base_str = base_path.as_os_str().to_string_lossy().replace('\\', "/");
            let base_url = Url::parse(&base_str)
                .map_err(|e| exec_datafusion_err!("failed to parse URL {base_str}: {e}"))?;
            ensure_store_registered(registry, &base_url)?;
            base_str
        }
        // No default + no scheme → a local filesystem path.
        (None, None) => {
            let abs = std::path::absolute(base_path.as_path()).map_err(|e| {
                exec_datafusion_err!("failed to absolutize path {}: {e}", base_path.display())
            })?;
            let file_url = Url::from_file_path(&abs).map_err(|_| {
                exec_datafusion_err!("failed to build file URL for {}", abs.display())
            })?;
            ensure_store_registered(registry, &file_url)?;
            file_url.to_string()
        }
    };

    let glob_pattern_parsed = if let Some(pattern) = glob_pattern {
        if !full_path.ends_with('/') {
            full_path.push('/');
        }
        Some(glob::Pattern::new(&pattern).map_err(|e| {
            tracing::warn!(pattern = %pattern, error = %e, "failed to parse glob pattern");
            exec_datafusion_err!("Failed to parse glob pattern: {}", e)
        })?)
    } else {
        None
    };

    let url = Url::parse(&full_path).map_err(|e| {
        tracing::warn!(url = %full_path, error = %e, "failed to parse listing URL");
        exec_datafusion_err!("Failed to parse URL: {}", e)
    })?;

    let table_url = ListingTableUrl::try_new(url, glob_pattern_parsed).map_err(|e| {
        tracing::warn!(path = %full_path, error = %e, "failed to create listing table URL");
        exec_datafusion_err!("Failed to create table URL: {}", e)
    })?;

    tracing::debug!(table_url = %table_url, "resolved listing table URL");
    Ok(table_url)
}

/// Ensure a store for `url`'s scheme+authority is registered, lazily building
/// it (via the shared [`build_object_store`]) on a miss. If `registry` is itself
/// a [`crate::object_store_registry::LazyObjectStoreRegistry`], the `get_store`
/// call already materializes the store, so this is a cheap no-op.
fn ensure_store_registered(
    registry: &dyn ObjectStoreRegistry,
    url: &Url,
) -> datafusion::error::Result<()> {
    let key = store_key_url(url);
    if registry.get_store(&key).is_err() {
        let store = build_object_store(&key, None)?;
        registry.register_store(&key, store);
        tracing::debug!(store = %key, "lazily registered object store");
    }
    Ok(())
}

/// Detect a leading URL scheme (`scheme://…`) on a raw path, returning the
/// scheme without the `://`. Returns `None` for relative paths, bare filenames,
/// and Windows drive paths (which contain no `://`).
///
/// scheme = ALPHA *( ALPHA / DIGIT / "+" / "-" / "." )
pub(crate) fn scheme_of(path: &str) -> Option<&str> {
    if !path.as_bytes().first().is_some_and(u8::is_ascii_alphabetic) {
        return None;
    }
    let idx = path.find("://")?;
    let scheme = &path[..idx];
    scheme
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '+' || c == '-' || c == '.')
        .then_some(scheme)
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
            PathBuf::from(parts[..i].join("/"))
        };
        let glob = parts[i..].join("/");
        (base, Some(glob))
    } else {
        // No glob pattern at all
        (PathBuf::from(input), None)
    }
}

#[cfg(test)]
mod tests {
    use std::path::MAIN_SEPARATOR;
    use std::sync::Arc;

    use datafusion::execution::object_store::{
        DefaultObjectStoreRegistry, ObjectStoreRegistry, ObjectStoreUrl,
    };
    use object_store::memory::InMemory;

    use super::*;

    /// A registry with a store registered under `datasets://`, mirroring the
    /// eager registration beacon performs at boot.
    fn registry_with_datasets() -> DefaultObjectStoreRegistry {
        let registry = DefaultObjectStoreRegistry::new();
        registry.register_store(
            &Url::parse("datasets://").unwrap(),
            Arc::new(InMemory::new()),
        );
        registry
    }

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
        assert_eq!(glob, Some("**/*.rs".to_string()));
    }

    #[test]
    fn test_glob_at_start() {
        let (base, glob) = split_path_and_glob("*.txt");
        assert_eq!(base, PathBuf::from("."));
        assert_eq!(glob, Some("*.txt".to_string()));
    }

    #[test]
    #[ignore = "This needs to be fixed"]
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
        assert_eq!(glob, Some("**/*.txt".to_string()));
    }

    #[test]
    fn test_scheme_of() {
        assert_eq!(scheme_of("s3://bucket/a"), Some("s3"));
        assert_eq!(scheme_of("gs://bucket/a"), Some("gs"));
        assert_eq!(scheme_of("file:///abs/x"), Some("file"));
        assert_eq!(scheme_of("http://host/a"), Some("http"));
        assert_eq!(scheme_of("rel/path/x.nc"), None);
        assert_eq!(scheme_of("/abs/path/x.nc"), None);
        assert_eq!(scheme_of("file[0-9].log"), None);
        // Windows drive path has no `://`
        assert_eq!(scheme_of("C:/data/x.nc"), None);
    }

    #[test]
    fn test_default_store_relative_no_glob() {
        let registry = registry_with_datasets();
        let store_url = ObjectStoreUrl::parse("datasets://").unwrap();
        let url = parse_listing_table_url(Some(store_url), "foo/zarr.json", &registry).unwrap();
        let dbg = format!("{:?}", url);
        assert!(dbg.contains("/foo/") || dbg.contains("foo"), "dbg={}", dbg);
    }

    #[test]
    fn test_default_store_relative_with_glob() {
        let registry = registry_with_datasets();
        let store_url = ObjectStoreUrl::parse("datasets://").unwrap();
        let url = parse_listing_table_url(Some(store_url), "subdir/*.json", &registry).unwrap();
        assert_eq!(url.prefix().to_string(), "subdir");
        assert_eq!(
            url.get_glob().as_ref().map(glob::Pattern::as_str),
            Some("*.json")
        );
    }

    #[test]
    fn test_default_store_parent_dir_and_glob() {
        let registry = registry_with_datasets();
        let store_url = ObjectStoreUrl::parse("datasets://").unwrap();
        let url =
            parse_listing_table_url(Some(store_url), "argo-floats/pub/**/*.nc", &registry).unwrap();
        assert_eq!(url.prefix().to_string(), "argo-floats/pub");
        assert_eq!(
            url.get_glob().as_ref().map(glob::Pattern::as_str),
            Some("**/*.nc")
        );
    }

    #[test]
    fn test_default_store_with_scheme_in_path_errors() {
        let registry = registry_with_datasets();
        let store_url = ObjectStoreUrl::parse("datasets://").unwrap();
        // A schemed path combined with a default store URL is rejected.
        let res = parse_listing_table_url(Some(store_url), "s3://bucket/x.parquet", &registry);
        assert!(res.is_err(), "expected error for default + schemed path");
    }

    #[test]
    fn test_default_store_not_registered_errors() {
        // Empty registry: `datasets://` is not registered.
        let registry = DefaultObjectStoreRegistry::new();
        let store_url = ObjectStoreUrl::parse("datasets://").unwrap();
        let res = parse_listing_table_url(Some(store_url), "foo/bar.parquet", &registry);
        assert!(
            res.is_err(),
            "expected error when default store not registered"
        );
    }

    #[test]
    fn test_default_store_invalid_glob() {
        let registry = registry_with_datasets();
        let store_url = ObjectStoreUrl::parse("datasets://").unwrap();
        let res = parse_listing_table_url(Some(store_url), "subdir/[invalid", &registry);
        assert!(res.is_err(), "expected error for invalid glob pattern");
    }

    #[test]
    fn test_schemed_s3_path_registers_store() {
        let registry = DefaultObjectStoreRegistry::new();
        // Store not present beforehand.
        assert!(
            registry
                .get_store(&Url::parse("s3://bucket").unwrap())
                .is_err()
        );

        let url = parse_listing_table_url(None, "s3://bucket/a/*.parquet", &registry).unwrap();
        assert!(
            url.as_str().starts_with("s3://bucket"),
            "url={}",
            url.as_str()
        );
        assert_eq!(
            url.get_glob().as_ref().map(glob::Pattern::as_str),
            Some("*.parquet")
        );

        // The s3://bucket store was lazily registered as a side effect.
        assert!(
            registry
                .get_store(&Url::parse("s3://bucket/a/x.parquet").unwrap())
                .is_ok()
        );
    }

    #[test]
    fn test_no_default_absolute_local_path() {
        let registry = DefaultObjectStoreRegistry::new();
        let url = parse_listing_table_url(None, "/abs/dir/x.nc", &registry).unwrap();
        assert!(url.as_str().starts_with("file:///"), "url={}", url.as_str());
        assert!(url.as_str().contains("/abs/dir/"), "url={}", url.as_str());
    }

    #[test]
    fn test_no_default_relative_local_path_with_glob() {
        let registry = DefaultObjectStoreRegistry::new();
        let url = parse_listing_table_url(None, "rel/dir/*.nc", &registry).unwrap();
        assert!(url.as_str().starts_with("file:///"), "url={}", url.as_str());
        assert!(url.as_str().contains("rel/dir"), "url={}", url.as_str());
        assert_eq!(
            url.get_glob().as_ref().map(glob::Pattern::as_str),
            Some("*.nc")
        );
    }

    #[test]
    fn test_object_store_urls() {
        let store_url = ObjectStoreUrl::parse("datasets://").unwrap();
        let url = Url::parse(store_url.as_str()).unwrap();
        assert_eq!(url.scheme(), "datasets");
    }
}
