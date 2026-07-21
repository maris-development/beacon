use std::{path::PathBuf, sync::Arc};

use datafusion::{
    catalog::Session, datasource::listing::ListingTableUrl, execution::object_store::ObjectStoreUrl,
};

use crate::format_ext::{DatasetMetadata, FileFormatFactoryExt};

#[derive(Debug, Clone)]
pub enum RootStore {
    FileSystem(PathBuf), // File System full path, e.g. /path/to/root
    HttpsStore(String), // Object Store full path, e.g. https://s3.amazonaws.com/bucket-name/path/to/root
}

pub fn try_listing_factory_from_session(session: &dyn Session) -> Option<Arc<ListingFactory>> {
    session.config().get_extension::<ListingFactory>().clone()
}

#[derive(Debug, Clone)]
pub struct ListingFactory {
    default_store_url: Option<ObjectStoreUrl>,
    root_store: Option<RootStore>,
}

impl ListingFactory {
    pub fn new(default_store_url: Option<ObjectStoreUrl>, root_store: Option<RootStore>) -> Self {
        Self {
            default_store_url,
            root_store,
        }
    }

    pub fn parse_listing_table_url(
        &self,
        session: &dyn Session,
        glob_path: &str,
    ) -> datafusion::error::Result<ListingTableUrl> {
        crate::listing_url_resolver::parse_listing_table_url(
            self.default_store_url.clone(),
            glob_path,
            session.runtime_env().object_store_registry.as_ref(),
        )
    }

    pub fn try_parse_obj_path_to_netcdf_path(
        &self,
        scheme: &str,
        object_path: &object_store::path::Path,
    ) -> Option<String> {
        // Check if default store is defined ()
        // If not defined, then its an open find, defined by the scheme.
        // If scheme is file, then its either a relative or absolute path, and we can just return the path as is.
        match &self.root_store {
            Some(store) => match store {
                RootStore::FileSystem(path_buf) =>
                // Append the object path to the root path and return the full path as a string.
                {
                    let full_path = path_buf.join(object_path.to_string());
                    Some(full_path.to_string_lossy().to_string())
                }
                RootStore::HttpsStore(url) => {
                    // Append the object path to the root url and return the full url as a string and #mode bytes.
                    let full_url =
                        format!("{}/{}#mode=bytes", url.trim_end_matches('/'), object_path);
                    Some(full_url)
                }
            },
            None => {
                // If no default store is defined, then we can only handle the file scheme or http/https with #mode bytes.
                if scheme == "file" {
                    let path_str = object_path.to_string();
                    // Check if is absolute path or relative path. If absolute, then we can just return the path as is. If relative, then we need to prepend the current working directory.
                    if PathBuf::from(&path_str).is_absolute() {
                        return Some(path_str);
                    } else {
                        let cwd = std::env::current_dir().ok()?;
                        let abs_path = cwd.join(path_str);
                        return Some(abs_path.to_string_lossy().to_string());
                    }
                } else if scheme == "http" || scheme == "https" {
                    // If the scheme is http or https, then we add the #mode=bytes to the path, and return it.
                    return Some(format!("{}#mode=bytes", object_path));
                }
                None
            }
        }
    }

    pub fn parse_listing_table_url_with_store(
        &self,
        session: &dyn Session,
        store_url: &ObjectStoreUrl,
        glob_path: &str,
    ) -> datafusion::error::Result<ListingTableUrl> {
        crate::listing_url_resolver::parse_listing_table_url(
            Some(store_url.clone()),
            glob_path,
            session.runtime_env().object_store_registry.as_ref(),
        )
    }

    pub fn parse_to_store(&self, session: &dyn Session, path: &str) -> Option<ObjectStoreUrl> {
        match &self.default_store_url {
            Some(store) => Some(store.clone()),
            None => None, // ToDo: We can try to resolve this using the scheme of the path or returning the file:// scheme for local files. But for now, we just return None.
        }
    }

    pub fn rewrite_path(&self, path: &str) -> String {
        match &self.root_store {
            Some(RootStore::FileSystem(root_path)) => {
                let full_path = root_path.join(path);
                full_path.to_string_lossy().to_string()
            }
            Some(RootStore::HttpsStore(root_url)) => {
                format!("{}/{}", root_url.trim_end_matches('/'), path)
            }
            None => path.to_string(),
        }
    }

    pub async fn list_datasets(
        &self,
        session: &dyn Session,
        file_formats: &[Arc<dyn FileFormatFactoryExt>],
        glob_path: &str,
    ) -> Vec<DatasetMetadata> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use object_store::path::Path as ObjectPath;

    use super::*;

    fn factory(root: Option<RootStore>) -> ListingFactory {
        ListingFactory::new(None, root)
    }

    #[test]
    fn filesystem_root_joins_the_object_path() {
        let factory = factory(Some(RootStore::FileSystem(PathBuf::from("/data/root"))));
        let path = factory
            .try_parse_obj_path_to_netcdf_path("file", &ObjectPath::from("argo/a.nc"))
            .unwrap();
        assert_eq!(path, "/data/root/argo/a.nc");
    }

    #[test]
    fn https_root_trims_its_trailing_slash_and_adds_byte_mode() {
        // netCDF-c needs the `#mode=bytes` suffix to range-read over HTTP.
        let factory = factory(Some(RootStore::HttpsStore(
            "https://example.org/bucket/".to_string(),
        )));
        let path = factory
            .try_parse_obj_path_to_netcdf_path("https", &ObjectPath::from("argo/a.nc"))
            .unwrap();
        assert_eq!(path, "https://example.org/bucket/argo/a.nc#mode=bytes");
    }

    #[test]
    fn a_root_store_wins_over_the_scheme() {
        // With a root store configured the scheme is irrelevant — the path is
        // always resolved against the root.
        let factory = factory(Some(RootStore::FileSystem(PathBuf::from("/data/root"))));
        assert_eq!(
            factory.try_parse_obj_path_to_netcdf_path("s3", &ObjectPath::from("a.nc")),
            Some("/data/root/a.nc".to_string())
        );
    }

    #[test]
    fn without_a_root_relative_file_paths_are_made_absolute() {
        let factory = factory(None);
        let path = factory
            .try_parse_obj_path_to_netcdf_path("file", &ObjectPath::from("argo/a.nc"))
            .unwrap();
        let expected = std::env::current_dir().unwrap().join("argo/a.nc");
        assert_eq!(path, expected.to_string_lossy());
    }

    #[test]
    fn without_a_root_http_paths_only_get_byte_mode() {
        let factory = factory(None);
        // `ObjectPath` normalizes the `//`, so the scheme is what matters here.
        assert_eq!(
            factory.try_parse_obj_path_to_netcdf_path("http", &ObjectPath::from("host/a.nc")),
            Some("host/a.nc#mode=bytes".to_string())
        );
    }

    #[test]
    fn without_a_root_unsupported_schemes_are_unresolvable() {
        // Only file/http/https can be handed to the netCDF reader by path; a
        // remote object store needs a root store to be expressible.
        let factory = factory(None);
        assert_eq!(
            factory.try_parse_obj_path_to_netcdf_path("s3", &ObjectPath::from("bucket/a.nc")),
            None
        );
    }

    #[test]
    fn rewrite_path_prefixes_the_root_store() {
        assert_eq!(
            factory(Some(RootStore::FileSystem(PathBuf::from("/data/root")))).rewrite_path("a/b.nc"),
            "/data/root/a/b.nc"
        );
        assert_eq!(
            factory(Some(RootStore::HttpsStore("https://example.org/".to_string())))
                .rewrite_path("a/b.nc"),
            "https://example.org/a/b.nc"
        );
        // No root store: the path passes through untouched.
        assert_eq!(factory(None).rewrite_path("a/b.nc"), "a/b.nc");
    }
}
