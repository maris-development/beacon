use std::{path::PathBuf, sync::Arc};

use datafusion::{
    catalog::Session, datasource::listing::ListingTableUrl, execution::object_store::ObjectStoreUrl,
};

use crate::format_ext::{DatasetMetadata, FileFormatFactoryExt};

pub enum RootStore {
    FileSystem(PathBuf), // File System full path, e.g. /path/to/root
    HttpsStore(String), // Object Store full path, e.g. https://s3.amazonaws.com/bucket-name/path/to/root
}

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

    pub async fn list_datasets(
        &self,
        session: &dyn Session,
        file_formats: &[Arc<dyn FileFormatFactoryExt>],
        glob_path: &str,
    ) -> Vec<DatasetMetadata> {
        todo!()
    }
}
