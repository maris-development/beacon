use std::path::PathBuf;

use object_store::ObjectMeta;

#[derive(Debug, Clone)]
pub struct NetCDFObjectResolver {
    endpoint: String,
    bucket: Option<String>,
    prefix: Option<String>,
}

impl NetCDFObjectResolver {
    pub fn new_s3(endpoint: String, bucket: String, prefix: Option<String>) -> Self {
        Self {
            endpoint,
            bucket: Some(bucket),
            prefix,
        }
    }

    pub fn new(endpoint: String, bucket: Option<String>, prefix: Option<String>) -> Self {
        Self {
            endpoint,
            bucket,
            prefix,
        }
    }

    pub fn resolve_object_meta(&self, object_meta: ObjectMeta) -> PathBuf {
        let mut path_builder = String::new();
        path_builder.push_str(&self.endpoint);

        if let Some(bucket) = &self.bucket {
            path_builder.push_str(&format!("/{bucket}"));
        }

        if let Some(prefix) = &self.prefix {
            if !prefix.starts_with('/') {
                path_builder.push('/');
            }
            path_builder.push_str(prefix);
        }

        // `netcdf::open` supports `#mode=bytes` for certain remote URLs (e.g. HTTP/S3).
        // For local filesystem paths, appending this fragment can break file opening.
        let location = object_meta.location.to_string();
        if !path_builder.ends_with('/') {
            path_builder.push('/');
        }
        path_builder.push_str(&location);

        let is_remote = self.endpoint.starts_with("http://")
            || self.endpoint.starts_with("https://")
            || self.endpoint.starts_with("s3://");
        if is_remote {
            path_builder.push_str("#mode=bytes");
        }

        PathBuf::from(path_builder)
    }
}

#[derive(Debug, Clone)]
pub struct NetCDFSinkResolver {
    data_directory: PathBuf,
}

impl NetCDFSinkResolver {
    pub fn new(data_directory: PathBuf) -> Self {
        Self { data_directory }
    }

    pub fn resolve_output_path(&self, path: &object_store::path::Path) -> PathBuf {
        self.data_directory.join(path.to_string())
    }
}
