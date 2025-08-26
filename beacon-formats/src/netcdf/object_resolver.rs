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

        path_builder.push_str(&format!("/{}#mode=bytes", object_meta.location));

        PathBuf::from(path_builder)
    }
}
