use std::path::PathBuf;
use std::sync::Arc;

use object_store::ObjectStore;
use object_store::aws::AmazonS3Builder;
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyDict};

#[derive(Clone, Debug, Default)]
pub(crate) struct StorageOptions {
    pub s3: Option<S3Config>,
}

impl StorageOptions {
    pub fn merge(mut self, overrides: StorageOptions) -> StorageOptions {
        if let Some(override_cfg) = overrides.s3 {
            self.s3 = Some(match self.s3 {
                Some(existing) => existing.merge(override_cfg),
                None => override_cfg,
            });
        }
        self
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct S3Config {
    pub region: Option<String>,
    pub endpoint_url: Option<String>,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
    pub session_token: Option<String>,
    pub allow_http: Option<bool>,
}

impl S3Config {
    fn merge(mut self, overrides: S3Config) -> S3Config {
        if overrides.region.is_some() {
            self.region = overrides.region;
        }
        if overrides.endpoint_url.is_some() {
            self.endpoint_url = overrides.endpoint_url;
        }
        if overrides.access_key_id.is_some() {
            self.access_key_id = overrides.access_key_id;
        }
        if overrides.secret_access_key.is_some() {
            self.secret_access_key = overrides.secret_access_key;
        }
        if overrides.session_token.is_some() {
            self.session_token = overrides.session_token;
        }
        if overrides.allow_http.is_some() {
            self.allow_http = overrides.allow_http;
        }
        self
    }
}

pub(crate) fn storage_options_from_py(
    options: Option<Bound<'_, PyDict>>,
) -> PyResult<StorageOptions> {
    let mut overrides = S3Config::default();
    let mut saw_s3_key = false;

    if let Some(mapping) = options {
        for (raw_key, value) in mapping.iter() {
            let key: String = raw_key.extract()?;
            match key.as_str() {
                "key" => {
                    if !value.is_none() {
                        overrides.access_key_id = Some(value.extract()?);
                        saw_s3_key = true;
                    }
                }
                "secret" => {
                    if !value.is_none() {
                        overrides.secret_access_key = Some(value.extract()?);
                        saw_s3_key = true;
                    }
                }
                "token" => {
                    if !value.is_none() {
                        overrides.session_token = Some(value.extract()?);
                        saw_s3_key = true;
                    }
                }
                "region" | "region_name" => {
                    if !value.is_none() {
                        overrides.region = Some(value.extract()?);
                        saw_s3_key = true;
                    }
                }
                "endpoint_url" => {
                    if !value.is_none() {
                        overrides.endpoint_url = Some(value.extract()?);
                        saw_s3_key = true;
                    }
                }
                "use_ssl" => {
                    let use_ssl: bool = value.extract()?;
                    overrides.allow_http = Some(!use_ssl);
                    saw_s3_key = true;
                }
                "client_kwargs" => {
                    if !value.is_none() {
                        let Ok(kwargs) = value.downcast::<PyDict>() else {
                            continue;
                        };
                        if let Some(endpoint) = kwargs.get_item("endpoint_url")? {
                            overrides.endpoint_url = Some(endpoint.extract()?);
                            saw_s3_key = true;
                        }
                        if let Some(allow_http) = kwargs.get_item("allow_http")? {
                            overrides.allow_http = Some(allow_http.extract()?);
                            saw_s3_key = true;
                        }
                    }
                }
                _ => {}
            }
        }
    }

    Ok(StorageOptions {
        s3: if saw_s3_key { Some(overrides) } else { None },
    })
}

pub(crate) fn prepare_store_inputs(
    base_dir: String,
    storage_options: Option<Bound<'_, PyDict>>,
    filesystem: Option<Bound<'_, PyAny>>,
) -> PyResult<(String, StorageOptions)> {
    let mut combined = StorageOptions::default();

    if let Some(fs) = filesystem.as_ref() {
        if let Some(fs_options) = storage_options_from_filesystem(fs)? {
            combined = combined.merge(fs_options);
        }
    }

    let explicit = storage_options_from_py(storage_options)?;
    combined = combined.merge(explicit);

    let mut normalized = base_dir.trim().to_string();
    if normalized.contains("://") {
        return Ok((normalized, combined));
    }

    if let Some(fs) = filesystem.as_ref() {
        if let Some(protocol) = protocol_from_filesystem(fs)? {
            if protocol != "file" && protocol != "local" {
                let trimmed = normalized.trim_matches('/');
                if trimmed.is_empty() {
                    return Err(PyValueError::new_err(
                        "base_dir cannot be empty when inferring protocol from filesystem",
                    ));
                }
                normalized = format!("{protocol}://{trimmed}");
            }
        }
    }

    Ok((normalized, combined))
}

pub(crate) struct StoreHandle {
    pub store: Arc<dyn ObjectStore>,
    prefix: Option<String>,
}

impl StoreHandle {
    fn new(store: Arc<dyn ObjectStore>, prefix: Option<String>) -> Self {
        Self { store, prefix }
    }

    pub fn resolve_collection_path(&self, collection_path: &str) -> PyResult<Path> {
        let suffix = collection_path.trim_matches('/');
        let joined = match (&self.prefix, suffix.is_empty()) {
            (Some(prefix), false) => format!("{prefix}/{suffix}"),
            (Some(prefix), true) => prefix.clone(),
            (None, false) => suffix.to_string(),
            (None, true) => {
                return Err(PyValueError::new_err(
                    "collection_path cannot be empty when base_dir has no embedded path",
                ));
            }
        };

        Ok(Path::from(joined.as_str()))
    }
}

pub(crate) fn init_store(base_dir: String, storage: StorageOptions) -> PyResult<StoreHandle> {
    let trimmed = base_dir.trim();
    if trimmed.starts_with("s3://") {
        init_s3_store(trimmed, storage)
    } else if let Some(path) = trimmed.strip_prefix("file://") {
        init_local_store(path.to_string())
    } else {
        init_local_store(trimmed.to_string())
    }
}

fn init_local_store(base_dir: String) -> PyResult<StoreHandle> {
    let mut base = PathBuf::from(base_dir);
    if base.to_string_lossy().is_empty() {
        base = std::env::temp_dir();
    }
    std::fs::create_dir_all(&base)
        .map_err(|err| PyRuntimeError::new_err(format!("failed to create base dir: {err}")))?;
    let fs = LocalFileSystem::new_with_prefix(base)
        .map_err(|err| PyRuntimeError::new_err(format!("failed to create store: {err}")))?;
    Ok(StoreHandle::new(Arc::new(fs), None))
}

fn init_s3_store(base_uri: &str, storage: StorageOptions) -> PyResult<StoreHandle> {
    let parsed = parse_s3_location(base_uri)?;
    let mut builder = AmazonS3Builder::new().with_bucket_name(&parsed.bucket);

    if let Some(overrides) = storage.s3 {
        if let Some(region) = overrides.region {
            builder = builder.with_region(region);
        }
        if let Some(endpoint) = overrides.endpoint_url.clone() {
            builder = builder.with_endpoint(&endpoint);
            if overrides
                .allow_http
                .unwrap_or_else(|| endpoint.starts_with("http://"))
            {
                builder = builder.with_allow_http(true);
            }
        } else if let Some(allow_http) = overrides.allow_http {
            if allow_http {
                builder = builder.with_allow_http(true);
            }
        }
        if let Some(access_key) = overrides.access_key_id {
            builder = builder.with_access_key_id(access_key);
        }
        if let Some(secret) = overrides.secret_access_key {
            builder = builder.with_secret_access_key(secret);
        }
        if let Some(token) = overrides.session_token {
            builder = builder.with_token(token);
        }
    }

    let store = builder
        .build()
        .map_err(|err| PyRuntimeError::new_err(format!("failed to create S3 store: {err}")))?;

    Ok(StoreHandle::new(
        Arc::new(store),
        parsed
            .prefix
            .map(|prefix| prefix.trim_matches('/').to_string()),
    ))
}

fn storage_options_from_filesystem(
    filesystem: &Bound<'_, PyAny>,
) -> PyResult<Option<StorageOptions>> {
    let storage_attr = match filesystem.getattr("storage_options") {
        Ok(attr) => attr,
        Err(_) => return Ok(None),
    };
    if storage_attr.is_none() {
        return Ok(None);
    }
    let mapping = storage_attr
        .downcast::<PyDict>()
        .map_err(|_| PyValueError::new_err("filesystem.storage_options must be a mapping"))?;
    storage_options_from_py(Some(mapping.clone())).map(Some)
}

fn protocol_from_filesystem(filesystem: &Bound<'_, PyAny>) -> PyResult<Option<String>> {
    let obj = match filesystem.getattr("protocol") {
        Ok(value) => value,
        Err(_) => return Ok(None),
    };
    if obj.is_none() {
        return Ok(None);
    }
    if let Ok(proto) = obj.extract::<String>() {
        return Ok(Some(proto));
    }
    if let Ok(list) = obj.extract::<Vec<String>>() {
        for entry in list {
            if !entry.is_empty() {
                return Ok(Some(entry));
            }
        }
    }
    Ok(None)
}

struct S3Location {
    bucket: String,
    prefix: Option<String>,
}

fn parse_s3_location(uri: &str) -> PyResult<S3Location> {
    let trimmed = uri.trim_start_matches("s3://");
    let (path_part, _) = trimmed.split_once('?').unwrap_or((trimmed, ""));
    let mut segments = path_part.splitn(2, '/');
    let bucket = segments
        .next()
        .filter(|segment| !segment.is_empty())
        .ok_or_else(|| PyValueError::new_err("s3 URI must include a bucket name"))?;
    let prefix = segments
        .next()
        .map(|segment| segment.trim_matches('/').to_string())
        .filter(|segment| !segment.is_empty());
    Ok(S3Location {
        bucket: bucket.to_string(),
        prefix,
    })
}

pub(crate) fn to_py_err<E: std::fmt::Display>(err: E) -> PyErr {
    PyRuntimeError::new_err(err.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dummy_store_handle(prefix: Option<&str>) -> StoreHandle {
        let store = LocalFileSystem::new_with_prefix(std::env::temp_dir()).expect("local store");
        StoreHandle::new(Arc::new(store), prefix.map(|p| p.to_string()))
    }

    #[test]
    fn s3_location_parsing_handles_prefix() {
        let location = parse_s3_location("s3://bucket/foo/bar").unwrap();
        assert_eq!(location.bucket, "bucket");
        assert_eq!(location.prefix.as_deref(), Some("foo/bar"));
    }

    #[test]
    fn resolve_requires_collection_path_without_prefix() {
        let handle = dummy_store_handle(None);
        assert!(handle.resolve_collection_path("").is_err());
    }

    #[test]
    fn resolve_combines_prefix_and_suffix() {
        let handle = dummy_store_handle(Some("datasets/base"));
        let path = handle.resolve_collection_path("collection").unwrap();
        assert_eq!(path.to_string(), "datasets/base/collection");
    }
}
