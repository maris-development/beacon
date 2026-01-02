#![allow(unsafe_op_in_unsafe_fn)]

use std::path::PathBuf;
use std::sync::Arc;

use object_store::ObjectStore;
use object_store::aws::AmazonS3Builder;
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;

#[pyclass(name = "ObjectStore")]
#[derive(Clone)]
pub struct ObjectStoreHandle {
    pub(crate) store: Arc<dyn ObjectStore>,
    prefix: Option<String>,
}

#[pymethods]
impl ObjectStoreHandle {
    /// Create an object store backed by the local filesystem.
    ///
    /// All Beacon Binary Format objects are stored under `base_dir`.
    #[staticmethod]
    pub fn local(base_dir: String) -> PyResult<Self> {
        let mut base = PathBuf::from(base_dir);
        if base.to_string_lossy().is_empty() {
            base = std::env::temp_dir();
        }
        std::fs::create_dir_all(&base)
            .map_err(|err| PyRuntimeError::new_err(format!("failed to create base dir: {err}")))?;
        let fs = LocalFileSystem::new_with_prefix(base)
            .map_err(|err| PyRuntimeError::new_err(format!("failed to create store: {err}")))?;
        Ok(Self {
            store: Arc::new(fs),
            prefix: None,
        })
    }

    /// Create an object store backed by AWS S3 (or S3-compatible object storage).
    ///
    /// The optional `prefix` scopes all operations under a common path inside the bucket.
    #[staticmethod]
    #[pyo3(signature = (bucket, prefix=None, region=None, endpoint_url=None, access_key_id=None, secret_access_key=None, session_token=None, allow_http=None))]
    pub fn s3(
        bucket: String,
        prefix: Option<String>,
        region: Option<String>,
        endpoint_url: Option<String>,
        access_key_id: Option<String>,
        secret_access_key: Option<String>,
        session_token: Option<String>,
        allow_http: Option<bool>,
    ) -> PyResult<Self> {
        if bucket.trim().is_empty() {
            return Err(PyValueError::new_err("bucket cannot be empty"));
        }

        let mut builder = AmazonS3Builder::new().with_bucket_name(&bucket);

        if let Some(region) = region {
            if !region.trim().is_empty() {
                builder = builder.with_region(region);
            }
        }
        if let Some(endpoint) = endpoint_url.as_ref() {
            if !endpoint.trim().is_empty() {
                builder = builder.with_endpoint(endpoint);
                if allow_http.unwrap_or_else(|| endpoint.starts_with("http://")) {
                    builder = builder.with_allow_http(true);
                }
            }
        } else if allow_http.unwrap_or(false) {
            builder = builder.with_allow_http(true);
        }

        if let Some(access_key) = access_key_id {
            if !access_key.trim().is_empty() {
                builder = builder.with_access_key_id(access_key);
            }
        }
        if let Some(secret) = secret_access_key {
            if !secret.trim().is_empty() {
                builder = builder.with_secret_access_key(secret);
            }
        }
        if let Some(token) = session_token {
            if !token.trim().is_empty() {
                builder = builder.with_token(token);
            }
        }

        let store = builder
            .build()
            .map_err(|err| PyRuntimeError::new_err(format!("failed to create S3 store: {err}")))?;

        Ok(Self {
            store: Arc::new(store),
            prefix: prefix
                .map(|p| p.trim_matches('/').to_string())
                .filter(|p| !p.is_empty()),
        })
    }
}

impl ObjectStoreHandle {
    pub(crate) fn resolve_collection_path(&self, collection_path: &str) -> PyResult<Path> {
        let suffix = collection_path.trim_matches('/');
        let joined = match (&self.prefix, suffix.is_empty()) {
            (Some(prefix), false) => format!("{prefix}/{suffix}"),
            (Some(prefix), true) => prefix.clone(),
            (None, false) => suffix.to_string(),
            (None, true) => {
                return Err(PyValueError::new_err(
                    "collection_path cannot be empty when store has no prefix",
                ));
            }
        };

        Ok(Path::from(joined.as_str()))
    }
}
