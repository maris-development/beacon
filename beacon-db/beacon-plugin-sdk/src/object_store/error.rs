//! Object store errors and results that keep their kind across the plugin boundary.

use std::any::Any;

use ::object_store::Error;
use stabby::string::String as SString;

use crate::panic_safe::panic_message;

/// The `object_store::Error` variant of an [`FFI_ObjectStoreError`].
///
/// The boundary carries the discriminant as a `u32`. `From<u32>` maps an unknown value to `Generic`.
#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(non_camel_case_types)]
pub enum FFI_ErrorKind {
    /// `Error::Generic`, and any variant that this SDK does not know.
    Generic = 0,
    /// `Error::NotFound`.
    NotFound = 1,
    /// `Error::InvalidPath`. Arrives as `Generic`.
    InvalidPath = 2,
    /// `Error::JoinError`. Arrives as `Generic`.
    JoinError = 3,
    /// `Error::NotSupported`.
    NotSupported = 4,
    /// `Error::AlreadyExists`.
    AlreadyExists = 5,
    /// `Error::Precondition`.
    Precondition = 6,
    /// `Error::NotModified`.
    NotModified = 7,
    /// `Error::NotImplemented`.
    NotImplemented = 8,
    /// `Error::PermissionDenied`.
    PermissionDenied = 9,
    /// `Error::Unauthenticated`.
    Unauthenticated = 10,
    /// `Error::UnknownConfigurationKey`.
    UnknownConfigurationKey = 11,
}

impl From<u32> for FFI_ErrorKind {
    fn from(value: u32) -> Self {
        match value {
            1 => Self::NotFound,
            2 => Self::InvalidPath,
            3 => Self::JoinError,
            4 => Self::NotSupported,
            5 => Self::AlreadyExists,
            6 => Self::Precondition,
            7 => Self::NotModified,
            8 => Self::NotImplemented,
            9 => Self::PermissionDenied,
            10 => Self::Unauthenticated,
            11 => Self::UnknownConfigurationKey,
            _ => Self::Generic,
        }
    }
}

/// An `object_store::Error` as FFI data.
///
/// The source error does not cross. Its text is in `message`.
#[repr(C)]
#[derive(Debug, Clone)]
pub struct FFI_ObjectStoreError {
    /// The [`FFI_ErrorKind`] discriminant. An unknown value arrives as `Generic`.
    pub kind: u32,
    /// The store name, or empty.
    pub store: SString,
    /// The object path, or empty.
    pub path: SString,
    /// The text of the source error, or of the full error.
    pub message: SString,
    /// `operation` for `NotImplemented`, `key` for `UnknownConfigurationKey`, else empty.
    pub detail_a: SString,
    /// `implementer` for `NotImplemented`, else empty.
    pub detail_b: SString,
}

impl FFI_ObjectStoreError {
    fn new(kind: FFI_ErrorKind, store: &str, path: &str, message: &str) -> Self {
        Self {
            kind: kind as u32,
            store: SString::from(store),
            path: SString::from(path),
            message: SString::from(message),
            detail_a: SString::new(),
            detail_b: SString::new(),
        }
    }
}

/// Returns the static name of a store that `object_store` knows, else `"Plugin"`.
fn static_store_name(name: &str) -> &'static str {
    match name {
        "S3" => "S3",
        "GCS" => "GCS",
        "MicrosoftAzure" => "MicrosoftAzure",
        "HTTP" => "HTTP",
        "LocalFileSystem" => "LocalFileSystem",
        "InMemory" => "InMemory",
        "Prefix" => "Prefix",
        "Limit" => "Limit",
        "Throttle" => "Throttle",
        _ => "Plugin",
    }
}

impl From<Error> for FFI_ObjectStoreError {
    fn from(error: Error) -> Self {
        use FFI_ErrorKind as K;
        match error {
            Error::Generic { store, source } => {
                Self::new(K::Generic, store, "", &source.to_string())
            }
            Error::NotFound { path, source } => {
                Self::new(K::NotFound, "", &path, &source.to_string())
            }
            Error::InvalidPath { source } => Self::new(K::InvalidPath, "", "", &source.to_string()),
            Error::JoinError { source } => Self::new(K::JoinError, "", "", &source.to_string()),
            Error::NotSupported { source } => {
                Self::new(K::NotSupported, "", "", &source.to_string())
            }
            Error::AlreadyExists { path, source } => {
                Self::new(K::AlreadyExists, "", &path, &source.to_string())
            }
            Error::Precondition { path, source } => {
                Self::new(K::Precondition, "", &path, &source.to_string())
            }
            Error::NotModified { path, source } => {
                Self::new(K::NotModified, "", &path, &source.to_string())
            }
            Error::PermissionDenied { path, source } => {
                Self::new(K::PermissionDenied, "", &path, &source.to_string())
            }
            Error::Unauthenticated { path, source } => {
                Self::new(K::Unauthenticated, "", &path, &source.to_string())
            }
            Error::NotImplemented {
                operation,
                implementer,
            } => Self {
                detail_a: SString::from(operation.as_str()),
                detail_b: SString::from(implementer.as_str()),
                ..Self::new(K::NotImplemented, "", "", "")
            },
            Error::UnknownConfigurationKey { store, key } => Self {
                detail_a: SString::from(key.as_str()),
                ..Self::new(K::UnknownConfigurationKey, store, "", "")
            },
            // A variant from a later `object_store` version.
            other => Self::new(K::Generic, "", "", &other.to_string()),
        }
    }
}

impl From<FFI_ObjectStoreError> for Error {
    fn from(error: FFI_ObjectStoreError) -> Self {
        use FFI_ErrorKind as K;
        let path = error.path.as_str().to_string();
        let source = error.message.as_str().to_string().into();
        let kind = FFI_ErrorKind::from(error.kind);
        match kind {
            K::Generic => Error::Generic {
                store: static_store_name(error.store.as_str()),
                source,
            },
            K::NotFound => Error::NotFound { path, source },
            K::InvalidPath | K::JoinError => Error::Generic {
                store: static_store_name(error.store.as_str()),
                source: format!("{kind:?}: {}", error.message.as_str()).into(),
            },
            K::NotSupported => Error::NotSupported { source },
            K::AlreadyExists => Error::AlreadyExists { path, source },
            K::Precondition => Error::Precondition { path, source },
            K::NotModified => Error::NotModified { path, source },
            K::NotImplemented => Error::NotImplemented {
                operation: error.detail_a.as_str().to_string(),
                implementer: error.detail_b.as_str().to_string(),
            },
            K::PermissionDenied => Error::PermissionDenied { path, source },
            K::Unauthenticated => Error::Unauthenticated { path, source },
            K::UnknownConfigurationKey => Error::UnknownConfigurationKey {
                store: static_store_name(error.store.as_str()),
                key: error.detail_a.as_str().to_string(),
            },
        }
    }
}

/// An `object_store::Result` as FFI data.
#[repr(C, u8)]
#[derive(Debug)]
pub enum FFI_ObjectStoreResult<T> {
    /// The value.
    Ok(T),
    /// The error.
    Err(FFI_ObjectStoreError),
}

impl<T> From<::object_store::Result<T>> for FFI_ObjectStoreResult<T> {
    fn from(result: ::object_store::Result<T>) -> Self {
        match result {
            Ok(value) => Self::Ok(value),
            Err(error) => Self::Err(error.into()),
        }
    }
}

impl<T> FFI_ObjectStoreResult<T> {
    /// Returns the value, or the rebuilt `object_store::Error`.
    pub fn into_result(self) -> ::object_store::Result<T> {
        match self {
            Self::Ok(value) => Ok(value),
            Self::Err(error) => Err(error.into()),
        }
    }
}

/// Returns the error for a panic in an object store call.
// The extern "C" store functions call this.
#[allow(dead_code)]
pub(crate) fn panic_error(payload: Box<dyn Any + Send>) -> FFI_ObjectStoreError {
    let message = format!(
        "panic in object store call: {}",
        panic_message(payload.as_ref())
    );
    FFI_ObjectStoreError::new(FFI_ErrorKind::Generic, "", "", &message)
}

#[cfg(test)]
mod tests {
    use ::object_store::path::Path;

    use super::*;

    fn round_trip(error: Error) -> Error {
        FFI_ObjectStoreResult::<()>::from(Err(error))
            .into_result()
            .unwrap_err()
    }

    fn source_text(error: &Error) -> String {
        std::error::Error::source(error).unwrap().to_string()
    }

    #[test]
    fn error_kinds_round_trip() {
        let path = || "a/b.nc".to_string();
        let source = || Box::<dyn std::error::Error + Send + Sync>::from("boom");

        let back = round_trip(Error::NotFound {
            path: path(),
            source: source(),
        });
        assert!(matches!(&back, Error::NotFound { path, .. } if path == "a/b.nc"));
        assert_eq!(source_text(&back), "boom");

        let back = round_trip(Error::AlreadyExists {
            path: path(),
            source: source(),
        });
        assert!(matches!(&back, Error::AlreadyExists { path, .. } if path == "a/b.nc"));
        assert_eq!(source_text(&back), "boom");

        let back = round_trip(Error::Precondition {
            path: path(),
            source: source(),
        });
        assert!(matches!(&back, Error::Precondition { path, .. } if path == "a/b.nc"));
        assert_eq!(source_text(&back), "boom");

        let back = round_trip(Error::NotModified {
            path: path(),
            source: source(),
        });
        assert!(matches!(&back, Error::NotModified { path, .. } if path == "a/b.nc"));
        assert_eq!(source_text(&back), "boom");

        let back = round_trip(Error::NotSupported { source: source() });
        assert!(matches!(&back, Error::NotSupported { .. }));
        assert_eq!(source_text(&back), "boom");

        let back = round_trip(Error::PermissionDenied {
            path: path(),
            source: source(),
        });
        assert!(matches!(&back, Error::PermissionDenied { path, .. } if path == "a/b.nc"));
        assert_eq!(source_text(&back), "boom");

        let back = round_trip(Error::Unauthenticated {
            path: path(),
            source: source(),
        });
        assert!(matches!(&back, Error::Unauthenticated { path, .. } if path == "a/b.nc"));
        assert_eq!(source_text(&back), "boom");

        let back = round_trip(Error::Generic {
            store: "InMemory",
            source: source(),
        });
        assert!(matches!(
            &back,
            Error::Generic {
                store: "InMemory",
                ..
            }
        ));
        assert_eq!(source_text(&back), "boom");
    }

    #[test]
    fn not_implemented_keeps_fields() {
        let back = round_trip(Error::NotImplemented {
            operation: "put".into(),
            implementer: "X".into(),
        });
        assert!(matches!(
            &back,
            Error::NotImplemented { operation, implementer }
                if operation == "put" && implementer == "X"
        ));
    }

    #[test]
    fn unknown_key_keeps_key() {
        let back = round_trip(Error::UnknownConfigurationKey {
            store: "S3",
            key: "k".into(),
        });
        assert!(matches!(
            &back,
            Error::UnknownConfigurationKey { store: "S3", key } if key == "k"
        ));
    }

    #[test]
    fn join_and_invalid_path_become_generic() {
        let invalid = Error::from(Path::parse("a//b").unwrap_err());
        let back = round_trip(invalid);
        assert!(matches!(&back, Error::Generic { .. }));
        assert!(source_text(&back).contains("InvalidPath"));

        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let join = runtime.block_on(async {
            let task = tokio::spawn(std::future::pending::<()>());
            task.abort();
            task.await.unwrap_err()
        });
        let back = round_trip(Error::from(join));
        assert!(matches!(&back, Error::Generic { .. }));
        assert!(source_text(&back).contains("JoinError"));
    }

    #[test]
    fn unknown_store_name_maps_to_plugin() {
        let back = round_trip(Error::Generic {
            store: "Custom",
            source: "x".into(),
        });
        assert!(matches!(
            &back,
            Error::Generic {
                store: "Plugin",
                ..
            }
        ));

        let back = round_trip(Error::Generic {
            store: "S3",
            source: "x".into(),
        });
        assert!(matches!(&back, Error::Generic { store: "S3", .. }));
    }

    #[test]
    fn ok_result_round_trip() {
        let result = FFI_ObjectStoreResult::from(Ok::<u32, Error>(7));
        assert_eq!(result.into_result().unwrap(), 7);
    }

    #[test]
    fn unknown_kind_becomes_generic() {
        let error = FFI_ObjectStoreError {
            kind: 99,
            store: SString::new(),
            path: SString::new(),
            message: SString::from("strange"),
            detail_a: SString::new(),
            detail_b: SString::new(),
        };
        let back = Error::from(error);
        assert!(matches!(
            &back,
            Error::Generic {
                store: "Plugin",
                ..
            }
        ));
        assert_eq!(source_text(&back), "strange");
        assert_eq!(FFI_ErrorKind::from(99), FFI_ErrorKind::Generic);
        assert_eq!(FFI_ErrorKind::from(6), FFI_ErrorKind::Precondition);
    }

    #[test]
    fn panic_error_keeps_message() {
        let error = panic_error(Box::new("oops"));
        assert_eq!(FFI_ErrorKind::from(error.kind), FFI_ErrorKind::Generic);
        assert_eq!(error.message.as_str(), "panic in object store call: oops");
        let back = Error::from(error);
        assert!(matches!(
            &back,
            Error::Generic {
                store: "Plugin",
                ..
            }
        ));
    }
}
