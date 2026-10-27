//! `#[repr(C)]` mirrors of the `object_store` option and result types.

use ::object_store::path::Path;
use ::object_store::{Error, ObjectMeta};
use chrono::DateTime;
use datafusion_ffi::ffi_option::FFI_Option;
use stabby::string::String as SString;

/// The store name in the errors of this module.
const STORE: &str = "Plugin";

/// An [`ObjectMeta`] as FFI data.
#[repr(C)]
#[derive(Debug, Clone)]
pub struct FFI_ObjectMeta {
    /// The full path to the object, in [`Path`] form.
    pub location: SString,
    /// The last modified time, in nanoseconds since the Unix epoch.
    pub last_modified_ns: i64,
    /// The size in bytes.
    pub size: u64,
    /// The entity tag.
    pub e_tag: FFI_Option<SString>,
    /// The version.
    pub version: FFI_Option<SString>,
}

fn to_sstring(value: &Option<String>) -> FFI_Option<SString> {
    value.as_deref().map(SString::from).into()
}

fn from_sstring(value: FFI_Option<SString>) -> Option<String> {
    value.into_option().map(|s| s.as_str().to_string())
}

impl TryFrom<&ObjectMeta> for FFI_ObjectMeta {
    type Error = Error;

    /// Fails when the last modified time is outside the `i64` nanosecond range.
    fn try_from(meta: &ObjectMeta) -> Result<Self, Error> {
        let last_modified_ns =
            meta.last_modified
                .timestamp_nanos_opt()
                .ok_or_else(|| Error::Generic {
                    store: STORE,
                    source: format!(
                        "last modified time of {} is out of range: {}",
                        meta.location, meta.last_modified
                    )
                    .into(),
                })?;
        Ok(Self {
            location: SString::from(meta.location.as_ref()),
            last_modified_ns,
            size: meta.size,
            e_tag: to_sstring(&meta.e_tag),
            version: to_sstring(&meta.version),
        })
    }
}

impl TryFrom<FFI_ObjectMeta> for ObjectMeta {
    type Error = Error;

    /// Fails when the location is not a valid [`Path`].
    fn try_from(meta: FFI_ObjectMeta) -> Result<Self, Error> {
        let location = Path::parse(meta.location.as_str()).map_err(|e| Error::Generic {
            store: STORE,
            source: Box::new(e),
        })?;
        Ok(Self {
            location,
            last_modified: DateTime::from_timestamp_nanos(meta.last_modified_ns),
            size: meta.size,
            e_tag: from_sstring(meta.e_tag),
            version: from_sstring(meta.version),
        })
    }
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, Utc};

    use super::*;

    #[test]
    fn object_meta_round_trip() {
        let meta = ObjectMeta {
            location: Path::from("a/b.nc"),
            last_modified: "2026-10-27T12:34:56.123456789Z"
                .parse::<DateTime<Utc>>()
                .unwrap(),
            size: 42,
            e_tag: Some("\"x\"".to_string()),
            version: Some("v1".to_string()),
        };
        let back = ObjectMeta::try_from(FFI_ObjectMeta::try_from(&meta).unwrap()).unwrap();
        assert_eq!(back, meta);
    }

    #[test]
    fn object_meta_without_options_round_trip() {
        let meta = ObjectMeta {
            location: Path::from("a"),
            last_modified: DateTime::<Utc>::UNIX_EPOCH,
            size: 0,
            e_tag: None,
            version: None,
        };
        let back = ObjectMeta::try_from(FFI_ObjectMeta::try_from(&meta).unwrap()).unwrap();
        assert_eq!(back, meta);
    }

    #[test]
    fn object_meta_before_1970_round_trip() {
        let meta = ObjectMeta {
            location: Path::from("old"),
            last_modified: "1969-12-31T23:59:59.5Z".parse::<DateTime<Utc>>().unwrap(),
            size: 1,
            e_tag: None,
            version: None,
        };
        let ffi = FFI_ObjectMeta::try_from(&meta).unwrap();
        assert_eq!(ffi.last_modified_ns, -500_000_000);
        let back = ObjectMeta::try_from(ffi).unwrap();
        assert_eq!(back, meta);
    }

    #[test]
    fn object_meta_out_of_range_time_fails() {
        let meta = ObjectMeta {
            location: Path::from("a"),
            last_modified: DateTime::<Utc>::MAX_UTC,
            size: 0,
            e_tag: None,
            version: None,
        };
        let error = FFI_ObjectMeta::try_from(&meta).unwrap_err();
        assert!(matches!(error, Error::Generic { .. }));
    }

    #[test]
    fn object_meta_invalid_path_fails() {
        let meta = FFI_ObjectMeta {
            location: SString::from("a//b"),
            last_modified_ns: 0,
            size: 0,
            e_tag: FFI_Option::None,
            version: FFI_Option::None,
        };
        let error = ObjectMeta::try_from(meta).unwrap_err();
        assert!(matches!(error, Error::Generic { .. }));
    }
}
