//! `#[repr(C)]` mirrors of the `object_store` option and result types.

use std::borrow::Cow;
use std::ops::Range;

use ::object_store::path::Path;
use ::object_store::{
    Attribute, AttributeValue, Attributes, CopyMode, CopyOptions, Error, Extensions, GetOptions,
    GetRange, GetResult, ListResult, ObjectMeta, PutMode, PutMultipartOptions, PutOptions,
    PutResult, RenameOptions, RenameTargetMode, TagSet, UpdateVersion,
};
use chrono::{DateTime, Utc};
use datafusion_ffi::ffi_option::FFI_Option;
use stabby::string::String as SString;
use stabby::vec::Vec as SVec;

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
        let location = parse_path(meta.location.as_str())?;
        Ok(Self {
            location,
            last_modified: DateTime::from_timestamp_nanos(meta.last_modified_ns),
            size: meta.size,
            e_tag: from_sstring(meta.e_tag),
            version: from_sstring(meta.version),
        })
    }
}

/// Returns `time` in nanoseconds since the Unix epoch, or `Generic` when out of range.
fn to_nanos(time: &DateTime<Utc>, field: &str) -> Result<i64, Error> {
    time.timestamp_nanos_opt().ok_or_else(|| Error::Generic {
        store: STORE,
        source: format!("{field} is out of range: {time}").into(),
    })
}

fn to_nanos_opt(time: &Option<DateTime<Utc>>, field: &str) -> Result<FFI_Option<i64>, Error> {
    Ok(time
        .as_ref()
        .map(|t| to_nanos(t, field))
        .transpose()?
        .into())
}

fn from_nanos_opt(nanos: FFI_Option<i64>) -> Option<DateTime<Utc>> {
    nanos.into_option().map(DateTime::from_timestamp_nanos)
}

pub(crate) fn parse_path(location: &str) -> Result<Path, Error> {
    Path::parse(location).map_err(|e| Error::Generic {
        store: STORE,
        source: Box::new(e),
    })
}

/// A [`GetRange`] as FFI data.
#[repr(C, u8)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FFI_GetRange {
    /// [`GetRange::Bounded`] with `start..end`.
    Bounded {
        /// The first byte.
        start: u64,
        /// The byte after the last byte.
        end: u64,
    },
    /// [`GetRange::Offset`].
    Offset(u64),
    /// [`GetRange::Suffix`].
    Suffix(u64),
}

impl From<&GetRange> for FFI_GetRange {
    fn from(range: &GetRange) -> Self {
        match range {
            GetRange::Bounded(r) => Self::Bounded {
                start: r.start,
                end: r.end,
            },
            GetRange::Offset(o) => Self::Offset(*o),
            GetRange::Suffix(s) => Self::Suffix(*s),
        }
    }
}

impl From<FFI_GetRange> for GetRange {
    fn from(range: FFI_GetRange) -> Self {
        match range {
            FFI_GetRange::Bounded { start, end } => Self::Bounded(start..end),
            FFI_GetRange::Offset(o) => Self::Offset(o),
            FFI_GetRange::Suffix(s) => Self::Suffix(s),
        }
    }
}

/// A [`GetOptions`] as FFI data. The `extensions` do not cross.
#[repr(C)]
#[derive(Debug, Clone)]
pub struct FFI_GetOptions {
    /// See [`GetOptions::if_match`].
    pub if_match: FFI_Option<SString>,
    /// See [`GetOptions::if_none_match`].
    pub if_none_match: FFI_Option<SString>,
    /// See [`GetOptions::if_modified_since`], in nanoseconds since the Unix epoch.
    pub if_modified_since: FFI_Option<i64>,
    /// See [`GetOptions::if_unmodified_since`], in nanoseconds since the Unix epoch.
    pub if_unmodified_since: FFI_Option<i64>,
    /// See [`GetOptions::range`].
    pub range: FFI_Option<FFI_GetRange>,
    /// See [`GetOptions::version`].
    pub version: FFI_Option<SString>,
    /// See [`GetOptions::head`].
    pub head: bool,
}

impl TryFrom<&GetOptions> for FFI_GetOptions {
    type Error = Error;

    /// Fails with `Generic` when a time is outside the `i64` nanosecond range.
    fn try_from(options: &GetOptions) -> Result<Self, Error> {
        Ok(Self {
            if_match: to_sstring(&options.if_match),
            if_none_match: to_sstring(&options.if_none_match),
            if_modified_since: to_nanos_opt(&options.if_modified_since, "if_modified_since")?,
            if_unmodified_since: to_nanos_opt(&options.if_unmodified_since, "if_unmodified_since")?,
            range: options.range.as_ref().map(FFI_GetRange::from).into(),
            version: to_sstring(&options.version),
            head: options.head,
        })
    }
}

impl From<FFI_GetOptions> for GetOptions {
    fn from(options: FFI_GetOptions) -> Self {
        Self {
            if_match: from_sstring(options.if_match),
            if_none_match: from_sstring(options.if_none_match),
            if_modified_since: from_nanos_opt(options.if_modified_since),
            if_unmodified_since: from_nanos_opt(options.if_unmodified_since),
            range: options.range.into_option().map(GetRange::from),
            version: from_sstring(options.version),
            head: options.head,
            extensions: Extensions::new(),
        }
    }
}

/// One [`Attribute`] and its value as FFI data.
///
/// The key is `content-disposition`, `content-encoding`, `content-language`,
/// `content-type`, `cache-control`, `storage-class` or `metadata:<name>`.
#[repr(C)]
#[derive(Debug, Clone)]
pub struct FFI_Attribute {
    /// The attribute key.
    pub key: SString,
    /// The attribute value.
    pub value: SString,
}

/// An [`Attributes`] map as FFI data.
#[repr(transparent)]
#[derive(Debug, Clone)]
#[allow(non_camel_case_types)]
pub struct FFI_Attributes(pub SVec<FFI_Attribute>);

const METADATA_PREFIX: &str = "metadata:";

fn attribute_key(attribute: &Attribute) -> Option<Cow<'_, str>> {
    let key = match attribute {
        Attribute::ContentDisposition => "content-disposition",
        Attribute::ContentEncoding => "content-encoding",
        Attribute::ContentLanguage => "content-language",
        Attribute::ContentType => "content-type",
        Attribute::CacheControl => "cache-control",
        Attribute::StorageClass => "storage-class",
        Attribute::Metadata(name) => return Some(format!("{METADATA_PREFIX}{name}").into()),
        // An attribute from a later `object_store` version does not cross.
        _ => return None,
    };
    Some(key.into())
}

fn attribute_from_key(key: &str) -> Option<Attribute> {
    let attribute = match key {
        "content-disposition" => Attribute::ContentDisposition,
        "content-encoding" => Attribute::ContentEncoding,
        "content-language" => Attribute::ContentLanguage,
        "content-type" => Attribute::ContentType,
        "cache-control" => Attribute::CacheControl,
        "storage-class" => Attribute::StorageClass,
        _ => {
            let name = key.strip_prefix(METADATA_PREFIX)?;
            Attribute::Metadata(Cow::Owned(name.to_string()))
        }
    };
    Some(attribute)
}

impl From<&Attributes> for FFI_Attributes {
    fn from(attributes: &Attributes) -> Self {
        Self(
            attributes
                .iter()
                .filter_map(|(attribute, value)| {
                    Some(FFI_Attribute {
                        key: SString::from(attribute_key(attribute)?.as_ref()),
                        value: SString::from(value.as_ref()),
                    })
                })
                .collect(),
        )
    }
}

impl From<FFI_Attributes> for Attributes {
    /// Drops an attribute with an unknown key.
    fn from(attributes: FFI_Attributes) -> Self {
        attributes
            .0
            .into_iter()
            .filter_map(|a| {
                let attribute = attribute_from_key(a.key.as_str())?;
                Some((
                    attribute,
                    AttributeValue::from(a.value.as_str().to_string()),
                ))
            })
            .collect()
    }
}

/// A [`TagSet`] as FFI data, in its URL-encoded form.
#[repr(C)]
#[derive(Debug, Clone)]
pub struct FFI_TagSet {
    /// See [`TagSet::encoded`].
    pub encoded: SString,
}

impl From<&TagSet> for FFI_TagSet {
    fn from(tags: &TagSet) -> Self {
        Self {
            encoded: SString::from(tags.encoded()),
        }
    }
}

impl From<FFI_TagSet> for TagSet {
    fn from(tags: FFI_TagSet) -> Self {
        let mut set = TagSet::default();
        for (key, value) in url::form_urlencoded::parse(tags.encoded.as_str().as_bytes()) {
            set.push(&key, &value);
        }
        set
    }
}

/// A [`PutMode`] as FFI data.
#[repr(C, u8)]
#[derive(Debug, Clone)]
pub enum FFI_PutMode {
    /// [`PutMode::Overwrite`].
    Overwrite,
    /// [`PutMode::Create`].
    Create,
    /// [`PutMode::Update`] with the fields of [`UpdateVersion`].
    Update {
        /// See [`UpdateVersion::e_tag`].
        e_tag: FFI_Option<SString>,
        /// See [`UpdateVersion::version`].
        version: FFI_Option<SString>,
    },
}

impl From<&PutMode> for FFI_PutMode {
    fn from(mode: &PutMode) -> Self {
        match mode {
            PutMode::Overwrite => Self::Overwrite,
            PutMode::Create => Self::Create,
            PutMode::Update(v) => Self::Update {
                e_tag: to_sstring(&v.e_tag),
                version: to_sstring(&v.version),
            },
        }
    }
}

impl From<FFI_PutMode> for PutMode {
    fn from(mode: FFI_PutMode) -> Self {
        match mode {
            FFI_PutMode::Overwrite => Self::Overwrite,
            FFI_PutMode::Create => Self::Create,
            FFI_PutMode::Update { e_tag, version } => Self::Update(UpdateVersion {
                e_tag: from_sstring(e_tag),
                version: from_sstring(version),
            }),
        }
    }
}

/// A [`PutOptions`] as FFI data. The `extensions` do not cross.
#[repr(C)]
#[derive(Debug, Clone)]
pub struct FFI_PutOptions {
    /// See [`PutOptions::mode`].
    pub mode: FFI_PutMode,
    /// See [`PutOptions::tags`].
    pub tags: FFI_TagSet,
    /// See [`PutOptions::attributes`].
    pub attributes: FFI_Attributes,
}

impl From<&PutOptions> for FFI_PutOptions {
    fn from(options: &PutOptions) -> Self {
        Self {
            mode: (&options.mode).into(),
            tags: (&options.tags).into(),
            attributes: (&options.attributes).into(),
        }
    }
}

impl From<FFI_PutOptions> for PutOptions {
    fn from(options: FFI_PutOptions) -> Self {
        Self {
            mode: options.mode.into(),
            tags: options.tags.into(),
            attributes: options.attributes.into(),
            extensions: Extensions::new(),
        }
    }
}

/// A [`PutMultipartOptions`] as FFI data. The `extensions` do not cross.
#[repr(C)]
#[derive(Debug, Clone)]
pub struct FFI_PutMultipartOptions {
    /// See [`PutMultipartOptions::tags`].
    pub tags: FFI_TagSet,
    /// See [`PutMultipartOptions::attributes`].
    pub attributes: FFI_Attributes,
}

impl From<&PutMultipartOptions> for FFI_PutMultipartOptions {
    fn from(options: &PutMultipartOptions) -> Self {
        Self {
            tags: (&options.tags).into(),
            attributes: (&options.attributes).into(),
        }
    }
}

impl From<FFI_PutMultipartOptions> for PutMultipartOptions {
    fn from(options: FFI_PutMultipartOptions) -> Self {
        Self {
            tags: options.tags.into(),
            attributes: options.attributes.into(),
            extensions: Extensions::new(),
        }
    }
}

/// A [`PutResult`] as FFI data.
#[repr(C)]
#[derive(Debug, Clone)]
pub struct FFI_PutResult {
    /// See [`PutResult::e_tag`].
    pub e_tag: FFI_Option<SString>,
    /// See [`PutResult::version`].
    pub version: FFI_Option<SString>,
}

impl From<&PutResult> for FFI_PutResult {
    fn from(result: &PutResult) -> Self {
        Self {
            e_tag: to_sstring(&result.e_tag),
            version: to_sstring(&result.version),
        }
    }
}

impl From<FFI_PutResult> for PutResult {
    fn from(result: FFI_PutResult) -> Self {
        Self {
            e_tag: from_sstring(result.e_tag),
            version: from_sstring(result.version),
        }
    }
}

const MODE_OVERWRITE: u8 = 0;
const MODE_CREATE: u8 = 1;

fn unknown_mode(field: &str, value: u8) -> Error {
    Error::NotSupported {
        source: format!("unknown {field} value {value}").into(),
    }
}

/// A [`CopyOptions`] as FFI data. The `extensions` do not cross.
#[repr(C)]
#[derive(Debug, Clone)]
pub struct FFI_CopyOptions {
    /// `0` for [`CopyMode::Overwrite`], `1` for [`CopyMode::Create`].
    pub mode: u8,
}

impl From<&CopyOptions> for FFI_CopyOptions {
    fn from(options: &CopyOptions) -> Self {
        let mode = match options.mode {
            CopyMode::Overwrite => MODE_OVERWRITE,
            CopyMode::Create => MODE_CREATE,
        };
        Self { mode }
    }
}

impl TryFrom<FFI_CopyOptions> for CopyOptions {
    type Error = Error;

    /// Fails with `NotSupported` when `mode` is not `0` or `1`.
    fn try_from(options: FFI_CopyOptions) -> Result<Self, Error> {
        let mode = match options.mode {
            MODE_OVERWRITE => CopyMode::Overwrite,
            MODE_CREATE => CopyMode::Create,
            other => return Err(unknown_mode("copy mode", other)),
        };
        Ok(Self {
            mode,
            extensions: Extensions::new(),
        })
    }
}

/// A [`RenameOptions`] as FFI data. The `extensions` do not cross.
#[repr(C)]
#[derive(Debug, Clone)]
pub struct FFI_RenameOptions {
    /// `0` for [`RenameTargetMode::Overwrite`], `1` for [`RenameTargetMode::Create`].
    pub target_mode: u8,
}

impl From<&RenameOptions> for FFI_RenameOptions {
    fn from(options: &RenameOptions) -> Self {
        let target_mode = match options.target_mode {
            RenameTargetMode::Overwrite => MODE_OVERWRITE,
            RenameTargetMode::Create => MODE_CREATE,
        };
        Self { target_mode }
    }
}

impl TryFrom<FFI_RenameOptions> for RenameOptions {
    type Error = Error;

    /// Fails with `NotSupported` when `target_mode` is not `0` or `1`.
    fn try_from(options: FFI_RenameOptions) -> Result<Self, Error> {
        let target_mode = match options.target_mode {
            MODE_OVERWRITE => RenameTargetMode::Overwrite,
            MODE_CREATE => RenameTargetMode::Create,
            other => return Err(unknown_mode("rename target mode", other)),
        };
        Ok(Self {
            target_mode,
            extensions: Extensions::new(),
        })
    }
}

/// A [`ListResult`] as FFI data.
#[repr(C)]
#[derive(Debug, Clone)]
pub struct FFI_ListResult {
    /// See [`ListResult::common_prefixes`], in [`Path`] form.
    pub common_prefixes: SVec<SString>,
    /// See [`ListResult::objects`].
    pub objects: SVec<FFI_ObjectMeta>,
}

impl TryFrom<&ListResult> for FFI_ListResult {
    type Error = Error;

    /// Fails with `Generic` when a last modified time is outside the `i64` nanosecond range.
    fn try_from(list: &ListResult) -> Result<Self, Error> {
        Ok(Self {
            common_prefixes: list
                .common_prefixes
                .iter()
                .map(|p| SString::from(p.as_ref()))
                .collect(),
            objects: list
                .objects
                .iter()
                .map(FFI_ObjectMeta::try_from)
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .collect(),
        })
    }
}

impl TryFrom<FFI_ListResult> for ListResult {
    type Error = Error;

    /// Fails with `Generic` when a location is not a valid [`Path`].
    fn try_from(list: FFI_ListResult) -> Result<Self, Error> {
        Ok(Self {
            common_prefixes: list
                .common_prefixes
                .iter()
                .map(|p| parse_path(p.as_str()))
                .collect::<Result<_, _>>()?,
            objects: list
                .objects
                .into_iter()
                .map(ObjectMeta::try_from)
                .collect::<Result<_, _>>()?,
        })
    }
}

/// A byte range as FFI data.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FFI_Range {
    /// The first byte.
    pub start: u64,
    /// The byte after the last byte.
    pub end: u64,
}

impl From<Range<u64>> for FFI_Range {
    fn from(range: Range<u64>) -> Self {
        Self {
            start: range.start,
            end: range.end,
        }
    }
}

impl From<FFI_Range> for Range<u64> {
    fn from(range: FFI_Range) -> Self {
        range.start..range.end
    }
}

/// The fields of a [`GetResult`] other than the payload, as FFI data.
#[repr(C)]
#[derive(Debug, Clone)]
pub struct FFI_GetResultMeta {
    /// See [`GetResult::meta`].
    pub meta: FFI_ObjectMeta,
    /// The start of [`GetResult::range`].
    pub range_start: u64,
    /// The end of [`GetResult::range`].
    pub range_end: u64,
    /// See [`GetResult::attributes`].
    pub attributes: FFI_Attributes,
}

impl TryFrom<&GetResult> for FFI_GetResultMeta {
    type Error = Error;

    /// Fails with `Generic` when the last modified time is outside the `i64` nanosecond range.
    fn try_from(result: &GetResult) -> Result<Self, Error> {
        Ok(Self {
            meta: FFI_ObjectMeta::try_from(&result.meta)?,
            range_start: result.range.start,
            range_end: result.range.end,
            attributes: (&result.attributes).into(),
        })
    }
}

impl FFI_GetResultMeta {
    /// Returns the meta, the range and the attributes of a [`GetResult`].
    ///
    /// Fails with `Generic` when the location is not a valid [`Path`].
    pub fn into_parts(self) -> Result<(ObjectMeta, Range<u64>, Attributes), Error> {
        Ok((
            ObjectMeta::try_from(self.meta)?,
            self.range_start..self.range_end,
            self.attributes.into(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use ::object_store::{
        Attribute, Attributes, CopyMode, CopyOptions, Extensions, GetOptions, GetRange, GetResult,
        GetResultPayload, ListResult, PutMode, PutMultipartOptions, PutOptions, PutResult,
        RenameOptions, RenameTargetMode, TagSet, UpdateVersion,
    };
    use chrono::{DateTime, Utc};
    use futures::StreamExt;

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

    fn time(text: &str) -> DateTime<Utc> {
        text.parse::<DateTime<Utc>>().unwrap()
    }

    fn meta(location: &str, size: u64) -> ObjectMeta {
        ObjectMeta {
            location: Path::from(location),
            last_modified: time("2026-10-27T12:00:00Z"),
            size,
            e_tag: Some(format!("tag-{size}")),
            version: None,
        }
    }

    fn sample_attributes() -> Attributes {
        let mut attributes = Attributes::new();
        attributes.insert(Attribute::ContentType, "application/x-netcdf".into());
        attributes.insert(Attribute::ContentDisposition, "inline".into());
        attributes.insert(Attribute::ContentEncoding, "gzip".into());
        attributes.insert(Attribute::ContentLanguage, "en".into());
        attributes.insert(Attribute::CacheControl, "no-cache".into());
        attributes.insert(Attribute::StorageClass, "STANDARD".into());
        attributes.insert(Attribute::Metadata("x".into()), "1".into());
        attributes
    }

    fn sample_tags() -> TagSet {
        let mut tags = TagSet::default();
        tags.push("project", "beacon");
        tags.push("level", "2");
        tags
    }

    #[test]
    fn get_options_round_trip() {
        let ranges = [
            GetRange::Bounded(3..9),
            GetRange::Offset(5),
            GetRange::Suffix(7),
        ];
        for range in ranges {
            let options = GetOptions {
                if_match: Some("m".into()),
                if_none_match: Some("n".into()),
                if_modified_since: Some(time("2026-01-02T03:04:05.000000006Z")),
                if_unmodified_since: Some(time("1969-12-31T23:59:59.5Z")),
                range: Some(range.clone()),
                version: Some("v2".into()),
                head: true,
                extensions: Extensions::new(),
            };
            let back = GetOptions::from(FFI_GetOptions::try_from(&options).unwrap());
            assert_eq!(back.if_match, options.if_match);
            assert_eq!(back.if_none_match, options.if_none_match);
            assert_eq!(back.if_modified_since, options.if_modified_since);
            assert_eq!(back.if_unmodified_since, options.if_unmodified_since);
            assert_eq!(back.range, Some(range));
            assert_eq!(back.version, options.version);
            assert!(back.head);
            assert!(back.extensions.is_empty());
        }

        let back = GetOptions::from(FFI_GetOptions::try_from(&GetOptions::default()).unwrap());
        assert_eq!(back.if_match, None);
        assert_eq!(back.if_modified_since, None);
        assert_eq!(back.range, None);
        assert!(!back.head);
    }

    #[test]
    fn get_options_out_of_range_time_fails() {
        let options = GetOptions {
            if_modified_since: Some(DateTime::<Utc>::MAX_UTC),
            ..Default::default()
        };
        let error = FFI_GetOptions::try_from(&options).unwrap_err();
        assert!(matches!(error, Error::Generic { .. }));
    }

    #[test]
    fn put_options_round_trip() {
        let modes = [
            PutMode::Overwrite,
            PutMode::Create,
            PutMode::Update(UpdateVersion {
                e_tag: Some("e".into()),
                version: Some("v".into()),
            }),
            PutMode::Update(UpdateVersion {
                e_tag: None,
                version: None,
            }),
        ];
        for mode in modes {
            let options = PutOptions {
                mode,
                tags: sample_tags(),
                attributes: sample_attributes(),
                extensions: Extensions::new(),
            };
            let back = PutOptions::from(FFI_PutOptions::from(&options));
            assert_eq!(back, options);
            assert!(back.extensions.is_empty());
        }

        let options = PutMultipartOptions {
            tags: sample_tags(),
            attributes: sample_attributes(),
            extensions: Extensions::new(),
        };
        let back = PutMultipartOptions::from(FFI_PutMultipartOptions::from(&options));
        assert_eq!(back, options);

        let result = PutResult {
            e_tag: Some("e".into()),
            version: None,
        };
        assert_eq!(PutResult::from(FFI_PutResult::from(&result)), result);
    }

    #[test]
    fn copy_and_rename_modes_round_trip() {
        for mode in [CopyMode::Overwrite, CopyMode::Create] {
            let options = CopyOptions {
                mode,
                extensions: Extensions::new(),
            };
            let ffi = FFI_CopyOptions::from(&options);
            let back = CopyOptions::try_from(ffi).unwrap();
            assert_eq!(back, options);
        }
        assert_eq!(
            FFI_CopyOptions::from(&CopyOptions {
                mode: CopyMode::Create,
                extensions: Extensions::new()
            })
            .mode,
            1
        );

        for target_mode in [RenameTargetMode::Overwrite, RenameTargetMode::Create] {
            let options = RenameOptions {
                target_mode,
                extensions: Extensions::new(),
            };
            let back = RenameOptions::try_from(FFI_RenameOptions::from(&options)).unwrap();
            assert_eq!(back, options);
        }

        let error = CopyOptions::try_from(FFI_CopyOptions { mode: 9 }).unwrap_err();
        assert!(matches!(error, Error::NotSupported { .. }));
        let error = RenameOptions::try_from(FFI_RenameOptions { target_mode: 9 }).unwrap_err();
        assert!(matches!(error, Error::NotSupported { .. }));
    }

    #[test]
    fn list_result_round_trip() {
        let list = ListResult {
            common_prefixes: vec![Path::from("a"), Path::from("b/c")],
            objects: vec![meta("a/x.nc", 1), meta("b/c/y.nc", 2)],
        };
        let back = ListResult::try_from(FFI_ListResult::try_from(&list).unwrap()).unwrap();
        assert_eq!(back.common_prefixes, list.common_prefixes);
        assert_eq!(back.objects, list.objects);
    }

    #[test]
    fn get_result_meta_round_trip() {
        let result = GetResult {
            payload: GetResultPayload::Stream(futures::stream::empty().boxed()),
            meta: meta("a/x.nc", 10),
            range: 2..8,
            attributes: sample_attributes(),
        };
        let (meta, range, attributes) = FFI_GetResultMeta::try_from(&result)
            .unwrap()
            .into_parts()
            .unwrap();
        assert_eq!(meta, result.meta);
        assert_eq!(range, 2..8);
        assert_eq!(attributes, result.attributes);
        assert_eq!(Range::from(FFI_Range::from(3..4)), 3..4);
    }

    #[test]
    fn unknown_attribute_key_is_dropped() {
        let ffi = FFI_Attributes(
            [
                FFI_Attribute {
                    key: SString::from("content-type"),
                    value: SString::from("text/plain"),
                },
                FFI_Attribute {
                    key: SString::from("x-unknown"),
                    value: SString::from("y"),
                },
            ]
            .into_iter()
            .collect(),
        );
        let attributes = Attributes::from(ffi);
        assert_eq!(attributes.len(), 1);
        assert_eq!(
            attributes.get(&Attribute::ContentType).map(|v| v.as_ref()),
            Some("text/plain")
        );
    }

    #[test]
    fn tags_with_special_characters_round_trip() {
        let mut tags = TagSet::default();
        tags.push("a b", "c&d=e");
        tags.push("k", "");
        let back = TagSet::from(FFI_TagSet::from(&tags));
        assert_eq!(back, tags);
        assert_eq!(back.encoded(), tags.encoded());
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
