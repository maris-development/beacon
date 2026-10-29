//! Value types of the file format ABI and their `#[repr(C)]` forms.

use std::path::PathBuf;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_expr::PhysicalExpr;
use datafusion_ffi::arrow_wrappers::WrappedSchema;
use datafusion_ffi::physical_expr::FFI_PhysicalExpr;
use datafusion_ffi::util::FFI_Option;
use stabby::string::String as SString;
use stabby::vec::Vec as SVec;

/// What a format returns from `open`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OutputKind {
    /// Normal Arrow columns, one row per record.
    Flat,
    /// `beacon.nd`-encoded columns. Beacon broadcasts them to rows.
    Nd,
}

impl OutputKind {
    /// The FFI value: `0 = Flat`, `1 = Nd`.
    pub fn to_u32(self) -> u32 {
        match self {
            Self::Flat => 0,
            Self::Nd => 1,
        }
    }

    /// Reads an FFI value. Returns `None` for an unknown value.
    pub fn from_u32(value: u32) -> Option<Self> {
        match value {
            0 => Some(Self::Flat),
            1 => Some(Self::Nd),
            _ => None,
        }
    }
}

/// The type of one option field of `read_<name>`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ArgKind {
    /// A string literal.
    String,
    /// A boolean literal.
    Bool,
    /// A list of strings. Beacon joins it with `,`.
    StringList,
}

impl ArgKind {
    /// The FFI value: `0 = String`, `1 = Bool`, `2 = StringList`.
    pub fn to_u32(self) -> u32 {
        match self {
            Self::String => 0,
            Self::Bool => 1,
            Self::StringList => 2,
        }
    }

    /// Reads an FFI value. Returns `None` for an unknown value.
    pub fn from_u32(value: u32) -> Option<Self> {
        match value {
            0 => Some(Self::String),
            1 => Some(Self::Bool),
            2 => Some(Self::StringList),
            _ => None,
        }
    }
}

/// One field of the options struct of `read_<name>(paths, {key: value, ...})`.
///
/// Beacon puts the value into the format options under `option_key`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArgSpec {
    /// The option key that gets the value.
    pub option_key: String,
    /// The type of the argument.
    pub kind: ArgKind,
}

/// [`ArgSpec`] as FFI data.
#[repr(C)]
#[derive(Debug)]
pub struct FFI_ArgSpec {
    /// The option key.
    pub option_key: SString,
    /// The [`ArgKind`] value.
    pub kind: u32,
}

impl From<&ArgSpec> for FFI_ArgSpec {
    fn from(spec: &ArgSpec) -> Self {
        Self {
            option_key: SString::from(spec.option_key.as_str()),
            kind: spec.kind.to_u32(),
        }
    }
}

impl TryFrom<&FFI_ArgSpec> for ArgSpec {
    type Error = DataFusionError;

    fn try_from(spec: &FFI_ArgSpec) -> Result<Self> {
        let kind = ArgKind::from_u32(spec.kind)
            .ok_or_else(|| DataFusionError::Plan(format!("unknown argument kind {}", spec.kind)))?;
        Ok(Self {
            option_key: spec.option_key.as_str().to_string(),
            kind,
        })
    }
}

/// One column of the input sort order that a writer needs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SortKey {
    /// The column name.
    pub column: String,
    /// Sort from high to low.
    pub descending: bool,
    /// Put nulls before the other values.
    pub nulls_first: bool,
}

/// [`SortKey`] as FFI data.
#[repr(C)]
#[derive(Debug)]
pub struct FFI_SortKey {
    /// The column name.
    pub column: SString,
    /// Sort from high to low.
    pub descending: bool,
    /// Put nulls before the other values.
    pub nulls_first: bool,
}

impl From<&SortKey> for FFI_SortKey {
    fn from(key: &SortKey) -> Self {
        Self {
            column: SString::from(key.column.as_str()),
            descending: key.descending,
            nulls_first: key.nulls_first,
        }
    }
}

impl From<&FFI_SortKey> for SortKey {
    fn from(key: &FFI_SortKey) -> Self {
        Self {
            column: key.column.as_str().to_string(),
            descending: key.descending,
            nulls_first: key.nulls_first,
        }
    }
}

/// What Beacon asks from one `open` call.
#[derive(Clone, Debug)]
pub struct ScanRequest {
    /// The wanted columns, in order, with the merged types. No partition columns.
    pub schema: SchemaRef,
    /// Filters against `schema`. They are hints: Beacon applies them again.
    pub filters: Vec<Arc<dyn PhysicalExpr>>,
    /// A row limit hint. The format may return more rows.
    pub limit: Option<usize>,
    /// The session batch size.
    pub batch_size: usize,
}

/// [`ScanRequest`] as FFI data.
#[repr(C)]
#[derive(Debug)]
pub struct FFI_ScanRequest {
    /// The wanted columns.
    pub schema: WrappedSchema,
    /// The filters.
    pub filters: SVec<FFI_PhysicalExpr>,
    /// The row limit hint.
    pub limit: FFI_Option<u64>,
    /// The session batch size.
    pub batch_size: u64,
}

impl From<&ScanRequest> for FFI_ScanRequest {
    fn from(request: &ScanRequest) -> Self {
        let mut filters = SVec::new();
        for filter in &request.filters {
            filters.push(FFI_PhysicalExpr::from(Arc::clone(filter)));
        }
        Self {
            schema: WrappedSchema::from(Arc::clone(&request.schema)),
            filters,
            limit: request.limit.map(|limit| limit as u64).into(),
            batch_size: request.batch_size as u64,
        }
    }
}

impl From<FFI_ScanRequest> for ScanRequest {
    fn from(request: FFI_ScanRequest) -> Self {
        let filters = request
            .filters
            .iter()
            .map(<Arc<dyn PhysicalExpr>>::from)
            .collect();
        let limit: Option<u64> = request.limit.into();
        Self {
            schema: SchemaRef::from(request.schema),
            filters,
            limit: limit.map(|limit| limit as usize),
            batch_size: request.batch_size as usize,
        }
    }
}

/// What Beacon asks from one `write` call: one output file.
#[derive(Clone, Debug)]
pub struct WriteRequest {
    /// The output path in the object store.
    pub path: object_store::path::Path,
    /// The schema of the input stream.
    pub schema: SchemaRef,
    /// A local path for the output. When set, the writer writes only there.
    pub local_path: Option<PathBuf>,
}

/// [`WriteRequest`] as FFI data.
#[repr(C)]
#[derive(Debug)]
pub struct FFI_WriteRequest {
    /// The output path in the object store.
    pub path: SString,
    /// The schema of the input stream.
    pub schema: WrappedSchema,
    /// The local output path, or none.
    pub local_path: FFI_Option<SString>,
}

impl From<&WriteRequest> for FFI_WriteRequest {
    fn from(request: &WriteRequest) -> Self {
        Self {
            path: SString::from(request.path.as_ref()),
            schema: WrappedSchema::from(Arc::clone(&request.schema)),
            local_path: request
                .local_path
                .as_ref()
                .map(|path| SString::from(path.to_string_lossy().as_ref()))
                .into(),
        }
    }
}

impl From<FFI_WriteRequest> for WriteRequest {
    fn from(request: FFI_WriteRequest) -> Self {
        let local_path: Option<SString> = request.local_path.into();
        Self {
            path: object_store::path::Path::from(request.path.as_str()),
            schema: SchemaRef::from(request.schema),
            local_path: local_path.map(|path| PathBuf::from(path.as_str())),
        }
    }
}
