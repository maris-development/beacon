//! File format factories: the plugin side trait, its FFI table, and the import.

use std::collections::HashMap;
use std::ffi::c_void;
use std::fmt;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;

use datafusion::error::{DataFusionError, Result};
use datafusion_ffi::util::FFI_Result;
use stabby::string::String as SString;
use stabby::vec::Vec as SVec;
use tokio::runtime::Handle;

use super::FILE_FORMAT_FFI_VERSION;
use super::export::{FFI_FileFormat, drop_quietly, panic_text};
use super::file_format_ffi_version;
use super::foreign::{ForeignFormatReader, into_result};
use super::reader::FormatReader;
use super::types::{ArgSpec, FFI_ArgSpec, OutputKind};
use crate::panic_safe::panic_message;

/// A file format that a plugin exports. Beacon makes one configured format per table.
pub trait FormatFactory: Send + Sync + 'static {
    /// The format name, lower case: `STORED AS <name>`, `read_<name>`, `<name>_schema`.
    fn name(&self) -> &str;

    /// File extensions without the dot. The first one is the default extension.
    fn extensions(&self) -> Vec<String>;

    /// What `open` returns.
    fn output_kind(&self) -> OutputKind;

    /// The positional arguments of `read_<name>` after the paths.
    fn table_function_args(&self) -> Vec<ArgSpec> {
        Vec::new()
    }

    /// `true` if the format has a writer.
    fn can_write(&self) -> bool {
        false
    }

    /// `true` if Beacon may spread the rows of one output over several files.
    fn split_output(&self) -> bool {
        true
    }

    /// The MIME type of an output file.
    fn content_type(&self) -> &str {
        "application/octet-stream"
    }

    /// `true` if one output is a set of objects under a prefix.
    fn writes_directory(&self) -> bool {
        false
    }

    /// `true` if the writer works best on a local file.
    fn wants_local_file(&self) -> bool {
        false
    }

    /// Makes a configured format from the table options (bare keys).
    fn create(&self, options: &HashMap<String, String>) -> Result<Arc<dyn FormatReader>>;
}

/// One option of `create`.
#[repr(C)]
#[derive(Debug)]
pub struct FFI_OptionPair {
    /// The option key.
    pub key: SString,
    /// The option value.
    pub value: SString,
}

/// A [`FormatFactory`] as FFI data.
///
/// The exporter reads the values of the factory one time, in [`FFI_FileFormatFactory::new`].
/// Only the FFI functions read or change these fields.
#[repr(C)]
pub struct FFI_FileFormatFactory {
    /// Returns [`FILE_FORMAT_FFI_VERSION`] of the exporter. It stays the first field.
    pub version: unsafe extern "C" fn() -> u64,
    /// Identifies the library that made the table.
    pub library_marker_id: extern "C" fn() -> usize,
    /// [`FormatFactory::name`].
    pub name: extern "C" fn(factory: &Self) -> SString,
    /// [`FormatFactory::extensions`].
    pub extensions: extern "C" fn(factory: &Self) -> SVec<SString>,
    /// [`FormatFactory::output_kind`]: `0 = Flat`, `1 = Nd`.
    pub output_kind: extern "C" fn(factory: &Self) -> u32,
    /// [`FormatFactory::table_function_args`].
    pub table_function_args: extern "C" fn(factory: &Self) -> SVec<FFI_ArgSpec>,
    /// [`FormatFactory::can_write`].
    pub can_write: extern "C" fn(factory: &Self) -> bool,
    /// [`FormatFactory::split_output`].
    pub split_output: extern "C" fn(factory: &Self) -> bool,
    /// [`FormatFactory::content_type`].
    pub content_type: extern "C" fn(factory: &Self) -> SString,
    /// [`FormatFactory::writes_directory`].
    pub writes_directory: extern "C" fn(factory: &Self) -> bool,
    /// [`FormatFactory::wants_local_file`].
    pub wants_local_file: extern "C" fn(factory: &Self) -> bool,
    /// [`FormatFactory::create`].
    pub create: unsafe extern "C" fn(
        factory: &Self,
        options: SVec<FFI_OptionPair>,
    ) -> FFI_Result<FFI_FileFormat>,
    /// Makes a new table over the same factory.
    pub clone: unsafe extern "C" fn(factory: &Self) -> Self,
    /// Frees `private_data`. The owner calls it one time.
    pub release: unsafe extern "C" fn(factory: &mut Self),
    /// The exporter state. Only the exporter reads it.
    pub private_data: *mut c_void,
}

// The factory is `Send + Sync`, and the functions take `&Self`.
unsafe impl Send for FFI_FileFormatFactory {}
unsafe impl Sync for FFI_FileFormatFactory {}

/// The values of a factory, read one time.
#[derive(Clone, Debug)]
struct Info {
    name: String,
    extensions: Vec<String>,
    output_kind: OutputKind,
    table_function_args: Vec<ArgSpec>,
    can_write: bool,
    split_output: bool,
    content_type: String,
    writes_directory: bool,
    wants_local_file: bool,
}

impl Info {
    fn of(factory: &dyn FormatFactory) -> Self {
        Self {
            name: factory.name().to_string(),
            extensions: factory.extensions(),
            output_kind: factory.output_kind(),
            table_function_args: factory.table_function_args(),
            can_write: factory.can_write(),
            split_output: factory.split_output(),
            content_type: factory.content_type().to_string(),
            writes_directory: factory.writes_directory(),
            wants_local_file: factory.wants_local_file(),
        }
    }
}

struct FactoryPrivateData {
    factory: Arc<dyn FormatFactory>,
    info: Info,
}

impl FFI_FileFormatFactory {
    /// Exports `factory`. It calls the value methods of the factory now.
    pub fn new(factory: Arc<dyn FormatFactory>) -> Self {
        let info = Info::of(factory.as_ref());
        Self::with_info(factory, info)
    }

    fn with_info(factory: Arc<dyn FormatFactory>, info: Info) -> Self {
        let private_data = Box::new(FactoryPrivateData { factory, info });
        Self {
            version: file_format_ffi_version,
            library_marker_id: datafusion_ffi::get_library_marker_id,
            name: name_fn,
            extensions: extensions_fn,
            output_kind: output_kind_fn,
            table_function_args: table_function_args_fn,
            can_write: can_write_fn,
            split_output: split_output_fn,
            content_type: content_type_fn,
            writes_directory: writes_directory_fn,
            wants_local_file: wants_local_file_fn,
            create: create_fn,
            clone: clone_fn,
            release: release_fn,
            private_data: Box::into_raw(private_data) as *mut c_void,
        }
    }

    fn data(&self) -> &FactoryPrivateData {
        // SAFETY: `new` set `private_data`; the caller holds a live table.
        unsafe { &*(self.private_data as *const FactoryPrivateData) }
    }
}

impl Clone for FFI_FileFormatFactory {
    fn clone(&self) -> Self {
        // SAFETY: `clone` matches the table, and `self` is live.
        unsafe { (self.clone)(self) }
    }
}

impl Drop for FFI_FileFormatFactory {
    fn drop(&mut self) {
        // SAFETY: `release` matches the table, and it ignores a null `private_data`.
        unsafe { (self.release)(self) }
    }
}

fn s(text: &str) -> SString {
    SString::from(text)
}

extern "C" fn name_fn(factory: &FFI_FileFormatFactory) -> SString {
    s(&factory.data().info.name)
}

extern "C" fn extensions_fn(factory: &FFI_FileFormatFactory) -> SVec<SString> {
    let mut out = SVec::new();
    for extension in &factory.data().info.extensions {
        out.push(s(extension));
    }
    out
}

extern "C" fn output_kind_fn(factory: &FFI_FileFormatFactory) -> u32 {
    factory.data().info.output_kind.to_u32()
}

extern "C" fn table_function_args_fn(factory: &FFI_FileFormatFactory) -> SVec<FFI_ArgSpec> {
    let mut out = SVec::new();
    for spec in &factory.data().info.table_function_args {
        out.push(FFI_ArgSpec::from(spec));
    }
    out
}

extern "C" fn can_write_fn(factory: &FFI_FileFormatFactory) -> bool {
    factory.data().info.can_write
}

extern "C" fn split_output_fn(factory: &FFI_FileFormatFactory) -> bool {
    factory.data().info.split_output
}

extern "C" fn content_type_fn(factory: &FFI_FileFormatFactory) -> SString {
    s(&factory.data().info.content_type)
}

extern "C" fn writes_directory_fn(factory: &FFI_FileFormatFactory) -> bool {
    factory.data().info.writes_directory
}

extern "C" fn wants_local_file_fn(factory: &FFI_FileFormatFactory) -> bool {
    factory.data().info.wants_local_file
}

unsafe extern "C" fn create_fn(
    factory: &FFI_FileFormatFactory,
    options: SVec<FFI_OptionPair>,
) -> FFI_Result<FFI_FileFormat> {
    let created = catch_unwind(AssertUnwindSafe(|| {
        let options: HashMap<String, String> = options
            .iter()
            .map(|pair| {
                (
                    pair.key.as_str().to_string(),
                    pair.value.as_str().to_string(),
                )
            })
            .collect();
        factory
            .data()
            .factory
            .create(&options)
            .map(FFI_FileFormat::new)
    }));
    match created {
        Ok(Ok(format)) => FFI_Result::Ok(format),
        Ok(Err(error)) => FFI_Result::Err(s(&error.to_string())),
        Err(payload) => FFI_Result::Err(s(&panic_text(payload.as_ref()))),
    }
}

unsafe extern "C" fn clone_fn(factory: &FFI_FileFormatFactory) -> FFI_FileFormatFactory {
    let cloned = catch_unwind(AssertUnwindSafe(|| {
        let data = factory.data();
        FFI_FileFormatFactory::with_info(Arc::clone(&data.factory), data.info.clone())
    }));
    // A clone has no error value; stop here instead of an unwind across the boundary.
    cloned.unwrap_or_else(|_| std::process::abort())
}

unsafe extern "C" fn release_fn(factory: &mut FFI_FileFormatFactory) {
    if factory.private_data.is_null() {
        return;
    }
    // SAFETY: `new` made `private_data` with `Box::into_raw`, and the owner releases one time.
    let data = unsafe { Box::from_raw(factory.private_data as *mut FactoryPrivateData) };
    factory.private_data = std::ptr::null_mut();
    drop_quietly(move || drop(data));
}

/// A [`FormatFactory`] over an [`FFI_FileFormatFactory`] from another library.
pub struct ForeignFormatFactory {
    ffi: FFI_FileFormatFactory,
    info: Info,
    handle: Handle,
}

impl fmt::Debug for ForeignFormatFactory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ForeignFormatFactory({})", self.info.name)
    }
}

/// Imports a factory. Returns the original factory when this library made the table.
///
/// Fails on another [`FILE_FORMAT_FFI_VERSION`], an unknown output kind or an unknown
/// argument kind. The configured formats that `create` returns run store I/O on `handle`.
pub fn import_format_factory(
    ffi: FFI_FileFormatFactory,
    handle: Handle,
) -> Result<Arc<dyn FormatFactory>> {
    if (ffi.library_marker_id)() == datafusion_ffi::get_library_marker_id() {
        return Ok(Arc::clone(&ffi.data().factory));
    }
    // SAFETY: `version` has the same place in every table version.
    let version = unsafe { (ffi.version)() };
    if version != FILE_FORMAT_FFI_VERSION {
        return Err(DataFusionError::Plan(format!(
            "file format FFI version {version} is not supported (want {FILE_FORMAT_FFI_VERSION})"
        )));
    }
    let kind = (ffi.output_kind)(&ffi);
    let output_kind = OutputKind::from_u32(kind)
        .ok_or_else(|| DataFusionError::Plan(format!("unknown output kind {kind}")))?;
    let table_function_args = (ffi.table_function_args)(&ffi)
        .iter()
        .map(ArgSpec::try_from)
        .collect::<Result<Vec<_>>>()?;
    let info = Info {
        name: (ffi.name)(&ffi).as_str().to_string(),
        extensions: (ffi.extensions)(&ffi)
            .iter()
            .map(|e| e.as_str().to_string())
            .collect(),
        output_kind,
        table_function_args,
        can_write: (ffi.can_write)(&ffi),
        split_output: (ffi.split_output)(&ffi),
        content_type: (ffi.content_type)(&ffi).as_str().to_string(),
        writes_directory: (ffi.writes_directory)(&ffi),
        wants_local_file: (ffi.wants_local_file)(&ffi),
    };
    Ok(Arc::new(ForeignFormatFactory { ffi, info, handle }))
}

impl FormatFactory for ForeignFormatFactory {
    fn name(&self) -> &str {
        &self.info.name
    }

    fn extensions(&self) -> Vec<String> {
        self.info.extensions.clone()
    }

    fn output_kind(&self) -> OutputKind {
        self.info.output_kind
    }

    fn table_function_args(&self) -> Vec<ArgSpec> {
        self.info.table_function_args.clone()
    }

    fn can_write(&self) -> bool {
        self.info.can_write
    }

    fn split_output(&self) -> bool {
        self.info.split_output
    }

    fn content_type(&self) -> &str {
        &self.info.content_type
    }

    fn writes_directory(&self) -> bool {
        self.info.writes_directory
    }

    fn wants_local_file(&self) -> bool {
        self.info.wants_local_file
    }

    fn create(&self, options: &HashMap<String, String>) -> Result<Arc<dyn FormatReader>> {
        let mut pairs = SVec::new();
        for (key, value) in options {
            pairs.push(FFI_OptionPair {
                key: s(key),
                value: s(value),
            });
        }
        // SAFETY: the live table has this version and matches its pointers.
        let format = into_result(unsafe { (self.ffi.create)(&self.ffi, pairs) })?;
        ForeignFormatReader::try_new(format, self.handle.clone())
    }
}

/// The name of the file formats symbol, with a trailing NUL.
pub const FILE_FORMATS_SYMBOL: &[u8] = b"beacon_plugin_file_formats\0";

/// The type of the [`FILE_FORMATS_SYMBOL`] function.
pub type FileFormatsFn = extern "C" fn() -> FFI_Result<SVec<FFI_FileFormatFactory>>;

/// Exports the factories that `make` returns. A panic becomes an error.
pub fn export_file_formats(
    make: fn() -> Vec<Arc<dyn FormatFactory>>,
) -> FFI_Result<SVec<FFI_FileFormatFactory>> {
    let exported = catch_unwind(AssertUnwindSafe(|| {
        let mut out = SVec::new();
        for factory in make() {
            out.push(FFI_FileFormatFactory::new(factory));
        }
        out
    }));
    match exported {
        Ok(factories) => FFI_Result::Ok(factories),
        Err(payload) => FFI_Result::Err(s(&format!(
            "plugin file formats panicked: {}",
            panic_message(payload.as_ref())
        ))),
    }
}
