//! Round trips of `FFI_FileFormatFactory` through the forced foreign path.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::plan_err;
use datafusion::error::Result;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_ffi::get_library_marker_id;
use datafusion_ffi::util::FFI_Result;
use futures::stream;
use object_store::memory::InMemory;
use object_store::{ObjectMeta, ObjectStore, ObjectStoreExt};
use tokio::runtime::Handle;

use super::tests::two_columns;
use super::*;

extern "C" fn foreign_marker_id() -> usize {
    get_library_marker_id() + 1
}

extern "C" fn other_version() -> u64 {
    2
}

extern "C" fn unknown_kind(_: &FFI_FileFormatFactory) -> u32 {
    7
}

#[derive(Debug)]
struct EmptyReader;

#[async_trait]
impl FormatReader for EmptyReader {
    async fn infer_schema(&self, _: Arc<dyn ObjectStore>, _: &ObjectMeta) -> Result<SchemaRef> {
        Ok(two_columns())
    }

    async fn open(
        &self,
        _: Arc<dyn ObjectStore>,
        _: &ObjectMeta,
        _: ScanRequest,
    ) -> Result<SendableRecordBatchStream> {
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            two_columns(),
            stream::empty(),
        )))
    }
}

/// A factory with a value in each field. It records the options of `create`.
#[derive(Debug, Default)]
pub(crate) struct PointText {
    seen: Mutex<Vec<HashMap<String, String>>>,
}

impl FormatFactory for PointText {
    fn name(&self) -> &str {
        "pointtext"
    }

    fn extensions(&self) -> Vec<String> {
        vec!["ptxt".to_string()]
    }

    fn output_kind(&self) -> OutputKind {
        OutputKind::Nd
    }

    fn table_function_args(&self) -> Vec<ArgSpec> {
        vec![
            ArgSpec {
                option_key: "read_dimensions".to_string(),
                kind: ArgKind::StringList,
            },
            ArgSpec {
                option_key: "skip".to_string(),
                kind: ArgKind::Bool,
            },
        ]
    }

    fn can_write(&self) -> bool {
        true
    }

    fn split_output(&self) -> bool {
        false
    }

    fn content_type(&self) -> &str {
        "text/plain"
    }

    fn wants_local_file(&self) -> bool {
        true
    }

    fn create(&self, options: &HashMap<String, String>) -> Result<Arc<dyn FormatReader>> {
        self.seen.lock().unwrap().push(options.clone());
        if options.contains_key("bad") {
            return plan_err!("unknown option bad");
        }
        if options.contains_key("panic") {
            panic!("create boom");
        }
        Ok(Arc::new(EmptyReader))
    }
}

/// A factory with only the required methods.
#[derive(Debug)]
pub(crate) struct Defaults;

impl FormatFactory for Defaults {
    fn name(&self) -> &str {
        "defaults"
    }

    fn extensions(&self) -> Vec<String> {
        vec!["dft".to_string()]
    }

    fn output_kind(&self) -> OutputKind {
        OutputKind::Flat
    }

    fn create(&self, _: &HashMap<String, String>) -> Result<Arc<dyn FormatReader>> {
        Ok(Arc::new(EmptyReader))
    }
}

pub(crate) fn make_formats() -> Vec<Arc<dyn FormatFactory>> {
    vec![Arc::new(PointText::default()), Arc::new(Defaults)]
}

fn foreign(factory: Arc<dyn FormatFactory>) -> Arc<dyn FormatFactory> {
    let mut ffi = FFI_FileFormatFactory::new(factory);
    ffi.library_marker_id = foreign_marker_id;
    import_format_factory(ffi, Handle::current()).expect("import the factory")
}

#[test]
fn sdk_abi_is_five() {
    assert_eq!(crate::BEACON_PLUGIN_SDK_ABI, 5);
}

#[tokio::test(flavor = "multi_thread")]
async fn every_table_reports_the_sdk_abi() {
    let want = u64::from(crate::BEACON_PLUGIN_SDK_ABI);
    assert_eq!(crate::sdk_abi_version(), want);
    let factory = FFI_FileFormatFactory::new(Arc::new(Defaults));
    assert_eq!(unsafe { (factory.version)() }, want);
    let format = FFI_FileFormat::new(Arc::new(EmptyReader));
    assert_eq!(unsafe { (format.version)() }, want);
    let store = InMemory::new();
    let upload = store.put_multipart(&"x".into()).await.unwrap();
    let upload = crate::object_store::FFI_MultipartUpload::new(upload, Handle::current());
    assert_eq!(unsafe { (upload.version)() }, want);
    let store = crate::object_store::FFI_ObjectStore::new(Arc::new(store), Handle::current());
    assert_eq!(unsafe { (store.version)() }, want);
}

#[tokio::test(flavor = "multi_thread")]
async fn factory_round_trip() {
    let factory = foreign(Arc::new(PointText::default()));
    assert_eq!(factory.name(), "pointtext");
    assert_eq!(factory.extensions(), vec!["ptxt".to_string()]);
    assert_eq!(factory.output_kind(), OutputKind::Nd);
    assert_eq!(
        factory.table_function_args(),
        PointText::default().table_function_args()
    );
    assert!(factory.can_write());
    assert!(!factory.split_output());
    assert_eq!(factory.content_type(), "text/plain");
    assert!(!factory.writes_directory());
    assert!(factory.wants_local_file());
}

#[tokio::test(flavor = "multi_thread")]
async fn factory_defaults_round_trip() {
    let factory = foreign(Arc::new(Defaults));
    assert!(factory.table_function_args().is_empty());
    assert!(!factory.can_write());
    assert!(factory.split_output());
    assert_eq!(factory.content_type(), "application/octet-stream");
    assert!(!factory.writes_directory());
    assert!(!factory.wants_local_file());
}

#[tokio::test(flavor = "multi_thread")]
async fn factory_create_passes_options() {
    let local = Arc::new(PointText::default());
    let factory = foreign(local.clone());
    let options = HashMap::from([("delimiter".to_string(), ";".to_string())]);
    let reader = factory.create(&options).unwrap();
    assert_eq!(*local.seen.lock().unwrap(), vec![options]);
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let meta = ObjectMeta {
        location: "x".into(),
        last_modified: chrono::DateTime::from_timestamp(0, 0).unwrap(),
        size: 0,
        e_tag: None,
        version: None,
    };
    assert_eq!(
        reader.infer_schema(store, &meta).await.unwrap(),
        two_columns()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn factory_create_error_keeps_the_text() {
    let factory = foreign(Arc::new(PointText::default()));
    let options = HashMap::from([("bad".to_string(), "x".to_string())]);
    let error = factory.create(&options).err().expect("a refused option");
    assert!(error.to_string().contains("unknown option bad"), "{error}");
}

#[tokio::test(flavor = "multi_thread")]
async fn factory_create_panic_is_an_error() {
    let factory = foreign(Arc::new(PointText::default()));
    let options = HashMap::from([("panic".to_string(), "x".to_string())]);
    let error = factory.create(&options).err().expect("a panic");
    assert!(
        error
            .to_string()
            .contains("panic in file format call: create boom"),
        "{error}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn local_marker_returns_original_arc() {
    let factory: Arc<dyn FormatFactory> = Arc::new(Defaults);
    let back = import_format_factory(
        FFI_FileFormatFactory::new(factory.clone()),
        Handle::current(),
    )
    .unwrap();
    assert!(Arc::ptr_eq(&factory, &back));
}

#[tokio::test(flavor = "multi_thread")]
async fn wrong_version_is_refused() {
    let mut ffi = FFI_FileFormatFactory::new(Arc::new(Defaults));
    ffi.library_marker_id = foreign_marker_id;
    ffi.version = other_version;
    let error = import_format_factory(ffi, Handle::current())
        .err()
        .expect("a refused version");
    assert!(
        error
            .to_string()
            .contains("file format FFI version 2 is not supported (want 5)"),
        "{error}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn unknown_output_kind_is_refused() {
    let mut ffi = FFI_FileFormatFactory::new(Arc::new(Defaults));
    ffi.library_marker_id = foreign_marker_id;
    ffi.output_kind = unknown_kind;
    let error = import_format_factory(ffi, Handle::current())
        .err()
        .expect("a refused kind");
    assert!(
        error.to_string().contains("unknown output kind 7"),
        "{error}"
    );
}

#[test]
fn export_file_formats_returns_all() {
    let FFI_Result::Ok(formats) = export_file_formats(make_formats) else {
        panic!("expected formats");
    };
    assert_eq!(formats.len(), 2);
}

fn panicking_formats() -> Vec<Arc<dyn FormatFactory>> {
    panic!("formats boom")
}

#[test]
fn export_file_formats_catches_a_panic() {
    let FFI_Result::Err(text) = export_file_formats(panicking_formats) else {
        panic!("expected an error");
    };
    assert!(
        text.as_str()
            .contains("plugin file formats panicked: formats boom"),
        "{}",
        text.as_str()
    );
}
